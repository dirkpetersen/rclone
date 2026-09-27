//go:build unix

package gda

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/walk"
)

// Names of data objects: packs, and standalone files stored under a
// versioned or compressed name.
var (
	packRe       = regexp.MustCompile(`\.gda\.(\d{8}T\d{6}Z)\.[^./]+\.\d+\.tar(\.zst)?$`)
	standaloneRe = regexp.MustCompile(`\.gda\.(\d{8}T\d{6}Z\.[^./]+(\.zst)?|zst)$`)
)

// Compaction thresholds, from the design: a pack is worth rewriting once
// it is past the Deep Archive minimum storage duration, less than half
// of its data is live, and the dead data is big enough to matter. Tests
// lower them.
var (
	compactMinAge  = 180 * 24 * time.Hour
	compactMinDead = int64(1 << 30)
)

// GCOptions configure GC.
type GCOptions struct {
	DeleteOrphans bool          // remove orphan data objects
	MinAge        time.Duration // orphans younger than this are kept
	LockTimeout   time.Duration // as for backups, when taking the lock to delete
}

// GCReport describes the data objects of a GDA tree.
type GCReport struct {
	Directories  int64       `json:"directories"`    // directories listed
	DataObjects  int64       `json:"data_objects"`   // packs and standalone objects
	DataBytes    int64       `json:"data_bytes"`     // their stored size
	Packs        int64       `json:"packs"`          // packs
	PackBytes    int64       `json:"pack_bytes"`     // their stored size
	LivePackData int64       `json:"live_pack_data"` // bytes of pack members in current indexes, before compression
	DeadPackData int64       `json:"dead_pack_data"` // bytes of pack members only in history, before compression
	Orphans      []GCObject  `json:"orphans"`        // data objects nothing refers to
	OrphanBytes  int64       `json:"orphan_bytes"`
	Deleted      int64       `json:"deleted"`     // orphans removed
	Unknown      []GCObject  `json:"unknown"`     // other objects no changeset refers to, never removed
	Compactable  []PackUsage `json:"compactable"` // packs worth rewriting
	Errors       []string    `json:"errors"`
	mu           sync.Mutex
	referenced   map[string]bool // keys referenced from other directories
	candidates   []gcCandidate   // unreferenced in their own directory
	sharedPacks  map[string]bool // packs with live members referenced from other directories
}

// gcCandidate is an object its own directory's changesets don't refer to.
type gcCandidate struct {
	GCObject
	data bool // whether it has the name of a data object
}

// GCObject is an object found by GC.
type GCObject struct {
	Key     string    `json:"key"`
	Size    int64     `json:"size"`
	ModTime time.Time `json:"mod_time"`
}

// PackUsage is how much of a pack is live.
type PackUsage struct {
	Key     string `json:"key"`
	Size    int64  `json:"size"`    // stored size
	Members int64  `json:"members"` // bytes of members, before compression
	Live    int64  `json:"live"`    // bytes of members in current indexes, before compression
	Shared  bool   `json:"shared"`  // other directories refer to live members
	Run     string `json:"run"`
}

// GC reports on the data objects of the GDA tree at dst: which are
// orphans, left by runs that crashed before committing them, and which
// packs are mostly dead data. With DeleteOrphans it removes the orphans
// older than MinAge, holding the destination lock so no run is adding
// data meanwhile. Objects which don't have GDA names are only reported.
func GC(ctx context.Context, dst fs.Fs, opt GCOptions) (*GCReport, error) {
	d := &dest{f: dst, metaTier: "STANDARD", retries: 3, dryRun: fs.GetConfig(ctx).DryRun}
	// Below the root, the lock and the dedup index aren't there to see.
	if _, err := dst.List(ctx, joinRemote(MetaDir, "runs")); err != nil {
		return nil, fmt.Errorf("%s isn't the root of a GDA tree: %w", fs.ConfigString(dst), err)
	}
	var b *backup
	if opt.DeleteOrphans {
		b = &backup{d: d, runID: NewRunID(timeNow().UTC()), opt: Options{LockTimeout: opt.LockTimeout, MetaTier: "STANDARD"}}
		host, _ := os.Hostname()
		if err := b.lock(ctx, host); err != nil {
			return nil, err
		}
		defer b.unlock(ctx)
		defer b.keepLock(ctx)()
	}
	r := &GCReport{Orphans: []GCObject{}, Unknown: []GCObject{}, Compactable: []PackUsage{}, Errors: []string{},
		referenced: map[string]bool{}, sharedPacks: map[string]bool{}}
	if err := r.loadDedupRefs(ctx, d); err != nil {
		return nil, err
	}
	type job struct {
		dir     string
		entries fs.DirEntries
	}
	jobs := make(chan job)
	var wg sync.WaitGroup
	for range max(fs.GetConfig(ctx).Checkers, 1) {
		wg.Go(func() {
			for j := range jobs {
				if err := r.scanDir(ctx, d, j.dir, j.entries); err != nil {
					r.errorf("%q: %v", j.dir, err)
				}
			}
		})
	}
	err := walk.Walk(ctx, dst, "", true, -1, func(dir string, entries fs.DirEntries, err error) error {
		if err != nil {
			return err
		}
		if dir == MetaDir {
			return walk.ErrorSkipDir
		}
		jobs <- job{dir: dir, entries: entries}
		return nil
	})
	close(jobs)
	wg.Wait()
	if err != nil {
		r.errorf("list: %v", err)
	}
	for _, c := range r.candidates {
		switch {
		case r.referenced[c.Key]:
		case c.data:
			r.Orphans = append(r.Orphans, c.GCObject)
			r.OrphanBytes += c.Size
		default:
			r.Unknown = append(r.Unknown, c.GCObject)
		}
	}
	for i := range r.Compactable {
		r.Compactable[i].Shared = r.sharedPacks[r.Compactable[i].Key]
	}
	sort.Slice(r.Orphans, func(i, j int) bool { return r.Orphans[i].Key < r.Orphans[j].Key })
	sort.Slice(r.Unknown, func(i, j int) bool { return r.Unknown[i].Key < r.Unknown[j].Key })
	sort.Slice(r.Compactable, func(i, j int) bool { return r.Compactable[i].Key < r.Compactable[j].Key })
	if !opt.DeleteOrphans {
		return r, nil
	}
	if len(r.Errors) > 0 {
		// A directory that couldn't be read may hold references.
		return r, errors.New("not removing orphans as the tree couldn't be read in full")
	}
	for _, o := range r.Orphans {
		if b.isStopped() {
			// Another run may be writing objects which look like orphans.
			return r, errors.New("stopped removing orphans as the destination lock was lost")
		}
		// Objects keep their source file's modification time, so the
		// age comes from the run which wrote them.
		written, ok := orphanRun(o.Key)
		if !ok {
			fs.Logf(o.Key, "gda: not removing orphan, as its name doesn't show which run wrote it")
			continue
		}
		if time.Since(written) < opt.MinAge {
			continue
		}
		if d.dryRun {
			fs.Logf(o.Key, "Not deleting as --dry-run is set")
			continue
		}
		obj, err := dst.NewObject(ctx, o.Key)
		if err == nil {
			err = obj.Remove(ctx)
		}
		if err != nil {
			r.errorf("remove %q: %v", o.Key, err)
			continue
		}
		r.Deleted++
	}
	if len(r.Errors) > 0 {
		return r, fmt.Errorf("gc finished with %d errors", len(r.Errors))
	}
	return r, nil
}

// orphanRun returns when the run which wrote the data object at key
// started, from the run ID in its name.
func orphanRun(key string) (time.Time, bool) {
	m := runInNameRe.FindStringSubmatch(path.Base(key))
	if m == nil {
		return time.Time{}, false
	}
	t, err := ParseRunID(m[1])
	return t, err == nil
}

// runInNameRe finds the run ID in the name of a pack or of a standalone
// file stored under a versioned name.
var runInNameRe = regexp.MustCompile(`\.gda\.(\d{8}T\d{6}Z)\.[^./]+(\.\d+\.tar)?(\.zst)?$`)

// errorf records an error.
func (r *GCReport) errorf(format string, args ...any) {
	err := fmt.Errorf(format, args...)
	fs.Errorf(nil, "gda: gc: %v", err)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Errors = append(r.Errors, err.Error())
}

// loadDedupRefs marks the objects the dedup index lists as referenced:
// a run records a stored copy there before committing its directory, so
// later directories may refer to an object whose own directory never
// committed it.
func (r *GCReport) loadDedupRefs(ctx context.Context, d *dest) error {
	entries, err := d.f.List(ctx, dedupDir)
	if errors.Is(err, fs.ErrorDirNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("list dedup index: %w", err)
	}
	for _, e := range entries {
		if path.Ext(e.Remote()) != ".csv" {
			continue
		}
		data, err := d.get(ctx, e.Remote())
		if err != nil {
			return err
		}
		rows, err := ReadEntries(bytes.NewReader(data))
		if err != nil {
			return fmt.Errorf("parse %q: %w", e.Remote(), err)
		}
		for _, row := range rows {
			r.referenced[row.Location] = true
		}
	}
	return nil
}

// scanDir reads the changesets and index of the directory dir, whose
// listing is entries, and accounts for its data objects.
func (r *GCReport) scanDir(ctx context.Context, d *dest, dir string, entries fs.DirEntries) error {
	var objects []fs.Object
	var changesets []string
	for _, e := range entries {
		o, ok := e.(fs.Object)
		if !ok {
			continue
		}
		name := path.Base(o.Remote())
		if changesetRe.MatchString(name) {
			changesets = append(changesets, o.Remote())
			continue
		}
		objects = append(objects, o)
	}
	// Local references, and for packs the size of each member by offset.
	local := map[string]bool{}
	members := map[string]map[int64]int64{}
	var remote []string
	for _, key := range changesets {
		data, err := d.get(ctx, key)
		if err != nil {
			return err
		}
		rows, err := ReadEntries(bytes.NewReader(data))
		if err != nil {
			return fmt.Errorf("parse %q: %w", key, err)
		}
		for i := range rows {
			row := &rows[i]
			if row.DedupOf != "" {
				l := Located{Entry: *row, IndexKey: dir}
				remote = append(remote, l.ObjectKey())
				continue
			}
			if row.Location == "" {
				continue
			}
			local[row.Location] = true
			if row.Offset >= 0 && row.Type == TypeFile && row.Size > 0 {
				if members[row.Location] == nil {
					members[row.Location] = map[int64]int64{}
				}
				members[row.Location][row.Offset] = row.Size
			}
		}
	}
	index, err := d.readIndex(ctx, dir)
	if err != nil {
		return err
	}
	live := map[string]map[int64]bool{}
	var shared []string
	for i := range index {
		row := &index[i]
		switch {
		case row.DedupOf != "":
			l := Located{Entry: *row, IndexKey: dir}
			shared = append(shared, l.ObjectKey())
		case row.Location != "" && row.Offset >= 0:
			if live[row.Location] == nil {
				live[row.Location] = map[int64]bool{}
			}
			live[row.Location][row.Offset] = true
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.Directories++
	for _, key := range remote {
		r.referenced[key] = true
	}
	for _, key := range shared {
		r.sharedPacks[key] = true
	}
	for _, o := range objects {
		name := path.Base(o.Remote())
		obj := GCObject{Key: o.Remote(), Size: o.Size(), ModTime: o.ModTime(ctx)}
		isPack := packRe.MatchString(name)
		isData := isPack || standaloneRe.MatchString(name)
		if !isData && (isReserved(name, dir == "") || path.Ext(name) == ".csv" && strings.HasPrefix(name, "gda-")) {
			// Indexes and checkpoints.
			continue
		}
		if !local[name] {
			r.candidates = append(r.candidates, gcCandidate{GCObject: obj, data: isData})
			if !isData {
				// Possibly not GDA's, so not counted as data.
				continue
			}
		}
		r.DataObjects++
		r.DataBytes += obj.Size
		if !isPack {
			continue
		}
		r.Packs++
		r.PackBytes += obj.Size
		u := PackUsage{Key: obj.Key, Size: obj.Size, Run: packRe.FindStringSubmatch(name)[1]}
		for offset, size := range members[name] {
			u.Members += size
			if live[name][offset] {
				u.Live += size
			}
		}
		r.LivePackData += u.Live
		r.DeadPackData += u.Members - u.Live
		started, err := ParseRunID(u.Run)
		if err == nil && time.Since(started) >= compactMinAge && u.Live*2 < u.Members && u.Members-u.Live >= compactMinDead {
			r.Compactable = append(r.Compactable, u)
		}
	}
	return nil
}
