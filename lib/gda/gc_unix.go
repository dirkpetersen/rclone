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
	KeepHistory   time.Duration // history to keep; 0 keeps all of it
	KeepFrom      string        // run from which to keep history, instead of KeepHistory
	DeleteExpired bool          // remove data only history older than KeepHistory needs
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
	Deleted      int64       `json:"deleted"`                // orphans removed
	Unknown      []GCObject  `json:"unknown"`                // other objects no changeset refers to, never removed
	StaleIndexes []GCObject  `json:"stale_indexes"`          // parts of split indexes which a later index replaced
	HistoryFrom  string      `json:"history_from,omitempty"` // with KeepHistory, the run from which history is kept
	Expired      []GCObject  `json:"expired"`                // data only history before HistoryFrom needs
	ExpiredBytes int64       `json:"expired_bytes"`
	Compactable  []PackUsage `json:"compactable"` // packs worth rewriting
	Errors       []string    `json:"errors"`
}

// gcScan is the state of a GC while it reads the tree.
type gcScan struct {
	*GCReport
	mu          sync.Mutex
	referenced  map[string]bool // keys referenced from other directories
	candidates  []gcCandidate   // unreferenced in their own directory
	sharedPacks map[string]bool // packs with live members referenced from other directories
	needed      map[string]bool // with KeepHistory, objects rows in other directories need, by reference
	expired     []GCObject      // with KeepHistory, objects only expired rows of their directory need
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
		return nil, fmt.Errorf("%s isn't the root of a GDA tree: %w", destName(dst), err)
	}
	var b *backup
	if opt.DeleteOrphans || opt.DeleteExpired {
		b = &backup{d: d, runID: NewRunID(timeNow().UTC()), opt: Options{LockTimeout: opt.LockTimeout, MetaTier: "STANDARD"}}
		host, _ := os.Hostname()
		if err := b.lock(ctx, host); err != nil {
			return nil, err
		}
		defer b.unlock(ctx)
		defer b.keepLock(ctx)()
	}
	r := &gcScan{
		GCReport:    &GCReport{Orphans: []GCObject{}, Unknown: []GCObject{}, StaleIndexes: []GCObject{}, Expired: []GCObject{}, Compactable: []PackUsage{}, Errors: []string{}},
		referenced:  map[string]bool{},
		sharedPacks: map[string]bool{},
		needed:      map[string]bool{},
	}
	switch {
	case opt.KeepFrom != "":
		r.HistoryFrom = opt.KeepFrom
	case opt.KeepHistory > 0:
		r.HistoryFrom = NewRunID(timeNow().Add(-opt.KeepHistory))
	}
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
	for _, o := range r.expired {
		if !r.needed[o.Key] {
			r.Expired = append(r.Expired, o)
			r.ExpiredBytes += o.Size
		}
	}
	sort.Slice(r.Expired, func(i, j int) bool { return r.Expired[i].Key < r.Expired[j].Key })
	sort.Slice(r.Orphans, func(i, j int) bool { return r.Orphans[i].Key < r.Orphans[j].Key })
	sort.Slice(r.Unknown, func(i, j int) bool { return r.Unknown[i].Key < r.Unknown[j].Key })
	sort.Slice(r.StaleIndexes, func(i, j int) bool { return r.StaleIndexes[i].Key < r.StaleIndexes[j].Key })
	sort.Slice(r.Compactable, func(i, j int) bool { return r.Compactable[i].Key < r.Compactable[j].Key })
	if !opt.DeleteOrphans && !opt.DeleteExpired {
		return r.GCReport, nil
	}
	if len(r.Errors) > 0 {
		// A directory that couldn't be read may hold references.
		return r.GCReport, errors.New("not removing anything as the tree couldn't be read in full")
	}
	if opt.DeleteOrphans {
		for _, o := range append(r.Orphans, r.StaleIndexes...) {
			if b.isStopped() {
				// Another run may be writing objects which look like orphans.
				return r.GCReport, errors.New("stopped removing orphans as the destination lock was lost")
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
	}
	if opt.DeleteExpired && r.HistoryFrom != "" {
		r.deleteExpired(ctx, d, b)
	}
	if len(r.Errors) > 0 {
		return r.GCReport, fmt.Errorf("gc finished with %d errors", len(r.Errors))
	}
	return r.GCReport, nil
}

// deleteExpired removes the expired data, records from which run history
// is kept, and takes the removed objects out of the dedup index, so no
// later run refers to them.
func (r *gcScan) deleteExpired(ctx context.Context, d *dest, b *backup) {
	if d.dryRun {
		for _, o := range r.Expired {
			fs.Logf(o.Key, "Not deleting as --dry-run is set")
		}
		return
	}
	// Readers learn that older history is gone before any of it is.
	if err := writeHistoryFrom(ctx, d, r.HistoryFrom); err != nil {
		r.errorf("record kept history: %v", err)
		return
	}
	removed := map[string]bool{}
	for _, o := range r.Expired {
		if b.isStopped() {
			r.errorf("stopped removing expired data as the destination lock was lost")
			break
		}
		obj, err := newDataObject(ctx, d.f, o.Key)
		if err == nil {
			err = obj.Remove(ctx)
		}
		if err != nil && !errors.Is(err, fs.ErrorObjectNotFound) {
			r.errorf("remove %q: %v", o.Key, err)
			continue
		}
		removed[o.Key] = true
		r.Deleted++
	}
	if len(removed) > 0 {
		if err := dropFromDedup(ctx, d, removed, b.runID); err != nil {
			r.errorf("update dedup index: %v", err)
		}
	}
}

// orphanRun returns when the run which wrote the data object at key
// started, from the run ID in its name.
func orphanRun(key string) (time.Time, bool) {
	m := runInNameRe.FindStringSubmatch(path.Base(key))
	if m == nil {
		return time.Time{}, false
	}
	t, err := ParseRunID(m[1] + m[4])
	return t, err == nil
}

// runInNameRe finds the run ID in the name of a pack, of a standalone
// file stored under a versioned name, or of a part of a split index.
var runInNameRe = regexp.MustCompile(`\.gda\.(\d{8}T\d{6}Z)\.[^./]+(\.\d+\.tar)?(\.zst)?$|^gda-index\.(\d{8}T\d{6}Z)\.\d+\.csv$`)

// indexPartRe matches the name of a part of a split index.
var indexPartRe = regexp.MustCompile(`^gda-index\.\d{8}T\d{6}Z\.\d+\.csv$`)

// errorf records an error.
func (r *gcScan) errorf(format string, args ...any) {
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
func (r *gcScan) loadDedupRefs(ctx context.Context, d *dest) error {
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
func (r *gcScan) scanDir(ctx context.Context, d *dest, dir string, entries fs.DirEntries) error {
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
	// Changesets in the order history replays them.
	sort.Slice(changesets, func(i, j int) bool {
		ri := changesetRe.FindStringSubmatch(path.Base(changesets[i]))[1]
		rj := changesetRe.FindStringSubmatch(path.Base(changesets[j]))[1]
		if ri != rj {
			return ri < rj
		}
		return changesets[i] < changesets[j]
	})
	hist := newHistory(dir)
	for _, key := range changesets {
		data, err := d.get(ctx, key)
		if err != nil {
			return err
		}
		rows, err := ReadEntries(bytes.NewReader(data))
		if err != nil {
			return fmt.Errorf("parse %q: %w", key, err)
		}
		run := changesetRe.FindStringSubmatch(path.Base(key))[1]
		for i := range rows {
			row := &rows[i]
			hist.add(run, row)
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
	// The parts of split indexes which the current one doesn't use.
	var stale []GCObject
	current := map[string]bool{}
	data, err := d.get(ctx, joinRemote(dir, IndexName))
	switch {
	case errors.Is(err, fs.ErrorObjectNotFound):
	case err != nil:
		// Without the table of contents every part would look stale.
		return err
	case isTOC(data):
		parts, err := readTOC(bytes.NewReader(data))
		if err != nil {
			return err
		}
		for _, part := range parts {
			current[part.name] = true
		}
	}
	for _, o := range objects {
		if name := path.Base(o.Remote()); indexPartRe.MatchString(name) && !current[name] {
			stale = append(stale, GCObject{Key: o.Remote(), Size: o.Size(), ModTime: o.ModTime(ctx)})
		}
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

	neededHere, neededElsewhere, expiredRefs := hist.expiry(r.HistoryFrom)

	r.mu.Lock()
	defer r.mu.Unlock()
	r.Directories++
	for ref := range neededElsewhere {
		r.needed[ref] = true
	}
	if r.HistoryFrom != "" {
		for _, o := range objects {
			name := path.Base(o.Remote())
			if _, ok := expiredRefs[name]; ok && !neededHere[name] {
				r.expired = append(r.expired, GCObject{Key: o.Remote(), Size: o.Size(), ModTime: o.ModTime(ctx)})
			}
		}
		// Versions of objects aren't listed, so come from the rows.
		for ref, size := range expiredRefs {
			if _, version := splitVersionKey(ref); version != "" && !neededHere[ref] {
				key, _ := splitVersionKey(ref)
				r.expired = append(r.expired, GCObject{Key: versionKey(joinRemote(dir, key), version), Size: size})
			}
		}
	}
	r.StaleIndexes = append(r.StaleIndexes, stale...)
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
