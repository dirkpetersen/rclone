//go:build unix

package gda

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/rclone/rclone/fs"
)

// Options configure a backup run.
type Options struct {
	PackSize      int64         // maximum size of a pack in bytes
	StandaloneMin int64         // files of at least this size are stored as their own objects
	RollupMax     int64         // subtrees smaller than this are packed as one unit; 0 disables
	DataTier      string        // storage class for packs and standalone files
	MetaTier      string        // storage class for changesets, indexes and run files
	Worker        string        // worker ID used in pack names
	TempDir       string        // directory for pack spool files; "" for the system default
	RootLabel     string        // name used for the top directory in pack names; "" to derive it
	LockTimeout   time.Duration // age after which another run's lock is taken over
	Retries       int           // upload attempts per object
}

// DefaultOptions returns the default options.
func DefaultOptions() Options {
	return Options{
		PackSize:      256 * 1024 * 1024,
		StandaloneMin: 64 * 1024 * 1024,
		RollupMax:     16 * 1024 * 1024,
		DataTier:      "DEEP_ARCHIVE",
		MetaTier:      "STANDARD",
		Worker:        "w01",
		LockTimeout:   24 * time.Hour,
		Retries:       3,
	}
}

// Stats counts what a run did.
type Stats struct {
	IndexedDirs     int64 // directories with their own index
	RollupDirs      int64 // directories packed inside a rollup
	Unchanged       int64 // entries unchanged since the last run
	Added           int64 // entries added
	Modified        int64 // entries whose data changed
	MetaOnly        int64 // entries whose metadata changed but not their data
	Deleted         int64 // entries no longer in the source
	Packs           int64 // packs uploaded
	PackBytes       int64 // bytes in packs uploaded
	Standalone      int64 // standalone objects uploaded
	StandaloneBytes int64 // bytes in standalone objects uploaded
	MetaObjects     int64 // changesets and index objects written
	Skipped         int64 // entries skipped, for example because of reserved names
	Deferred        int64 // entries that changed while being read, left for the next run
	Errors          int64 // errors
}

// Ledger records a run. It is written to _gda/runs/<run>/<worker>.json.
type Ledger struct {
	FormatVersion int
	RunID         string
	Worker        string
	Host          string
	Source        string
	Destination   string
	DryRun        bool
	Started       time.Time
	Finished      time.Time
	Options       Options
	Stats         Stats
	Errors        []string
}

// timeNow returns the current time. Tests replace it so that runs get
// distinct run IDs.
var timeNow = time.Now

// maxLedgerErrors is the number of error messages kept in the ledger.
const maxLedgerErrors = 100

// dirSummary describes a source directory's subtree.
type dirSummary struct {
	treeSize   int64 // bytes in regular files
	treeFiles  int64 // non-directory entries
	standalone bool  // holds a file of at least StandaloneMin
	unreadable bool  // couldn't be read in full
	badName    bool  // holds a name which is not valid UTF-8
}

// backup is the state of one run.
type backup struct {
	opt       Options
	srcRoot   string
	d         *dest
	rootKey   string // destination root, for key length checks
	runID     string
	names     *idNames
	summaries map[string]*dirSummary // by source path relative to srcRoot

	mu     sync.Mutex
	stats  Stats
	errors []string
}

// Backup backs up the directory tree at srcRoot to dst.
func Backup(ctx context.Context, srcRoot string, dst fs.Fs, opt Options) (*Ledger, error) {
	if opt.PackSize < opt.StandaloneMin {
		return nil, fmt.Errorf("pack size %d must be at least the standalone minimum %d", opt.PackSize, opt.StandaloneMin)
	}
	if opt.Worker == "" || strings.ContainsAny(opt.Worker, "./") {
		return nil, fmt.Errorf("invalid worker ID %q", opt.Worker)
	}
	if opt.Retries < 1 {
		opt.Retries = 1
	}
	info, err := os.Stat(srcRoot)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("source %q is not a directory", srcRoot)
	}
	ci := fs.GetConfig(ctx)
	started := timeNow().UTC()
	b := &backup{
		opt:     opt,
		srcRoot: srcRoot,
		d: &dest{
			f:        dst,
			dataTier: opt.DataTier,
			metaTier: opt.MetaTier,
			dryRun:   ci.DryRun,
			retries:  opt.Retries,
		},
		rootKey:   strings.Trim(dst.Root(), "/"),
		runID:     NewRunID(started),
		names:     newIDNames(),
		summaries: map[string]*dirSummary{},
	}
	if b.opt.RootLabel == "" {
		b.opt.RootLabel = "root"
		if base := path.Base(b.rootKey); b.rootKey != "" && base != "." && base != "/" {
			b.opt.RootLabel = base
		}
	}
	host, _ := os.Hostname()
	ledger := &Ledger{
		FormatVersion: FormatVersion,
		RunID:         b.runID,
		Worker:        opt.Worker,
		Host:          host,
		Source:        srcRoot,
		Destination:   fs.ConfigString(dst),
		DryRun:        ci.DryRun,
		Started:       started,
		Options:       b.opt,
	}

	// Run IDs have a resolution of one second, so make sure a run
	// that finished within the same second can't share this one's names.
	for {
		used, err := b.d.exists(ctx, joinRemote(MetaDir, "runs", b.runID, opt.Worker+".json"))
		if err != nil {
			return nil, fmt.Errorf("check run ID: %w", err)
		}
		if !used {
			break
		}
		time.Sleep(time.Second)
		b.runID = NewRunID(timeNow())
		ledger.RunID = b.runID
	}

	if err := b.lock(ctx, host); err != nil {
		return nil, err
	}
	defer b.unlock(ctx)
	fs.Infof(nil, "gda: run %s: scanning %s", b.runID, srcRoot)
	b.summarize("")
	fs.Infof(nil, "gda: run %s: backing up to %s", b.runID, fs.ConfigString(dst))
	b.processDir(ctx, "", "", b.isRollupRoot(""))

	ledger.Finished = time.Now().UTC()
	ledger.Stats = b.stats
	ledger.Errors = b.errors
	data, err := json.MarshalIndent(ledger, "", "  ")
	if err != nil {
		return ledger, err
	}
	ledgerKey := joinRemote(MetaDir, "runs", b.runID, opt.Worker+".json")
	if err := b.d.putBytes(ctx, ledgerKey, data, opt.MetaTier); err != nil {
		b.errorf("write run ledger: %v", err)
		ledger.Stats = b.stats
	}
	if b.stats.Errors > 0 {
		return ledger, fmt.Errorf("gda: run %s finished with %d errors", b.runID, b.stats.Errors)
	}
	return ledger, nil
}

// errorf records an error.
func (b *backup) errorf(format string, args ...any) {
	err := fmt.Errorf(format, args...)
	fs.Errorf(nil, "gda: %v", err)
	b.mu.Lock()
	defer b.mu.Unlock()
	b.stats.Errors++
	if len(b.errors) < maxLedgerErrors {
		b.errors = append(b.errors, err.Error())
	}
}

// count adds n to the counter chosen by f.
func (b *backup) count(f func(*Stats) *int64, n int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	*f(&b.stats) += n
}

// lockInfo is the content of the destination lock.
type lockInfo struct {
	RunID   string
	Host    string
	PID     int
	Started time.Time
}

var lockKey = joinRemote(MetaDir, "lock")

// lock takes the destination lock, unless another run holds a lock
// younger than LockTimeout.
func (b *backup) lock(ctx context.Context, host string) error {
	data, err := b.d.get(ctx, lockKey)
	if err == nil {
		var held lockInfo
		if jsonErr := json.Unmarshal(data, &held); jsonErr == nil {
			age := time.Since(held.Started)
			if age < b.opt.LockTimeout {
				return fmt.Errorf("destination is locked by run %s on %s (pid %d) since %s", held.RunID, held.Host, held.PID, held.Started.Format(time.RFC3339))
			}
			fs.Logf(nil, "gda: taking over lock of run %s on %s, which is %v old", held.RunID, held.Host, age.Round(time.Second))
		}
	} else if !errors.Is(err, fs.ErrorObjectNotFound) {
		return fmt.Errorf("read lock: %w", err)
	}
	data, err = json.Marshal(lockInfo{RunID: b.runID, Host: host, PID: os.Getpid(), Started: time.Now().UTC()})
	if err != nil {
		return err
	}
	return b.d.putBytes(ctx, lockKey, data, b.opt.MetaTier)
}

// unlock removes the destination lock.
func (b *backup) unlock(ctx context.Context) {
	if b.d.dryRun {
		return
	}
	o, err := b.d.f.NewObject(ctx, lockKey)
	if err == nil {
		err = o.Remove(ctx)
	}
	if err != nil {
		fs.Errorf(nil, "gda: failed to remove lock: %v", err)
	}
}

// summarize computes the summaries of the subtree at rel.
func (b *backup) summarize(rel string) *dirSummary {
	s := &dirSummary{}
	b.summaries[rel] = s
	names, err := readDir(sourcePath(b.srcRoot, rel))
	if err != nil {
		b.errorf("read directory %q: %v", rel, err)
		s.unreadable = true
		return s
	}
	for _, name := range names {
		childRel := joinRemote(rel, name)
		if !utf8.ValidString(name) {
			s.badName = true
		}
		info, err := os.Lstat(sourcePath(b.srcRoot, childRel))
		if err != nil {
			b.errorf("stat %q: %v", childRel, err)
			s.unreadable = true
			continue
		}
		if info.IsDir() {
			child := b.summarize(childRel)
			s.treeSize += child.treeSize
			s.treeFiles += child.treeFiles
			s.standalone = s.standalone || child.standalone
			s.unreadable = s.unreadable || child.unreadable
			s.badName = s.badName || child.badName
			continue
		}
		s.treeFiles++
		if info.Mode().IsRegular() {
			s.treeSize += info.Size()
			if info.Size() >= b.opt.StandaloneMin {
				s.standalone = true
			}
		}
	}
	return s
}

// rollupEligible returns true if the subtree at rel could be packed as
// one unit.
func (b *backup) rollupEligible(rel string) bool {
	s := b.summaries[rel]
	return b.opt.RollupMax > 0 && s != nil && s.treeSize < b.opt.RollupMax &&
		!s.standalone && !s.unreadable && !s.badName
}

// isRollupRoot returns true if the directory at rel is the top of a
// rolled up subtree.
func (b *backup) isRollupRoot(rel string) bool {
	if !b.rollupEligible(rel) {
		return false
	}
	return rel == "" || !b.rollupEligible(parentRel(rel))
}

// parentRel returns the parent of rel.
func parentRel(rel string) string {
	if i := strings.LastIndexByte(rel, '/'); i >= 0 {
		return rel[:i]
	}
	return ""
}

// dirChange is the outcome of comparing a directory with its previous index.
type dirChange struct {
	index   []Entry        // new index
	changes []Entry        // changeset rows
	store   []*sourceEntry // entries whose data must be stored
	retire  []string       // keys of subdirectories whose indexes must be retired
	recurse []childDir     // subdirectories with their own index to process next
}

// childDir is a subdirectory to process.
type childDir struct {
	rel, key string
}

// processDir backs up the directory at rel, whose destination key is key.
// If rollup is set, the whole subtree is packed as one unit.
func (b *backup) processDir(ctx context.Context, rel, key string, rollup bool) {
	if ctx.Err() != nil {
		return
	}
	b.count(func(s *Stats) *int64 { return &s.IndexedDirs }, 1)
	prevEntries, err := b.d.readIndex(ctx, key)
	if err != nil {
		b.errorf("%v", err)
		return
	}
	prev := make(map[string]Entry, len(prevEntries))
	for _, e := range prevEntries {
		prev[e.Name] = e
	}
	cur, keep, err := b.collect(rel, key, rollup)
	if err != nil {
		b.errorf("%v", err)
		return
	}
	change := b.compare(key, prev, cur, keep)
	if b.commitDir(ctx, rel, key, prevEntries, change) {
		for _, childKey := range change.retire {
			b.retireIndex(ctx, childKey)
		}
	}
	for _, child := range change.recurse {
		b.processDir(ctx, child.rel, child.key, b.isRollupRoot(child.rel))
	}
}

// collect reads the entries of the directory at rel. For a rollup it
// reads the whole subtree, naming entries relative to rel. keep holds the
// names of entries that couldn't be read, whose previous rows are kept.
func (b *backup) collect(rel, key string, rollup bool) (cur []sourceEntry, keep map[string]bool, err error) {
	keep = map[string]bool{}
	names, err := readDir(sourcePath(b.srcRoot, rel))
	if err != nil {
		return nil, nil, fmt.Errorf("read directory %q: %w", rel, err)
	}
	for _, name := range names {
		childRel := joinRemote(rel, name)
		encName, encoding := encodeName(name)
		if !rollup && isReserved(name, rel == "") {
			fs.Logf(nil, "gda: skipping %q: name is reserved for GDA's own objects", childRel)
			b.count(func(s *Stats) *int64 { return &s.Skipped }, 1)
			continue
		}
		e, err := statEntry(sourcePath(b.srcRoot, childRel), encName, b.names)
		if err != nil {
			b.errorf("stat %q: %v", childRel, err)
			keep[encName] = true
			continue
		}
		e.NameEncoding = encoding
		if !e.IsDir() {
			cur = append(cur, e)
			continue
		}
		s := b.summaries[childRel]
		if s == nil {
			// Created since the scan; the next run backs it up.
			keep[encName] = true
			continue
		}
		e.TreeSize, e.TreeFiles = s.treeSize, s.treeFiles
		if rollup {
			e.Listing = ListingRollup
			cur = append(cur, e)
			sub, subKeep, err := b.collect(childRel, "", true)
			if err != nil {
				return nil, nil, err
			}
			for _, se := range sub {
				se.Name = encName + "/" + se.Name
				cur = append(cur, se)
			}
			for name := range subKeep {
				keep[encName+"/"+name] = true
			}
			b.count(func(s *Stats) *int64 { return &s.RollupDirs }, 1)
			continue
		}
		childKey := joinRemote(key, encName)
		if keyTooLong(b.rootKey, joinRemote(childKey, packName(dirLabel(childKey, b.opt.RootLabel), b.runID, b.opt.Worker, 999)+".csv")) {
			b.errorf("skipping directory %q: its keys would be longer than %d bytes", childRel, maxKeyLength)
			keep[encName] = true
			continue
		}
		e.Listing = ListingIndex
		cur = append(cur, e)
	}
	return cur, keep, nil
}

// sameMeta returns true if the metadata other than data and times of a and b match.
func sameMeta(a *Entry, b *Entry) bool {
	return a.Mode == b.Mode && a.UID == b.UID && a.GID == b.GID &&
		a.Owner == b.Owner && a.Group == b.Group
}

// withMeta returns prev with the metadata of cur.
func withMeta(prev Entry, cur *Entry) Entry {
	prev.ModTime = cur.ModTime
	prev.Mode = cur.Mode
	prev.UID, prev.GID = cur.UID, cur.GID
	prev.Owner, prev.Group = cur.Owner, cur.Group
	prev.Action = ""
	return prev
}

// changeRow returns e as a changeset row with action.
func changeRow(e Entry, action string) Entry {
	e.Action = action
	return e
}

// compare works out what changed in a directory since its previous index.
func (b *backup) compare(key string, prev map[string]Entry, cur []sourceEntry, keep map[string]bool) *dirChange {
	c := &dirChange{}
	seen := make(map[string]bool, len(cur))
	for i := range cur {
		e := &cur[i]
		seen[e.Name] = true
		p, had := prev[e.Name]
		if e.IsDir() {
			row := e.Entry
			switch {
			case !had:
				c.changes = append(c.changes, changeRow(row, ActionAdd))
				b.count(func(s *Stats) *int64 { return &s.Added }, 1)
				if row.Listing == ListingRollup {
					c.store = append(c.store, e)
				}
			case p.Type != TypeDir:
				c.changes = append(c.changes, changeRow(row, ActionModify))
				b.count(func(s *Stats) *int64 { return &s.Modified }, 1)
				if row.Listing == ListingRollup {
					c.store = append(c.store, e)
				}
			default:
				row.Location, row.Offset, row.Codec, row.Run = p.Location, p.Offset, p.Codec, p.Run
				if p.Listing == ListingIndex && row.Listing == ListingRollup {
					c.retire = append(c.retire, joinRemote(key, e.Name))
				}
			}
			if row.Listing == ListingIndex {
				c.recurse = append(c.recurse, childDir{
					rel: b.childRel(key, e),
					key: joinRemote(key, e.Name),
				})
			}
			if len(c.store) == 0 || c.store[len(c.store)-1] != e {
				c.index = append(c.index, row)
			}
			continue
		}
		switch {
		case !had:
			c.changes = append(c.changes, changeRow(e.Entry, ActionAdd))
			c.store = append(c.store, e)
			b.count(func(s *Stats) *int64 { return &s.Added }, 1)
		case p.Type != e.Type || e.Size != p.Size || e.LinkTarget != p.LinkTarget:
			c.changes = append(c.changes, changeRow(e.Entry, ActionModify))
			c.store = append(c.store, e)
			b.count(func(s *Stats) *int64 { return &s.Modified }, 1)
		case e.Type == TypeFile && !e.ModTime.Equal(p.ModTime):
			sum, err := hashFile(e.path)
			if err != nil {
				b.errorf("hash %q: %v", e.path, err)
				c.index = append(c.index, p)
				continue
			}
			if sum != p.MD5 {
				c.changes = append(c.changes, changeRow(e.Entry, ActionModify))
				c.store = append(c.store, e)
				b.count(func(s *Stats) *int64 { return &s.Modified }, 1)
				continue
			}
			row := withMeta(p, &e.Entry)
			c.changes = append(c.changes, changeRow(row, ActionMeta))
			c.index = append(c.index, row)
			b.count(func(s *Stats) *int64 { return &s.MetaOnly }, 1)
		case !sameMeta(&e.Entry, &p) || !e.ModTime.Equal(p.ModTime):
			row := withMeta(p, &e.Entry)
			c.changes = append(c.changes, changeRow(row, ActionMeta))
			c.index = append(c.index, row)
			b.count(func(s *Stats) *int64 { return &s.MetaOnly }, 1)
		default:
			c.index = append(c.index, p)
			b.count(func(s *Stats) *int64 { return &s.Unchanged }, 1)
		}
	}
	for name, p := range prev {
		if seen[name] {
			continue
		}
		if keep[name] || keepsPrefix(keep, name) {
			c.index = append(c.index, p)
			continue
		}
		c.changes = append(c.changes, changeRow(p, ActionDelete))
		b.count(func(s *Stats) *int64 { return &s.Deleted }, 1)
		if p.IsDir() && p.Listing == ListingIndex && isDirectChild(name) {
			c.retire = append(c.retire, joinRemote(key, name))
		}
	}
	return c
}

// keepsPrefix returns true if name is inside a directory in keep.
func keepsPrefix(keep map[string]bool, name string) bool {
	for i := strings.IndexByte(name, '/'); i >= 0; i = nextSlash(name, i) {
		if keep[name[:i]] {
			return true
		}
	}
	return false
}

func nextSlash(s string, i int) int {
	j := strings.IndexByte(s[i+1:], '/')
	if j < 0 {
		return -1
	}
	return i + 1 + j
}

// childRel returns the source path of the subdirectory e of the indexed
// directory at key.
func (b *backup) childRel(key string, e *sourceEntry) string {
	rel, err := relFromPath(b.srcRoot, e.path)
	if err != nil {
		return joinRemote(key, e.Name)
	}
	return rel
}

// relFromPath returns p relative to root with "/" separators.
func relFromPath(root, p string) (string, error) {
	if p == root {
		return "", nil
	}
	prefix := strings.TrimSuffix(root, "/") + "/"
	if !strings.HasPrefix(p, prefix) {
		return "", fmt.Errorf("%q is not below %q", p, root)
	}
	return strings.TrimPrefix(p, prefix), nil
}

// hashFile returns the hex MD5 of the file at p.
func hashFile(p string) (sum string, err error) {
	in, err := os.Open(p)
	if err != nil {
		return "", err
	}
	defer fs.CheckClose(in, &err)
	h := md5.New()
	if _, err := io.Copy(h, in); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// commitDir stores the data of a directory's changed entries, then writes
// its changeset and index. It returns false if the directory couldn't be
// committed, in which case its previous index stays in place.
func (b *backup) commitDir(ctx context.Context, rel, key string, prevEntries []Entry, c *dirChange) bool {
	label := dirLabel(key, b.opt.RootLabel)
	stored, failed := b.storeData(ctx, key, label, c.store)
	if failed {
		return false
	}
	// Entries whose data couldn't be stored this run keep their
	// previous rows and are left out of the changeset.
	skip := map[string]bool{}
	for _, e := range c.store {
		if _, ok := stored[e.Name]; !ok {
			skip[e.Name] = true
		}
	}
	var changes []Entry
	for _, row := range c.changes {
		if skip[row.Name] {
			continue
		}
		if e, ok := stored[row.Name]; ok {
			row = changeRow(e, row.Action)
		}
		changes = append(changes, row)
	}
	index := c.index
	for _, e := range c.store {
		if s, ok := stored[e.Name]; ok {
			s.Action = ""
			index = append(index, s)
		}
	}
	prevByName := make(map[string]Entry, len(prevEntries))
	for _, p := range prevEntries {
		prevByName[p.Name] = p
	}
	for name := range skip {
		if p, ok := prevByName[name]; ok {
			index = append(index, p)
		}
	}
	changed, err := indexChanged(prevEntries, index)
	if err != nil {
		b.errorf("compare index of %q: %v", rel, err)
		return false
	}
	if len(changes) > 0 {
		sortEntries(changes)
		if err := b.d.writeEntries(ctx, joinRemote(key, changesetName(label, b.runID)), changes); err != nil {
			b.errorf("write changeset of %q: %v", rel, err)
			return false
		}
		b.count(func(s *Stats) *int64 { return &s.MetaObjects }, 1)
	}
	if changed {
		if err := b.d.writeIndex(ctx, key, index); err != nil {
			b.errorf("write index of %q: %v", rel, err)
			return false
		}
		b.count(func(s *Stats) *int64 { return &s.MetaObjects }, 1)
	}
	return true
}

// indexChanged returns true if the index would change.
func indexChanged(prev, next []Entry) (bool, error) {
	if prev == nil {
		return true, nil
	}
	prev = append([]Entry(nil), prev...)
	sortEntries(prev)
	sortEntries(next)
	var a, b bytes.Buffer
	if err := WriteEntries(&a, prev); err != nil {
		return false, err
	}
	if err := WriteEntries(&b, next); err != nil {
		return false, err
	}
	return !bytes.Equal(a.Bytes(), b.Bytes()), nil
}

// storeData stores the data of entries in packs and standalone objects.
// It returns the stored entries by name. failed is set if an upload
// failed, in which case nothing from this directory may be committed.
func (b *backup) storeData(ctx context.Context, key, label string, entries []*sourceEntry) (stored map[string]Entry, failed bool) {
	stored = map[string]Entry{}
	if b.d.dryRun {
		b.planData(key, label, entries, stored)
		return stored, false
	}
	var packed []*sourceEntry
	for _, e := range entries {
		if e.Type == TypeSocket {
			// Sockets can't be stored in tar, so only the index records them.
			row := e.Entry
			row.Run = b.runID
			stored[e.Name] = row
			continue
		}
		if e.Type == TypeFile && e.Size >= b.opt.StandaloneMin && isDirectChild(e.Name) {
			if !b.storeStandalone(ctx, key, e, stored) {
				return nil, true
			}
			continue
		}
		packed = append(packed, e)
	}
	if len(packed) == 0 {
		return stored, false
	}
	var pw *packWriter
	part := 0
	closePack := func() bool {
		if pw == nil {
			return true
		}
		defer pw.remove()
		ok := b.uploadPack(ctx, key, pw, stored)
		pw = nil
		return ok
	}
	for _, e := range packed {
		if pw != nil && !pw.fits(&e.Entry, b.opt.PackSize) {
			if !closePack() {
				return nil, true
			}
		}
		if pw == nil {
			part++
			var err error
			pw, err = newPackWriter(packName(label, b.runID, b.opt.Worker, part), b.opt.TempDir)
			if err != nil {
				b.errorf("%v", err)
				return nil, true
			}
		}
		e.Run = b.runID
		err := pw.add(e)
		switch {
		case errors.Is(err, errSourceChanged):
			fs.Logf(e.path, "gda: changed while being read, leaving it for the next run")
			b.count(func(s *Stats) *int64 { return &s.Deferred }, 1)
			removeMember(pw, e.Name)
		case err != nil:
			b.errorf("%v", err)
			removeMember(pw, e.Name)
		}
	}
	if !closePack() {
		return nil, true
	}
	return stored, false
}

// planData fills in stored as storeData would, without reading or
// uploading any data. It is used for --dry-run.
func (b *backup) planData(key, label string, entries []*sourceEntry, stored map[string]Entry) {
	var packSize int64
	part := 0
	for _, e := range entries {
		row := e.Entry
		row.Run = b.runID
		switch {
		case e.Type == TypeSocket:
		case e.Type == TypeFile && e.Size >= b.opt.StandaloneMin && isDirectChild(e.Name):
			row.Location = e.Name
			b.count(func(s *Stats) *int64 { return &s.Standalone }, 1)
			b.count(func(s *Stats) *int64 { return &s.StandaloneBytes }, e.Size)
		default:
			size := max(e.Size, 0) + packOverhead(&e.Entry)
			if part == 0 || packSize+size > b.opt.PackSize {
				part++
				packSize = 0
				b.count(func(s *Stats) *int64 { return &s.Packs }, 1)
			}
			packSize += size
			b.count(func(s *Stats) *int64 { return &s.PackBytes }, size)
			row.Location = packName(label, b.runID, b.opt.Worker, part)
		}
		stored[e.Name] = row
	}
}

// removeMember drops name from a pack's manifest, so an entry whose data
// is incomplete isn't recorded. Its bytes stay in the pack unreferenced.
func removeMember(pw *packWriter, name string) {
	for i := range pw.members {
		if pw.members[i].Name == name {
			pw.members = append(pw.members[:i], pw.members[i+1:]...)
			return
		}
	}
}

// uploadPack finishes and uploads a pack, adding its members to stored.
func (b *backup) uploadPack(ctx context.Context, key string, pw *packWriter, stored map[string]Entry) bool {
	size, sum, err := pw.finish(time.Now())
	if err != nil {
		b.errorf("finish pack %q: %v", pw.name, err)
		return false
	}
	remote := joinRemote(key, pw.name)
	if err := b.d.putFile(ctx, remote, pw.file.Name(), size, sum, b.opt.DataTier); err != nil {
		b.errorf("upload pack %q: %v", remote, err)
		return false
	}
	b.count(func(s *Stats) *int64 { return &s.Packs }, 1)
	b.count(func(s *Stats) *int64 { return &s.PackBytes }, size)
	for _, m := range pw.members {
		stored[m.Name] = m
	}
	return true
}

// storeStandalone uploads a large file as its own object, never
// overwriting an existing object.
func (b *backup) storeStandalone(ctx context.Context, key string, e *sourceEntry, stored map[string]Entry) bool {
	name := e.Name
	remote := joinRemote(key, name)
	exists, err := b.d.exists(ctx, remote)
	if err != nil {
		b.errorf("check %q: %v", remote, err)
		return false
	}
	if exists {
		name = versionedName(e.Name, b.runID)
		remote = joinRemote(key, name)
	}
	if keyTooLong(b.rootKey, remote) {
		b.errorf("skipping %q: its key would be longer than %d bytes", e.path, maxKeyLength)
		return true
	}
	sum, err := b.d.putSourceFile(ctx, remote, e, b.opt.DataTier)
	if err != nil {
		b.errorf("upload %q: %v", remote, err)
		return false
	}
	info, err := os.Lstat(e.path)
	if err != nil || info.Size() != e.Size || !info.ModTime().Equal(e.ModTime) {
		fs.Logf(e.path, "gda: changed while being uploaded, leaving it for the next run")
		b.count(func(s *Stats) *int64 { return &s.Deferred }, 1)
		return true
	}
	row := e.Entry
	row.Location = name
	row.Codec = CodecNone
	row.MD5 = sum
	row.StoredSize = e.Size
	row.StoredMD5 = sum
	row.Run = b.runID
	stored[e.Name] = row
	b.count(func(s *Stats) *int64 { return &s.Standalone }, 1)
	b.count(func(s *Stats) *int64 { return &s.StandaloneBytes }, e.Size)
	return true
}

// retireIndex marks every entry in the index at key as deleted and
// replaces the index with an empty one. It is used when a directory is
// deleted or becomes part of a rollup. Subdirectories with their own
// index are retired too.
func (b *backup) retireIndex(ctx context.Context, key string) {
	entries, err := b.d.readIndex(ctx, key)
	if err != nil {
		b.errorf("%v", err)
		return
	}
	if len(entries) == 0 {
		return
	}
	changes := make([]Entry, 0, len(entries))
	for _, e := range entries {
		changes = append(changes, changeRow(e, ActionDelete))
		if e.IsDir() && e.Listing == ListingIndex && isDirectChild(e.Name) {
			b.retireIndex(ctx, joinRemote(key, e.Name))
		}
	}
	if len(changes) > 0 {
		if err := b.d.writeEntries(ctx, joinRemote(key, changesetName(dirLabel(key, b.opt.RootLabel), b.runID)), changes); err != nil {
			b.errorf("write changeset of %q: %v", key, err)
			return
		}
	}
	if err := b.d.writeIndex(ctx, key, []Entry{}); err != nil {
		b.errorf("write index of %q: %v", key, err)
	}
}
