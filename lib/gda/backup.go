//go:build unix

package gda

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
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
	Compression   string        // CodecZstd to compress data where it helps, or CodecNone
	Level         int           // zstd compression level, 1 to 22
	Workers       int           // directories processed in parallel
	Changes       []string      // if set, back up only the directories these source paths are in
	DedupMin      int64         // identical files of at least this size are stored once; -1 disables
	CompressMax   int64         // standalone files bigger than this are stored uncompressed
	IndexCache    string        // directory for local copies of indexes; "" to read them all from the destination
	Xattrs        bool          // keep extended attributes, which include ACLs
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
		Compression:   CodecZstd,
		Level:         3,
		Workers:       1,
		DedupMin:      1024 * 1024,
		// Compressed files are uploaded as streams of unknown size, which
		// S3 limits to 10,000 parts of --s3-chunk-size, 48.8 GiB by default.
		CompressMax: 32 * 1024 * 1024 * 1024,
	}
}

// Stats counts what a run did.
type Stats struct {
	IndexedDirs       int64 // directories with their own index
	RollupDirs        int64 // directories packed inside a rollup
	Unchanged         int64 // entries unchanged since the last run
	Added             int64 // entries added
	Modified          int64 // entries whose data changed
	MetaOnly          int64 // entries whose metadata changed but not their data
	Deleted           int64 // entries no longer in the source
	Packs             int64 // packs uploaded
	PackBytes         int64 // bytes in packs uploaded, as stored
	CompressedFrom    int64 // bytes of compressed packs and files before compression
	Standalone        int64 // standalone objects uploaded
	StandaloneBytes   int64 // bytes in standalone objects uploaded
	MetaObjects       int64 // changesets and index objects written
	Rebased           int64 // unchanged files packed again to gather a directory's files in fewer packs
	Deduplicated      int64 // files which were copies of stored content, so weren't stored again
	DeduplicatedBytes int64 // bytes in those files
	Skipped           int64 // entries skipped, for example because of reserved names
	Deferred          int64 // entries that changed while being read, left for the next run
	Errors            int64 // errors
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
	Partition     *int `json:",omitempty"` // index of the partition of a planned run
	Attempt       int  `json:",omitempty"` // attempt at the partition, from 2 when it is run again
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
	badName    bool  // holds a name which needs encoding, so can't be rolled up
	noRollup   bool  // holds a directory which must keep its own index
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

	mu              sync.Mutex
	stats           Stats
	errors          []string
	dataTierChecked bool // whether the storage class of data uploads was checked
	stopped         bool // whether the run was stopped by a fatal error

	enc *zstd.Encoder // for compressing packs, nil if not compressing

	dirty   map[string]bool // for change runs, directories to process
	newDirs map[string]bool // for change runs, tops of subtrees new since the last run, or to scan in full

	dedup *dedupIndex // stored copies, nil if not deduplicating

	catalog *catalogWriter // this run's catalog, nil for --dry-run

	claim *partitionClaim // this worker's claim on its partition of a planned run

	hardLinks map[string]Entry // stored files with more than one link, by HardLink, located by full key
}

// stop stops the run after the directory in progress.
func (b *backup) stop() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.stopped = true
}

func (b *backup) isStopped() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.stopped
}

// Backup backs up the directory tree at srcRoot to dst.
func Backup(ctx context.Context, srcRoot string, dst fs.Fs, opt Options) (*Ledger, error) {
	b, ledger, err := newBackup(ctx, srcRoot, dst, opt)
	if err != nil {
		return nil, err
	}
	defer b.close()
	if err := b.chooseRunID(ctx, ledger); err != nil {
		return nil, err
	}
	if err := b.lock(ctx, ledger.Host); err != nil {
		return nil, err
	}
	defer b.unlock(ctx)
	defer b.keepLock(ctx)()
	if err := b.loadDedup(ctx); err != nil {
		return nil, err
	}
	if err := b.openCache(ctx); err != nil {
		fs.Errorf(nil, "gda: not using the index cache: %v", err)
	}
	// A ledger without a finish time shows the run started, so even if
	// it doesn't finish, other hosts' index caches see it wrote here.
	if err := b.putLedger(ctx, ledger); err != nil {
		return nil, fmt.Errorf("write run ledger: %w", err)
	}
	if len(opt.Changes) > 0 {
		fs.Infof(nil, "gda: run %s: backing up %d changes in %s", b.runID, len(opt.Changes), b.srcRoot)
		if err := b.backupChanges(ctx, opt.Changes); err != nil {
			b.errorf("%v", err)
		}
		ledger, err := b.finishLedger(ctx, ledger)
		b.finishCache(ctx)
		return ledger, err
	}
	fs.Infof(nil, "gda: run %s: scanning %s with %d workers", b.runID, b.srcRoot, b.opt.Workers)
	b.summarizeAll()
	fs.Infof(nil, "gda: run %s: backing up to %s", b.runID, fs.ConfigString(dst))
	b.processAll(ctx)
	ledger, err = b.finishLedger(ctx, ledger)
	b.finishCache(ctx)
	if b.dedup != nil {
		if cErr := compactDedup(ctx, b.d, b.runID); cErr != nil {
			fs.Errorf(nil, "gda: compact dedup index: %v", cErr)
		}
	}
	return ledger, err
}

// newBackup checks the options and sets up a run.
func newBackup(ctx context.Context, srcRoot string, dst fs.Fs, opt Options) (*backup, *Ledger, error) {
	if opt.PackSize < opt.StandaloneMin {
		return nil, nil, fmt.Errorf("pack size %d must be at least the standalone minimum %d", opt.PackSize, opt.StandaloneMin)
	}
	if !workerIDRe.MatchString(opt.Worker) {
		return nil, nil, fmt.Errorf("invalid worker ID %q: use letters, digits and _", opt.Worker)
	}
	if opt.Retries < 1 {
		opt.Retries = 1
	}
	if opt.Workers < 1 {
		opt.Workers = 1
	}
	if last := workerID(opt, opt.Workers-1); len(last) > maxWorkerID {
		return nil, nil, fmt.Errorf("worker ID %q is longer than %d bytes", last, maxWorkerID)
	}
	if opt.Xattrs && !xattrsSupported {
		return nil, nil, errors.New("extended attributes can only be kept on Linux")
	}
	if opt.Compression != CodecZstd && opt.Compression != CodecNone {
		return nil, nil, fmt.Errorf("unknown compression %q: use %s or %s", opt.Compression, CodecZstd, CodecNone)
	}
	srcRoot = filepath.Clean(srcRoot)
	info, err := os.Stat(srcRoot)
	if err != nil {
		return nil, nil, err
	}
	if !info.IsDir() {
		return nil, nil, fmt.Errorf("source %q is not a directory", srcRoot)
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
	if opt.Compression == CodecZstd {
		if b.enc, err = newEncoder(opt.Level, opt.Workers); err != nil {
			return nil, nil, err
		}
	}
	if !ci.DryRun {
		if b.catalog, err = newCatalogWriter(opt.TempDir); err != nil {
			return nil, nil, fmt.Errorf("create catalog: %w", err)
		}
	}
	if b.opt.RootLabel == "" {
		// The last element of the destination path, or "root" when the
		// destination is a bucket or file system root.
		b.opt.RootLabel = "root"
		if i := strings.LastIndexByte(b.rootKey, '/'); i >= 0 && i < len(b.rootKey)-1 {
			b.opt.RootLabel = b.rootKey[i+1:]
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
	return b, ledger, nil
}

// openCache opens the index cache, if one is configured.
func (b *backup) openCache(ctx context.Context) error {
	if b.opt.IndexCache == "" || b.d.dryRun {
		return nil
	}
	runs, err := b.runsDigest(ctx)
	if err != nil {
		return err
	}
	cache, err := openIndexCache(b.opt.IndexCache, fs.ConfigString(b.d.f), runs)
	if err != nil {
		return err
	}
	b.d.cache = cache
	return nil
}

// finishCache records in the index cache that the run finished, so the
// next run can trust it.
func (b *backup) finishCache(ctx context.Context) {
	if b.d.cache == nil || b.isStopped() || ctx.Err() != nil {
		return
	}
	runs, err := b.runsDigest(ctx)
	if err != nil {
		fs.Errorf(nil, "gda: index cache: %v", err)
		return
	}
	b.d.cache.finish(fs.ConfigString(b.d.f), b.runID, runs)
}

// runsDigest returns a digest of the IDs of the runs on the destination,
// which changes whenever a run is added, whatever its ID, or "" if there
// are none.
func (b *backup) runsDigest(ctx context.Context) (string, error) {
	entries, err := b.d.f.List(ctx, joinRemote(MetaDir, "runs"))
	if errors.Is(err, fs.ErrorDirNotFound) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	var runs []string
	for _, e := range entries {
		runs = append(runs, path.Base(e.Remote()))
	}
	if len(runs) == 0 {
		return "", nil
	}
	sort.Strings(runs)
	sum := sha256.Sum256([]byte(strings.Join(runs, "\n")))
	return hex.EncodeToString(sum[:]), nil
}

// loadDedup reads the dedup index if deduplicating.
func (b *backup) loadDedup(ctx context.Context) error {
	if b.opt.DedupMin < 0 {
		return nil
	}
	var err error
	b.dedup, err = loadDedup(ctx, b.d)
	return err
}

// close releases what the run holds.
func (b *backup) close() {
	if b.enc != nil {
		_ = b.enc.Close()
	}
	if b.catalog != nil {
		b.catalog.remove()
	}
}

// chooseRunID makes sure the run ID isn't shared with a run which
// finished within the same second, as run IDs have a resolution of one
// second.
func (b *backup) chooseRunID(ctx context.Context, ledger *Ledger) error {
	for {
		entries, err := b.d.f.List(ctx, joinRemote(MetaDir, "runs", b.runID))
		if err != nil && !errors.Is(err, fs.ErrorDirNotFound) {
			return fmt.Errorf("check run ID: %w", err)
		}
		if len(entries) == 0 {
			return nil
		}
		time.Sleep(time.Second)
		b.runID = NewRunID(timeNow())
		ledger.RunID = b.runID
	}
}

// finishLedger writes the ledger of this worker's part of the run and
// returns an error if anything failed.
func (b *backup) finishLedger(ctx context.Context, ledger *Ledger) (*Ledger, error) {
	if b.isStopped() {
		fs.Errorf(nil, "gda: run %s stopped early", b.runID)
	}
	if b.dedup != nil && !b.d.dryRun {
		if err := b.dedup.save(ctx, b.d, b.runID, b.opt.Worker); err != nil {
			b.errorf("write dedup index: %v", err)
		}
	}
	if b.catalog != nil {
		if err := b.saveCatalog(ctx); err != nil {
			b.errorf("write catalog: %v", err)
		}
		b.catalog = nil
	}
	ledger.Finished = time.Now().UTC()
	ledger.Stats = b.stats
	ledger.Errors = b.errors
	if err := b.putLedger(ctx, ledger); err != nil {
		b.errorf("write run ledger: %v", err)
		ledger.Stats = b.stats
	}
	if b.stats.Errors > 0 {
		return ledger, fmt.Errorf("gda: run %s finished with %d errors", b.runID, b.stats.Errors)
	}
	return ledger, nil
}

// putLedger writes the ledger of this worker's part of the run.
func (b *backup) putLedger(ctx context.Context, ledger *Ledger) error {
	data, err := json.MarshalIndent(ledger, "", "  ")
	if err != nil {
		return err
	}
	return b.d.putBytes(ctx, joinRemote(MetaDir, "runs", b.runID, ledger.Worker+".json"), data, b.opt.MetaTier)
}

// saveCatalog uploads the run's catalog.
func (b *backup) saveCatalog(ctx context.Context) error {
	return b.catalog.finish(ctx, b.d, joinRemote(MetaDir, "catalog", "runs", b.runID, b.opt.Worker+".csv.zst"))
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
	RunID     string
	Host      string
	PID       int
	Started   time.Time
	Refreshed time.Time     // when the run last showed it is still going
	Timeout   time.Duration // the holder's LockTimeout
}

// age returns how long ago the run holding the lock last refreshed it.
func (l *lockInfo) age() time.Duration {
	latest := l.Started
	if l.Refreshed.After(latest) {
		latest = l.Refreshed
	}
	return time.Since(latest)
}

var lockKey = joinRemote(MetaDir, "lock")

// lockRefreshEvery is how often a run refreshes the destination lock, so
// that a run lasting longer than LockTimeout keeps it. Tests lower it.
var lockRefreshEvery = time.Hour

// lock takes the destination lock, unless another run holds a lock
// younger than LockTimeout or that run's own timeout, whichever is
// longer.
func (b *backup) lock(ctx context.Context, host string) error {
	data, err := b.d.get(ctx, lockKey)
	if err == nil {
		var held lockInfo
		if jsonErr := json.Unmarshal(data, &held); jsonErr == nil {
			age := held.age()
			if age < max(b.opt.LockTimeout, held.Timeout) {
				return fmt.Errorf("destination is locked by run %s on %s (pid %d) since %s", held.RunID, held.Host, held.PID, held.Started.Format(time.RFC3339))
			}
			fs.Logf(nil, "gda: taking over lock of run %s on %s, which is %v old", held.RunID, held.Host, age.Round(time.Second))
		}
	} else if !errors.Is(err, fs.ErrorObjectNotFound) {
		return fmt.Errorf("read lock: %w", err)
	}
	data, err = json.Marshal(lockInfo{RunID: b.runID, Host: host, PID: os.Getpid(), Started: time.Now().UTC(), Timeout: b.opt.LockTimeout})
	if err != nil {
		return err
	}
	if err := b.d.putBytes(ctx, lockKey, data, b.opt.MetaTier); err != nil {
		return err
	}
	if b.d.dryRun {
		return nil
	}
	// Writing the lock isn't atomic, so check that another run which
	// started at the same moment didn't overwrite it.
	data, err = b.d.get(ctx, lockKey)
	if err != nil {
		return fmt.Errorf("read lock back: %w", err)
	}
	var held lockInfo
	if err := json.Unmarshal(data, &held); err != nil || held.RunID != b.runID || held.Host != host {
		return fmt.Errorf("destination lock was taken by run %s on %s", held.RunID, held.Host)
	}
	return b.d.checkTier(ctx, lockKey, b.opt.MetaTier)
}

// keepLock refreshes the destination lock every lockRefreshEvery until
// the function it returns is called. If the lock is lost, the run stops.
func (b *backup) keepLock(ctx context.Context) (done func()) {
	if b.d.dryRun {
		return func() {}
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTicker(lockRefreshEvery)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ctx.Done():
				return
			case <-t.C:
			}
			if err := b.refreshLock(ctx); err != nil {
				b.errorf("%v", err)
				b.stop()
				return
			}
		}
	}()
	return func() {
		close(stop)
		wg.Wait()
	}
}

// refreshLock rewrites the destination lock with the current time if
// the run still holds it. Failing to read it is left for the next try,
// as the workers of a planned run refresh the same lock.
func (b *backup) refreshLock(ctx context.Context) error {
	data, err := b.d.get(ctx, lockKey)
	var held lockInfo
	if err == nil {
		err = json.Unmarshal(data, &held)
	}
	if err != nil && !errors.Is(err, fs.ErrorObjectNotFound) {
		fs.Errorf(nil, "gda: refresh lock: %v", err)
		return nil
	}
	if held.RunID != b.runID {
		return fmt.Errorf("run %s lost the destination lock, now held by run %q on %q", b.runID, held.RunID, held.Host)
	}
	held.Refreshed = time.Now().UTC()
	if data, err = json.Marshal(held); err != nil {
		return err
	}
	if err := b.d.putBytes(ctx, lockKey, data, b.opt.MetaTier); err != nil {
		fs.Errorf(nil, "gda: refresh lock: %v", err)
	}
	return nil
}

// unlock removes the destination lock, unless another run has taken it
// over.
func (b *backup) unlock(ctx context.Context) {
	if b.d.dryRun {
		return
	}
	data, err := b.d.get(ctx, lockKey)
	if errors.Is(err, fs.ErrorObjectNotFound) {
		return
	}
	if err != nil {
		fs.Errorf(nil, "gda: not removing the lock, as it can't be read: %v", err)
		return
	}
	var held lockInfo
	if json.Unmarshal(data, &held) == nil && held.RunID != b.runID {
		fs.Errorf(nil, "gda: not removing the lock, which run %s on %s holds now", held.RunID, held.Host)
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

// summarize computes the summaries of the subtree at rel. If sem isn't
// nil, subdirectories are summarized in parallel while it has room.
func (b *backup) summarize(rel string, sem chan struct{}) *dirSummary {
	s := &dirSummary{}
	b.setSummary(rel, s)
	names, err := readDir(sourcePath(b.srcRoot, rel))
	if err != nil {
		b.errorf("read directory %q: %v", rel, err)
		s.unreadable = true
		return s
	}
	var (
		wg       sync.WaitGroup
		children []*dirSummary
		childMu  sync.Mutex
	)
	addChild := func(child *dirSummary) {
		childMu.Lock()
		children = append(children, child)
		childMu.Unlock()
	}
	for _, name := range names {
		childRel := joinRemote(rel, name)
		if _, encoding := encodeName(name); encoding != "" {
			s.badName = true
		}
		info, err := os.Lstat(sourcePath(b.srcRoot, childRel))
		if err != nil {
			b.errorf("stat %q: %v", childRel, err)
			s.unreadable = true
			continue
		}
		if info.IsDir() {
			select {
			case sem <- struct{}{}:
				wg.Add(1)
				go func() {
					defer wg.Done()
					defer func() { <-sem }()
					addChild(b.summarize(childRel, sem))
				}()
			default:
				addChild(b.summarize(childRel, sem))
			}
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
	wg.Wait()
	for _, child := range children {
		s.treeSize += child.treeSize
		s.treeFiles += child.treeFiles
		s.standalone = s.standalone || child.standalone
		s.unreadable = s.unreadable || child.unreadable
		s.badName = s.badName || child.badName
	}
	return s
}

func (b *backup) setSummary(rel string, s *dirSummary) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.summaries[rel] = s
}

// summary returns the summary of the subtree at rel, or nil. Change runs
// add summaries while directories are being processed.
func (b *backup) summary(rel string) *dirSummary {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.summaries[rel]
}

// rollupEligible returns true if the subtree at rel could be packed as
// one unit.
func (b *backup) rollupEligible(rel string) bool {
	s := b.summary(rel)
	return b.opt.RollupMax > 0 && s != nil && s.treeSize < b.opt.RollupMax &&
		!s.standalone && !s.unreadable && !s.badName && !s.noRollup
}

// isRollupRoot returns true if the directory at rel is the top of a
// rolled up subtree.
func (b *backup) isRollupRoot(rel string) bool {
	if !b.rollupEligible(rel) {
		return false
	}
	return rel == "" || !b.rollupEligible(parentRel(rel))
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
	shallow  bool // process only this directory, not the ones below it
	unrolled bool // was inside a rollup, so has no index of its own yet
}

// processDir backs up the directory at rel, whose destination key is key,
// as worker w. If rollup is set, the whole subtree is packed as one unit.
// It returns the subdirectories with their own index, to process next.
func (b *backup) processDir(ctx context.Context, w, rel, key string, rollup bool) []childDir {
	if ctx.Err() != nil || b.isStopped() {
		return nil
	}
	prevEntries, err := b.d.readIndex(ctx, key)
	if err != nil {
		b.errorf("%v", err)
		return nil
	}
	b.count(func(s *Stats) *int64 { return &s.IndexedDirs }, 1)
	prev := make(map[string]*Entry, len(prevEntries))
	for i := range prevEntries {
		prev[prevEntries[i].Name] = &prevEntries[i]
	}
	cur, keep, err := b.collect(w, rel, key, rollup)
	if err != nil {
		b.errorf("%v", err)
		return nil
	}
	change := b.compare(key, prev, cur, keep)
	b.commitDir(ctx, w, rel, key, prevEntries, change)
	return change.recurse
}

// collect reads the entries of the directory at rel. For a rollup it
// reads the whole subtree, naming entries relative to rel. keep holds the
// names of entries that couldn't be read, whose previous rows are kept.
func (b *backup) collect(w, rel, key string, rollup bool) (cur []sourceEntry, keep map[string]bool, err error) {
	keep = map[string]bool{}
	names, err := readDir(sourcePath(b.srcRoot, rel))
	if err != nil {
		return nil, nil, fmt.Errorf("read directory %q: %w", rel, err)
	}
	for _, name := range names {
		childRel := joinRemote(rel, name)
		encName, encoding := encodeName(name)
		// Checked inside rollups too, so that the same files are skipped
		// whether or not their subtree is rolled up.
		if isReserved(name, rel == "") {
			fs.Logf(nil, "gda: skipping %q: name is reserved for GDA's own objects", childRel)
			b.count(func(s *Stats) *int64 { return &s.Skipped }, 1)
			continue
		}
		e, err := statEntry(sourcePath(b.srcRoot, childRel), encName, b.names)
		if err == nil && b.opt.Xattrs {
			e.Xattrs, err = readXattrs(e.path)
		}
		if err != nil {
			b.errorf("stat %q: %v", childRel, err)
			keep[encName] = true
			continue
		}
		e.NameEncoding = encoding
		e.rel = childRel
		if !e.IsDir() {
			cur = append(cur, e)
			continue
		}
		s := b.summary(childRel)
		if s == nil {
			// Created since the scan; the next run backs it up.
			keep[encName] = true
			continue
		}
		e.TreeSize, e.TreeFiles = s.treeSize, s.treeFiles
		if rollup {
			e.Listing = ListingRollup
			cur = append(cur, e)
			sub, subKeep, err := b.collect(w, childRel, "", true)
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
		if b.dirKeysTooLong(joinRemote(key, encName)) {
			b.errorf("skipping directory %q: its keys would be longer than %d bytes", childRel, maxKeyLength)
			keep[encName] = true
			continue
		}
		e.Listing = ListingIndex
		cur = append(cur, e)
	}
	return cur, keep, nil
}

// dirKeysTooLong returns true if keys of objects in the directory at key
// could be longer than S3 allows. The longest is a pack's manifest name,
// checked with the longest worker ID so the answer doesn't depend on
// which worker handles the directory.
func (b *backup) dirKeysTooLong(key string) bool {
	return keyTooLong(b.rootKey, joinRemote(key, packName(dirLabel(key, b.opt.RootLabel), b.runID, longestWorkerID, 99999)+".csv"))
}

// sameMeta returns true if the metadata other than data and times of a and b match.
func sameMeta(a *Entry, b *Entry) bool {
	return a.Mode == b.Mode && a.UID == b.UID && a.GID == b.GID &&
		a.Owner == b.Owner && a.Group == b.Group && a.HardLink == b.HardLink &&
		a.Xattrs == b.Xattrs
}

// withMeta returns prev with the metadata of cur.
func withMeta(prev Entry, cur *Entry) Entry {
	prev.ModTime = cur.ModTime
	prev.Mode = cur.Mode
	prev.UID, prev.GID = cur.UID, cur.GID
	prev.Owner, prev.Group = cur.Owner, cur.Group
	prev.HardLink = cur.HardLink
	prev.Xattrs = cur.Xattrs
	prev.Action = ""
	return prev
}

// changeRow returns e as a changeset row with action.
func changeRow(e Entry, action string) Entry {
	e.Action = action
	return e
}

// dirMetaChanged returns true if a directory row differs from its
// previous row in anything but its data location.
func dirMetaChanged(cur, prev *Entry) bool {
	return !sameMeta(cur, prev) || !cur.ModTime.Equal(prev.ModTime) ||
		cur.TreeSize != prev.TreeSize || cur.TreeFiles != prev.TreeFiles ||
		cur.Listing != prev.Listing
}

// compare works out what changed in a directory since its previous index.
func (b *backup) compare(key string, prev map[string]*Entry, cur []sourceEntry, keep map[string]bool) *dirChange {
	c := &dirChange{}
	seen := make(map[string]bool, len(cur))
	for i := range cur {
		e := &cur[i]
		seen[e.Name] = true
		var p Entry
		prevRow, had := prev[e.Name]
		if had {
			p = *prevRow
		}
		if had && p.IsDir() && !e.IsDir() && p.Listing == ListingIndex && isDirectChild(e.Name) {
			// A directory with its own index was replaced by something else.
			c.retire = append(c.retire, joinRemote(key, e.Name))
		}
		if e.IsDir() {
			row := e.Entry
			stored := false
			switch {
			case !had || p.Type != TypeDir:
				action := ActionAdd
				if had {
					action = ActionModify
				}
				c.changes = append(c.changes, changeRow(row, action))
				if row.Listing == ListingRollup {
					// Packed so that plain tar recreates the directory.
					c.store = append(c.store, e)
					stored = true
				}
			default:
				row.Location, row.Offset, row.Codec, row.Run = p.Location, p.Offset, p.Codec, p.Run
				if dirMetaChanged(&row, &p) {
					c.changes = append(c.changes, changeRow(row, ActionMeta))
				}
				if p.Listing == ListingIndex && row.Listing == ListingRollup {
					c.retire = append(c.retire, joinRemote(key, e.Name))
				}
			}
			if row.Listing == ListingIndex {
				unrolled := had && p.Type == TypeDir && p.Listing == ListingRollup
				c.recurse = append(c.recurse, childDir{rel: e.rel, key: joinRemote(key, e.Name), unrolled: unrolled})
			}
			if !stored {
				c.index = append(c.index, row)
			}
			continue
		}
		switch {
		case !had:
			c.changes = append(c.changes, changeRow(e.Entry, ActionAdd))
			c.store = append(c.store, e)
		case p.Type != e.Type || e.Size != p.Size || e.LinkTarget != p.LinkTarget ||
			e.DevMajor != p.DevMajor || e.DevMinor != p.DevMinor:
			c.changes = append(c.changes, changeRow(e.Entry, ActionModify))
			c.store = append(c.store, e)
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
				continue
			}
			row := withMeta(p, &e.Entry)
			c.changes = append(c.changes, changeRow(row, ActionMeta))
			c.index = append(c.index, row)
		case !sameMeta(&e.Entry, &p) || !e.ModTime.Equal(p.ModTime):
			row := withMeta(p, &e.Entry)
			c.changes = append(c.changes, changeRow(row, ActionMeta))
			c.index = append(c.index, row)
		default:
			c.index = append(c.index, p)
		}
	}
	for name, prevRow := range prev {
		if seen[name] {
			continue
		}
		p := *prevRow
		if keep[name] || keepsPrefix(keep, name) {
			c.index = append(c.index, p)
			continue
		}
		c.changes = append(c.changes, changeRow(p, ActionDelete))
		if p.IsDir() && p.Listing == ListingIndex && isDirectChild(name) {
			c.retire = append(c.retire, joinRemote(key, name))
		}
	}
	rebase(c, cur, b.opt.PackSize)
	return c
}

// rebaseMaxPacks is the number of packs a directory's unchanged files
// may be spread over before a run packs them again. Tests lower it.
var rebaseMaxPacks = 20

// rebase moves the unchanged packed files of a directory whose files are
// fragmented to c.store, so they are packed again from the source and
// restoring the directory needs fewer objects. The files are fragmented
// if they are spread over more than rebaseMaxPacks packs and more than
// twice the packs of packSize they would fill. The old packs are kept,
// as history refers to them.
func rebase(c *dirChange, cur []sourceEntry, packSize int64) {
	isPacked := func(row *Entry) bool {
		return row.Type == TypeFile && row.Size > 0 && row.Offset >= 0 && row.DedupOf == "" && row.Location != ""
	}
	packs := map[string]bool{}
	var bytes int64
	for i := range c.index {
		if isPacked(&c.index[i]) {
			packs[c.index[i].Location] = true
			bytes += c.index[i].Size + packOverhead(&c.index[i])
		}
	}
	needed := int((bytes + packSize - 1) / max(packSize, 1))
	if len(packs) <= rebaseMaxPacks || len(packs) <= 2*needed {
		return
	}
	source := make(map[string]*sourceEntry, len(cur))
	for i := range cur {
		source[cur[i].Name] = &cur[i]
	}
	changed := make(map[string]bool, len(c.changes))
	for _, row := range c.changes {
		changed[row.Name] = true
	}
	kept := c.index[:0]
	for _, row := range c.index {
		e := source[row.Name]
		if !isPacked(&row) || e == nil || changed[row.Name] {
			kept = append(kept, row)
			continue
		}
		e.rebase = true
		c.store = append(c.store, e)
		c.changes = append(c.changes, changeRow(e.Entry, ActionRebase))
	}
	c.index = kept
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

// commitDir stores the data of a directory's changed entries, writes its
// changeset, retires the indexes of subdirectories which no longer have
// one, and writes its index, in that order. It returns false if the
// directory couldn't be committed, in which case its previous index stays
// in place and the next run tries again.
func (b *backup) commitDir(ctx context.Context, w, rel, key string, prevEntries []Entry, c *dirChange) bool {
	label := dirLabel(key, b.opt.RootLabel)
	stored, failed := b.storeData(ctx, w, key, label, c.store)
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
	// Points into prevEntries, which indexChanged sorts, so it must not
	// be used after that.
	prevByName := make(map[string]*Entry, len(prevEntries))
	for i := range prevEntries {
		prevByName[prevEntries[i].Name] = &prevEntries[i]
	}
	// Filtered in place, as a directory may have millions of rows.
	changes := c.changes[:0]
	for _, row := range c.changes {
		if skip[row.Name] {
			continue
		}
		if e, ok := stored[row.Name]; ok {
			action := row.Action
			if p := prevByName[row.Name]; action == ActionRebase && p != nil && e.MD5 != p.MD5 {
				// Changed without its size or time changing.
				action = ActionModify
			}
			row = changeRow(e, action)
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
	for name := range skip {
		if p, ok := prevByName[name]; ok {
			index = append(index, *p)
		}
	}
	changed, err := indexChanged(prevEntries, index)
	if err != nil {
		b.errorf("compare index of %q: %v", rel, err)
		return false
	}
	if len(changes) > 0 {
		sortEntries(changes)
		if err := b.d.writeEntries(ctx, joinRemote(key, changesetName(label, b.runID, w)), changes, columns); err != nil {
			b.errorf("write changeset of %q: %v", rel, err)
			return false
		}
		b.count(func(s *Stats) *int64 { return &s.MetaObjects }, 1)
		if b.catalog != nil {
			if err := b.catalog.add(key, changes); err != nil {
				b.errorf("catalog: %v", err)
			}
		}
	}
	// Retire before writing this index: if that fails, the previous index
	// still lists the subdirectories, so the next run retires them again.
	for _, childKey := range c.retire {
		if !b.retireIndex(ctx, w, childKey) {
			return false
		}
	}
	if changed {
		if err := b.d.writeIndex(ctx, key, index, b.runID); err != nil {
			b.errorf("write index of %q: %v", rel, err)
			return false
		}
		b.count(func(s *Stats) *int64 { return &s.MetaObjects }, 1)
		if err := b.checkpoint(ctx, key, index); err != nil {
			// Checkpoints only speed up replaying history.
			fs.Errorf(nil, "gda: checkpoint %q: %v", rel, err)
		}
	}
	b.countChanges(changes, len(index))
	return true
}

// checkpointEvery is how often a directory's index is saved as a
// checkpoint, from which its history can be replayed rather than from
// its first changeset.
var checkpointEvery = 30 * 24 * time.Hour

// checkpoint saves a copy of the directory's new index once
// checkpointEvery has passed since its latest checkpoint, or, if it has
// none, since its first changeset, so a directory's first runs don't
// write one.
func (b *backup) checkpoint(ctx context.Context, key string, index []Entry) error {
	if b.d.dryRun {
		return nil
	}
	entries, err := b.d.f.List(ctx, key)
	if err != nil && !errors.Is(err, fs.ErrorDirNotFound) {
		return err
	}
	latest, first := "", ""
	for _, e := range entries {
		name := path.Base(e.Remote())
		if m := checkpointRe.FindStringSubmatch(name); m != nil && m[1] > latest {
			latest = m[1]
		}
		if m := changesetRe.FindStringSubmatch(name); m != nil && (first == "" || m[1] < first) {
			first = m[1]
		}
	}
	since := latest
	if since == "" {
		since = first
	}
	start, err := ParseRunID(since)
	if err != nil {
		return nil
	}
	now, err := ParseRunID(b.runID)
	if err != nil || now.Sub(start) < checkpointEvery {
		return err
	}
	return b.d.writeEntries(ctx, joinRemote(key, checkpointName(b.runID)), index, columns)
}

// countChanges adds a committed directory's changes to the stats.
func (b *backup) countChanges(changes []Entry, indexRows int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	live := 0
	for _, row := range changes {
		switch row.Action {
		case ActionAdd:
			b.stats.Added++
		case ActionModify:
			b.stats.Modified++
		case ActionMeta:
			b.stats.MetaOnly++
		case ActionRebase:
			b.stats.Rebased++
		case ActionDelete:
			b.stats.Deleted++
			continue
		}
		live++
	}
	b.stats.Unchanged += int64(indexRows - live)
}

// indexChanged returns true if the index would change. It sorts both
// slices.
func indexChanged(prev, next []Entry) (bool, error) {
	if prev == nil || len(prev) != len(next) {
		return true, nil
	}
	sortEntries(prev)
	sortEntries(next)
	for i := range prev {
		same, err := sameRow(&prev[i], &next[i])
		if err != nil || !same {
			return !same, err
		}
	}
	return false, nil
}

// sameRow returns true if a and b are written as the same index row.
func sameRow(a, b *Entry) (bool, error) {
	// Most rows are equal field by field. Times needn't be, as their
	// locations differ, and fields can differ in ways the row doesn't
	// show, so compare the written rows before saying they differ.
	ac, bc := *a, *b
	ac.ModTime, bc.ModTime = time.Time{}, time.Time{}
	if ac == bc && a.ModTime.Equal(b.ModTime) {
		return true, nil
	}
	var aRow, bRow bytes.Buffer
	if err := WriteEntries(&aRow, []Entry{*a}); err != nil {
		return false, err
	}
	if err := WriteEntries(&bRow, []Entry{*b}); err != nil {
		return false, err
	}
	return bytes.Equal(aRow.Bytes(), bRow.Bytes()), nil
}

// storeData stores the data of entries in packs and standalone objects.
// It returns the stored entries by name. failed is set if an upload
// failed, in which case nothing from this directory may be committed.
func (b *backup) storeData(ctx context.Context, w, key, label string, entries []*sourceEntry) (stored map[string]Entry, failed bool) {
	stored = map[string]Entry{}
	if b.d.dryRun {
		b.planData(w, label, entries, stored)
		return stored, false
	}
	entries = b.dedupEntries(key, entries, stored)
	entries, linked := b.linkedEntries(key, entries, stored)
	defer func() {
		if !failed {
			b.storeLinked(key, linked, stored)
			b.recordCopies(key, stored)
			b.recordLinks(key, stored)
		}
	}()
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
			if !b.storeStandalone(ctx, w, key, e, stored) {
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
			pw, err = newPackWriter(packName(label, b.runID, w, part), b.opt.TempDir)
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

// linkedEntries takes the files in entries whose data is stored as
// another link to the same file out of entries, adding their rows to
// stored, so the data of files with several links is stored once. It
// returns the rest, and the files linked to another of entries, to be
// recorded by storeLinked once that one is stored.
func (b *backup) linkedEntries(key string, entries []*sourceEntry, stored map[string]Entry) (rest, linked []*sourceEntry) {
	first := map[string]bool{}
	for _, e := range entries {
		if e.Type != TypeFile || e.HardLink == "" || e.Size <= 0 {
			rest = append(rest, e)
			continue
		}
		b.mu.Lock()
		copyRow, ok := b.hardLinks[e.HardLink]
		b.mu.Unlock()
		switch {
		case ok && sameLink(&copyRow, &e.Entry):
			stored[e.Name] = dedupRow(key, e, copyRow, b.runID)
		case first[e.HardLink]:
			linked = append(linked, e)
		default:
			first[e.HardLink] = true
			rest = append(rest, e)
		}
	}
	return rest, linked
}

// storeLinked adds the rows of the files in linked, whose data is another
// link to a file stored with them, to stored.
func (b *backup) storeLinked(key string, linked []*sourceEntry, stored map[string]Entry) {
	if len(linked) == 0 {
		return
	}
	byLink := map[string]Entry{}
	for _, row := range stored {
		if row.HardLink != "" && row.Type == TypeFile {
			byLink[row.HardLink] = row
		}
	}
	for _, e := range linked {
		row, ok := byLink[e.HardLink]
		if !ok || !sameLink(&row, &e.Entry) {
			// Not stored, so this link is left for the next run too.
			continue
		}
		stored[e.Name] = dedupRow(key, e, fullRow(key, row), b.runID)
	}
}

// sameLink returns true if the file e can be another link to the file
// stored as row. Links to one file share its size, time and metadata; a
// file which reuses the inode of one removed since has its own.
func sameLink(row, e *Entry) bool {
	return row.Size == e.Size && row.ModTime.Equal(e.ModTime) && sameMeta(row, e)
}

// recordLinks remembers where the files with several links in stored,
// from the directory at key, are stored, for the other links elsewhere.
func (b *backup) recordLinks(key string, stored map[string]Entry) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, row := range stored {
		if row.HardLink == "" || row.Type != TypeFile || row.Size <= 0 {
			continue
		}
		if b.hardLinks == nil {
			b.hardLinks = map[string]Entry{}
		}
		if _, ok := b.hardLinks[row.HardLink]; !ok {
			b.hardLinks[row.HardLink] = fullRow(key, row)
		}
	}
}

// fullRow returns row, from the index at key, with its location as the
// full key of the object holding its data.
func fullRow(key string, row Entry) Entry {
	if row.DedupOf != "" {
		row.Location = strings.TrimPrefix(path.Join("/", key, row.DedupOf), "/")
	} else {
		row.Location = joinRemote(key, row.Location)
	}
	row.DedupOf = ""
	return row
}

// planData fills in stored as storeData would, without reading or
// uploading any data. It is used for --dry-run.
func (b *backup) planData(w, label string, entries []*sourceEntry, stored map[string]Entry) {
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
			row.Location = packName(label, b.runID, w, part)
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

// checkDataTier checks the storage class of the first data object the
// run uploads. It returns false, and stops the run, if it is wrong.
func (b *backup) checkDataTier(ctx context.Context, remote string) bool {
	b.mu.Lock()
	checked := b.dataTierChecked
	b.dataTierChecked = true
	b.mu.Unlock()
	if checked {
		return true
	}
	if err := b.d.checkTier(ctx, remote, b.opt.DataTier); err != nil {
		b.errorf("%v", err)
		b.stop()
		return false
	}
	return true
}

// uploadPack finishes and uploads a pack, adding its members to stored.
// A pack whose members were all dropped isn't uploaded.
func (b *backup) uploadPack(ctx context.Context, key string, pw *packWriter, stored map[string]Entry) bool {
	if len(pw.members) == 0 {
		return true
	}
	size, sum, err := pw.finish(time.Now())
	if err != nil {
		b.errorf("finish pack %q: %v", pw.name, err)
		return false
	}
	name, file := pw.name, pw.file.Name()
	if b.enc != nil {
		sample, err := memberSample(file, pw.members)
		if err != nil {
			b.errorf("read pack %q: %v", pw.name, err)
			return false
		}
		if worthCompressing(b.enc, sample) {
			cPath, cSize, cSum, frames, err := compressFile(b.enc, file, b.opt.TempDir, size, frameCuts(size, pw.starts))
			if err != nil {
				b.errorf("compress pack %q: %v", pw.name, err)
				return false
			}
			defer func() { _ = os.Remove(cPath) }()
			b.count(func(s *Stats) *int64 { return &s.CompressedFrom }, size)
			name, file, size, sum = pw.name+".zst", cPath, cSize, cSum
			for i := range pw.members {
				m := &pw.members[i]
				m.Location, m.Codec = name, CodecZstd
				storedRange(m, frames)
			}
		}
	}
	remote := joinRemote(key, name)
	if err := b.d.putFile(ctx, remote, file, size, sum, b.opt.DataTier); err != nil {
		b.errorf("upload pack %q: %v", remote, err)
		return false
	}
	if !b.checkDataTier(ctx, remote) {
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
// overwriting an existing object. A file which can't be stored is left
// out of stored, so the directory keeps its previous row. It returns
// false only if the run must stop.
func (b *backup) storeStandalone(ctx context.Context, w, key string, e *sourceEntry, stored map[string]Entry) bool {
	// Checked with the longest name the file could be stored under, so
	// whether it is stored doesn't depend on the worker or on whether a
	// copy exists already.
	if keyTooLong(b.rootKey, joinRemote(key, versionedName(e.Name, b.runID, longestWorkerID)+".zst")) {
		b.errorf("skipping %q: its key would be longer than %d bytes", e.path, maxKeyLength)
		return true
	}
	compress, err := b.compressStandalone(e)
	if err != nil {
		b.errorf("read %q: %v", e.path, err)
		return true
	}
	suffix := ""
	if compress {
		suffix = ".zst"
	}
	name := e.Name
	if compress {
		name += ".gda.zst"
	}
	remote := joinRemote(key, name)
	exists, err := b.d.exists(ctx, remote)
	if err != nil {
		b.errorf("check %q: %v", remote, err)
		return true
	}
	if exists {
		name = versionedName(e.Name, b.runID, w) + suffix
		remote = joinRemote(key, name)
	}
	var sum, storedSum string
	storedSize := e.Size
	if compress {
		sum, storedSum, storedSize, err = b.d.putCompressed(ctx, remote, e, b.opt.DataTier, b.opt.Level)
	} else {
		sum, err = b.d.putSourceFile(ctx, remote, e, b.opt.DataTier)
		storedSum = sum
	}
	if err != nil {
		b.errorf("upload %q: %v", remote, err)
		return true
	}
	if !b.checkDataTier(ctx, remote) {
		return false
	}
	info, err := os.Lstat(e.path)
	if err != nil || info.Size() != e.Size || !info.ModTime().Equal(e.ModTime) {
		fs.Logf(e.path, "gda: changed while being uploaded, leaving it for the next run")
		b.count(func(s *Stats) *int64 { return &s.Deferred }, 1)
		// Nothing refers to the incomplete copy, and leaving it would
		// take the file's own name for good.
		if o, err := b.d.f.NewObject(ctx, remote); err == nil {
			if err := o.Remove(ctx); err != nil {
				fs.Errorf(remote, "gda: failed to remove incomplete copy: %v", err)
			}
		}
		return true
	}
	row := e.Entry
	row.Location = name
	row.Codec = CodecNone
	if compress {
		row.Codec = CodecZstd
		b.count(func(s *Stats) *int64 { return &s.CompressedFrom }, e.Size)
	}
	row.MD5 = sum
	row.StoredSize = storedSize
	row.StoredMD5 = storedSum
	row.Run = b.runID
	stored[e.Name] = row
	b.count(func(s *Stats) *int64 { return &s.Standalone }, 1)
	b.count(func(s *Stats) *int64 { return &s.StandaloneBytes }, storedSize)
	return true
}

// compressStandalone returns true if the standalone file e should be
// compressed: compression is on, the destination can take uploads of
// unknown size, the file is no bigger than CompressMax and isn't in a
// compressed format by its name, and a trial compression of its start
// saves enough.
func (b *backup) compressStandalone(e *sourceEntry) (bool, error) {
	if b.enc == nil || b.d.f.Features().PutStream == nil || e.Size > b.opt.CompressMax || isCompressedName(e.Name) {
		return false, nil
	}
	sample, err := readStart(e.path, trialBytes)
	if err != nil {
		return false, err
	}
	return worthCompressing(b.enc, sample), nil
}

// memberSample returns data from the start of each file member of the
// pack at p, up to trialBytes in all, so that tar headers and the
// manifest, which always compress well, don't decide whether the files'
// data is worth compressing.
func memberSample(p string, members []Entry) (sample []byte, err error) {
	in, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	defer fs.CheckClose(in, &err)
	const perMember = 64 * 1024
	for i := range members {
		m := &members[i]
		if m.Type != TypeFile || m.Size <= 0 {
			continue
		}
		n := min(m.Size, perMember, trialBytes-int64(len(sample)))
		if n <= 0 {
			break
		}
		buf := make([]byte, n)
		if _, err := in.ReadAt(buf, m.Offset); err != nil {
			return nil, err
		}
		sample = append(sample, buf...)
	}
	return sample, nil
}

// readStart returns up to n bytes from the start of the file at p.
func readStart(p string, n int64) (data []byte, err error) {
	in, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	defer fs.CheckClose(in, &err)
	return io.ReadAll(io.LimitReader(in, n))
}

// retireIndex marks every entry in the index at key as deleted and
// replaces the index with an empty one. It is used when a directory is
// deleted, becomes part of a rollup or is replaced by a file.
// Subdirectories with their own index are retired first. It returns
// false if anything failed.
func (b *backup) retireIndex(ctx context.Context, w, key string) bool {
	entries, err := b.d.readIndex(ctx, key)
	if err != nil {
		b.errorf("%v", err)
		return false
	}
	if len(entries) == 0 {
		return true
	}
	changes := make([]Entry, 0, len(entries))
	for _, e := range entries {
		changes = append(changes, changeRow(e, ActionDelete))
		if e.IsDir() && e.Listing == ListingIndex && isDirectChild(e.Name) {
			if !b.retireIndex(ctx, w, joinRemote(key, e.Name)) {
				return false
			}
		}
	}
	if err := b.d.writeEntries(ctx, joinRemote(key, changesetName(dirLabel(key, b.opt.RootLabel), b.runID, w)), changes, columns); err != nil {
		b.errorf("write changeset of %q: %v", key, err)
		return false
	}
	if b.catalog != nil {
		if err := b.catalog.add(key, changes); err != nil {
			b.errorf("catalog: %v", err)
		}
	}
	if err := b.d.writeIndex(ctx, key, []Entry{}, b.runID); err != nil {
		b.errorf("write index of %q: %v", key, err)
		return false
	}
	return true
}
