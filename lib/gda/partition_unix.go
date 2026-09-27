//go:build unix

package gda

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/rclone/rclone/fs"
)

// A run can be split over several hosts, for example as a job array:
//
//   - Plan scans the source once, takes the destination lock for the whole
//     run, splits the tree into partitions and saves the plan.
//   - BackupPartition, one per worker index, backs up the partitions
//     assigned to that index.
//   - FinishRun merges the workers' ledgers and releases the lock.
//
// A partition is either a whole subtree, or a single directory whose
// subdirectories are partitions of their own. Partitions never overlap,
// so every directory still has exactly one writer.

// partition is one unit of a planned run.
type partition struct {
	rel     string // source path relative to the source root
	key     string // destination key
	shallow bool   // only this directory, not the ones below it
	files   int64  // files it covers, for balancing
	worker  int    // index of the worker it is assigned to
}

func runKey(runID, name string) string {
	return joinRemote(MetaDir, "runs", runID, name)
}

// Plan scans srcRoot and splits a backup of it to dst into partitions for
// workers workers. It takes the destination lock, which FinishRun
// releases, and returns the run ID.
func Plan(ctx context.Context, srcRoot string, dst fs.Fs, opt Options, workers int) (string, error) {
	if workers < 1 {
		return "", errors.New("a plan needs at least one worker")
	}
	b, ledger, err := newBackup(ctx, srcRoot, dst, opt)
	if err != nil {
		return "", err
	}
	defer b.close()
	if err := b.chooseRunID(ctx, ledger); err != nil {
		return "", err
	}
	if err := b.lock(ctx, ledger.Host); err != nil {
		return "", err
	}
	fs.Infof(nil, "gda: run %s: scanning %s to plan %d workers", b.runID, b.srcRoot, workers)
	b.summarizeAll()
	parts := b.partition(workers)
	var plan bytes.Buffer
	w := csv.NewWriter(&plan)
	_ = w.Write([]string{"worker", "kind", "rel", "key", "files"})
	for _, p := range parts {
		kind := "tree"
		if p.shallow {
			kind = "dir"
		}
		_ = w.Write([]string{strconv.Itoa(p.worker), kind, p.rel, p.key, strconv.FormatInt(p.files, 10)})
	}
	w.Flush()
	// The totals of the shallow directories and of the partitions'
	// roots, which workers need for their parents' index rows and rollup
	// decisions without scanning above their partitions.
	var sums bytes.Buffer
	sw := csv.NewWriter(&sums)
	_ = sw.Write([]string{"rel", "tree_size", "tree_files", "standalone", "unreadable", "bad_name"})
	for _, p := range parts {
		s := b.summary(p.rel)
		_ = sw.Write([]string{p.rel, strconv.FormatInt(s.treeSize, 10), strconv.FormatInt(s.treeFiles, 10),
			strconv.FormatBool(s.standalone), strconv.FormatBool(s.unreadable), strconv.FormatBool(s.badName)})
	}
	sw.Flush()
	if err := w.Error(); err != nil {
		return "", err
	}
	if err := sw.Error(); err != nil {
		return "", err
	}
	saved := b.opt
	saved.IndexCache = ""
	// Workers back up whole directories, not a change list.
	saved.Changes = nil
	planOpt, err := json.MarshalIndent(saved, "", "  ")
	if err != nil {
		b.unlock(ctx)
		return "", err
	}
	if err := b.d.putBytes(ctx, runKey(b.runID, "plan.json"), planOpt, b.opt.MetaTier); err != nil {
		b.unlock(ctx)
		return "", fmt.Errorf("save plan: %w", err)
	}
	if err := b.d.putBytes(ctx, runKey(b.runID, "plan.csv"), plan.Bytes(), b.opt.MetaTier); err != nil {
		b.unlock(ctx)
		return "", fmt.Errorf("save plan: %w", err)
	}
	if err := b.d.putBytes(ctx, runKey(b.runID, "summaries.csv"), sums.Bytes(), b.opt.MetaTier); err != nil {
		b.unlock(ctx)
		return "", fmt.Errorf("save plan: %w", err)
	}
	fs.Infof(nil, "gda: run %s: planned %d partitions for %d workers", b.runID, len(parts), workers)
	return b.runID, nil
}

// partition splits the scanned tree into partitions for workers workers,
// splitting a directory into itself and its subdirectories while its
// subtree holds more than its share of files, then assigns them to
// workers largest first.
func (b *backup) partition(workers int) []partition {
	total := b.summary("").treeFiles
	// Several partitions per worker even out their sizes.
	limit := max(total/int64(workers*4), 1)
	var parts []partition
	queue := []partition{{}}
	for len(queue) > 0 {
		p := queue[0]
		queue = queue[1:]
		s := b.summary(p.rel)
		children := b.childDirs(p.rel, p.key)
		if s.treeFiles <= limit || b.rollupEligible(p.rel) || len(children) == 0 {
			p.files = s.treeFiles
			parts = append(parts, p)
			continue
		}
		// A shallow partition covers only the files directly in it.
		p.shallow = true
		p.files = s.treeFiles
		for _, c := range children {
			p.files -= b.summary(c.rel).treeFiles
		}
		p.files = max(p.files, 1)
		parts = append(parts, p)
		queue = append(queue, children...)
	}
	sort.SliceStable(parts, func(i, j int) bool { return parts[i].files > parts[j].files })
	load := make([]int64, workers)
	for i := range parts {
		least := 0
		for w := range load {
			if load[w] < load[least] {
				least = w
			}
		}
		parts[i].worker = least
		load[least] += parts[i].files
	}
	return parts
}

// childDirs returns the subdirectories of the directory at rel which get
// their own index, as collect finds them.
func (b *backup) childDirs(rel, key string) []partition {
	names, err := readDir(sourcePath(b.srcRoot, rel))
	if err != nil {
		return nil
	}
	var out []partition
	for _, name := range names {
		childRel := joinRemote(rel, name)
		if isReserved(name, rel == "") {
			continue
		}
		info, err := os.Lstat(sourcePath(b.srcRoot, childRel))
		if err != nil || !info.IsDir() || b.summary(childRel) == nil || b.excluded(childRel, true, 0, info.ModTime()) {
			continue
		}
		encName, _ := encodeName(name)
		childKey := joinRemote(key, encName)
		if b.dirKeysTooLong(childKey) {
			continue
		}
		out = append(out, partition{rel: childRel, key: childKey})
	}
	return out
}

// BackupPartition backs up the partitions of the planned run runID which
// are assigned to worker index. opt.Worker must be unique among the run's
// workers. The options which shape the backup come from the plan; only
// the worker ID, worker count, temporary directory and retries come
// from opt.
//
// A partition which didn't finish, or finished with errors, can be run
// again with the same worker ID. Each attempt names its objects with its
// own suffix on the worker ID, such as -r2, so it never replaces what an
// earlier attempt committed.
func BackupPartition(ctx context.Context, srcRoot string, dst fs.Fs, opt Options, runID string, index int) (*Ledger, error) {
	d := &dest{f: dst, retries: 1}
	data, err := d.get(ctx, runKey(runID, "plan.json"))
	if err != nil {
		return nil, fmt.Errorf("read plan of run %s: %w", runID, err)
	}
	var planOpt Options
	if err := json.Unmarshal(data, &planOpt); err != nil {
		return nil, fmt.Errorf("parse plan of run %s: %w", runID, err)
	}
	planOpt.Worker, planOpt.Workers, planOpt.TempDir, planOpt.Retries = opt.Worker, opt.Workers, opt.TempDir, opt.Retries
	// Split runs don't use the index cache.
	planOpt.IndexCache = ""
	b, ledger, err := newBackup(ctx, srcRoot, dst, planOpt)
	if err != nil {
		return nil, err
	}
	defer b.close()
	b.runID, ledger.RunID, ledger.Partition = runID, runID, &index
	if isRunFile(b.opt.Worker + ".json") {
		return nil, fmt.Errorf("worker ID %q is reserved", b.opt.Worker)
	}
	if err := b.checkRunLock(ctx); err != nil {
		return nil, err
	}
	parts, err := b.readPlan(ctx)
	if err != nil {
		return nil, err
	}
	attempt, err := b.checkLedger(ctx, index)
	if err != nil {
		return nil, err
	}
	if err := b.claimPartition(ctx, index, ledger.Host); err != nil {
		return nil, err
	}
	if attempt > 1 {
		ledger.Attempt = attempt
		b.opt.Worker = fmt.Sprintf("%s-r%d", b.opt.Worker, attempt)
	}
	defer b.keepLock(ctx)()
	// A ledger without a finish time shows the partition is under way.
	if err := b.putLedger(ctx, ledger); err != nil {
		return nil, fmt.Errorf("write run ledger: %w", err)
	}
	// A shallow partition takes its subdirectories' totals from the plan;
	// a whole subtree is read once, as a full run reads the tree.
	var shallow, trees []partition
	for _, p := range parts {
		switch {
		case p.worker != index:
		case p.shallow:
			shallow = append(shallow, p)
		default:
			trees = append(trees, p)
		}
	}
	if err := b.loadDedup(ctx); err != nil {
		return nil, err
	}
	b.checkVersioned(ctx)
	if b.rebaseDirs, err = readRebase(ctx, b.d); err != nil {
		return nil, err
	}
	fs.Infof(nil, "gda: run %s: worker %d backing up %d partitions", runID, index, len(shallow)+len(trees))
	var items []childDir
	for _, p := range shallow {
		items = append(items, childDir{rel: p.rel, key: p.key, shallow: true})
	}
	b.processItems(ctx, items)
	for _, p := range trees {
		b.scanFrom(ctx, p.rel, p.key)
	}
	if err := b.checkClaim(ctx, index); err != nil {
		b.errorf("%v", err)
	}
	return b.finishLedger(ctx, ledger)
}

// partitionClaim records which worker backs up a partition of a run.
type partitionClaim struct {
	Worker  string
	Host    string
	PID     int
	Started time.Time
}

func partitionKey(runID string, index int) string {
	return runKey(runID, fmt.Sprintf("partition-%d.json", index))
}

// isRunFile returns true if name is one of the files of a planned run
// other than a worker ledger.
func isRunFile(name string) bool {
	return name == "plan.json" || name == "run.json" || strings.HasPrefix(name, "partition-")
}

// checkLedger checks that the worker ID isn't used by another partition
// of the run and that this partition hasn't finished already. It returns
// the number of this attempt at the partition.
func (b *backup) checkLedger(ctx context.Context, index int) (attempt int, err error) {
	data, err := b.d.get(ctx, runKey(b.runID, b.opt.Worker+".json"))
	if errors.Is(err, fs.ErrorObjectNotFound) {
		return 1, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read run ledger: %w", err)
	}
	var prev Ledger
	if err := json.Unmarshal(data, &prev); err != nil || prev.Partition == nil || *prev.Partition != index {
		return 0, fmt.Errorf("worker ID %q is used by another partition of run %s; give each worker its own ID", b.opt.Worker, b.runID)
	}
	if !prev.Finished.IsZero() && prev.Stats.Errors == 0 {
		return 0, fmt.Errorf("partition %d of run %s has finished already", index, b.runID)
	}
	attempt = max(prev.Attempt, 1) + 1
	if attempt > maxAttempts {
		return 0, fmt.Errorf("partition %d of run %s has been run %d times; plan a new run", index, b.runID, maxAttempts)
	}
	fs.Logf(nil, "gda: run %s: running partition %d again, attempt %d", b.runID, index, attempt)
	return attempt, nil
}

// claimPartition records that this worker backs up partition index,
// unless another worker has started it.
func (b *backup) claimPartition(ctx context.Context, index int, host string) error {
	key := partitionKey(b.runID, index)
	data, err := b.d.get(ctx, key)
	if err == nil {
		var held partitionClaim
		if err := json.Unmarshal(data, &held); err != nil {
			return fmt.Errorf("parse %q: %w", key, err)
		}
		if held.Worker != b.opt.Worker {
			return fmt.Errorf("partition %d of run %s was started by worker %q on %s; run it again with --worker %s", index, b.runID, held.Worker, held.Host, held.Worker)
		}
	} else if !errors.Is(err, fs.ErrorObjectNotFound) {
		return fmt.Errorf("read %q: %w", key, err)
	}
	claim := partitionClaim{Worker: b.opt.Worker, Host: host, PID: os.Getpid(), Started: time.Now().UTC()}
	if data, err = json.Marshal(claim); err != nil {
		return err
	}
	if err := b.d.putBytes(ctx, key, data, b.opt.MetaTier); err != nil {
		return err
	}
	if b.d.dryRun {
		return nil
	}
	// Writing the claim isn't atomic, so check that another worker which
	// started at the same moment didn't overwrite it.
	b.claim = &claim
	return b.checkClaim(ctx, index)
}

// checkClaim checks that this worker's claim on partition index stands.
func (b *backup) checkClaim(ctx context.Context, index int) error {
	if b.d.dryRun {
		return nil
	}
	data, err := b.d.get(ctx, partitionKey(b.runID, index))
	if err != nil {
		return fmt.Errorf("read claim of partition %d: %w", index, err)
	}
	var held partitionClaim
	if err := json.Unmarshal(data, &held); err != nil || b.claim == nil || held.Worker != b.claim.Worker ||
		held.Host != b.claim.Host || held.PID != b.claim.PID || !held.Started.Equal(b.claim.Started) {
		return fmt.Errorf("partition %d of run %s was taken by worker %q on %s", index, b.runID, held.Worker, held.Host)
	}
	return nil
}

// checkRunLock checks that the destination lock is held by the run.
func (b *backup) checkRunLock(ctx context.Context) error {
	data, err := b.d.get(ctx, lockKey)
	if err != nil {
		return fmt.Errorf("run %s: read lock: %w", b.runID, err)
	}
	var held lockInfo
	if err := json.Unmarshal(data, &held); err != nil || held.RunID != b.runID {
		return fmt.Errorf("the destination lock isn't held by run %s", b.runID)
	}
	return nil
}

// readPlan reads the run's partitions and loads the summaries saved with
// them.
func (b *backup) readPlan(ctx context.Context) ([]partition, error) {
	data, err := b.d.get(ctx, runKey(b.runID, "plan.csv"))
	if err != nil {
		return nil, fmt.Errorf("read plan of run %s: %w", b.runID, err)
	}
	var parts []partition
	err = readCSVRows(data, 5, func(r []string) error {
		worker, err := strconv.Atoi(r[0])
		if err != nil {
			return err
		}
		files, err := strconv.ParseInt(r[4], 10, 64)
		if err != nil {
			return err
		}
		if r[2] != "" && validTarget(r[2]) != nil {
			return fmt.Errorf("invalid path %q", r[2])
		}
		parts = append(parts, partition{worker: worker, shallow: r[1] == "dir", rel: r[2], key: r[3], files: files})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("parse plan of run %s: %w", b.runID, err)
	}
	data, err = b.d.get(ctx, runKey(b.runID, "summaries.csv"))
	if err != nil {
		return nil, fmt.Errorf("read plan of run %s: %w", b.runID, err)
	}
	err = readCSVRows(data, 6, func(r []string) error {
		s := &dirSummary{}
		var err1, err2 error
		s.treeSize, err1 = strconv.ParseInt(r[1], 10, 64)
		s.treeFiles, err2 = strconv.ParseInt(r[2], 10, 64)
		s.standalone, s.unreadable, s.badName = r[3] == "true", r[4] == "true", r[5] == "true"
		b.setSummary(r[0], s)
		return errors.Join(err1, err2)
	})
	if err != nil {
		return nil, fmt.Errorf("parse plan of run %s: %w", b.runID, err)
	}
	return parts, nil
}

// readCSVRows calls fn for each row of CSV data after the header.
func readCSVRows(data []byte, fields int, fn func([]string) error) error {
	r := csv.NewReader(bytes.NewReader(data))
	r.FieldsPerRecord = fields
	if _, err := r.Read(); err != nil {
		return err
	}
	for {
		record, err := r.Read()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := fn(record); err != nil {
			return err
		}
	}
}

// FinishRun merges the ledgers of the workers of run runID into
// _gda/runs/<run>/run.json and releases the destination lock. It returns
// an error, keeping the lock, if a partition of the plan hasn't
// finished, and an error after releasing it if any worker reported
// errors.
func FinishRun(ctx context.Context, dst fs.Fs, runID string, opt Options) (*Ledger, error) {
	d := &dest{f: dst, metaTier: opt.MetaTier, retries: max(opt.Retries, 1), dryRun: fs.GetConfig(ctx).DryRun}
	planned, err := plannedPartitions(ctx, d, runID)
	if err != nil {
		return nil, err
	}
	entries, err := dst.List(ctx, joinRemote(MetaDir, "runs", runID))
	if err != nil {
		return nil, fmt.Errorf("list run %s: %w", runID, err)
	}
	merged := &Ledger{FormatVersion: FormatVersion, RunID: runID, Worker: "all", Destination: destName(dst)}
	done := map[int]string{} // worker by partition
	for _, e := range entries {
		name := path.Base(e.Remote())
		if path.Ext(name) != ".json" || isRunFile(name) {
			continue
		}
		data, err := d.get(ctx, e.Remote())
		if err != nil {
			return nil, err
		}
		var l Ledger
		if err := json.Unmarshal(data, &l); err != nil {
			return nil, fmt.Errorf("parse %q: %w", e.Remote(), err)
		}
		if l.RunID != runID || l.Partition == nil {
			return nil, fmt.Errorf("%q isn't the ledger of a partition of run %s", e.Remote(), runID)
		}
		if l.Finished.IsZero() {
			return nil, fmt.Errorf("partition %d of run %s, worker %s, hasn't finished", *l.Partition, runID, l.Worker)
		}
		if other, ok := done[*l.Partition]; ok {
			return nil, fmt.Errorf("partition %d of run %s was run by both worker %s and worker %s", *l.Partition, runID, other, l.Worker)
		}
		done[*l.Partition] = l.Worker
		merged.Source, merged.Options, merged.DryRun = l.Source, l.Options, l.DryRun
		if merged.Started.IsZero() || l.Started.Before(merged.Started) {
			merged.Started = l.Started
		}
		if l.Finished.After(merged.Finished) {
			merged.Finished = l.Finished
		}
		merged.Stats.add(&l.Stats)
		for _, msg := range l.Errors {
			if len(merged.Errors) < maxLedgerErrors {
				merged.Errors = append(merged.Errors, l.Worker+": "+msg)
			}
		}
	}
	var missing []int
	for index := range planned {
		if _, ok := done[index]; !ok {
			missing = append(missing, index)
		}
	}
	if len(missing) > 0 {
		sort.Ints(missing)
		return nil, fmt.Errorf("partitions %v of run %s have no ledger yet", missing, runID)
	}
	data, err := json.MarshalIndent(merged, "", "  ")
	if err != nil {
		return nil, err
	}
	if err := d.putBytes(ctx, runKey(runID, "run.json"), data, opt.MetaTier); err != nil {
		return merged, fmt.Errorf("save run ledger: %w", err)
	}
	b := &backup{d: d, runID: runID}
	if err := b.checkRunLock(ctx); err == nil {
		// Every partition has finished, so every marked directory has
		// been packed again.
		clearRebase(ctx, d)
		if err := compactDedup(ctx, d, runID, false); err != nil {
			fs.Errorf(nil, "gda: compact dedup index: %v", err)
		}
		b.unlock(ctx)
	}
	if merged.Stats.Errors > 0 {
		return merged, fmt.Errorf("gda: run %s finished with %d errors", runID, merged.Stats.Errors)
	}
	return merged, nil
}

// plannedPartitions returns the indexes of the partitions of the planned
// run runID which have directories assigned.
func plannedPartitions(ctx context.Context, d *dest, runID string) (map[int]bool, error) {
	data, err := d.get(ctx, runKey(runID, "plan.csv"))
	if err != nil {
		return nil, fmt.Errorf("read plan of run %s: %w", runID, err)
	}
	planned := map[int]bool{}
	err = readCSVRows(data, 5, func(r []string) error {
		index, err := strconv.Atoi(r[0])
		planned[index] = true
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("parse plan of run %s: %w", runID, err)
	}
	return planned, nil
}

// add adds o to s.
func (s *Stats) add(o *Stats) {
	s.IndexedDirs += o.IndexedDirs
	s.RollupDirs += o.RollupDirs
	s.Unchanged += o.Unchanged
	s.Added += o.Added
	s.Modified += o.Modified
	s.MetaOnly += o.MetaOnly
	s.Deleted += o.Deleted
	s.Packs += o.Packs
	s.PackBytes += o.PackBytes
	s.CompressedFrom += o.CompressedFrom
	s.Standalone += o.Standalone
	s.StandaloneBytes += o.StandaloneBytes
	s.MetaObjects += o.MetaObjects
	s.Rebased += o.Rebased
	s.Deduplicated += o.Deduplicated
	s.DeduplicatedBytes += o.DeduplicatedBytes
	s.Skipped += o.Skipped
	s.Deferred += o.Deferred
	s.Errors += o.Errors
}
