//go:build unix

package gda

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// parallelTree creates a tree of many directories, some small enough to
// roll up.
func parallelTree(t *testing.T) string {
	t.Helper()
	src := t.TempDir()
	for i := range 30 {
		dir := fmt.Sprintf("d%02d", i)
		for j := range 4 {
			writeFile(t, src, fmt.Sprintf("%s/f%d.dat", dir, j), 200+i*10+j)
		}
		writeFile(t, src, fmt.Sprintf("%s/sub/deep/x%d.txt", dir, i), 5)
		if i%5 == 0 {
			writeFile(t, src, fmt.Sprintf("%s/big.bin", dir), 9000)
		}
	}
	writeFile(t, src, "tiny/a.txt", 3)
	return src
}

// indexRows returns every row of every index below dst, keyed by
// directory and name, without the fields which depend on the worker.
func indexRows(t *testing.T, dst string) map[string]string {
	t.Helper()
	rows := map[string]string{}
	for _, o := range objects(t, dst) {
		if filepath.Base(o) != IndexName {
			continue
		}
		dir := filepath.Dir(o)
		for name, e := range readCSV(t, filepath.Join(dst, o)) {
			rows[dir+"/"+name] = fmt.Sprintf("%s %d %s %s %d %d", e.Type, e.Size, e.MD5, e.Listing, e.TreeSize, e.TreeFiles)
		}
	}
	return rows
}

func TestBackupParallel(t *testing.T) {
	fakeClock(t)
	src := parallelTree(t)
	serial, parallel := filepath.Join(t.TempDir(), "lab"), filepath.Join(t.TempDir(), "lab")
	opt := testOptions()
	opt.RollupMax = 16
	runBackup(t, src, serial, opt)
	opt.Workers = 8
	l := runBackup(t, src, parallel, opt)
	assert.Equal(t, int64(0), l.Stats.Errors)
	assert.Equal(t, indexRows(t, serial), indexRows(t, parallel))

	// Packs from different workers have different names.
	workers := map[string]bool{}
	for _, o := range objects(t, parallel) {
		if strings.HasSuffix(o, ".tar") {
			parts := strings.Split(filepath.Base(o), ".")
			workers[parts[len(parts)-3]] = true
		}
	}
	assert.Greater(t, len(workers), 1)

	target := t.TempDir()
	st, err := StartRestore(context.Background(), newDst(t, parallel), target, DefaultRestoreOptions())
	require.NoError(t, err)
	assert.Equal(t, StateDone, st.State)
	assertSameTree(t, src, target)

	// An incremental parallel run changes only what changed.
	writeFile(t, src, "d07/f1.dat", 99)
	writeFile(t, src, "d19/new.dat", 42)
	l2 := runBackup(t, src, parallel, opt)
	assert.Equal(t, int64(0), l2.Stats.Errors)
	assert.Equal(t, int64(1), l2.Stats.Modified)
	var added []string
	for name, e := range readIndexFile(t, parallel, "d19") {
		if e.Run == l2.RunID {
			added = append(added, name)
		}
	}
	sort.Strings(added)
	assert.Equal(t, []string{"new.dat"}, added)
	target2 := t.TempDir()
	_, err = StartRestore(context.Background(), newDst(t, parallel), target2, DefaultRestoreOptions())
	require.NoError(t, err)
	assertSameTree(t, src, target2)
}

func TestWorkQueue(t *testing.T) {
	q := newWorkQueue()
	q.push(childDir{rel: "a"})
	item, ok := q.pop()
	require.True(t, ok)
	assert.Equal(t, "a", item.rel)
	q.push(childDir{rel: "b"})
	q.done()
	item, ok = q.pop()
	require.True(t, ok)
	assert.Equal(t, "b", item.rel)
	q.done()
	_, ok = q.pop()
	assert.False(t, ok)
}

func TestBackupPartitioned(t *testing.T) {
	fakeClock(t)
	src := parallelTree(t)
	serial, split := filepath.Join(t.TempDir(), "lab"), filepath.Join(t.TempDir(), "lab")
	opt := testOptions()
	opt.RollupMax = 16
	runBackup(t, src, serial, opt)

	f := newDst(t, split)
	runID, err := Plan(context.Background(), src, f, opt, 4)
	require.NoError(t, err)
	// The plan holds the lock for the whole run.
	_, err = Backup(context.Background(), src, f, opt)
	assert.ErrorContains(t, err, "locked by run "+runID)

	// Four workers, as a job array would run them; the fourth crashes
	// after starting.
	workerOpt := func(i int) Options {
		wopt := opt
		wopt.Worker = fmt.Sprintf("p%03d", i)
		wopt.Workers = 2
		return wopt
	}
	errs := make(chan error, 3)
	for i := range 3 {
		go func() {
			_, err := BackupPartition(context.Background(), src, f, workerOpt(i), runID, i)
			errs <- err
		}()
	}
	for range 3 {
		require.NoError(t, <-errs)
	}
	three := 3
	crashed, err := json.Marshal(Ledger{RunID: runID, Worker: "p003", Partition: &three})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(split, MetaDir, "runs", runID, "p003.json"), crashed, 0o644))

	// The run can't finish until every partition has.
	_, err = FinishRun(context.Background(), f, runID, opt)
	assert.ErrorContains(t, err, "partition 3 of run "+runID+", worker p003, hasn't finished")
	assert.FileExists(t, filepath.Join(split, MetaDir, "lock"))

	// Each worker ID and partition belongs to one worker.
	_, err = BackupPartition(context.Background(), src, f, workerOpt(0), runID, 0)
	assert.ErrorContains(t, err, "has finished already")
	_, err = BackupPartition(context.Background(), src, f, workerOpt(0), runID, 3)
	assert.ErrorContains(t, err, "used by another partition")
	_, err = BackupPartition(context.Background(), src, f, workerOpt(9), runID, 1)
	assert.ErrorContains(t, err, `was started by worker "p001"`)
	reserved := opt
	reserved.Worker = "plan"
	_, err = BackupPartition(context.Background(), src, f, reserved, runID, 3)
	assert.ErrorContains(t, err, "reserved")

	// The crashed partition is run again with its worker ID.
	_, err = BackupPartition(context.Background(), src, f, workerOpt(3), runID, 3)
	require.NoError(t, err)

	merged, err := FinishRun(context.Background(), f, runID, opt)
	require.NoError(t, err)
	assert.Equal(t, int64(0), merged.Stats.Errors)
	assert.NoFileExists(t, filepath.Join(split, MetaDir, "lock"))
	assert.Equal(t, indexRows(t, serial), indexRows(t, split))

	target := t.TempDir()
	st, err := StartRestore(context.Background(), f, target, DefaultRestoreOptions())
	require.NoError(t, err)
	assert.Equal(t, StateDone, st.State)
	assertSameTree(t, src, target)

	// The partitions really were split.
	parts, err := os.ReadFile(filepath.Join(split, MetaDir, "runs", runID, "plan.csv"))
	require.NoError(t, err)
	assert.Contains(t, string(parts), ",dir,")
	assert.Greater(t, strings.Count(string(parts), "\n"), 10)
}

func TestBackupPartitionRetry(t *testing.T) {
	fakeClock(t)
	src := parallelTree(t)
	dst := filepath.Join(t.TempDir(), "lab")
	opt := testOptions()
	opt.RollupMax = 16
	f := newDst(t, dst)
	runID, err := Plan(context.Background(), src, f, opt, 1)
	require.NoError(t, err)
	wopt := opt
	wopt.Worker = "p0"
	_, err = BackupPartition(context.Background(), src, f, wopt, runID, 0)
	require.NoError(t, err)

	// The partition is recorded as having failed after committing, and
	// a file changes before it is run again.
	ledgerPath := filepath.Join(dst, MetaDir, "runs", runID, "p0.json")
	data, err := os.ReadFile(ledgerPath)
	require.NoError(t, err)
	var l Ledger
	require.NoError(t, json.Unmarshal(data, &l))
	l.Stats.Errors = 1
	data, err = json.Marshal(l)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(ledgerPath, data, 0o644))
	before := readIndexFile(t, dst, "d07")
	writeFile(t, src, "d07/f1.dat", 99)

	l2, err := BackupPartition(context.Background(), src, f, wopt, runID, 0)
	require.NoError(t, err)
	assert.Equal(t, 2, l2.Attempt)
	assert.Equal(t, "p0", l2.Worker)
	_, err = FinishRun(context.Background(), f, runID, opt)
	require.NoError(t, err)

	// The retry wrote under new names, so what the first attempt
	// committed is intact, and history agrees with the index.
	index := readIndexFile(t, dst, "d07")
	assert.Equal(t, before["f0.dat"], index["f0.dat"])
	assert.Contains(t, index["f1.dat"].Location, ".p0-r2.")
	checkStored(t, dst, "d07", index)
	listed, err := List(context.Background(), f, "d07", runID)
	require.NoError(t, err)
	assert.Len(t, listed, len(index))
	target := t.TempDir()
	st, err := StartRestore(context.Background(), f, target, DefaultRestoreOptions())
	require.NoError(t, err)
	assert.Equal(t, StateDone, st.State)
	assertSameTree(t, src, target)

	// Worker IDs can't hold the separator of generated suffixes.
	bad := opt
	bad.Worker = "p0-r2"
	_, err = BackupPartition(context.Background(), src, f, bad, runID, 0)
	assert.ErrorContains(t, err, "invalid worker ID")
}

// smallStreams makes directories of more than a few entries be
// committed in chunks.
func smallStreams(t *testing.T, min, chunk int) {
	oldMin, oldChunk := streamMin, streamChunk
	streamMin, streamChunk = min, chunk
	t.Cleanup(func() { streamMin, streamChunk = oldMin, oldChunk })
}

func TestBackupStreamed(t *testing.T) {
	fakeClock(t)
	src := parallelTree(t)
	for i := range 12 {
		writeFile(t, src, fmt.Sprintf("big/f%02d.dat", i), 100+i)
	}
	writeFile(t, src, "big/huge.bin", 9000)
	writeFile(t, src, "big/sub/x.txt", 5)
	serial, streamed := filepath.Join(t.TempDir(), "lab"), filepath.Join(t.TempDir(), "lab")
	opt := testOptions()
	opt.RollupMax = 16
	runBackup(t, src, serial, opt)
	func() {
		smallStreams(t, 5, 3)
		old := maxIndexRows
		maxIndexRows = 4
		defer func() { maxIndexRows = old }()
		runBackup(t, src, streamed, opt)

		// Changes, deletions and additions across chunks.
		writeFile(t, src, "big/f03.dat", 7)
		require.NoError(t, os.Remove(filepath.Join(src, "big/f07.dat")))
		writeFile(t, src, "big/f99.dat", 9)
		writeFile(t, src, "big/a00.dat", 9)
		l := runBackup(t, src, streamed, opt)
		assert.Equal(t, int64(0), l.Stats.Errors)
		assert.Equal(t, int64(2), l.Stats.Added)
		assert.Equal(t, int64(1), l.Stats.Modified)
		assert.Equal(t, int64(1), l.Stats.Deleted)
		l = runBackup(t, src, streamed, opt)
		assert.Zero(t, l.Stats.Added+l.Stats.Modified+l.Stats.Deleted+l.Stats.MetaOnly)
	}()
	serial2 := filepath.Join(t.TempDir(), "lab")
	runBackup(t, src, serial2, opt)
	target := t.TempDir()
	st, err := StartRestore(context.Background(), newDst(t, streamed), target, DefaultRestoreOptions())
	require.NoError(t, err)
	assert.Equal(t, StateDone, st.State)
	assertSameTree(t, src, target)
	entries, err := List(context.Background(), newDst(t, streamed), "big", "")
	require.NoError(t, err)
	assert.Len(t, entries, 15)
}
