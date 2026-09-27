//go:build unix

package gda

import (
	"context"
	"fmt"
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
