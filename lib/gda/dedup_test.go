//go:build unix

package gda

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func copyFile(t *testing.T, root, from, to string) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, from))
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Dir(filepath.Join(root, to)), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(root, to), data, 0o644))
}

func TestBackupDedup(t *testing.T) {
	fakeClock(t)
	src, dst := t.TempDir(), filepath.Join(t.TempDir(), "lab")
	opt := testOptions()
	opt.RollupMax = 0
	opt.DedupMin = 1000
	writeFile(t, src, "a/data.bin", 3000)
	writeFile(t, src, "a/big.bin", 9000)
	writeFile(t, src, "a/small.txt", 50)
	runBackup(t, src, dst, opt)

	// Copies of stored content are recorded without storing them again,
	// whether the copy was packed or standalone. Small files aren't
	// deduplicated.
	copyFile(t, src, "a/data.bin", "b/copy.bin")
	copyFile(t, src, "a/big.bin", "b/bigcopy.bin")
	copyFile(t, src, "a/small.txt", "b/small.txt")
	before := objects(t, dst)
	l := runBackup(t, src, dst, opt)
	assert.Equal(t, int64(2), l.Stats.Deduplicated)
	assert.Equal(t, int64(0), l.Stats.Standalone)
	assert.Equal(t, int64(1), l.Stats.Packs) // small.txt
	b := readIndexFile(t, dst, "b")
	a := readIndexFile(t, dst, "a")
	assert.Equal(t, "../a/"+a["data.bin"].Location, b["copy.bin"].DedupOf)
	assert.Equal(t, a["data.bin"].Offset, b["copy.bin"].Offset)
	assert.Equal(t, "../a/big.bin", b["bigcopy.bin"].DedupOf)
	assert.Equal(t, "", b["copy.bin"].Location)
	assert.Equal(t, "", b["small.txt"].DedupOf)
	var newData []string
	for _, o := range objects(t, dst) {
		if filepath.Ext(o) != ".csv" && !contains(before, o) {
			newData = append(newData, o)
		}
	}
	assert.Len(t, newData, 1)

	// A renamed directory isn't uploaded again.
	require.NoError(t, os.Rename(filepath.Join(src, "a"), filepath.Join(src, "renamed")))
	l = runBackup(t, src, dst, opt)
	assert.Equal(t, int64(2), l.Stats.Deduplicated)
	assert.Equal(t, int64(0), l.Stats.Standalone)

	// Restore and browsing follow the references.
	target := t.TempDir()
	st, err := StartRestore(context.Background(), newDst(t, dst), target, DefaultRestoreOptions())
	require.NoError(t, err)
	assert.Equal(t, StateDone, st.State)
	assertSameTree(t, src, target)
	br, err := NewBrowser(newDst(t, dst), "")
	require.NoError(t, err)
	entries, _, err := br.List(context.Background(), "b")
	require.NoError(t, err)
	for i := range entries {
		l := &entries[i]
		in, err := br.Open(context.Background(), l)
		require.NoError(t, err)
		got, err := io.ReadAll(in)
		require.NoError(t, err)
		require.NoError(t, in.Close())
		want, _ := os.ReadFile(filepath.Join(src, "b", l.LocalPath))
		assert.Equal(t, want, got, l.Path)
	}
}

func TestBackupDedupCompressed(t *testing.T) {
	fakeClock(t)
	smallFrames(t)
	src, dst := t.TempDir(), filepath.Join(t.TempDir(), "lab")
	opt := compressOptions()
	opt.DedupMin = 1000
	writeText(t, src, "a/reads.fastq", 50000)
	writeText(t, src, "a/one.txt", 10000)
	runBackup(t, src, dst, opt)
	copyFile(t, src, "a/reads.fastq", "b/reads.fastq")
	copyFile(t, src, "a/one.txt", "b/one.txt")
	l := runBackup(t, src, dst, opt)
	assert.Equal(t, int64(2), l.Stats.Deduplicated)
	target := t.TempDir()
	st, err := StartRestore(context.Background(), newDst(t, dst), target, restoreOptions("b"))
	require.NoError(t, err)
	assert.Equal(t, StateDone, st.State)
	assertSameTree(t, filepath.Join(src, "b"), filepath.Join(target, "b"))
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

func TestRelKey(t *testing.T) {
	for _, test := range []struct{ from, to, want string }{
		{"", "a/x.tar", "a/x.tar"},
		{"b", "a/x.tar", "../a/x.tar"},
		{"a", "a/x.tar", "x.tar"},
		{"a/b/c", "a/d/x.tar", "../../d/x.tar"},
		{"a/b", "x.tar", "../../x.tar"},
	} {
		got := relKey(test.from, test.to)
		assert.Equal(t, test.want, got, "%+v", test)
		l := Located{IndexKey: test.from, Entry: Entry{DedupOf: got}}
		assert.Equal(t, test.to, l.ObjectKey())
		// And from above the tree's root.
		l = Located{IndexKey: joinRemote("bucket/lab", test.from), Entry: Entry{DedupOf: got}}
		assert.Equal(t, "bucket/lab/"+test.to, l.ObjectKey())
	}
}
