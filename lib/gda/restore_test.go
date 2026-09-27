//go:build unix

package gda

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// makeTree creates a source tree with packed, standalone and rolled up
// files, a symlink, a FIFO and varied permissions and times.
func makeTree(t *testing.T) string {
	t.Helper()
	src := t.TempDir()
	writeFile(t, src, "README.txt", 10)
	writeFile(t, src, "results/a.dat", 3000)
	writeFile(t, src, "results/big.bin", 9000)
	writeFile(t, src, "results/sub/deep/c.dat", 100)
	writeFile(t, src, "tiny/a/b.txt", 5)
	require.NoError(t, os.Symlink("../README.txt", filepath.Join(src, "results/link")))
	require.NoError(t, syscall.Mkfifo(filepath.Join(src, "results/fifo"), 0o640))
	require.NoError(t, os.Chmod(filepath.Join(src, "results/a.dat"), 0o600))
	require.NoError(t, os.Chmod(filepath.Join(src, "tiny/a"), 0o750))
	setMtime(t, src, "results/a.dat", time.Hour)
	setMtime(t, src, "results", 2*time.Hour)
	setMtime(t, src, "tiny/a", 3*time.Hour)
	return src
}

func restoreOptions(paths ...string) RestoreOptions {
	opt := DefaultRestoreOptions()
	opt.Paths = paths
	return opt
}

func newDst(t *testing.T, dst string) fs.Fs {
	t.Helper()
	f, err := fs.NewFs(context.Background(), dst)
	require.NoError(t, err)
	return f
}

// assertSameTree checks that every entry below src is at the same path
// below dst with the same type, content, permissions and times.
func assertSameTree(t *testing.T, src, dst string) {
	t.Helper()
	err := filepath.Walk(src, func(p string, info os.FileInfo, err error) error {
		require.NoError(t, err)
		rel, _ := filepath.Rel(src, p)
		if rel == "." {
			return nil
		}
		got, err := os.Lstat(filepath.Join(dst, rel))
		require.NoError(t, err, rel)
		assert.Equal(t, info.Mode(), got.Mode(), rel)
		assert.True(t, info.ModTime().Equal(got.ModTime()), "%s: mtime %v, want %v", rel, got.ModTime(), info.ModTime())
		switch {
		case info.Mode().IsRegular():
			want, _ := os.ReadFile(p)
			have, _ := os.ReadFile(filepath.Join(dst, rel))
			assert.Equal(t, want, have, rel)
		case info.Mode()&os.ModeSymlink != 0:
			want, _ := os.Readlink(p)
			have, _ := os.Readlink(filepath.Join(dst, rel))
			assert.Equal(t, want, have, rel)
		}
		return nil
	})
	require.NoError(t, err)
}

func TestRestoreRoundTrip(t *testing.T) {
	fakeClock(t)
	src := makeTree(t)
	dst := filepath.Join(t.TempDir(), "lab")
	opt := testOptions()
	runBackup(t, src, dst, opt)
	f := newDst(t, dst)
	target := filepath.Join(t.TempDir(), "restore")

	st, err := StartRestore(context.Background(), f, target, restoreOptions())
	require.NoError(t, err)
	assert.Equal(t, StateDone, st.State)
	assert.Equal(t, 0, st.Files.Failed)
	assert.Equal(t, st.Files.Total, st.Files.Fetched)
	assertSameTree(t, src, target)

	// Restoring again finds everything in place and fetches nothing.
	st, err = StartRestore(context.Background(), f, target, restoreOptions())
	require.NoError(t, err)
	assert.Equal(t, st.Files.Total, st.Files.SkippedIdentical)
	assert.Equal(t, 0, st.Objects.Requested)

	// A local file that differs stops the restore unless overwriting.
	require.NoError(t, os.WriteFile(filepath.Join(target, "README.txt"), []byte("changed"), 0o644))
	_, err = StartRestore(context.Background(), f, target, restoreOptions())
	assert.ErrorContains(t, err, "README.txt")
	have, _ := os.ReadFile(filepath.Join(target, "README.txt"))
	assert.Equal(t, "changed", string(have))
	overwrite := restoreOptions()
	overwrite.Overwrite = true
	st, err = StartRestore(context.Background(), f, target, overwrite)
	require.NoError(t, err)
	assert.Equal(t, StateDone, st.State)
	assertSameTree(t, src, target)
}

func TestRestorePaths(t *testing.T) {
	fakeClock(t)
	src := makeTree(t)
	dst := filepath.Join(t.TempDir(), "lab")
	runBackup(t, src, dst, testOptions())
	f := newDst(t, dst)
	target := t.TempDir()

	// A packed file, a standalone file, a directory with its own index
	// and a directory inside a rollup.
	st, err := StartRestore(context.Background(), f, target, restoreOptions("results/a.dat", "results/big.bin", "results/sub", "tiny/a"))
	require.NoError(t, err)
	assert.Equal(t, StateDone, st.State)
	for _, rel := range []string{"results/a.dat", "results/big.bin", "results/sub/deep/c.dat", "tiny/a/b.txt"} {
		want, _ := os.ReadFile(filepath.Join(src, rel))
		have, err := os.ReadFile(filepath.Join(target, rel))
		require.NoError(t, err, rel)
		assert.Equal(t, want, have, rel)
	}
	assert.NoFileExists(t, filepath.Join(target, "README.txt"))
	assert.NoFileExists(t, filepath.Join(target, "results/link"))
	info, err := os.Stat(filepath.Join(target, "tiny/a"))
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o750), info.Mode().Perm())

	_, err = StartRestore(context.Background(), f, target, restoreOptions("results/missing"))
	assert.True(t, errors.Is(err, errNotFound), "%v", err)
}

func TestRestoreResume(t *testing.T) {
	fakeClock(t)
	src := makeTree(t)
	dst := filepath.Join(t.TempDir(), "lab")
	runBackup(t, src, dst, testOptions())
	f := newDst(t, dst)
	target := t.TempDir()

	st, err := StartRestore(context.Background(), f, target, restoreOptions("results"))
	require.NoError(t, err)
	assert.FileExists(t, filepath.Join(dst, MetaDir, "restores", st.RestoreID+".csv"))
	assert.FileExists(t, filepath.Join(dst, MetaDir, "restores", st.RestoreID+".json"))

	// Resuming into a new target fetches the whole plan there.
	other := t.TempDir()
	st2, err := ResumeRestore(context.Background(), f, st.RestoreID, other, false)
	require.NoError(t, err)
	assert.Equal(t, StateDone, st2.State)
	assert.Equal(t, st.Files.Total, st2.Files.Fetched)
	assertSameTree(t, filepath.Join(src, "results"), filepath.Join(other, "results"))

	_, err = ResumeRestore(context.Background(), f, "20260101T000000Z-0000", other, false)
	assert.Error(t, err)
}

func TestRestoreAt(t *testing.T) {
	fakeClock(t)
	src := makeTree(t)
	dst := filepath.Join(t.TempDir(), "lab")
	opt := testOptions()
	l1 := runBackup(t, src, dst, opt)
	old, err := os.ReadFile(filepath.Join(src, "results/a.dat"))
	require.NoError(t, err)
	writeFile(t, src, "results/a.dat", 500)
	writeFile(t, src, "results/new.dat", 50)
	require.NoError(t, os.Remove(filepath.Join(src, "README.txt")))
	runBackup(t, src, dst, opt)
	f := newDst(t, dst)

	// The latest indexes show the second run.
	now, err := List(context.Background(), f, "results", "")
	require.NoError(t, err)
	names := map[string]bool{}
	for _, e := range now {
		names[e.Path] = true
	}
	assert.True(t, names["new.dat"])

	// As of the first run, new.dat didn't exist and README.txt did.
	then, err := List(context.Background(), f, "results", l1.RunID)
	require.NoError(t, err)
	names = map[string]bool{}
	for _, e := range then {
		names[e.Path] = true
	}
	assert.False(t, names["new.dat"])
	root, err := List(context.Background(), f, "", l1.RunID)
	require.NoError(t, err)
	found := false
	for _, e := range root {
		found = found || e.Path == "README.txt"
	}
	assert.True(t, found)

	target := t.TempDir()
	at := restoreOptions("results/a.dat", "README.txt")
	at.At = l1.RunID
	st, err := StartRestore(context.Background(), f, target, at)
	require.NoError(t, err)
	assert.Equal(t, StateDone, st.State)
	have, err := os.ReadFile(filepath.Join(target, "results/a.dat"))
	require.NoError(t, err)
	assert.Equal(t, old, have)
	assert.FileExists(t, filepath.Join(target, "README.txt"))
}

func TestListRollup(t *testing.T) {
	fakeClock(t)
	src := makeTree(t)
	dst := filepath.Join(t.TempDir(), "lab")
	runBackup(t, src, dst, testOptions())
	f := newDst(t, dst)

	entries, err := List(context.Background(), f, "tiny/a", "")
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "b.txt", entries[0].Path)
	assert.Equal(t, "tiny", entries[0].IndexKey)

	// Listing a file lists just that file.
	entries, err = List(context.Background(), f, "results/a.dat", "")
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "a.dat", entries[0].Path)
}

func TestParseAt(t *testing.T) {
	got, err := ParseAt("")
	require.NoError(t, err)
	assert.Equal(t, "", got)
	got, err = ParseAt("20260926T120000Z")
	require.NoError(t, err)
	assert.Equal(t, "20260926T120000Z", got)
	got, err = ParseAt("2026-09-26T05:00:00-07:00")
	require.NoError(t, err)
	assert.Equal(t, "20260926T120000Z", got)
	_, err = ParseAt("yesterday")
	assert.Error(t, err)
}
