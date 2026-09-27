//go:build unix

package gda

import (
	"archive/tar"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	_ "github.com/rclone/rclone/backend/local"
	"github.com/rclone/rclone/fs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeClock makes each run start a minute after the previous one.
func fakeClock(t *testing.T) {
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	old := timeNow
	timeNow = func() time.Time {
		now = now.Add(time.Minute)
		return now
	}
	t.Cleanup(func() { timeNow = old })
}

func testOptions() Options {
	opt := DefaultOptions()
	opt.PackSize = 10 * 1024
	opt.StandaloneMin = 8 * 1024
	opt.RollupMax = 64
	opt.TempDir = os.TempDir()
	return opt
}

func writeFile(t *testing.T, root, rel string, size int) {
	t.Helper()
	p := filepath.Join(root, filepath.FromSlash(rel))
	require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(len(rel) + i)
	}
	require.NoError(t, os.WriteFile(p, data, 0o644))
}

// setMtime sets a file's modification time a fixed distance from a base
// so the test doesn't depend on file system timestamp resolution.
func setMtime(t *testing.T, root, rel string, offset time.Duration) {
	t.Helper()
	mtime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).Add(offset)
	require.NoError(t, os.Chtimes(filepath.Join(root, filepath.FromSlash(rel)), mtime, mtime))
}

func runBackup(t *testing.T, src, dst string, opt Options) *Ledger {
	t.Helper()
	f, err := fs.NewFs(context.Background(), dst)
	require.NoError(t, err)
	ledger, err := Backup(context.Background(), src, f, opt)
	require.NoError(t, err)
	return ledger
}

// objects returns the files under dst, excluding the run ledgers.
func objects(t *testing.T, dst string) []string {
	t.Helper()
	var out []string
	err := filepath.Walk(dst, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(dst, p)
		if !info.IsDir() && !strings.HasPrefix(rel, MetaDir+"/") {
			out = append(out, filepath.ToSlash(rel))
		}
		return nil
	})
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	require.NoError(t, err)
	sort.Strings(out)
	return out
}

func readCSV(t *testing.T, p string) map[string]Entry {
	t.Helper()
	in, err := os.Open(p)
	require.NoError(t, err)
	defer func() { require.NoError(t, in.Close()) }()
	entries, err := ReadEntries(in)
	require.NoError(t, err)
	out := map[string]Entry{}
	for _, e := range entries {
		out[e.Name] = e
	}
	return out
}

func readIndexFile(t *testing.T, dst, dir string) map[string]Entry {
	t.Helper()
	return readCSV(t, filepath.Join(dst, filepath.FromSlash(dir), IndexName))
}

// checkStored reads the data of the file entries in the index of dir
// through their location and offset and checks it against their MD5.
func checkStored(t *testing.T, dst, dir string, index map[string]Entry) {
	t.Helper()
	for name, e := range index {
		if e.Type != TypeFile {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dst, filepath.FromSlash(dir), e.Location))
		require.NoError(t, err, name)
		if e.Offset >= 0 {
			data = data[e.Offset : e.Offset+e.Size]
		}
		sum := md5.Sum(data)
		assert.Equal(t, e.MD5, hex.EncodeToString(sum[:]), name)
		assert.Equal(t, e.Size, int64(len(data)), name)
	}
}

func tarNames(t *testing.T, p string) []string {
	t.Helper()
	in, err := os.Open(p)
	require.NoError(t, err)
	defer func() { require.NoError(t, in.Close()) }()
	var names []string
	tr := tar.NewReader(in)
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return names
		}
		require.NoError(t, err)
		names = append(names, hdr.Name)
	}
}

func TestBackupIncremental(t *testing.T) {
	fakeClock(t)
	src, dst := t.TempDir(), filepath.Join(t.TempDir(), "lab")
	opt := testOptions()
	writeFile(t, src, "README.txt", 10)
	writeFile(t, src, "results/a.dat", 3000)
	writeFile(t, src, "results/b.dat", 3000)
	writeFile(t, src, "results/c.dat", 3000)
	writeFile(t, src, "results/big.bin", 9000)
	require.NoError(t, os.Symlink("../README.txt", filepath.Join(src, "results/link")))

	// First run: everything is added.
	l1 := runBackup(t, src, dst, opt)
	assert.Equal(t, int64(0), l1.Stats.Errors)
	assert.Equal(t, int64(1), l1.Stats.Standalone)
	results := readIndexFile(t, dst, "results")
	assert.Len(t, results, 5)
	assert.Equal(t, "big.bin", results["big.bin"].Location)
	assert.Equal(t, "../README.txt", results["link"].LinkTarget)
	checkStored(t, dst, "results", results)
	checkStored(t, dst, "", readIndexFile(t, dst, ""))
	root := readIndexFile(t, dst, "")
	assert.Equal(t, ListingIndex, root["results"].Listing)
	assert.Equal(t, int64(18000), root["results"].TreeSize)
	assert.Equal(t, int64(5), root["results"].TreeFiles)

	// Packs respect the pack size and end with their own manifest.
	var packs []string
	for _, o := range objects(t, dst) {
		if strings.HasPrefix(o, "results/") && strings.HasSuffix(o, ".tar") {
			packs = append(packs, o)
		}
	}
	require.GreaterOrEqual(t, len(packs), 2)
	members := 0
	for _, p := range packs {
		info, err := os.Stat(filepath.Join(dst, p))
		require.NoError(t, err)
		assert.LessOrEqual(t, info.Size(), opt.PackSize)
		names := tarNames(t, filepath.Join(dst, p))
		assert.Equal(t, filepath.Base(p)+".csv", names[len(names)-1])
		members += len(names) - 1
	}
	assert.Equal(t, 4, members)

	// Second run with no changes writes nothing but its ledger.
	before := objects(t, dst)
	l2 := runBackup(t, src, dst, opt)
	assert.Equal(t, int64(6), l2.Stats.Unchanged+l2.Stats.Added)
	assert.Equal(t, int64(0), l2.Stats.Added+l2.Stats.Modified+l2.Stats.MetaOnly+l2.Stats.Deleted)
	assert.Equal(t, before, objects(t, dst))

	// A modification time change without a content change uploads nothing.
	setMtime(t, src, "results/a.dat", time.Hour)
	l3 := runBackup(t, src, dst, opt)
	assert.Equal(t, int64(1), l3.Stats.MetaOnly)
	assert.Equal(t, int64(0), l3.Stats.Packs)
	results3 := readIndexFile(t, dst, "results")
	assert.Equal(t, results["a.dat"].Location, results3["a.dat"].Location)
	changes := readCSV(t, filepath.Join(dst, "results", changesetName("results", l3.RunID)))
	assert.Equal(t, ActionMeta, changes["a.dat"].Action)

	// Changing two small files puts just those in a new pack; the old
	// packs are left alone.
	writeFile(t, src, "results/b.dat", 101)
	writeFile(t, src, "results/c.dat", 102)
	l4 := runBackup(t, src, dst, opt)
	assert.Equal(t, int64(2), l4.Stats.Modified)
	assert.Equal(t, int64(1), l4.Stats.Packs)
	results4 := readIndexFile(t, dst, "results")
	newPack := packName("results", l4.RunID, opt.Worker, 1)
	assert.Equal(t, newPack, results4["b.dat"].Location)
	assert.Equal(t, newPack, results4["c.dat"].Location)
	assert.Equal(t, results["a.dat"].Location, results4["a.dat"].Location)
	for _, p := range packs {
		assert.FileExists(t, filepath.Join(dst, p))
	}
	checkStored(t, dst, "results", results4)

	// A changed standalone file gets a new key; the old object stays.
	writeFile(t, src, "results/big.bin", 9001)
	l5 := runBackup(t, src, dst, opt)
	results5 := readIndexFile(t, dst, "results")
	assert.Equal(t, versionedName("big.bin", l5.RunID), results5["big.bin"].Location)
	assert.FileExists(t, filepath.Join(dst, "results", "big.bin"))
	checkStored(t, dst, "results", results5)

	// Deletes are recorded but nothing is deleted from the destination.
	before = objects(t, dst)
	require.NoError(t, os.Remove(filepath.Join(src, "results/a.dat")))
	l6 := runBackup(t, src, dst, opt)
	assert.Equal(t, int64(1), l6.Stats.Deleted)
	assert.NotContains(t, readIndexFile(t, dst, "results"), "a.dat")
	changes = readCSV(t, filepath.Join(dst, "results", changesetName("results", l6.RunID)))
	assert.Equal(t, ActionDelete, changes["a.dat"].Action)
	for _, o := range before {
		assert.FileExists(t, filepath.Join(dst, o))
	}
}

func TestBackupRollup(t *testing.T) {
	fakeClock(t)
	src, dst := t.TempDir(), filepath.Join(t.TempDir(), "lab")
	opt := testOptions()
	writeFile(t, src, "big/data.dat", 100)
	writeFile(t, src, "tiny/a/b/deep.txt", 5)
	writeFile(t, src, "tiny/a/y.txt", 5)
	require.NoError(t, os.MkdirAll(filepath.Join(src, "tiny/empty"), 0o755))

	l1 := runBackup(t, src, dst, opt)
	assert.Equal(t, int64(0), l1.Stats.Errors)
	tiny := readIndexFile(t, dst, "tiny")
	assert.Equal(t, ListingRollup, tiny["a"].Listing)
	assert.Equal(t, ListingRollup, tiny["a/b"].Listing)
	assert.Equal(t, ListingRollup, tiny["empty"].Listing)
	assert.Contains(t, tiny, "a/b/deep.txt")
	checkStored(t, dst, "tiny", tiny)
	assert.NoFileExists(t, filepath.Join(dst, "tiny/a", IndexName))
	var packed []string
	for _, o := range objects(t, dst) {
		if strings.HasPrefix(o, "tiny/") && strings.HasSuffix(o, ".tar") {
			packed = append(packed, tarNames(t, filepath.Join(dst, o))...)
		}
	}
	assert.Contains(t, packed, "empty/")
	assert.Contains(t, packed, "a/b/deep.txt")

	// When the subtree grows past the rollup limit, its directories get
	// their own indexes and the rolled up rows are recorded as deleted.
	writeFile(t, src, "tiny/a/grown.dat", 100)
	l2 := runBackup(t, src, dst, opt)
	assert.Equal(t, int64(0), l2.Stats.Errors)
	tiny = readIndexFile(t, dst, "tiny")
	assert.Equal(t, ListingIndex, tiny["a"].Listing)
	assert.NotContains(t, tiny, "a/y.txt")
	a := readIndexFile(t, dst, "tiny/a")
	assert.Contains(t, a, "y.txt")
	assert.Contains(t, a, "grown.dat")
	checkStored(t, dst, "tiny/a", a)
	changes := readCSV(t, filepath.Join(dst, "tiny", changesetName("tiny", l2.RunID)))
	assert.Equal(t, ActionDelete, changes["a/y.txt"].Action)

	// When it shrinks again, the directories' own indexes are retired.
	require.NoError(t, os.Remove(filepath.Join(src, "tiny/a/grown.dat")))
	l3 := runBackup(t, src, dst, opt)
	assert.Equal(t, int64(0), l3.Stats.Errors)
	assert.Equal(t, ListingRollup, readIndexFile(t, dst, "tiny")["a"].Listing)
	assert.Empty(t, readIndexFile(t, dst, "tiny/a"))
	assert.Empty(t, readIndexFile(t, dst, "tiny/a/b"))
}

func TestBackupDeletedDirectory(t *testing.T) {
	fakeClock(t)
	src, dst := t.TempDir(), filepath.Join(t.TempDir(), "lab")
	opt := testOptions()
	opt.RollupMax = 0
	writeFile(t, src, "keep.txt", 5)
	writeFile(t, src, "gone/sub/file.txt", 5)
	runBackup(t, src, dst, opt)
	assert.NotEmpty(t, readIndexFile(t, dst, "gone/sub"))

	require.NoError(t, os.RemoveAll(filepath.Join(src, "gone")))
	l2 := runBackup(t, src, dst, opt)
	assert.Equal(t, int64(0), l2.Stats.Errors)
	assert.NotContains(t, readIndexFile(t, dst, ""), "gone")
	assert.Empty(t, readIndexFile(t, dst, "gone"))
	assert.Empty(t, readIndexFile(t, dst, "gone/sub"))
	changes := readCSV(t, filepath.Join(dst, "gone/sub", changesetName("sub", l2.RunID)))
	assert.Equal(t, ActionDelete, changes["file.txt"].Action)
}

func TestBackupSpecialFiles(t *testing.T) {
	fakeClock(t)
	src, dst := t.TempDir(), filepath.Join(t.TempDir(), "lab")
	opt := testOptions()
	opt.RollupMax = 0
	writeFile(t, src, "file.txt", 5)
	require.NoError(t, syscall.Mkfifo(filepath.Join(src, "fifo"), 0o600))
	require.NoError(t, os.Chmod(filepath.Join(src, "file.txt"), os.ModeSetuid|0o755))
	writeFile(t, src, IndexName, 5)
	writeFile(t, src, "x.gda.clash.csv", 5)

	l := runBackup(t, src, dst, opt)
	assert.Equal(t, int64(2), l.Stats.Skipped)
	root := readIndexFile(t, dst, "")
	assert.Equal(t, TypeFifo, root["fifo"].Type)
	assert.Equal(t, uint32(0o600), root["fifo"].Mode)
	assert.Equal(t, uint32(0o4755), root["file.txt"].Mode)
	assert.NotContains(t, root, "x.gda.clash.csv")
	assert.Contains(t, tarNames(t, filepath.Join(dst, root["fifo"].Location)), "fifo")
}

func TestBackupDryRun(t *testing.T) {
	fakeClock(t)
	src, dst := t.TempDir(), filepath.Join(t.TempDir(), "lab")
	writeFile(t, src, "a/file.txt", 5)
	ctx, ci := fs.AddConfig(context.Background())
	ci.DryRun = true
	f, err := fs.NewFs(ctx, dst)
	require.NoError(t, err)
	l, err := Backup(ctx, src, f, testOptions())
	require.NoError(t, err)
	assert.True(t, l.DryRun)
	assert.Equal(t, int64(2), l.Stats.Added) // the directory and the file
	assert.Equal(t, int64(1), l.Stats.Packs)
	assert.Empty(t, objects(t, dst))
	assert.NoDirExists(t, filepath.Join(dst, MetaDir))
}

func TestBackupLock(t *testing.T) {
	fakeClock(t)
	src, dst := t.TempDir(), filepath.Join(t.TempDir(), "lab")
	writeFile(t, src, "file.txt", 5)
	require.NoError(t, os.MkdirAll(filepath.Join(dst, MetaDir), 0o755))
	lock := `{"RunID":"20260926T000000Z","Host":"other","PID":1,"Started":"` + time.Now().UTC().Format(time.RFC3339) + `"}`
	require.NoError(t, os.WriteFile(filepath.Join(dst, MetaDir, "lock"), []byte(lock), 0o644))
	f, err := fs.NewFs(context.Background(), dst)
	require.NoError(t, err)

	_, err = Backup(context.Background(), src, f, testOptions())
	assert.ErrorContains(t, err, "locked by run 20260926T000000Z on other")

	opt := testOptions()
	opt.LockTimeout = time.Nanosecond
	_, err = Backup(context.Background(), src, f, opt)
	require.NoError(t, err)
	assert.NoFileExists(t, filepath.Join(dst, MetaDir, "lock"))
}

func TestBackupSplitIndex(t *testing.T) {
	fakeClock(t)
	src, dst := t.TempDir(), filepath.Join(t.TempDir(), "lab")
	f, err := fs.NewFs(context.Background(), dst)
	require.NoError(t, err)
	d := &dest{f: f, metaTier: "STANDARD", retries: 1}
	var entries []Entry
	for _, name := range []string{"c", "a", "b"} {
		entries = append(entries, NewEntry(name, TypeFile))
	}
	objects, err := encodeIndex(entries, 2)
	require.NoError(t, err)
	for name, data := range objects {
		require.NoError(t, d.putBytes(context.Background(), joinRemote("dir", name), data, d.metaTier))
	}
	got, err := d.readIndex(context.Background(), "dir")
	require.NoError(t, err)
	var names []string
	for _, e := range got {
		names = append(names, e.Name)
	}
	assert.Equal(t, []string{"a", "b", "c"}, names)
	_ = src
}
