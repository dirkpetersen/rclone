//go:build unix

package gda

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	gosync "sync"
	"testing"
	"time"

	_ "github.com/rclone/rclone/backend/local"
	_ "github.com/rclone/rclone/backend/memory"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/hash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// fakeClock makes each run start a minute after the previous one.
func fakeClock(t *testing.T) {
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	var mu gosync.Mutex
	old := timeNow
	timeNow = func() time.Time {
		mu.Lock()
		defer mu.Unlock()
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
	// Compression has its own tests; the test data compresses well.
	opt.Compression = CodecNone
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
	assert.Equal(t, int64(7), l2.Stats.Unchanged) // 2 rows in the root index and 5 in results
	assert.Equal(t, int64(0), l2.Stats.Added+l2.Stats.Modified+l2.Stats.MetaOnly+l2.Stats.Deleted)
	assert.Equal(t, before, objects(t, dst))

	// A modification time change without a content change uploads nothing.
	setMtime(t, src, "results/a.dat", time.Hour)
	l3 := runBackup(t, src, dst, opt)
	assert.Equal(t, int64(1), l3.Stats.MetaOnly)
	assert.Equal(t, int64(0), l3.Stats.Packs)
	results3 := readIndexFile(t, dst, "results")
	assert.Equal(t, results["a.dat"].Location, results3["a.dat"].Location)
	changes := readCSV(t, filepath.Join(dst, "results", changesetName("results", l3.RunID, "w01")))
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
	assert.Equal(t, versionedName("big.bin", l5.RunID, "w01"), results5["big.bin"].Location)
	assert.FileExists(t, filepath.Join(dst, "results", "big.bin"))
	checkStored(t, dst, "results", results5)

	// Deletes are recorded but nothing is deleted from the destination.
	before = objects(t, dst)
	require.NoError(t, os.Remove(filepath.Join(src, "results/a.dat")))
	l6 := runBackup(t, src, dst, opt)
	assert.Equal(t, int64(1), l6.Stats.Deleted)
	assert.NotContains(t, readIndexFile(t, dst, "results"), "a.dat")
	changes = readCSV(t, filepath.Join(dst, "results", changesetName("results", l6.RunID, "w01")))
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
	changes := readCSV(t, filepath.Join(dst, "tiny", changesetName("tiny", l2.RunID, "w01")))
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
	changes := readCSV(t, filepath.Join(dst, "gone/sub", changesetName("sub", l2.RunID, "w01")))
	assert.Equal(t, ActionDelete, changes["file.txt"].Action)
}

func TestBackupSpecialFiles(t *testing.T) {
	fakeClock(t)
	src, dst := t.TempDir(), filepath.Join(t.TempDir(), "lab")
	opt := testOptions()
	opt.RollupMax = 0
	writeFile(t, src, "file.txt", 5)
	require.NoError(t, unix.Mkfifo(filepath.Join(src, "fifo"), 0o600))
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

	// The holder's own timeout counts when it is longer.
	opt := testOptions()
	opt.LockTimeout = time.Nanosecond
	long := `{"RunID":"20260926T000000Z","Host":"other","PID":1,"Started":"` + time.Now().UTC().Format(time.RFC3339) + `","Timeout":3600000000000}`
	require.NoError(t, os.WriteFile(filepath.Join(dst, MetaDir, "lock"), []byte(long), 0o644))
	_, err = Backup(context.Background(), src, f, opt)
	assert.ErrorContains(t, err, "locked by run 20260926T000000Z on other")

	require.NoError(t, os.WriteFile(filepath.Join(dst, MetaDir, "lock"), []byte(lock), 0o644))
	_, err = Backup(context.Background(), src, f, opt)
	require.NoError(t, err)
	assert.NoFileExists(t, filepath.Join(dst, MetaDir, "lock"))
}

func TestBackupLockRefresh(t *testing.T) {
	fakeClock(t)
	old := lockRefreshEvery
	lockRefreshEvery = time.Millisecond
	t.Cleanup(func() { lockRefreshEvery = old })
	src, dst := t.TempDir(), filepath.Join(t.TempDir(), "lab")
	writeFile(t, src, "file.txt", 5)
	b, _, err := newBackup(context.Background(), src, newDst(t, dst), testOptions())
	require.NoError(t, err)
	defer b.close()
	require.NoError(t, b.lock(context.Background(), "here"))
	// The lock may be read while it is being written.
	tryReadLock := func() (held lockInfo, ok bool) {
		data, err := os.ReadFile(filepath.Join(dst, MetaDir, "lock"))
		return held, err == nil && json.Unmarshal(data, &held) == nil
	}
	readLock := func() lockInfo {
		held, ok := tryReadLock()
		require.True(t, ok)
		return held
	}

	// A long run keeps showing it is still going.
	done := b.keepLock(context.Background())
	require.Eventually(t, func() bool {
		held, ok := tryReadLock()
		return ok && !held.Refreshed.IsZero()
	}, 5*time.Second, time.Millisecond)
	done()
	held := readLock()
	assert.Equal(t, b.runID, held.RunID)
	assert.Less(t, held.age(), time.Minute)
	assert.False(t, b.isStopped())

	// A run whose lock was taken over stops.
	held.RunID = "20260926T000000Z"
	data, err := json.Marshal(held)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dst, MetaDir, "lock"), data, 0o644))
	done = b.keepLock(context.Background())
	require.Eventually(t, b.isStopped, 5*time.Second, time.Millisecond)
	done()
	assert.Equal(t, "20260926T000000Z", readLock().RunID)
	assert.Equal(t, int64(1), b.stats.Errors)
	// Nor does it remove the lock the other run holds.
	b.unlock(context.Background())
	assert.Equal(t, "20260926T000000Z", readLock().RunID)
}

func TestBackupSplitIndex(t *testing.T) {
	fakeClock(t)
	old := maxIndexRows
	maxIndexRows = 2
	t.Cleanup(func() { maxIndexRows = old })
	src, dst := t.TempDir(), filepath.Join(t.TempDir(), "lab")
	opt := testOptions()
	opt.RollupMax = 0
	for _, name := range []string{"a", "b", "c", "d", "e"} {
		writeFile(t, src, name, 5)
	}
	l1 := runBackup(t, src, dst, opt)
	toc, err := os.ReadFile(filepath.Join(dst, IndexName))
	require.NoError(t, err)
	assert.True(t, isTOC(toc))
	assert.FileExists(t, filepath.Join(dst, indexPartName(l1.RunID, 3)))
	f := newDst(t, dst)
	d := &dest{f: f, retries: 1}
	entries, err := d.readIndex(context.Background(), "")
	require.NoError(t, err)
	assert.Len(t, entries, 5)

	// A later run writes new parts rather than overwriting the old ones.
	writeFile(t, src, "f", 5)
	l2 := runBackup(t, src, dst, opt)
	assert.FileExists(t, filepath.Join(dst, indexPartName(l1.RunID, 1)))
	assert.FileExists(t, filepath.Join(dst, indexPartName(l2.RunID, 3)))
	entries, err = d.readIndex(context.Background(), "")
	require.NoError(t, err)
	assert.Len(t, entries, 6)
}

// hookFs wraps an Fs so tests can watch and fail uploads.
type hookFs struct {
	fs.Fs
	put func(remote, tier string) error
}

func (h *hookFs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	tier := ""
	if t, ok := src.(fs.GetTierer); ok {
		tier = t.GetTier()
	}
	if err := h.put(src.Remote(), tier); err != nil {
		return nil, err
	}
	return h.Fs.Put(ctx, in, src, options...)
}

func TestBackupTiers(t *testing.T) {
	fakeClock(t)
	src, dst := t.TempDir(), filepath.Join(t.TempDir(), "lab")
	opt := testOptions()
	writeFile(t, src, "big.bin", 9000)
	writeFile(t, src, "dir/small.txt", 100)
	tiers := map[string]string{}
	h := &hookFs{Fs: newDst(t, dst), put: func(remote, tier string) error {
		tiers[remote] = tier
		return nil
	}}
	_, err := Backup(context.Background(), src, h, opt)
	require.NoError(t, err)
	require.NotEmpty(t, tiers)
	for remote, tier := range tiers {
		want := opt.MetaTier
		if strings.HasSuffix(remote, ".tar") || remote == "big.bin" {
			want = opt.DataTier
		}
		assert.Equal(t, want, tier, remote)
	}
	assert.Equal(t, opt.DataTier, tiers["big.bin"])
}

func TestBackupFailedIndexWrite(t *testing.T) {
	fakeClock(t)
	src, dst := t.TempDir(), filepath.Join(t.TempDir(), "lab")
	opt := testOptions()
	opt.RollupMax = 0
	writeFile(t, src, "dir/a.txt", 100)
	runBackup(t, src, dst, opt)
	before, err := os.ReadFile(filepath.Join(dst, "dir", IndexName))
	require.NoError(t, err)

	// The index write fails after the pack and changeset are stored.
	writeFile(t, src, "dir/b.txt", 100)
	h := &hookFs{Fs: newDst(t, dst), put: func(remote, tier string) error {
		if remote == "dir/"+IndexName {
			return errors.New("injected failure")
		}
		return nil
	}}
	_, err = Backup(context.Background(), src, h, opt)
	assert.Error(t, err)
	after, err := os.ReadFile(filepath.Join(dst, "dir", IndexName))
	require.NoError(t, err)
	assert.Equal(t, before, after)

	// The next run records the change.
	runBackup(t, src, dst, opt)
	index := readIndexFile(t, dst, "dir")
	assert.Contains(t, index, "b.txt")
	checkStored(t, dst, "dir", index)
}

func TestBackupNameEncoding(t *testing.T) {
	fakeClock(t)
	src, dst := t.TempDir(), filepath.Join(t.TempDir(), "lab")
	opt := testOptions()
	opt.RollupMax = 0
	// Both names encode to "x%FF" unless names containing "%" are
	// encoded too.
	writeFile(t, src, "x\xff", 5)
	writeFile(t, src, "x%FF", 6)
	writeFile(t, src, "bad\xffdir/inner.txt", 7)
	l := runBackup(t, src, dst, opt)
	assert.Equal(t, int64(0), l.Stats.Errors)
	root := readIndexFile(t, dst, "")
	names := map[string]bool{}
	for _, e := range root {
		decoded, err := e.DecodeName()
		require.NoError(t, err)
		names[decoded] = true
	}
	assert.True(t, names["x\xff"])
	assert.True(t, names["x%FF"])
	assert.True(t, names["bad\xffdir"])

	// Both files come back under their original names.
	target := t.TempDir()
	st, err := StartRestore(context.Background(), newDst(t, dst), target, DefaultRestoreOptions())
	require.NoError(t, err)
	assert.Equal(t, StateDone, st.State)
	for name, size := range map[string]int{"x\xff": 5, "x%FF": 6, "bad\xffdir/inner.txt": 7} {
		info, err := os.Stat(filepath.Join(target, name))
		require.NoError(t, err, name)
		assert.Equal(t, int64(size), info.Size(), name)
	}
}

func TestBackupRelativeSource(t *testing.T) {
	fakeClock(t)
	src, dst := t.TempDir(), filepath.Join(t.TempDir(), "lab")
	opt := testOptions()
	opt.RollupMax = 0
	writeFile(t, src, "bad\xffdir/inner.txt", 7)
	t.Chdir(src)
	l := runBackup(t, "./", dst, opt)
	assert.Equal(t, int64(0), l.Stats.Errors)
	assert.Equal(t, int64(2), l.Stats.IndexedDirs)
}

func TestBackupDirectoryChanges(t *testing.T) {
	fakeClock(t)
	src, dst := t.TempDir(), filepath.Join(t.TempDir(), "lab")
	opt := testOptions()
	opt.RollupMax = 0
	writeFile(t, src, "thing/inner.txt", 5)
	writeFile(t, src, "keep.txt", 5)
	runBackup(t, src, dst, opt)

	// A permission change on a directory is a changeset row.
	require.NoError(t, os.Chmod(filepath.Join(src, "thing"), 0o700))
	l2 := runBackup(t, src, dst, opt)
	changes := readCSV(t, filepath.Join(dst, changesetName("lab", l2.RunID, "w01")))
	assert.Equal(t, ActionMeta, changes["thing"].Action)
	assert.Equal(t, uint32(0o700), changes["thing"].Mode)

	// A directory replaced by a file has its index retired.
	require.NoError(t, os.RemoveAll(filepath.Join(src, "thing")))
	writeFile(t, src, "thing", 5)
	l3 := runBackup(t, src, dst, opt)
	assert.Equal(t, int64(0), l3.Stats.Errors)
	assert.Equal(t, TypeFile, readIndexFile(t, dst, "")["thing"].Type)
	assert.Empty(t, readIndexFile(t, dst, "thing"))
}

// corruptFs reports a wrong MD5 for every object it stores, like a
// backend whose stored bytes differ from what was sent.
type corruptFs struct {
	fs.Fs
}

type corruptObject struct {
	fs.Object
}

func (corruptObject) Hash(ctx context.Context, ty hash.Type) (string, error) {
	return "00000000000000000000000000000000", nil
}

func (f *corruptFs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	o, err := f.Fs.Put(ctx, in, src, options...)
	if err != nil {
		return nil, err
	}
	return corruptObject{Object: o}, nil
}

func TestBackupStandaloneMD5Mismatch(t *testing.T) {
	fakeClock(t)
	src, dst := t.TempDir(), filepath.Join(t.TempDir(), "lab")
	opt := testOptions()
	opt.RollupMax = 0
	writeFile(t, src, "big.bin", 9000)
	_, err := Backup(context.Background(), src, &corruptFs{Fs: newDst(t, dst)}, opt)
	assert.ErrorContains(t, err, "errors")
	// The corrupt object is removed and nothing refers to it.
	assert.NoFileExists(t, filepath.Join(dst, "big.bin"))
	assert.NotContains(t, readIndexFile(t, dst, ""), "big.bin")
}

func TestBackupStandaloneUploadFailure(t *testing.T) {
	fakeClock(t)
	src, dst := t.TempDir(), filepath.Join(t.TempDir(), "lab")
	opt := testOptions()
	opt.RollupMax = 0
	writeFile(t, src, "a.bin", 9000)
	writeFile(t, src, "b.bin", 9000)
	writeFile(t, src, "small.txt", 100)
	h := &hookFs{Fs: newDst(t, dst), put: func(remote, tier string) error {
		if remote == "a.bin" {
			return errors.New("injected failure")
		}
		return nil
	}}
	_, err := Backup(context.Background(), src, h, opt)
	assert.ErrorContains(t, err, "1 errors")
	// The rest of the directory is committed without the failed file.
	index := readIndexFile(t, dst, "")
	assert.NotContains(t, index, "a.bin")
	assert.Contains(t, index, "b.bin")
	assert.Contains(t, index, "small.txt")

	l := runBackup(t, src, dst, opt)
	assert.Equal(t, int64(1), l.Stats.Added)
	index = readIndexFile(t, dst, "")
	assert.Contains(t, index, "a.bin")
	checkStored(t, dst, "", index)
}

func TestBackupWorkerIDLength(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	opt := testOptions()
	opt.Worker = strings.Repeat("w", maxWorkerID+1)
	_, _, err := newBackup(context.Background(), src, newDst(t, dst), opt)
	assert.ErrorContains(t, err, "longer than 16 bytes")
	// With several workers the suffix counts too.
	opt.Worker = strings.Repeat("w", maxWorkerID-2)
	opt.Workers = 2
	_, _, err = newBackup(context.Background(), src, newDst(t, dst), opt)
	assert.ErrorContains(t, err, "-02")
	opt.Workers = 1
	b, _, err := newBackup(context.Background(), src, newDst(t, dst), opt)
	require.NoError(t, err)
	b.close()
}

func TestBackupIndexCache(t *testing.T) {
	fakeClock(t)
	src, dst := t.TempDir(), filepath.Join(t.TempDir(), "lab")
	opt := testOptions()
	opt.RollupMax = 0
	opt.IndexCache = t.TempDir()
	writeFile(t, src, "dir/a.txt", 100)
	writeFile(t, src, "dir/b.txt", 100)
	runBackup(t, src, dst, opt)

	// With the cache from the latest run, a run doesn't read the index,
	// so it doesn't see this edit, which drops a file from it.
	indexPath := filepath.Join(dst, "dir", IndexName)
	index := readIndexFile(t, dst, "dir")
	delete(index, "b.txt")
	var edited []Entry
	for _, e := range index {
		edited = append(edited, e)
	}
	var buf bytes.Buffer
	require.NoError(t, WriteEntries(&buf, edited))
	require.NoError(t, os.WriteFile(indexPath, buf.Bytes(), 0o644))
	l := runBackup(t, src, dst, opt)
	assert.Equal(t, int64(0), l.Stats.Added)

	// Once another run has written to the destination, the cache isn't
	// trusted, so the index is read again. That run's ID may sort before
	// the cache's, as another host's clock may be behind.
	require.NoError(t, os.WriteFile(indexPath, buf.Bytes(), 0o644))
	require.NoError(t, os.MkdirAll(filepath.Join(dst, MetaDir, "runs", "20000101T000000Z"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dst, MetaDir, "runs", "20000101T000000Z", "other.json"), []byte("{}"), 0o644))
	l = runBackup(t, src, dst, opt)
	assert.Equal(t, int64(1), l.Stats.Added)
}

func TestBackupRebase(t *testing.T) {
	fakeClock(t)
	old := rebaseMaxPacks
	rebaseMaxPacks = 2
	t.Cleanup(func() { rebaseMaxPacks = old })
	src, dst := t.TempDir(), filepath.Join(t.TempDir(), "lab")
	opt := testOptions()
	opt.RollupMax = 0
	opt.DedupMin = 100
	opt.PackSize = 64 * 1024
	for _, name := range []string{"a", "b", "c"} {
		writeFile(t, src, "d/"+name, 1000)
	}
	runBackup(t, src, dst, opt)
	// Each change adds a pack, spreading the directory's files.
	writeFile(t, src, "d/a", 1001)
	runBackup(t, src, dst, opt)
	writeFile(t, src, "d/b", 1002)
	l := runBackup(t, src, dst, opt)
	assert.Zero(t, l.Stats.Rebased)
	packs := func() map[string]bool {
		out := map[string]bool{}
		for _, e := range readIndexFile(t, dst, "d") {
			out[e.Location] = true
		}
		return out
	}
	require.Len(t, packs(), 3)

	// The next run packs the unchanged files again into one pack.
	l = runBackup(t, src, dst, opt)
	assert.Equal(t, int64(3), l.Stats.Rebased)
	assert.Zero(t, l.Stats.Deduplicated)
	assert.Len(t, packs(), 1)
	index := readIndexFile(t, dst, "d")
	checkStored(t, dst, "d", index)
	changes := readCSV(t, filepath.Join(dst, "d", "d.gda."+l.RunID+".w01.csv"))
	assert.Equal(t, ActionRebase, changes["c"].Action)

	target := t.TempDir()
	st, err := StartRestore(context.Background(), newDst(t, dst), target, DefaultRestoreOptions())
	require.NoError(t, err)
	assert.Equal(t, StateDone, st.State)
	assertSameTree(t, src, target)
	l = runBackup(t, src, dst, opt)
	assert.Zero(t, l.Stats.Rebased)

	// A directory whose files fill many packs isn't fragmented.
	opt.PackSize = 10 * 1024
	for i := range 12 {
		writeFile(t, src, fmt.Sprintf("big/f%02d", i), 3000)
	}
	runBackup(t, src, dst, opt)
	l = runBackup(t, src, dst, opt)
	assert.Zero(t, l.Stats.Rebased)
}

func TestBackupLedgerAtStart(t *testing.T) {
	fakeClock(t)
	src, dst := t.TempDir(), filepath.Join(t.TempDir(), "lab")
	writeFile(t, src, "dir/a.txt", 100)
	// When data is uploaded, the run's ledger is already there, so a
	// run which doesn't finish still shows it wrote here.
	var ledger *Ledger
	h := &hookFs{Fs: newDst(t, dst), put: func(remote, tier string) error {
		if strings.HasSuffix(remote, ".tar") && ledger == nil {
			matches, _ := filepath.Glob(filepath.Join(dst, MetaDir, "runs", "*", "w01.json"))
			require.Len(t, matches, 1)
			data, err := os.ReadFile(matches[0])
			require.NoError(t, err)
			ledger = &Ledger{}
			require.NoError(t, json.Unmarshal(data, ledger))
		}
		return nil
	}}
	_, err := Backup(context.Background(), src, h, testOptions())
	require.NoError(t, err)
	require.NotNil(t, ledger)
	assert.True(t, ledger.Finished.IsZero())
}

func TestSameLink(t *testing.T) {
	now := time.Now()
	a := Entry{Type: TypeFile, Size: 10, ModTime: now, Mode: 0o644, HardLink: "1:2"}
	b := a
	assert.True(t, sameLink(&a, &b))
	// A file reusing the inode of a removed one has its own time.
	b.ModTime = now.Add(time.Second)
	assert.False(t, sameLink(&a, &b))
	b = a
	b.Mode = 0o600
	assert.False(t, sameLink(&a, &b))
}
