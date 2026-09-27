//go:build unix

package gda

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/filter"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// pendingFs reports every object as DEEP_ARCHIVE and makes the pending
// ones unreadable until they are released, as S3 does for archived
// objects until they are restored, and records restore requests made
// through its restore command.
type pendingFs struct {
	fs.Fs
	pending  map[string]bool // keys which can't be read yet
	requests [][]string      // keys of each restore request
	status   string          // status the restore command reports
}

type pendingObject struct {
	fs.Object
	f *pendingFs
}

func (o pendingObject) GetTier() string { return "DEEP_ARCHIVE" }

func (o pendingObject) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	if o.f.pending[o.Remote()] {
		return nil, errors.New("Object in DEEP_ARCHIVE, restore first: bucket=\"b\", key=\"k\"")
	}
	return o.Object.Open(ctx, options...)
}

func (f *pendingFs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	o, err := f.Fs.NewObject(ctx, remote)
	if err != nil {
		return nil, err
	}
	return pendingObject{Object: o, f: f}, nil
}

func (f *pendingFs) List(ctx context.Context, dir string) (fs.DirEntries, error) {
	entries, err := f.Fs.List(ctx, dir)
	for i, e := range entries {
		if o, ok := e.(fs.Object); ok {
			entries[i] = pendingObject{Object: o, f: f}
		}
	}
	return entries, err
}

func (f *pendingFs) Features() *fs.Features {
	features := *f.Fs.Features()
	features.Command = f.command
	features.GetTier = true
	return &features
}

// command implements the restore command of the s3 backend: it reports
// a status for each object the filter names.
func (f *pendingFs) command(ctx context.Context, name string, arg []string, opt map[string]string) (any, error) {
	if name != "restore" {
		return nil, fs.ErrorCommandNotFound
	}
	if !fs.GetConfig(ctx).NoTraverse {
		return nil, errors.New("restore must not list the tree")
	}
	type status struct{ Status, Remote string }
	var keys []string
	out := []status{}
	for key := range filter.GetConfig(ctx).Files() {
		keys = append(keys, key)
		if _, err := f.Fs.NewObject(ctx, key); err != nil {
			continue
		}
		s := f.status
		if s == "" {
			s = "OK"
		}
		out = append(out, status{Status: s, Remote: key})
	}
	f.requests = append(f.requests, keys)
	return out, nil
}

// archiveAll marks every data object below dst as pending.
func archiveAll(t *testing.T, f *pendingFs, dst string) {
	t.Helper()
	f.pending = map[string]bool{}
	for _, o := range objects(t, dst) {
		if filepath.Ext(o) != ".csv" {
			f.pending[o] = true
		}
	}
}

func TestRestorePendingThenResume(t *testing.T) {
	fakeClock(t)
	src := makeTree(t)
	writeFile(t, src, "results/empty.txt", 0)
	dst := filepath.Join(t.TempDir(), "lab")
	runBackup(t, src, dst, testOptions())
	f := &pendingFs{Fs: newDst(t, dst)}
	archiveAll(t, f, dst)
	target := t.TempDir()

	st, err := StartRestore(context.Background(), f, target, DefaultRestoreOptions())
	require.NoError(t, err)
	assert.Equal(t, StateRestoring, st.State)
	assert.Equal(t, st.Objects.Requested, st.Objects.Restoring)
	require.Len(t, f.requests, 1)
	assert.ElementsMatch(t, planObjectsFor(t, f, target), f.requests[0])
	// Empty files need no data, so they are there already.
	info, err := os.Stat(filepath.Join(target, "results/empty.txt"))
	require.NoError(t, err)
	assert.Equal(t, int64(0), info.Size())

	// While waiting, the user writes a file the restore will fetch. A
	// resume mustn't replace it without --overwrite.
	require.NoError(t, os.WriteFile(filepath.Join(target, "results/a.dat"), []byte("mine"), 0o644))
	f.pending = nil
	st, err = ResumeRestore(context.Background(), f, st.RestoreID, target, false)
	require.NoError(t, err)
	assert.Equal(t, StateFailed, st.State)
	assert.Equal(t, 1, st.Files.Failed)
	require.NotEmpty(t, st.Errors)
	assert.Contains(t, st.Errors[0], "results/a.dat")
	have, _ := os.ReadFile(filepath.Join(target, "results/a.dat"))
	assert.Equal(t, "mine", string(have))

	// Once that file is out of the way everything matches the source,
	// including the directory metadata held back while waiting.
	require.NoError(t, os.Remove(filepath.Join(target, "results/a.dat")))
	st, err = ResumeRestore(context.Background(), f, st.RestoreID, target, false)
	require.NoError(t, err)
	assert.Equal(t, StateDone, st.State)
	assertSameTree(t, src, target)
}

// planObjectsFor returns the objects a whole restore into target needs.
func planObjectsFor(t *testing.T, f fs.Fs, target string) []string {
	t.Helper()
	plan, err := buildPlan(context.Background(), newTree(&dest{f: f, retries: 1}, ""), nil)
	require.NoError(t, err)
	_, err = markIdentical(plan, target)
	require.NoError(t, err)
	return planObjects(plan)
}

func TestRestoreExpired(t *testing.T) {
	fakeClock(t)
	src := makeTree(t)
	dst := filepath.Join(t.TempDir(), "lab")
	runBackup(t, src, dst, testOptions())
	f := &pendingFs{Fs: newDst(t, dst)}
	archiveAll(t, f, dst)
	target := t.TempDir()
	st, err := StartRestore(context.Background(), f, target, DefaultRestoreOptions())
	require.NoError(t, err)
	require.Len(t, f.requests, 1)

	// Before the restore is due, resuming doesn't ask again.
	_, err = ResumeRestore(context.Background(), f, st.RestoreID, target, false)
	require.NoError(t, err)
	assert.Len(t, f.requests, 1)

	// After it, whatever still isn't readable is requested again.
	later := st.ReadyBy.Add(time.Hour)
	old := timeNow
	timeNow = func() time.Time { return later }
	defer func() { timeNow = old }()
	st2, err := ResumeRestore(context.Background(), f, st.RestoreID, target, false)
	require.NoError(t, err)
	require.Len(t, f.requests, 2)
	assert.ElementsMatch(t, st2.Pending, f.requests[1])
	assert.True(t, st2.ReadyBy.After(later))
}

func TestRestoreRequestFailures(t *testing.T) {
	fakeClock(t)
	src := makeTree(t)
	dst := filepath.Join(t.TempDir(), "lab")
	runBackup(t, src, dst, testOptions())
	f := &pendingFs{Fs: newDst(t, dst), status: "operation error S3: RestoreObject, AccessDenied"}
	archiveAll(t, f, dst)
	target := t.TempDir()

	_, err := StartRestore(context.Background(), f, target, DefaultRestoreOptions())
	assert.ErrorContains(t, err, "AccessDenied")
	entries, _ := os.ReadDir(filepath.Join(dst, MetaDir))
	for _, e := range entries {
		assert.NotEqual(t, "restores", e.Name(), "no plan is saved when the request fails")
	}

	// Objects which are missing are an error too.
	f.status = ""
	err = requestRestore(context.Background(), f, []string{"results/nope.tar"}, "Bulk", 1)
	assert.ErrorContains(t, err, "results/nope.tar: not found")
	err = requestRestore(context.Background(), f, []string{"results/big.bin"}, "Bulk", 1)
	assert.NoError(t, err)
}

func TestRestoreSymlinkInTarget(t *testing.T) {
	fakeClock(t)
	src := makeTree(t)
	dst := filepath.Join(t.TempDir(), "lab")
	runBackup(t, src, dst, testOptions())
	f := newDst(t, dst)
	target, outside := t.TempDir(), t.TempDir()
	require.NoError(t, os.Symlink(outside, filepath.Join(target, "results")))

	// A symlink where the backup has a directory is a conflict ...
	_, err := StartRestore(context.Background(), f, target, DefaultRestoreOptions())
	assert.ErrorContains(t, err, "results")
	left, _ := os.ReadDir(outside)
	assert.Empty(t, left)

	// ... and with --overwrite it is replaced, never written through.
	opt := DefaultRestoreOptions()
	opt.Overwrite = true
	st, err := StartRestore(context.Background(), f, target, opt)
	require.NoError(t, err)
	assert.Equal(t, StateDone, st.State)
	left, _ = os.ReadDir(outside)
	assert.Empty(t, left)
	info, err := os.Lstat(filepath.Join(target, "results"))
	require.NoError(t, err)
	assert.True(t, info.IsDir())
	assertSameTree(t, src, target)
}

func TestRestoreUnsupported(t *testing.T) {
	fakeClock(t)
	src := t.TempDir()
	writeFile(t, src, "file.txt", 5)
	l, err := net.Listen("unix", filepath.Join(src, "sock"))
	require.NoError(t, err)
	defer func() { _ = l.Close() }()
	dst := filepath.Join(t.TempDir(), "lab")
	runBackup(t, src, dst, testOptions())

	st, err := StartRestore(context.Background(), newDst(t, dst), t.TempDir(), DefaultRestoreOptions())
	require.NoError(t, err)
	assert.Equal(t, StateDone, st.State)
	assert.Equal(t, 1, st.Files.Unsupported)
	assert.Equal(t, 1, st.Files.Fetched)
}

func TestValidTarget(t *testing.T) {
	for _, rel := range []string{"a", "a/b", "a.b/c..d"} {
		assert.NoError(t, validTarget(rel), rel)
	}
	for _, rel := range []string{"", "/a", "a/../b", "..", "a//b", "./a", "a/."} {
		assert.Error(t, validTarget(rel), rel)
	}
}

func TestRestoreSkipsRestoredObjects(t *testing.T) {
	fakeClock(t)
	src := makeTree(t)
	dst := filepath.Join(t.TempDir(), "lab")
	runBackup(t, src, dst, testOptions())
	f := &pendingFs{Fs: newDst(t, dst)}
	archiveAll(t, f, dst)
	// big.bin has a restored copy already, so it isn't requested again.
	delete(f.pending, "results/big.bin")
	target := t.TempDir()
	st, err := StartRestore(context.Background(), f, target, DefaultRestoreOptions())
	require.NoError(t, err)
	require.Len(t, f.requests, 1)
	assert.NotContains(t, f.requests[0], "results/big.bin")
	assert.FileExists(t, filepath.Join(target, "results/big.bin"))
	assert.Equal(t, StateRestoring, st.State)
}

func TestRestoreResumeTakesTargetFromCaller(t *testing.T) {
	fakeClock(t)
	src := makeTree(t)
	dst := filepath.Join(t.TempDir(), "lab")
	runBackup(t, src, dst, testOptions())
	f := &pendingFs{Fs: newDst(t, dst)}
	archiveAll(t, f, dst)
	target := t.TempDir()
	st, err := StartRestore(context.Background(), f, target, DefaultRestoreOptions())
	require.NoError(t, err)

	// Someone with write access to the bucket points the saved restore
	// somewhere else; resuming still writes only to the caller's target.
	elsewhere := t.TempDir()
	recPath := filepath.Join(dst, MetaDir, "restores", st.RestoreID+".json")
	data, err := os.ReadFile(recPath)
	require.NoError(t, err)
	data = []byte(strings.ReplaceAll(string(data), target, elsewhere))
	require.NoError(t, os.WriteFile(recPath, data, 0o644))
	f.pending = nil
	st, err = ResumeRestore(context.Background(), f, st.RestoreID, target, false)
	require.NoError(t, err)
	assert.Equal(t, StateDone, st.State)
	left, _ := os.ReadDir(elsewhere)
	assert.Empty(t, left)
	assertSameTree(t, src, target)

	_, err = ResumeRestore(context.Background(), f, st.RestoreID, "", false)
	assert.Error(t, err)
}

func TestRestoreConcurrentResume(t *testing.T) {
	fakeClock(t)
	src := makeTree(t)
	dst := filepath.Join(t.TempDir(), "lab")
	runBackup(t, src, dst, testOptions())
	f := &pendingFs{Fs: newDst(t, dst)}
	archiveAll(t, f, dst)
	target := t.TempDir()
	st, err := StartRestore(context.Background(), f, target, DefaultRestoreOptions())
	require.NoError(t, err)

	// Hold the lock another run would hold.
	lock, err := os.OpenFile(filepath.Join(target, ".gda-restore-"+st.RestoreID+".lock"), os.O_RDWR, 0)
	require.NoError(t, err)
	defer func() { _ = lock.Close() }()
	require.NoError(t, unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB))
	f.pending = nil
	st2, err := ResumeRestore(context.Background(), f, st.RestoreID, target, false)
	require.NoError(t, err)
	assert.Equal(t, StateRestoring, st2.State)
	require.NotEmpty(t, st2.Errors)
	assert.Contains(t, st2.Errors[0], "another run")
	assert.NoFileExists(t, filepath.Join(target, "results/a.dat"))

	require.NoError(t, unix.Flock(int(lock.Fd()), unix.LOCK_UN))
	st3, err := ResumeRestore(context.Background(), f, st.RestoreID, target, false)
	require.NoError(t, err)
	assert.Equal(t, StateDone, st3.State)
	assert.Equal(t, st3.Objects.Requested, st3.Objects.Fetched)
}

func TestRestoreRequestsObjectsNeededLater(t *testing.T) {
	fakeClock(t)
	src := makeTree(t)
	dst := filepath.Join(t.TempDir(), "lab")
	runBackup(t, src, dst, testOptions())
	target := t.TempDir()
	// a.dat is in place, so its pack isn't requested at the start.
	_, err := StartRestore(context.Background(), newDst(t, dst), target, restoreOptions("results/big.bin", "results/a.dat"))
	require.NoError(t, err)
	require.NoError(t, os.Remove(filepath.Join(target, "results/big.bin")))
	f := &pendingFs{Fs: newDst(t, dst)}
	archiveAll(t, f, dst)
	st, err := StartRestore(context.Background(), f, target, restoreOptions("results/big.bin", "results/a.dat"))
	require.NoError(t, err)
	require.Len(t, f.requests, 1)
	assert.Equal(t, []string{"results/big.bin"}, f.requests[0])

	// If a.dat goes before the restore finishes, the next resume asks for
	// its pack straight away rather than after the restore is due.
	require.NoError(t, os.Remove(filepath.Join(target, "results/a.dat")))
	st, err = ResumeRestore(context.Background(), f, st.RestoreID, target, false)
	require.NoError(t, err)
	require.Len(t, f.requests, 2)
	assert.Len(t, f.requests[1], 1)
	assert.NotEqual(t, "results/big.bin", f.requests[1][0])

	// The record keeps what was requested, and when it should be ready.
	st, err = ResumeRestore(context.Background(), f, st.RestoreID, target, false)
	require.NoError(t, err)
	assert.Len(t, f.requests, 2)
	assert.Equal(t, st.ReadyBy, st.ReadyBy.Truncate(time.Second))
}

func TestRestoreDirectoryInTheWay(t *testing.T) {
	fakeClock(t)
	src := makeTree(t)
	dst := filepath.Join(t.TempDir(), "lab")
	runBackup(t, src, dst, testOptions())
	f := newDst(t, dst)
	target := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(target, "results/a.dat"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(target, "results/link/full"), 0o755))

	_, err := StartRestore(context.Background(), f, target, DefaultRestoreOptions())
	assert.ErrorContains(t, err, "results/a.dat")

	// With --overwrite an empty directory is replaced; one with contents
	// isn't.
	opt := DefaultRestoreOptions()
	opt.Overwrite = true
	st, err := StartRestore(context.Background(), f, target, opt)
	require.NoError(t, err)
	assert.Equal(t, StateFailed, st.State)
	assert.Equal(t, 1, st.Files.Failed)
	have, err := os.ReadFile(filepath.Join(target, "results/a.dat"))
	require.NoError(t, err)
	assert.Len(t, have, 3000)
	assert.DirExists(t, filepath.Join(target, "results/link/full"))
}
