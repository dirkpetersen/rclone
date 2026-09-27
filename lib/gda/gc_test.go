//go:build unix

package gda

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGC(t *testing.T) {
	fakeClock(t)
	src := makeTree(t)
	dst := filepath.Join(t.TempDir(), "lab")
	opt := testOptions()
	opt.RollupMax = 0
	runBackup(t, src, dst, opt)
	f := newDst(t, dst)

	r, err := GC(context.Background(), f, GCOptions{})
	require.NoError(t, err)
	assert.Empty(t, r.Orphans)
	assert.Empty(t, r.Unknown)
	assert.Empty(t, r.Errors)
	assert.Positive(t, r.Packs)
	assert.Greater(t, r.DataObjects, r.Packs)
	assert.Positive(t, r.LivePackData)
	assert.Zero(t, r.DeadPackData)

	// A pack left by a crashed run, one listed only in the dedup index,
	// and a file which isn't GDA's.
	orphanName := "results.gda." + NewRunID(time.Now()) + ".w09.001.tar"
	orphan := filepath.Join(dst, "results", orphanName)
	require.NoError(t, os.WriteFile(orphan, []byte("orphan"), 0o644))
	// Its modification time is the source file's, as for standalone
	// objects, so it can't tell the age.
	old := time.Now().Add(-365 * 24 * time.Hour)
	require.NoError(t, os.Chtimes(orphan, old, old))
	noRun := filepath.Join(dst, "results", "big.bin.gda.zst")
	require.NoError(t, os.WriteFile(noRun, []byte("no run"), 0o644))
	kept := filepath.Join(dst, "results", "results.gda.20200101T000000Z.w09.002.tar")
	require.NoError(t, os.WriteFile(kept, []byte("kept"), 0o644))
	require.NoError(t, os.MkdirAll(filepath.Join(dst, MetaDir, "dedup"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dst, MetaDir, "dedup", "x.csv"),
		[]byte("name,type,location\nabc,file,results/results.gda.20200101T000000Z.w09.002.tar\n"), 0o644))
	foreign := filepath.Join(dst, "results", "notes.txt")
	require.NoError(t, os.WriteFile(foreign, []byte("mine"), 0o644))

	r, err = GC(context.Background(), f, GCOptions{})
	require.NoError(t, err)
	require.Len(t, r.Orphans, 2)
	assert.Equal(t, "results/big.bin.gda.zst", r.Orphans[0].Key)
	assert.Equal(t, "results/"+orphanName, r.Orphans[1].Key)
	assert.Equal(t, int64(12), r.OrphanBytes)
	require.Len(t, r.Unknown, 1)
	assert.Equal(t, "results/notes.txt", r.Unknown[0].Key)
	assert.FileExists(t, orphan)

	// Orphans younger than the minimum age are kept.
	r, err = GC(context.Background(), f, GCOptions{DeleteOrphans: true, MinAge: time.Hour, LockTimeout: time.Hour})
	require.NoError(t, err)
	assert.Zero(t, r.Deleted)
	assert.FileExists(t, orphan)

	r, err = GC(context.Background(), f, GCOptions{DeleteOrphans: true, LockTimeout: time.Hour})
	require.NoError(t, err)
	assert.Equal(t, int64(1), r.Deleted)
	assert.NoFileExists(t, orphan)
	// Without a run in its name, an orphan's age isn't known.
	assert.FileExists(t, noRun)
	require.NoError(t, os.Remove(noRun))
	assert.FileExists(t, kept)
	assert.FileExists(t, foreign)
	assert.NoFileExists(t, filepath.Join(dst, MetaDir, "lock"))

	// Removing orphans needs the lock.
	lock := `{"RunID":"20260926T000000Z","Host":"other","PID":1,"Started":"` + time.Now().UTC().Format(time.RFC3339) + `"}`
	require.NoError(t, os.WriteFile(filepath.Join(dst, MetaDir, "lock"), []byte(lock), 0o644))
	_, err = GC(context.Background(), f, GCOptions{DeleteOrphans: true, LockTimeout: time.Hour})
	assert.ErrorContains(t, err, "locked by run")
	require.NoError(t, os.Remove(filepath.Join(dst, MetaDir, "lock")))

	// Below the root it can't see the lock or the dedup index.
	_, err = GC(context.Background(), newDst(t, filepath.Join(dst, "results")), GCOptions{})
	assert.ErrorContains(t, err, "isn't the root of a GDA tree")

	// Everything still restores.
	require.NoError(t, os.Remove(foreign))
	require.NoError(t, os.Remove(kept))
	st, err := StartRestore(context.Background(), f, t.TempDir(), DefaultRestoreOptions())
	require.NoError(t, err)
	assert.Equal(t, StateDone, st.State)
}

func TestGCCompactable(t *testing.T) {
	fakeClock(t)
	oldAge, oldDead := compactMinAge, compactMinDead
	compactMinAge, compactMinDead = 0, 1
	t.Cleanup(func() { compactMinAge, compactMinDead = oldAge, oldDead })
	src, dst := t.TempDir(), filepath.Join(t.TempDir(), "lab")
	opt := testOptions()
	opt.RollupMax = 0
	opt.PackSize = 64 * 1024
	for _, name := range []string{"x", "y", "z"} {
		writeFile(t, src, "d/"+name, 1000)
	}
	first := runBackup(t, src, dst, opt)
	writeFile(t, src, "d/x", 1001)
	writeFile(t, src, "d/y", 1002)
	runBackup(t, src, dst, opt)

	r, err := GC(context.Background(), newDst(t, dst), GCOptions{})
	require.NoError(t, err)
	require.Len(t, r.Compactable, 1)
	u := r.Compactable[0]
	assert.Equal(t, "d/d.gda."+first.RunID+".w01.001.tar", u.Key)
	assert.Equal(t, int64(3000), u.Members)
	assert.Equal(t, int64(1000), u.Live)
	assert.False(t, u.Shared)
	assert.Equal(t, int64(2000), r.DeadPackData)
}

func TestGCStaleIndexParts(t *testing.T) {
	fakeClock(t)
	old := maxIndexRows
	maxIndexRows = 2
	t.Cleanup(func() { maxIndexRows = old })
	src, dst := t.TempDir(), filepath.Join(t.TempDir(), "lab")
	opt := testOptions()
	opt.RollupMax = 0
	for i := range 5 {
		writeFile(t, src, fmt.Sprintf("d/f%d", i), 10)
	}
	first := runBackup(t, src, dst, opt)
	writeFile(t, src, "d/f5", 10)
	runBackup(t, src, dst, opt)
	f := newDst(t, dst)

	r, err := GC(context.Background(), f, GCOptions{})
	require.NoError(t, err)
	require.Len(t, r.StaleIndexes, 3)
	for _, o := range r.StaleIndexes {
		assert.Contains(t, o.Key, first.RunID)
	}
	r, err = GC(context.Background(), f, GCOptions{DeleteOrphans: true, LockTimeout: time.Hour})
	require.NoError(t, err)
	assert.Equal(t, int64(3), r.Deleted)
	entries, err := List(context.Background(), f, "d", "")
	require.NoError(t, err)
	assert.Len(t, entries, 6)
	r, err = GC(context.Background(), f, GCOptions{})
	require.NoError(t, err)
	assert.Empty(t, r.StaleIndexes)
}

// secondReadFails fails the second read of an object.
type secondReadFails struct {
	fs.Fs
	key   string
	mu    sync.Mutex
	reads int
}

func (f *secondReadFails) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	if remote == f.key {
		f.mu.Lock()
		f.reads++
		n := f.reads
		f.mu.Unlock()
		if n == 2 {
			return nil, errors.New("injected read failure")
		}
	}
	return f.Fs.NewObject(ctx, remote)
}

func TestGCStaleIndexReadError(t *testing.T) {
	fakeClock(t)
	old := maxIndexRows
	maxIndexRows = 2
	t.Cleanup(func() { maxIndexRows = old })
	src, dst := t.TempDir(), filepath.Join(t.TempDir(), "lab")
	opt := testOptions()
	opt.RollupMax = 0
	for i := range 5 {
		writeFile(t, src, fmt.Sprintf("d/f%d", i), 10)
	}
	runBackup(t, src, dst, opt)

	// Without the table of contents nothing is taken to be stale.
	f := &secondReadFails{Fs: newDst(t, dst), key: "d/" + IndexName}
	r, err := GC(context.Background(), f, GCOptions{DeleteOrphans: true, LockTimeout: time.Hour})
	assert.Error(t, err)
	assert.Zero(t, r.Deleted)
	entries, err := List(context.Background(), newDst(t, dst), "d", "")
	require.NoError(t, err)
	assert.Len(t, entries, 5)
}

func TestGCExpireHistory(t *testing.T) {
	fakeClock(t)
	src, dst := t.TempDir(), filepath.Join(t.TempDir(), "lab")
	opt := testOptions()
	opt.RollupMax = 0
	opt.PackSize = 64 * 1024
	opt.StandaloneMin = 8 * 1024
	writeFile(t, src, "d/a.txt", 100)
	writeFile(t, src, "d/b.txt", 100)
	writeFile(t, src, "d/big.bin", 9000)
	run1 := runBackup(t, src, dst, opt)
	writeFile(t, src, "d/a.txt", 101)
	writeFile(t, src, "d/big.bin", 9001)
	run2 := runBackup(t, src, dst, opt)
	require.NoError(t, os.Remove(filepath.Join(src, "d/a.txt")))
	run3 := runBackup(t, src, dst, opt)
	f := newDst(t, dst)

	// Keeping history from run 3, the pack holding only a.txt's second
	// version and big.bin's first version are no longer needed; the first
	// pack still holds b.txt.
	r, err := GC(context.Background(), f, GCOptions{KeepFrom: run3.RunID})
	require.NoError(t, err)
	var expired []string
	for _, o := range r.Expired {
		expired = append(expired, o.Key)
	}
	assert.ElementsMatch(t, []string{"d/d.gda." + run2.RunID + ".w01.001.tar", "d/big.bin"}, expired)
	// Keeping everything expires nothing.
	r, err = GC(context.Background(), f, GCOptions{})
	require.NoError(t, err)
	assert.Empty(t, r.Expired)

	r, err = GC(context.Background(), f, GCOptions{KeepFrom: run3.RunID, DeleteExpired: true, LockTimeout: time.Hour})
	require.NoError(t, err)
	assert.Equal(t, int64(2), r.Deleted)
	assert.NoFileExists(t, filepath.Join(dst, "d", "big.bin"))

	// The tree as it is, and as it was at run 3, still restores; earlier
	// runs are refused.
	for _, at := range []string{"", run3.RunID} {
		ropt := DefaultRestoreOptions()
		ropt.At = at
		target := t.TempDir()
		st, err := StartRestore(context.Background(), f, target, ropt)
		require.NoError(t, err, at)
		assert.Equal(t, StateDone, st.State, at)
		if at == "" {
			assertSameTree(t, src, target)
		}
	}
	ropt := DefaultRestoreOptions()
	ropt.At = run1.RunID
	_, err = StartRestore(context.Background(), f, t.TempDir(), ropt)
	assert.ErrorContains(t, err, "history before run "+run3.RunID+" has been removed")
	report, err := Check(context.Background(), f, "", "", CheckOptions{Download: true})
	require.NoError(t, err)
	assert.False(t, report.Failed(), "%+v", report)
}
