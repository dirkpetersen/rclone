//go:build unix

package gda

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

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
	orphan := filepath.Join(dst, "results", "results.gda.20200101T000000Z.w09.001.tar")
	require.NoError(t, os.WriteFile(orphan, []byte("orphan"), 0o644))
	kept := filepath.Join(dst, "results", "results.gda.20200101T000000Z.w09.002.tar")
	require.NoError(t, os.WriteFile(kept, []byte("kept"), 0o644))
	require.NoError(t, os.MkdirAll(filepath.Join(dst, MetaDir, "dedup"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dst, MetaDir, "dedup", "x.csv"),
		[]byte("name,type,location\nabc,file,results/results.gda.20200101T000000Z.w09.002.tar\n"), 0o644))
	foreign := filepath.Join(dst, "results", "notes.txt")
	require.NoError(t, os.WriteFile(foreign, []byte("mine"), 0o644))

	r, err = GC(context.Background(), f, GCOptions{})
	require.NoError(t, err)
	require.Len(t, r.Orphans, 1)
	assert.Equal(t, "results/results.gda.20200101T000000Z.w09.001.tar", r.Orphans[0].Key)
	assert.Equal(t, int64(6), r.OrphanBytes)
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
	assert.FileExists(t, kept)
	assert.FileExists(t, foreign)
	assert.NoFileExists(t, filepath.Join(dst, MetaDir, "lock"))

	// Removing orphans needs the lock.
	lock := `{"RunID":"20260926T000000Z","Host":"other","PID":1,"Started":"` + time.Now().UTC().Format(time.RFC3339) + `"}`
	require.NoError(t, os.WriteFile(filepath.Join(dst, MetaDir, "lock"), []byte(lock), 0o644))
	_, err = GC(context.Background(), f, GCOptions{DeleteOrphans: true, LockTimeout: time.Hour})
	assert.ErrorContains(t, err, "locked by run")

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
