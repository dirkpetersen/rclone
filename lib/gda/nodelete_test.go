//go:build unix

package gda

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/rclone/rclone/fs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// noDeleteFs is a destination whose credentials can't delete anything.
type noDeleteFs struct {
	fs.Fs
}

type noDeleteObject struct {
	fs.Object
}

func (o noDeleteObject) Remove(ctx context.Context) error {
	return errors.New("AccessDenied")
}

func (f noDeleteFs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	o, err := f.Fs.NewObject(ctx, remote)
	if err != nil {
		return nil, err
	}
	return noDeleteObject{o}, nil
}

func (f noDeleteFs) List(ctx context.Context, dir string) (fs.DirEntries, error) {
	entries, err := f.Fs.List(ctx, dir)
	for i, e := range entries {
		if o, ok := e.(fs.Object); ok {
			entries[i] = noDeleteObject{o}
		}
	}
	return entries, err
}

func TestBackupWithoutDelete(t *testing.T) {
	fakeClock(t)
	old := dedupCompactAt
	dedupCompactAt = 2
	t.Cleanup(func() { dedupCompactAt = old })
	ctx := context.Background()
	src, dst := t.TempDir(), filepath.Join(t.TempDir(), "lab")
	f := noDeleteFs{newDst(t, dst)}
	opt := testOptions()
	opt.DedupMin = 1000
	dedupCSVs := func() int {
		files, err := filepath.Glob(filepath.Join(dst, MetaDir, "dedup", "*.csv"))
		require.NoError(t, err)
		return len(files)
	}

	// Each run releases the lock for the next, which doesn't have to wait
	// for it to time out, and merging the dedup index is left to gc once
	// merged files can't be removed.
	require.NoError(t, markRebase(ctx, &dest{f: f, metaTier: opt.MetaTier}, []string{"d"}))
	for i := range 5 {
		writeFile(t, src, filepath.Join("d", string(rune('a'+i))+".bin"), 1000+i)
		_, err := Backup(ctx, src, f, opt)
		require.NoError(t, err)
	}
	data, err := os.ReadFile(filepath.Join(dst, filepath.FromSlash(lockKey)))
	require.NoError(t, err)
	var held lockInfo
	require.NoError(t, json.Unmarshal(data, &held))
	assert.True(t, held.Released)
	assert.FileExists(t, filepath.Join(dst, filepath.FromSlash(dedupGCKey)))
	assert.Equal(t, 6, dedupCSVs(), "5 runs and one merge")
	marks, err := readRebase(ctx, &dest{f: f})
	require.NoError(t, err)
	assert.Empty(t, marks)

	// gc, which can delete, merges the dedup index and takes merging back.
	_, err = GC(ctx, newDst(t, dst), GCOptions{DeleteOrphans: true})
	require.NoError(t, err)
	assert.Equal(t, 1, dedupCSVs())
	assert.NoFileExists(t, filepath.Join(dst, filepath.FromSlash(dedupGCKey)))
	assert.NoFileExists(t, filepath.Join(dst, filepath.FromSlash(lockKey)))

	// Deduplication still finds every copy.
	for i := range 5 {
		copyFile(t, src, filepath.Join("d", string(rune('a'+i))+".bin"), filepath.Join("copies", string(rune('a'+i))+".bin"))
	}
	l := runBackup(t, src, dst, opt)
	assert.Equal(t, int64(5), l.Stats.Deduplicated)
}
