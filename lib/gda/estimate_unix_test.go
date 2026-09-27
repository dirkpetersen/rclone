//go:build unix

package gda

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/rclone/rclone/fs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// tierFs reports every object as stored in one storage class, as S3
// does for archived objects.
type tierFs struct {
	fs.Fs
	tier string
}

type tierObject struct {
	fs.Object
	tier string
}

func (o tierObject) GetTier() string { return o.tier }

func (f *tierFs) Features() *fs.Features {
	features := *f.Fs.Features()
	features.GetTier = true
	return &features
}

func (f *tierFs) List(ctx context.Context, dir string) (fs.DirEntries, error) {
	entries, err := f.Fs.List(ctx, dir)
	for i, e := range entries {
		if o, ok := e.(fs.Object); ok {
			entries[i] = tierObject{Object: o, tier: f.tier}
		}
	}
	return entries, err
}

func TestEstimateRestore(t *testing.T) {
	fakeClock(t)
	src := makeTree(t)
	dst := filepath.Join(t.TempDir(), "lab")
	runBackup(t, src, dst, testOptions())
	archived := &tierFs{Fs: newDst(t, dst), tier: "DEEP_ARCHIVE"}
	eopt := EstimateOptions{Prices: DefaultPrices(), EgressPath: EgressInternet}
	target := t.TempDir()

	est, err := EstimateRestore(context.Background(), archived, target, DefaultRestoreOptions(), eopt)
	require.NoError(t, err)
	sel := est.Selection
	assert.Equal(t, 5, sel.Files)
	assert.Equal(t, int64(10+3000+9000+100+5), sel.Bytes)
	assert.Positive(t, sel.ObjectsToRestore)
	assert.GreaterOrEqual(t, sel.BytesToRestore, sel.Bytes)
	assert.Equal(t, 0, sel.NoRetrievalFiles)
	require.Len(t, est.Options, 2)
	assert.Equal(t, "Bulk", est.Options[0].Tier)
	assert.Empty(t, est.Warnings)

	// A single small file from a pack is fetched with a ranged read, so
	// only its bytes count as egress, while the whole pack is restored.
	one := DefaultRestoreOptions()
	one.Paths = []string{"results/a.dat"}
	est, err = EstimateRestore(context.Background(), archived, target, one, eopt)
	require.NoError(t, err)
	assert.Equal(t, 1, est.Selection.ObjectsToRestore)
	assert.Equal(t, int64(3000), est.Selection.BytesToDownload)
	assert.Greater(t, est.Selection.BytesToRestore, int64(3000))

	// Files already in place need no retrieval.
	_, err = StartRestore(context.Background(), newDst(t, dst), target, one)
	require.NoError(t, err)
	est, err = EstimateRestore(context.Background(), archived, target, one, eopt)
	require.NoError(t, err)
	assert.Equal(t, 1, est.Selection.NoRetrievalFiles)
	assert.Equal(t, 0, est.Selection.ObjectsToRestore)
	assert.Equal(t, NoTier, est.Options[0].Tier)

	// Objects which aren't archived are available now.
	est, err = EstimateRestore(context.Background(), newDst(t, dst), t.TempDir(), DefaultRestoreOptions(), eopt)
	require.NoError(t, err)
	assert.Equal(t, 0, est.Selection.ObjectsToRestore)
	assert.Equal(t, 5, est.Selection.NoRetrievalFiles)
	assert.Equal(t, NoTier, est.Options[0].Tier)
}
