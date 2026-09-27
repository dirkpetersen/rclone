//go:build unix

package gda

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEstimateRestore(t *testing.T) {
	fakeClock(t)
	src := makeTree(t)
	dst := filepath.Join(t.TempDir(), "lab")
	runBackup(t, src, dst, testOptions())
	archived := &pendingFs{Fs: newDst(t, dst)}
	archiveAll(t, archived, dst)
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

	// Objects with a restored copy need no retrieval.
	archived.pending = nil
	est, err = EstimateRestore(context.Background(), archived, t.TempDir(), DefaultRestoreOptions(), eopt)
	require.NoError(t, err)
	assert.Equal(t, 0, est.Selection.ObjectsToRestore)
	assert.Equal(t, 5, est.Selection.NoRetrievalFiles)
	assert.Equal(t, NoTier, est.Options[0].Tier)

	// Objects which aren't archived are available now.
	est, err = EstimateRestore(context.Background(), newDst(t, dst), t.TempDir(), DefaultRestoreOptions(), eopt)
	require.NoError(t, err)
	assert.Equal(t, 0, est.Selection.ObjectsToRestore)
	assert.Equal(t, 5, est.Selection.NoRetrievalFiles)
	assert.Equal(t, NoTier, est.Options[0].Tier)
}
