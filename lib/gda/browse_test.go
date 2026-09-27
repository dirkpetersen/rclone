//go:build unix

package gda

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/rclone/rclone/fs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// bucketFs is a bucket based Fs whose root can't hold objects, like S3
// wrapped above its buckets, where reading a key "" is an error.
type bucketFs struct {
	fs.Fs
}

func (f *bucketFs) Features() *fs.Features {
	features := *f.Fs.Features()
	features.BucketBased = true
	return &features
}

// Root is "" as the Fs is above the buckets.
func (f *bucketFs) Root() string {
	return ""
}

func (f *bucketFs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	if remote == IndexName {
		return nil, errors.New("input member Key must not be empty")
	}
	return f.Fs.NewObject(ctx, remote)
}

func TestBrowserBucketRootAndTiers(t *testing.T) {
	fakeClock(t)
	src := makeTree(t)
	root := t.TempDir()
	dst := filepath.Join(root, "lab")
	runBackup(t, src, dst, testOptions())
	writeFile(t, root, "plain/notes.txt", 3)

	// Listing outside GDA trees never reads an index above the buckets.
	b, err := NewBrowser(&bucketFs{Fs: newDst(t, root)}, "")
	require.NoError(t, err)
	_, ok, err := b.List(context.Background(), "plain")
	require.NoError(t, err)
	assert.False(t, ok)

	// Tiers come from one listing of the directory holding the objects.
	archived := &pendingFs{Fs: newDst(t, dst)}
	b, err = NewBrowser(archived, "")
	require.NoError(t, err)
	tiers, err := b.Tiers(context.Background(), "results")
	require.NoError(t, err)
	require.NotEmpty(t, tiers)
	for key, tier := range tiers {
		assert.Equal(t, "DEEP_ARCHIVE", tier, key)
	}
}

func TestBrowseUnderRetiredIndexes(t *testing.T) {
	fakeClock(t)
	src, dst := t.TempDir(), filepath.Join(t.TempDir(), "lab")
	opt := testOptions()
	opt.RollupMax = 64
	writeFile(t, src, "other.txt", 100)
	writeFile(t, src, "p/d/big.txt", 100)
	writeFile(t, src, "p/d/sub/x.txt", 5)
	runBackup(t, src, dst, opt)
	// p shrinks enough to be packed as one unit, retiring the indexes
	// of the directories below it.
	require.NoError(t, os.Remove(filepath.Join(src, "p/d/big.txt")))
	runBackup(t, src, dst, opt)
	require.Equal(t, ListingRollup, readIndexFile(t, dst, "p")["d/sub"].Listing)

	b, err := NewBrowser(newDst(t, dst), "")
	require.NoError(t, err)
	entries, ok, err := b.List(context.Background(), "p/d/sub")
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, entries, 1)
	assert.Equal(t, "x.txt", entries[0].Path)
}
