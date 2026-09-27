//go:build unix

package gda

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheck(t *testing.T) {
	fakeClock(t)
	src := makeTree(t)
	dst := filepath.Join(t.TempDir(), "lab")
	opt := testOptions()
	opt.RollupMax = 0
	runBackup(t, src, dst, opt)
	f := newDst(t, dst)

	r, err := Check(context.Background(), f, "", "", CheckOptions{Download: true})
	require.NoError(t, err)
	assert.False(t, r.Failed())
	assert.Equal(t, int64(5), r.Files)
	assert.Equal(t, int64(5), r.Downloaded)

	// A missing pack, a truncated standalone object and a pack with a
	// corrupted member are found.
	index := readIndexFile(t, dst, "results")
	require.NoError(t, os.Truncate(filepath.Join(dst, "results", index["big.bin"].Location), 100))
	data, err := os.ReadFile(filepath.Join(dst, "results", index["a.dat"].Location))
	require.NoError(t, err)
	data[index["a.dat"].Offset] ^= 0xff
	require.NoError(t, os.WriteFile(filepath.Join(dst, "results", index["a.dat"].Location), data, 0o644))
	require.NoError(t, os.Remove(filepath.Join(dst, "tiny", "a", readIndexFile(t, dst, "tiny/a")["b.txt"].Location)))

	r, err = Check(context.Background(), f, "", "", CheckOptions{Download: true})
	require.NoError(t, err)
	assert.True(t, r.Failed())
	assert.Equal(t, []string{"tiny/a/b.txt"}, r.Missing)
	assert.Equal(t, []string{"results/big.bin"}, r.WrongSize)
	assert.Contains(t, r.BadMD5, "results/a.dat")

	// Without downloading only what the listings show is checked.
	r, err = Check(context.Background(), f, "results", "", CheckOptions{})
	require.NoError(t, err)
	assert.Empty(t, r.Missing)
	assert.Equal(t, []string{"big.bin"}, r.WrongSize)
	assert.Zero(t, r.Downloaded)
}
