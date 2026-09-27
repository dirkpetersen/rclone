//go:build unix

package gda

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	_ "github.com/rclone/rclone/backend/local"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/fs/object"
	"github.com/rclone/rclone/fs/sync"
	libgda "github.com/rclone/rclone/lib/gda"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeFile(t *testing.T, root, rel string, size int) []byte {
	t.Helper()
	p := filepath.Join(root, filepath.FromSlash(rel))
	require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(len(rel)*7 + i)
	}
	require.NoError(t, os.WriteFile(p, data, 0o644))
	return data
}

// backup makes a GDA backup of src below dst and returns the run ID.
func backup(t *testing.T, src, dst string) string {
	t.Helper()
	f, err := fs.NewFs(context.Background(), dst)
	require.NoError(t, err)
	opt := libgda.DefaultOptions()
	opt.PackSize = 10 * 1024
	opt.StandaloneMin = 8 * 1024
	opt.RollupMax = 64
	ledger, err := libgda.Backup(context.Background(), src, f, opt)
	require.NoError(t, err)
	// Run IDs have a resolution of one second.
	time.Sleep(1100 * time.Millisecond)
	return ledger.RunID
}

func names(t *testing.T, entries fs.DirEntries) []string {
	t.Helper()
	var out []string
	for _, e := range entries {
		out = append(out, e.Remote())
	}
	sort.Strings(out)
	return out
}

func makeSource(t *testing.T) string {
	src := t.TempDir()
	writeFile(t, src, "README.txt", 10)
	writeFile(t, src, "results/a.dat", 3000)
	writeFile(t, src, "results/big.bin", 9000)
	writeFile(t, src, "tiny/a/b.txt", 5)
	require.NoError(t, os.Symlink("../README.txt", filepath.Join(src, "results/link")))
	return src
}

func TestBrowse(t *testing.T) {
	ctx := context.Background()
	src := makeSource(t)
	root := t.TempDir()
	dst := filepath.Join(root, "lab")
	backup(t, src, dst)
	f, err := fs.NewFs(ctx, ":gda:"+dst)
	require.NoError(t, err)

	// The files, not the packs and CSVs holding them.
	entries, err := f.List(ctx, "")
	require.NoError(t, err)
	assert.Equal(t, []string{"README.txt", "results", "tiny"}, names(t, entries))
	entries, err = f.List(ctx, "results")
	require.NoError(t, err)
	assert.Equal(t, []string{"results/a.dat", "results/big.bin"}, names(t, entries))

	// A rolled up directory, listed from its ancestor's index.
	entries, err = f.List(ctx, "tiny/a")
	require.NoError(t, err)
	assert.Equal(t, []string{"tiny/a/b.txt"}, names(t, entries))
	_, err = f.List(ctx, "tiny/missing")
	assert.ErrorIs(t, err, fs.ErrorDirNotFound)

	// Directories carry the subtree totals.
	entries, err = f.List(ctx, "")
	require.NoError(t, err)
	for _, e := range entries {
		if d, ok := e.(fs.Directory); ok && d.Remote() == "results" {
			assert.Equal(t, int64(12000), d.Size())
			assert.Equal(t, int64(3), d.Items())
		}
	}

	// Files read back whole and in part, from packs and standalone.
	for _, rel := range []string{"results/a.dat", "results/big.bin", "README.txt", "tiny/a/b.txt"} {
		want, err := os.ReadFile(filepath.Join(src, rel))
		require.NoError(t, err)
		o, err := f.NewObject(ctx, rel)
		require.NoError(t, err, rel)
		assert.Equal(t, int64(len(want)), o.Size())
		sum := md5.Sum(want)
		got, err := o.Hash(ctx, hash.MD5)
		require.NoError(t, err)
		assert.Equal(t, hex.EncodeToString(sum[:]), got, rel)
		info, err := os.Stat(filepath.Join(src, rel))
		require.NoError(t, err)
		assert.True(t, info.ModTime().Equal(o.ModTime(ctx)), rel)

		data := readAll(t, o)
		assert.Equal(t, want, data, rel)
		data = readAll(t, o, &fs.RangeOption{Start: 2, End: 5})
		assert.Equal(t, want[2:min(6, len(want))], data, rel)
		data = readAll(t, o, &fs.SeekOption{Offset: 3})
		assert.Equal(t, want[3:], data, rel)
		data = readAll(t, o, &fs.RangeOption{Start: -1, End: 4})
		assert.Equal(t, want[len(want)-4:], data, rel)
	}
	_, err = f.NewObject(ctx, "results/nope")
	assert.ErrorIs(t, err, fs.ErrorObjectNotFound)
	_, err = f.NewObject(ctx, "results")
	assert.ErrorIs(t, err, fs.ErrorIsDir)

	// Metadata from the index.
	o, err := f.NewObject(ctx, "results/a.dat")
	require.NoError(t, err)
	m, err := o.(fs.Metadataer).Metadata(ctx)
	require.NoError(t, err)
	assert.Equal(t, "100644", m["mode"])
	assert.True(t, strings.HasPrefix(m["gda-location"], "results.gda."))

	// Read only.
	_, err = f.Put(ctx, strings.NewReader("x"), object.NewStaticObjectInfo("new", time.Now(), 1, true, nil, nil))
	assert.ErrorIs(t, err, errReadOnly)
	assert.ErrorIs(t, o.Remove(ctx), errReadOnly)
	assert.ErrorIs(t, f.Mkdir(ctx, "x"), errReadOnly)
}

func readAll(t *testing.T, o fs.Object, options ...fs.OpenOption) []byte {
	t.Helper()
	in, err := o.Open(context.Background(), options...)
	require.NoError(t, err)
	data, err := io.ReadAll(in)
	require.NoError(t, err)
	require.NoError(t, in.Close())
	return data
}

func TestBrowsePassThrough(t *testing.T) {
	ctx := context.Background()
	src := makeSource(t)
	root := t.TempDir()
	backup(t, src, filepath.Join(root, "lab"))
	writeFile(t, root, "plain/notes.txt", 7)

	// Above the GDA root, and beside it, the remote is shown as it is.
	f, err := fs.NewFs(ctx, ":gda:"+root)
	require.NoError(t, err)
	entries, err := f.List(ctx, "")
	require.NoError(t, err)
	assert.Equal(t, []string{"lab", "plain"}, names(t, entries))
	entries, err = f.List(ctx, "plain")
	require.NoError(t, err)
	assert.Equal(t, []string{"plain/notes.txt"}, names(t, entries))
	o, err := f.NewObject(ctx, "plain/notes.txt")
	require.NoError(t, err)
	assert.Equal(t, int64(7), o.Size())

	// Below it, the backup is shown as files.
	entries, err = f.List(ctx, "lab/results")
	require.NoError(t, err)
	assert.Equal(t, []string{"lab/results/a.dat", "lab/results/big.bin"}, names(t, entries))
	entries, err = f.List(ctx, "lab/tiny/a")
	require.NoError(t, err)
	assert.Equal(t, []string{"lab/tiny/a/b.txt"}, names(t, entries))

	// show_internals shows what is stored.
	internal, err := fs.NewFs(ctx, ":gda,show_internals:"+root)
	require.NoError(t, err)
	entries, err = internal.List(ctx, "lab")
	require.NoError(t, err)
	assert.Contains(t, names(t, entries), "lab/gda-index.csv")
}

func TestBrowseAt(t *testing.T) {
	ctx := context.Background()
	src := makeSource(t)
	dst := filepath.Join(t.TempDir(), "lab")
	first := backup(t, src, dst)
	old, err := os.ReadFile(filepath.Join(src, "results/a.dat"))
	require.NoError(t, err)
	writeFile(t, src, "results/a.dat", 100)
	writeFile(t, src, "results/new.dat", 50)
	backup(t, src, dst)

	f, err := fs.NewFs(ctx, ":gda,at="+first+":"+dst)
	require.NoError(t, err)
	entries, err := f.List(ctx, "results")
	require.NoError(t, err)
	assert.Equal(t, []string{"results/a.dat", "results/big.bin"}, names(t, entries))
	o, err := f.NewObject(ctx, "results/a.dat")
	require.NoError(t, err)
	assert.Equal(t, old, readAll(t, o))
}

func TestBrowseCopy(t *testing.T) {
	ctx := context.Background()
	src := makeSource(t)
	dst := filepath.Join(t.TempDir(), "lab")
	backup(t, src, dst)
	f, err := fs.NewFs(ctx, ":gda:"+dst)
	require.NoError(t, err)
	out := t.TempDir()
	local, err := fs.NewFs(ctx, out)
	require.NoError(t, err)

	// rclone copy out of the GDA view gives back every regular file.
	require.NoError(t, sync.CopyDir(ctx, local, f, false))
	for _, rel := range []string{"README.txt", "results/a.dat", "results/big.bin", "tiny/a/b.txt"} {
		want, _ := os.ReadFile(filepath.Join(src, rel))
		got, err := os.ReadFile(filepath.Join(out, rel))
		require.NoError(t, err, rel)
		assert.Equal(t, want, got, rel)
	}
}

func TestBrowseRoots(t *testing.T) {
	ctx := context.Background()
	src := makeSource(t)
	root := t.TempDir()
	dst := filepath.Join(root, "lab")
	backup(t, src, dst)
	writeFile(t, root, "plain/notes.txt", 7)

	// A root inside a rolled up subtree, as front ends pass the directory
	// being browsed as the root.
	f, err := fs.NewFs(ctx, ":gda:"+dst+"/tiny/a")
	require.NoError(t, err)
	entries, err := f.List(ctx, "")
	require.NoError(t, err)
	assert.Equal(t, []string{"b.txt"}, names(t, entries))
	o, err := f.NewObject(ctx, "b.txt")
	require.NoError(t, err)
	want, _ := os.ReadFile(filepath.Join(src, "tiny/a/b.txt"))
	assert.Equal(t, want, readAll(t, o))

	// A root naming a file, as rclone cat uses it.
	f, err = fs.NewFs(ctx, ":gda:"+dst+"/results/a.dat")
	assert.ErrorIs(t, err, fs.ErrorIsFile)
	require.NotNil(t, f)
	o, err = f.NewObject(ctx, "a.dat")
	require.NoError(t, err)
	want, _ = os.ReadFile(filepath.Join(src, "results/a.dat"))
	assert.Equal(t, want, readAll(t, o))

	// And one outside any GDA tree.
	f, err = fs.NewFs(ctx, ":gda:"+root+"/plain/notes.txt")
	assert.ErrorIs(t, err, fs.ErrorIsFile)
	o, err = f.NewObject(ctx, "notes.txt")
	require.NoError(t, err)
	assert.Equal(t, "notes.txt", o.Remote())
	assert.Equal(t, int64(7), o.Size())
	assert.ErrorIs(t, o.Remove(ctx), errReadOnly)
}
