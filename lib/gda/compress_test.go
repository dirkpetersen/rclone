//go:build unix

package gda

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/rclone/rclone/fs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// smallFrames makes frames small enough for packs of a few files to
// have several.
func smallFrames(t *testing.T) {
	oldMin, oldMax := packFrameMin, frameMax
	packFrameMin, frameMax = 4096, 16384
	t.Cleanup(func() { packFrameMin, frameMax = oldMin, oldMax })
}

// writeText writes a compressible file of size bytes.
func writeText(t *testing.T, root, rel string, size int) {
	t.Helper()
	p := filepath.Join(root, filepath.FromSlash(rel))
	require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
	line := "ACGTACGGTTACGATCGATCGGCTAGCTAGGCTA " + rel + "\n"
	require.NoError(t, os.WriteFile(p, []byte(strings.Repeat(line, size/len(line)+1)[:size]), 0o644))
}

func writeRandom(t *testing.T, root, rel string, size int) {
	t.Helper()
	p := filepath.Join(root, filepath.FromSlash(rel))
	require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
	data := make([]byte, size)
	_, err := rand.Read(data)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(p, data, 0o644))
}

func compressOptions() Options {
	opt := testOptions()
	opt.Compression = CodecZstd
	opt.PackSize = 64 * 1024
	opt.StandaloneMin = 40 * 1024
	opt.RollupMax = 0
	return opt
}

func TestBackupCompression(t *testing.T) {
	fakeClock(t)
	smallFrames(t)
	src, dst := t.TempDir(), filepath.Join(t.TempDir(), "lab")
	for _, name := range []string{"a.txt", "b.txt", "c.txt", "d.txt"} {
		writeText(t, src, "text/"+name, 10000)
	}
	writeRandom(t, src, "random/a.bin", 10000)
	writeRandom(t, src, "random/b.bin", 10000)
	writeText(t, src, "big/reads.fastq", 50000)
	writeText(t, src, "big/reads.fastq.gz", 50000)
	l := runBackup(t, src, dst, compressOptions())
	assert.Greater(t, l.Stats.CompressedFrom, l.Stats.PackBytes)

	// Text is packed and compressed in several frames; each file's stored
	// range is only the frames holding it.
	text := readIndexFile(t, dst, "text")
	var packSize int64
	for name, e := range text {
		assert.Equal(t, CodecZstd, e.Codec, name)
		assert.True(t, strings.HasSuffix(e.Location, ".tar.zst"), name)
		info, err := os.Stat(filepath.Join(dst, "text", e.Location))
		require.NoError(t, err)
		packSize = info.Size()
		assert.LessOrEqual(t, e.StoredStart, e.Offset, name)
		assert.Less(t, e.StoredLength, packSize, name)
	}
	assert.Less(t, packSize, int64(40000))

	// Random data isn't worth compressing.
	for name, e := range readIndexFile(t, dst, "random") {
		assert.Equal(t, CodecNone, e.Codec, name)
		assert.True(t, strings.HasSuffix(e.Location, ".tar"), name)
	}

	// A large text file is compressed on its own; a .gz one isn't.
	big := readIndexFile(t, dst, "big")
	assert.Equal(t, "reads.fastq.gda.zst", big["reads.fastq"].Location)
	assert.Equal(t, CodecZstd, big["reads.fastq"].Codec)
	assert.Less(t, big["reads.fastq"].StoredSize, int64(50000))
	assert.Equal(t, "reads.fastq.gz", big["reads.fastq.gz"].Location)
	assert.Equal(t, CodecNone, big["reads.fastq.gz"].Codec)

	// The compressed pack is a standard zstd file holding a normal tar.
	var names []string
	for _, e := range text {
		in, err := os.Open(filepath.Join(dst, "text", e.Location))
		require.NoError(t, err)
		dec, err := zstd.NewReader(in)
		require.NoError(t, err)
		tr := tar.NewReader(dec)
		for {
			hdr, err := tr.Next()
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(t, err)
			names = append(names, hdr.Name)
		}
		dec.Close()
		require.NoError(t, in.Close())
		break
	}
	assert.Contains(t, names, "a.txt")

	// Everything comes back through both kinds of fetch.
	target := t.TempDir()
	st, err := StartRestore(context.Background(), newDst(t, dst), target, DefaultRestoreOptions())
	require.NoError(t, err)
	assert.Equal(t, StateDone, st.State)
	assertSameTree(t, src, target)
	one := DefaultRestoreOptions()
	one.Paths = []string{"text/c.txt"}
	other := t.TempDir()
	st, err = StartRestore(context.Background(), newDst(t, dst), other, one)
	require.NoError(t, err)
	assert.Equal(t, StateDone, st.State)
	want, _ := os.ReadFile(filepath.Join(src, "text/c.txt"))
	got, _ := os.ReadFile(filepath.Join(other, "text/c.txt"))
	assert.Equal(t, want, got)
}

func TestBackupCompressMax(t *testing.T) {
	fakeClock(t)
	src, dst := t.TempDir(), filepath.Join(t.TempDir(), "lab")
	writeText(t, src, "reads.fastq", 50000)
	writeText(t, src, "small.fastq", 42000)
	opt := compressOptions()
	opt.CompressMax = 45000
	runBackup(t, src, dst, opt)
	index := readIndexFile(t, dst, "")
	assert.Equal(t, "reads.fastq", index["reads.fastq"].Location)
	assert.Equal(t, CodecNone, index["reads.fastq"].Codec)
	assert.Equal(t, "small.fastq.gda.zst", index["small.fastq"].Location)
}

func TestBrowseCompressed(t *testing.T) {
	fakeClock(t)
	smallFrames(t)
	src, dst := t.TempDir(), filepath.Join(t.TempDir(), "lab")
	for _, name := range []string{"a.txt", "b.txt", "c.txt"} {
		writeText(t, src, name, 10000)
	}
	writeText(t, src, "big.fastq", 50000)
	runBackup(t, src, dst, compressOptions())
	b, err := NewBrowser(newDst(t, dst), "")
	require.NoError(t, err)
	entries, ok, err := b.List(context.Background(), "")
	require.NoError(t, err)
	require.True(t, ok)
	for i := range entries {
		l := &entries[i]
		want, err := os.ReadFile(filepath.Join(src, l.LocalPath))
		require.NoError(t, err)
		for _, r := range [][2]int64{{0, l.Size - 1}, {5000, 5099}, {l.Size - 10, l.Size - 1}} {
			in, err := b.Open(context.Background(), l, &fs.RangeOption{Start: r[0], End: r[1]})
			require.NoError(t, err)
			got, err := io.ReadAll(in)
			require.NoError(t, err)
			require.NoError(t, in.Close())
			assert.True(t, bytes.Equal(want[r[0]:r[1]+1], got), "%s %v", l.Path, r)
		}
	}
}

func TestFrameCuts(t *testing.T) {
	smallFrames(t)
	// Cut at the first boundary after packFrameMin, and at frameMax when
	// there is none.
	assert.Equal(t, []int64{0}, frameCuts(1000, []int64{0, 500}))
	assert.Equal(t, []int64{0, 5000, 10000}, frameCuts(12000, []int64{0, 3000, 5000, 7000, 10000}))
	assert.Equal(t, []int64{0, 16384, 32768}, frameCuts(40000, []int64{0}))
}

func TestCatalog(t *testing.T) {
	fakeClock(t)
	src := makeTree(t)
	dst := filepath.Join(t.TempDir(), "lab")
	opt := testOptions()
	opt.RollupMax = 0
	l := runBackup(t, src, dst, opt)
	in, err := os.Open(filepath.Join(dst, MetaDir, "catalog", "runs", l.RunID, "w01.csv.zst"))
	require.NoError(t, err)
	defer func() { require.NoError(t, in.Close()) }()
	dec, err := zstd.NewReader(in)
	require.NoError(t, err)
	defer dec.Close()
	records, err := csv.NewReader(dec).ReadAll()
	require.NoError(t, err)
	require.Greater(t, len(records), 1)
	assert.Equal(t, catalogColumns, records[0])
	rows := map[string][]string{}
	for _, r := range records[1:] {
		rows[r[0]] = r
	}
	a := rows["results/a.dat"]
	require.NotNil(t, a)
	assert.Equal(t, ActionAdd, a[2])
	assert.Equal(t, "3000", a[4])
	// The object column is the full key of the object holding the data.
	assert.FileExists(t, filepath.Join(dst, filepath.FromSlash(a[10])))
	assert.Equal(t, "results/big.bin", rows["results/big.bin"][10])
	assert.Equal(t, int64(len(records)-1), l.Stats.Added+l.Stats.Modified+l.Stats.MetaOnly+l.Stats.Deleted)
}

// writeMixed writes a file of size bytes which is half random, so it
// compresses by about half.
func writeMixed(t *testing.T, root, rel string, size int) {
	t.Helper()
	p := filepath.Join(root, filepath.FromSlash(rel))
	require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
	data := make([]byte, size)
	_, err := rand.Read(data[:size/2])
	require.NoError(t, err)
	copy(data[size/2:], strings.Repeat("ACGT", size))
	require.NoError(t, os.WriteFile(p, data, 0o644))
}

func TestRestoreSharedFrames(t *testing.T) {
	fakeClock(t)
	smallFrames(t)
	src, dst := t.TempDir(), filepath.Join(t.TempDir(), "lab")
	for i := range 12 {
		writeMixed(t, src, fmt.Sprintf("text/f%02d.txt", i), 1500)
	}
	opt := compressOptions()
	opt.DedupMin = 1000
	runBackup(t, src, dst, opt)
	// A copy made later refers to the data already stored.
	data, err := os.ReadFile(filepath.Join(src, "text/f00.txt"))
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Join(src, "copy"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(src, "copy/f00.txt"), data, 0o644))
	l := runBackup(t, src, dst, opt)
	require.Equal(t, int64(1), l.Stats.Deduplicated)

	// Two files in the first frame and the copy of one of them are read
	// with a single ranged read.
	ropt := restoreOptions("text/f00.txt", "text/f01.txt", "copy/f00.txt")
	p, err := PlanRestore(context.Background(), newDst(t, dst), t.TempDir(), ropt)
	require.NoError(t, err)
	var todo []int
	for i := range p.entries {
		if needsData(&p.entries[i]) {
			todo = append(todo, i)
		}
	}
	require.Len(t, todo, 3)
	spans := packSpans(p.entries, todo)
	require.Len(t, spans, 1)
	assert.Len(t, spans[0].members, 2)
	info, err := os.Stat(filepath.Join(dst, filepath.FromSlash(p.entries[todo[0]].Location)))
	require.NoError(t, err)
	_, requests := downloadPlan(p.entries, todo, info.Size())
	assert.Equal(t, 1, requests)

	target := t.TempDir()
	st, err := StartRestore(context.Background(), newDst(t, dst), target, ropt)
	require.NoError(t, err)
	assert.Equal(t, StateDone, st.State)
	for _, rel := range ropt.Paths {
		want, _ := os.ReadFile(filepath.Join(src, rel))
		got, err := os.ReadFile(filepath.Join(target, rel))
		require.NoError(t, err)
		assert.Equal(t, want, got, rel)
	}
}
