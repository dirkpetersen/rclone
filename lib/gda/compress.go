package gda

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"os"
	"path"
	"strings"

	"github.com/klauspost/compress/zstd"
)

// Codecs other than CodecNone.
const (
	CodecZstd = "zstd"
)

// Frame sizes for compressed data. Frames are independent, so a file can
// be read by decompressing only the frames holding it. Tests lower them.
var (
	// packFrameMin is the size a pack's frame grows to before it is cut
	// at the next member.
	packFrameMin int64 = 1 << 20
	// frameMax is the largest a frame gets, cutting large files.
	frameMax int64 = 16 << 20
)

// trialBytes is how much of the data is compressed to decide whether
// compressing is worthwhile.
const trialBytes = 1 << 20

// minSaving is the share of the trial data compression must save.
const minSaving = 0.1

// compressedExtensions are file name extensions of formats which are
// compressed already, so aren't worth compressing again.
var compressedExtensions = map[string]bool{
	".gz": true, ".tgz": true, ".bgz": true, ".bz2": true, ".xz": true, ".zst": true,
	".zstd": true, ".lz4": true, ".lz": true, ".7z": true, ".zip": true, ".rar": true,
	".bam": true, ".cram": true, ".sra": true, ".parquet": true, ".npz": true,
	".jpg": true, ".jpeg": true, ".png": true, ".gif": true, ".webp": true, ".heic": true,
	".mp4": true, ".mkv": true, ".mov": true, ".avi": true, ".webm": true,
	".mp3": true, ".flac": true, ".ogg": true, ".m4a": true, ".pdf": true,
}

// isCompressedName returns true if name looks like an already compressed file.
func isCompressedName(name string) bool {
	return compressedExtensions[strings.ToLower(path.Ext(name))]
}

// newEncoder returns a zstd encoder for level, 1 to 22 as for the zstd
// command, for use by up to concurrency goroutines at once.
func newEncoder(level, concurrency int) (*zstd.Encoder, error) {
	return zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(level)), zstd.WithEncoderConcurrency(concurrency))
}

// worthCompressing returns true if compressing sample saves at least
// minSaving of its size.
func worthCompressing(enc *zstd.Encoder, sample []byte) bool {
	if len(sample) == 0 {
		return false
	}
	compressed := enc.EncodeAll(sample, nil)
	return float64(len(compressed)) <= float64(len(sample))*(1-minSaving)
}

// countingWriter counts and hashes the bytes written through it.
type countingWriter struct {
	w io.Writer
	n int64
	h hash.Hash
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	c.h.Write(p[:n])
	return n, err
}

// frame is one independent zstd frame of compressed data.
type frame struct {
	uStart int64 // offset of the frame's data in the uncompressed data
	cStart int64 // offset of the frame in the compressed data
	cLen   int64 // compressed length of the frame
}

// frameCuts returns where frames of data of size bytes start, cutting
// only at the given boundaries once a frame has reached packFrameMin, and
// anywhere at frameMax.
func frameCuts(size int64, boundaries []int64) []int64 {
	cuts := []int64{0}
	last := int64(0)
	i := 0
	for {
		for i < len(boundaries) && boundaries[i] < last+packFrameMin {
			i++
		}
		next := last + frameMax
		if i < len(boundaries) && boundaries[i] < next {
			next = boundaries[i]
		}
		if next >= size {
			return cuts
		}
		cuts = append(cuts, next)
		last = next
	}
}

// compressFile compresses the file at src into a new file in tempDir as
// independent zstd frames starting at cuts. It returns the new file's
// path, size, MD5 and frames.
func compressFile(enc *zstd.Encoder, src, tempDir string, size int64, cuts []int64) (dstPath string, dstSize int64, sum string, frames []frame, err error) {
	in, err := os.Open(src)
	if err != nil {
		return "", 0, "", nil, err
	}
	defer func() { _ = in.Close() }()
	out, err := os.CreateTemp(tempDir, "gda-pack-*.tar.zst")
	if err != nil {
		return "", 0, "", nil, err
	}
	defer func() {
		closeErr := out.Close()
		if err == nil {
			err = closeErr
		}
		if err != nil {
			_ = os.Remove(out.Name())
		}
	}()
	h := md5.New()
	w := io.MultiWriter(out, h)
	var buf, compressed []byte
	var cStart int64
	for n, uStart := range cuts {
		end := size
		if n+1 < len(cuts) {
			end = cuts[n+1]
		}
		buf = buf[:0]
		if int64(cap(buf)) < end-uStart {
			buf = make([]byte, 0, end-uStart)
		}
		buf = buf[:end-uStart]
		if _, err := io.ReadFull(in, buf); err != nil {
			return "", 0, "", nil, fmt.Errorf("read %q: %w", src, err)
		}
		compressed = enc.EncodeAll(buf, compressed[:0])
		if _, err := w.Write(compressed); err != nil {
			return "", 0, "", nil, err
		}
		frames = append(frames, frame{uStart: uStart, cStart: cStart, cLen: int64(len(compressed))})
		cStart += int64(len(compressed))
	}
	return out.Name(), cStart, hex.EncodeToString(h.Sum(nil)), frames, nil
}

// storedRange sets the stored range of e, whose data is at Offset in the
// uncompressed pack, to the frames holding it.
func storedRange(e *Entry, frames []frame) {
	if e.Type != TypeFile || e.Size <= 0 {
		e.StoredOffset, e.StoredLength, e.StoredStart = -1, -1, -1
		return
	}
	first, last := -1, -1
	for i := range frames {
		end := int64(1<<62 - 1)
		if i+1 < len(frames) {
			end = frames[i+1].uStart
		}
		if first < 0 && e.Offset < end {
			first = i
		}
		if e.Offset+e.Size-1 < end {
			last = i
			break
		}
	}
	e.StoredStart = frames[first].uStart
	e.StoredOffset = frames[first].cStart
	e.StoredLength = frames[last].cStart + frames[last].cLen - frames[first].cStart
}

// frameWriter compresses what is written to it as independent zstd
// frames of at most frameMax uncompressed bytes.
type frameWriter struct {
	enc   *zstd.Encoder
	out   io.Writer
	inFrm int64
}

func newFrameWriter(out io.Writer, level int) (*frameWriter, error) {
	enc, err := zstd.NewWriter(out, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(level)), zstd.WithEncoderConcurrency(1))
	if err != nil {
		return nil, err
	}
	return &frameWriter{enc: enc, out: out}, nil
}

func (w *frameWriter) Write(p []byte) (n int, err error) {
	for len(p) > 0 {
		// A full frame is ended only when more data comes, so Close
		// doesn't add an empty one.
		if w.inFrm >= frameMax {
			if err := w.enc.Close(); err != nil {
				return n, err
			}
			w.enc.Reset(w.out)
			w.inFrm = 0
		}
		chunk := min(int64(len(p)), frameMax-w.inFrm)
		written, err := w.enc.Write(p[:chunk])
		n += written
		w.inFrm += int64(written)
		if err != nil {
			return n, err
		}
		p = p[chunk:]
	}
	return n, nil
}

// Close ends the last frame.
func (w *frameWriter) Close() error {
	return w.enc.Close()
}

// compressingReader reads a file, compressing it as zstd frames. It
// records the MD5 of both the file and the compressed stream.
type compressingReader struct {
	pr      *io.PipeReader
	done    chan error
	src     hash.Hash
	stored  hash.Hash
	storedN int64
}

// newCompressingReader starts compressing in into a pipe.
func newCompressingReader(in io.ReadCloser, level int) (*compressingReader, error) {
	pr, pw := io.Pipe()
	r := &compressingReader{pr: pr, done: make(chan error, 1), src: md5.New(), stored: md5.New()}
	counter := &countingWriter{w: pw, h: r.stored}
	fw, err := newFrameWriter(counter, level)
	if err != nil {
		_ = in.Close()
		return nil, err
	}
	go func() {
		_, err := io.Copy(io.MultiWriter(fw, r.src), in)
		closeErr := fw.Close()
		if err == nil {
			err = closeErr
		}
		_ = in.Close()
		r.storedN = counter.n
		_ = pw.CloseWithError(err)
		r.done <- err
	}()
	return r, nil
}

func (r *compressingReader) Read(p []byte) (int, error) {
	return r.pr.Read(p)
}

// Close stops the compression, waiting for it to finish.
func (r *compressingReader) Close() error {
	_ = r.pr.Close()
	return <-r.done
}

// decompressRange returns a reader of length bytes starting skip bytes
// into the decompressed data from in, which must start at a frame.
func decompressRange(in io.ReadCloser, skip, length int64) (io.ReadCloser, error) {
	dec, err := zstd.NewReader(in, zstd.WithDecoderConcurrency(1))
	if err != nil {
		_ = in.Close()
		return nil, err
	}
	rc := &decompressor{dec: dec, in: in}
	if skip > 0 {
		if _, err := io.CopyN(io.Discard, dec, skip); err != nil {
			_ = rc.Close()
			return nil, fmt.Errorf("skip to data: %w", err)
		}
	}
	if length >= 0 {
		rc.r = io.LimitReader(dec, length)
	} else {
		rc.r = dec
	}
	return rc, nil
}

// decompressor closes both the decoder and the compressed stream.
type decompressor struct {
	dec *zstd.Decoder
	in  io.ReadCloser
	r   io.Reader
}

func (d *decompressor) Read(p []byte) (int, error) {
	return d.r.Read(p)
}

func (d *decompressor) Close() error {
	d.dec.Close()
	return d.in.Close()
}
