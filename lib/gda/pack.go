//go:build unix

package gda

import (
	"archive/tar"
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"time"
)

// tarBlockSize is the tar record block size.
const tarBlockSize = 512

// roundBlock rounds n up to a whole number of tar blocks.
func roundBlock(n int64) int64 {
	return (n + tarBlockSize - 1) / tarBlockSize * tarBlockSize
}

// packOverhead estimates the bytes a member adds to a pack beyond its
// data: a header block, padding of the data to the block size and a PAX
// header. The PAX header is always counted, as sub-second modification
// times, which are the norm, need one.
func packOverhead(e *Entry) int64 {
	pax := roundBlock(int64(100 + 2*(len(e.Name)+len(e.LinkTarget)+len(e.Owner)+len(e.Group))))
	return tarBlockSize + (roundBlock(max(e.Size, 0)) - max(e.Size, 0)) + tarBlockSize + pax
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

// packWriter writes one pack to a temporary file.
type packWriter struct {
	name          string
	file          *os.File
	cw            *countingWriter
	tw            *tar.Writer
	members       []Entry
	manifestBytes int64 // estimated size of the manifest so far
}

// manifestRowEstimate estimates the bytes e adds to a pack's manifest.
func manifestRowEstimate(e *Entry) int64 {
	n := int64(len(e.Name)+len(e.LinkTarget)+len(e.Owner)+len(e.Group)) * 2
	return n + 256
}

// fits returns true if e can be added without the finished pack,
// including its manifest and end of archive blocks, exceeding limit.
// An empty pack always fits.
func (p *packWriter) fits(e *Entry, limit int64) bool {
	if len(p.members) == 0 {
		return true
	}
	size := p.cw.n + max(e.Size, 0) + packOverhead(e)
	manifest := roundBlock(p.manifestBytes + manifestRowEstimate(e) + 512)
	// The manifest's header, and the two zero blocks ending the archive.
	trailer := int64(3 * tarBlockSize)
	return size+manifest+trailer <= limit
}

// newPackWriter creates a pack called name spooled in tempDir.
func newPackWriter(name, tempDir string) (*packWriter, error) {
	f, err := os.CreateTemp(tempDir, "gda-pack-*.tar")
	if err != nil {
		return nil, fmt.Errorf("create pack spool file: %w", err)
	}
	cw := &countingWriter{w: f, h: md5.New()}
	return &packWriter{
		name: name,
		file: f,
		cw:   cw,
		tw:   tar.NewWriter(cw),
	}, nil
}

// errSourceChanged is returned when a source file changed while being packed.
var errSourceChanged = errors.New("source changed while reading")

// add writes the source entry e to the pack. It fills in the entry's
// location, offset and MD5. If the file changed while it was being read
// it returns errSourceChanged and the entry must not be recorded.
func (p *packWriter) add(e *sourceEntry) error {
	hdr := &tar.Header{
		Name:    e.Name,
		Mode:    int64(e.Mode),
		Uid:     int(max(e.UID, 0)),
		Gid:     int(max(e.GID, 0)),
		Uname:   e.Owner,
		Gname:   e.Group,
		ModTime: e.ModTime,
		Format:  tar.FormatPAX,
	}
	switch e.Type {
	case TypeFile:
		hdr.Typeflag = tar.TypeReg
		hdr.Size = e.Size
	case TypeDir:
		// Only rolled up subdirectories are packed, so that plain tar
		// recreates them with their permissions, even when empty.
		hdr.Typeflag = tar.TypeDir
		hdr.Name = e.Name + "/"
	case TypeSymlink:
		hdr.Typeflag = tar.TypeSymlink
		hdr.Linkname = e.LinkTarget
	case TypeFifo:
		hdr.Typeflag = tar.TypeFifo
	case TypeCharDev:
		hdr.Typeflag = tar.TypeChar
		hdr.Devmajor, hdr.Devminor = e.DevMajor, e.DevMinor
	case TypeBlockDev:
		hdr.Typeflag = tar.TypeBlock
		hdr.Devmajor, hdr.Devminor = e.DevMajor, e.DevMinor
	default:
		return fmt.Errorf("pack %s: can't store %q of type %s", p.name, e.Name, e.Type)
	}
	if err := p.tw.WriteHeader(hdr); err != nil {
		return fmt.Errorf("pack %s: write header for %q: %w", p.name, e.Name, err)
	}
	e.Location = p.name
	e.Codec = CodecNone
	if e.Type != TypeFile {
		p.addMember(e)
		return nil
	}
	// The data starts after the header, which archive/tar has written
	// out in full by now.
	e.Offset = p.cw.n
	in, err := os.Open(e.path)
	if err != nil {
		return p.padMember(e, 0, err)
	}
	h := md5.New()
	n, copyErr := io.CopyN(io.MultiWriter(p.tw, h), in, e.Size)
	closeErr := in.Close()
	if copyErr != nil {
		return p.padMember(e, n, copyErr)
	}
	if closeErr != nil {
		return p.padMember(e, n, closeErr)
	}
	info, err := os.Lstat(e.path)
	if err != nil || info.Size() != e.Size || !info.ModTime().Equal(e.ModTime) {
		return errSourceChanged
	}
	e.MD5 = hex.EncodeToString(h.Sum(nil))
	e.StoredOffset = e.Offset
	e.StoredLength = e.Size
	p.addMember(e)
	return nil
}

// addMember records e in the pack's manifest.
func (p *packWriter) addMember(e *sourceEntry) {
	p.members = append(p.members, e.Entry)
	p.manifestBytes += manifestRowEstimate(&e.Entry)
}

// padMember fills the rest of a member whose file couldn't be read in
// full with zeros, so the tar stays valid, and returns the read error.
func (p *packWriter) padMember(e *sourceEntry, written int64, err error) error {
	if _, padErr := io.CopyN(p.tw, zeroReader{}, e.Size-written); padErr != nil {
		return fmt.Errorf("pack %s: pad %q: %w", p.name, e.Name, padErr)
	}
	if errors.Is(err, io.EOF) {
		return errSourceChanged
	}
	return fmt.Errorf("read %q: %w", e.path, err)
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

// finish appends the pack's own manifest as the last member and closes
// the tar. It returns the pack's size and MD5.
func (p *packWriter) finish(now time.Time) (int64, string, error) {
	var manifest bytes.Buffer
	if err := WriteEntries(&manifest, p.members); err != nil {
		return 0, "", err
	}
	hdr := &tar.Header{
		Typeflag: tar.TypeReg,
		Name:     p.name + ".csv",
		Mode:     0o644,
		Size:     int64(manifest.Len()),
		// Whole seconds, so the manifest needs no PAX header.
		ModTime: now.Truncate(time.Second),
		Format:  tar.FormatPAX,
	}
	if err := p.tw.WriteHeader(hdr); err != nil {
		return 0, "", err
	}
	if _, err := p.tw.Write(manifest.Bytes()); err != nil {
		return 0, "", err
	}
	if err := p.tw.Close(); err != nil {
		return 0, "", err
	}
	if err := p.file.Sync(); err != nil {
		return 0, "", err
	}
	return p.cw.n, hex.EncodeToString(p.cw.h.Sum(nil)), nil
}

// remove deletes the spool file.
func (p *packWriter) remove() {
	_ = p.file.Close()
	_ = os.Remove(p.file.Name())
}
