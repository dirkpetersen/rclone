package gda

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/accounting"
	hashpkg "github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/fs/object"
)

// tieredInfo is an ObjectInfo which asks the backend to store the
// object in a given storage class.
type tieredInfo struct {
	*object.StaticObjectInfo
	tier string
}

// GetTier returns the storage class to upload with.
func (t tieredInfo) GetTier() string {
	return t.tier
}

var _ fs.GetTierer = tieredInfo{}

// dest writes and reads GDA objects on the destination.
type dest struct {
	f        fs.Fs
	dataTier string // storage class for packs and standalone files
	metaTier string // storage class for changesets, indexes and run files
	dryRun   bool
	retries  int
}

func (d *dest) info(remote string, size int64, modTime time.Time, md5sum, tier string) tieredInfo {
	var hashes map[hashpkg.Type]string
	if md5sum != "" {
		hashes = map[hashpkg.Type]string{hashpkg.MD5: md5sum}
	}
	return tieredInfo{
		StaticObjectInfo: object.NewStaticObjectInfo(remote, modTime, size, true, hashes, d.f),
		tier:             tier,
	}
}

// put uploads the content from open to remote, retrying with a fresh
// reader on failure.
func (d *dest) put(ctx context.Context, remote string, size int64, modTime time.Time, md5sum, tier string, open func() (io.ReadCloser, error)) error {
	if d.dryRun {
		fs.Logf(remote, "Not uploading as --dry-run is set")
		return nil
	}
	var err error
	for try := 1; try <= d.retries; try++ {
		err = d.putOnce(ctx, remote, size, modTime, md5sum, tier, open)
		if err == nil || ctx.Err() != nil {
			return err
		}
		fs.Errorf(remote, "Upload failed (try %d/%d): %v", try, d.retries, err)
	}
	return err
}

func (d *dest) putOnce(ctx context.Context, remote string, size int64, modTime time.Time, md5sum, tier string, open func() (io.ReadCloser, error)) (err error) {
	in, err := open()
	if err != nil {
		return err
	}
	tr := accounting.Stats(ctx).NewTransferRemoteSize(remote, size, nil, d.f)
	defer func() {
		tr.Done(ctx, err)
	}()
	acc := tr.Account(ctx, in)
	defer fs.CheckClose(acc, &err)
	_, err = d.f.Put(ctx, acc, d.info(remote, size, modTime, md5sum, tier))
	return err
}

// putBytes uploads data to remote.
func (d *dest) putBytes(ctx context.Context, remote string, data []byte, tier string) error {
	sum := md5.Sum(data)
	return d.put(ctx, remote, int64(len(data)), time.Now(), hex.EncodeToString(sum[:]), tier, func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(data)), nil
	})
}

// putFile uploads the local file at p, whose MD5 is known, to remote.
func (d *dest) putFile(ctx context.Context, remote, p string, size int64, md5sum, tier string) error {
	return d.put(ctx, remote, size, time.Now(), md5sum, tier, func() (io.ReadCloser, error) {
		return os.Open(p)
	})
}

// hashingReader hashes what is read through it.
type hashingReader struct {
	io.ReadCloser
	h hash.Hash
}

func (r *hashingReader) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	r.h.Write(p[:n])
	return n, err
}

// putSourceFile uploads a source file to remote in a single read and
// returns the MD5 of what was read.
func (d *dest) putSourceFile(ctx context.Context, remote string, e *sourceEntry, tier string) (string, error) {
	var hr *hashingReader
	err := d.put(ctx, remote, e.Size, e.ModTime, "", tier, func() (io.ReadCloser, error) {
		in, err := os.Open(e.path)
		if err != nil {
			return nil, err
		}
		hr = &hashingReader{ReadCloser: in, h: md5.New()}
		return hr, nil
	})
	if err != nil || d.dryRun {
		return "", err
	}
	return hex.EncodeToString(hr.h.Sum(nil)), nil
}

// get reads the whole object at remote. It returns fs.ErrorObjectNotFound
// if it doesn't exist.
func (d *dest) get(ctx context.Context, remote string) (data []byte, err error) {
	o, err := d.f.NewObject(ctx, remote)
	if err != nil {
		return nil, err
	}
	in, err := o.Open(ctx)
	if err != nil {
		return nil, err
	}
	defer fs.CheckClose(in, &err)
	return io.ReadAll(in)
}

// exists returns true if an object exists at remote.
func (d *dest) exists(ctx context.Context, remote string) (bool, error) {
	_, err := d.f.NewObject(ctx, remote)
	if errors.Is(err, fs.ErrorObjectNotFound) {
		return false, nil
	}
	return err == nil, err
}

// readIndex reads the index of the directory at dirKey. It returns nil
// entries and no error if there is no index.
func (d *dest) readIndex(ctx context.Context, dirKey string) ([]Entry, error) {
	data, err := d.get(ctx, joinRemote(dirKey, IndexName))
	if errors.Is(err, fs.ErrorObjectNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read index of %q: %w", dirKey, err)
	}
	if !isTOC(data) {
		entries, err := ReadEntries(bytes.NewReader(data))
		if err != nil {
			return nil, fmt.Errorf("parse index of %q: %w", dirKey, err)
		}
		return entries, nil
	}
	parts, err := readTOC(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("index of %q: %w", dirKey, err)
	}
	var entries []Entry
	for _, part := range parts {
		data, err := d.get(ctx, joinRemote(dirKey, part.name))
		if err != nil {
			return nil, fmt.Errorf("read index part %q of %q: %w", part.name, dirKey, err)
		}
		partEntries, err := ReadEntries(bytes.NewReader(data))
		if err != nil {
			return nil, fmt.Errorf("parse index part %q of %q: %w", part.name, dirKey, err)
		}
		entries = append(entries, partEntries...)
	}
	return entries, nil
}

// writeIndex writes the index of the directory at dirKey.
func (d *dest) writeIndex(ctx context.Context, dirKey string, entries []Entry) error {
	objects, err := encodeIndex(entries, maxIndexRows)
	if err != nil {
		return err
	}
	// Write the parts before the table of contents that refers to them.
	for name, data := range objects {
		if name == IndexName {
			continue
		}
		if err := d.putBytes(ctx, joinRemote(dirKey, name), data, d.metaTier); err != nil {
			return err
		}
	}
	return d.putBytes(ctx, joinRemote(dirKey, IndexName), objects[IndexName], d.metaTier)
}

// writeEntries writes entries as a CSV object at remote.
func (d *dest) writeEntries(ctx context.Context, remote string, entries []Entry) error {
	var buf bytes.Buffer
	if err := WriteEntries(&buf, entries); err != nil {
		return err
	}
	return d.putBytes(ctx, remote, buf.Bytes(), d.metaTier)
}
