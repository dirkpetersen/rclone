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
	"strings"
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
	_, err := d.putObject(ctx, remote, size, modTime, md5sum, tier, open)
	return err
}

// putObject is put returning the uploaded object, which is nil for --dry-run.
func (d *dest) putObject(ctx context.Context, remote string, size int64, modTime time.Time, md5sum, tier string, open func() (io.ReadCloser, error)) (fs.Object, error) {
	if d.dryRun {
		fs.Logf(remote, "Not uploading as --dry-run is set")
		return nil, nil
	}
	var err error
	for try := 1; try <= d.retries; try++ {
		var o fs.Object
		o, err = d.putOnce(ctx, remote, size, modTime, md5sum, tier, open)
		if err == nil || ctx.Err() != nil {
			return o, err
		}
		fs.Errorf(remote, "Upload failed (try %d/%d): %v", try, d.retries, err)
	}
	return nil, err
}

// putOnce makes one upload attempt.
//
// TODO: each attempt is a new transfer in the stats, so retried uploads
// are counted more than once.
func (d *dest) putOnce(ctx context.Context, remote string, size int64, modTime time.Time, md5sum, tier string, open func() (io.ReadCloser, error)) (o fs.Object, err error) {
	in, err := open()
	if err != nil {
		return nil, err
	}
	tr := accounting.Stats(ctx).NewTransferRemoteSize(remote, size, nil, d.f)
	defer func() {
		tr.Done(ctx, err)
	}()
	acc := tr.Account(ctx, in)
	defer fs.CheckClose(acc, &err)
	if size < 0 {
		putStream := d.f.Features().PutStream
		if putStream == nil {
			return nil, errors.New("destination can't take uploads of unknown size")
		}
		return putStream(ctx, acc, d.info(remote, size, modTime, md5sum, tier))
	}
	return d.f.Put(ctx, acc, d.info(remote, size, modTime, md5sum, tier))
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
//
// The MD5 isn't known until the upload is done, so it can't be sent with
// the upload. Instead it is compared with the MD5 the backend reports for
// the stored object, where it reports one (for S3, single part uploads
// without SSE-KMS or SSE-C), and a mismatching object is removed.
func (d *dest) putSourceFile(ctx context.Context, remote string, e *sourceEntry, tier string) (string, error) {
	var hr *hashingReader
	o, err := d.putObject(ctx, remote, e.Size, e.ModTime, "", tier, func() (io.ReadCloser, error) {
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
	sum := hex.EncodeToString(hr.h.Sum(nil))
	stored, err := o.Hash(ctx, hashpkg.MD5)
	if err == nil && stored != "" && !strings.EqualFold(stored, sum) {
		if err := o.Remove(ctx); err != nil {
			fs.Errorf(remote, "gda: failed to remove corrupt upload: %v", err)
		}
		return "", fmt.Errorf("stored object has MD5 %s but %s was uploaded", stored, sum)
	}
	return sum, nil
}

// putCompressed uploads a source file to remote compressed with zstd, in
// a single read. It returns the MD5 of the file, and the MD5 and size of
// what was stored, checking them against the stored object where the
// backend reports them.
func (d *dest) putCompressed(ctx context.Context, remote string, e *sourceEntry, tier string, level int) (sum, storedSum string, storedSize int64, err error) {
	var cr *compressingReader
	o, err := d.putObject(ctx, remote, -1, e.ModTime, "", tier, func() (io.ReadCloser, error) {
		in, err := os.Open(e.path)
		if err != nil {
			return nil, err
		}
		cr, err = newCompressingReader(in, level)
		return cr, err
	})
	if err != nil || d.dryRun {
		return "", "", 0, err
	}
	sum = hex.EncodeToString(cr.src.Sum(nil))
	storedSum = hex.EncodeToString(cr.stored.Sum(nil))
	storedSize = cr.storedN
	bad := ""
	if size := o.Size(); size >= 0 && size != storedSize {
		bad = fmt.Sprintf("stored object has %d bytes but %d were uploaded", size, storedSize)
	} else if got, err := o.Hash(ctx, hashpkg.MD5); err == nil && got != "" && !strings.EqualFold(got, storedSum) {
		bad = fmt.Sprintf("stored object has MD5 %s but %s was uploaded", got, storedSum)
	}
	if bad != "" {
		if err := o.Remove(ctx); err != nil {
			fs.Errorf(remote, "gda: failed to remove corrupt upload: %v", err)
		}
		return "", "", 0, errors.New(bad)
	}
	return sum, storedSum, storedSize, nil
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

// writeIndex writes the index of the directory at dirKey for run runID.
func (d *dest) writeIndex(ctx context.Context, dirKey string, entries []Entry, runID string) error {
	objects, err := encodeIndex(entries, maxIndexRows, runID)
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

// writeEntries writes the columns cols of entries as a CSV object at remote.
func (d *dest) writeEntries(ctx context.Context, remote string, entries []Entry, cols []string) error {
	var buf bytes.Buffer
	if err := writeColumns(&buf, entries, cols); err != nil {
		return err
	}
	return d.putBytes(ctx, remote, buf.Bytes(), d.metaTier)
}

// checkTier checks that the object at remote was stored with the storage
// class want, for backends which report one. Remote configuration, such
// as the s3 storage_class option, can override the class GDA asks for.
func (d *dest) checkTier(ctx context.Context, remote, want string) error {
	if d.dryRun || want == "" {
		return nil
	}
	o, err := d.f.NewObject(ctx, remote)
	if err != nil {
		return err
	}
	tierer, ok := o.(fs.GetTierer)
	if !ok || !d.f.Features().GetTier {
		return nil
	}
	if got := tierer.GetTier(); !strings.EqualFold(got, want) {
		return fmt.Errorf("%q was stored as %s, not %s: remove storage_class from the destination remote's configuration", remote, got, want)
	}
	return nil
}
