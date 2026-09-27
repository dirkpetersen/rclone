//go:build unix

package gda

import (
	"context"
	"crypto/md5"
	"encoding/csv"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"

	"github.com/rclone/rclone/fs"
)

// Identical files of at least Options.DedupMin bytes are stored once per
// destination. Each run writes the copies it stored to
// _gda/dedup/<run>-<worker>.csv, and the dedup index is the union of
// those files. A duplicate gets an index row whose dedup_of names the
// object holding the copy, with that copy's offsets.

// dedupDir is where the dedup index lives.
var dedupDir = joinRemote(MetaDir, "dedup")

// dedupIndex finds stored copies by size and MD5. It is safe for
// concurrent use. It can hold many millions of copies, so it keeps only
// what a reference to a copy needs, and shares repeated strings.
type dedupIndex struct {
	mu      sync.Mutex
	bySum   map[dedupKey]dedupCopy
	sizes   map[int64]struct{} // sizes of stored copies
	strings map[string]string  // one copy of each location and codec
	added   []Entry            // copies stored by this run
}

// dedupKey identifies content by its MD5 and size.
type dedupKey struct {
	md5  [md5.Size]byte
	size int64
}

// dedupCopy is where a copy is stored: the fields of its row which a
// reference to it copies.
type dedupCopy struct {
	location, codec, storedMD5, versionID           string
	offset, storedOffset, storedLength, storedStart int64
	storedSize                                      int64
}

// keyOf returns the key of content with md5sum and size, or false if
// md5sum isn't a hex MD5.
func keyOf(md5sum string, size int64) (dedupKey, bool) {
	k := dedupKey{size: size}
	if len(md5sum) != 2*md5.Size {
		return k, false
	}
	n, err := hex.Decode(k.md5[:], []byte(md5sum))
	return k, err == nil && n == md5.Size
}

// intern returns the shared copy of s. It must be called with idx.mu
// held.
func (idx *dedupIndex) intern(s string) string {
	if shared, ok := idx.strings[s]; ok {
		return shared
	}
	s = strings.Clone(s)
	idx.strings[s] = s
	return s
}

// put records row, whose location is a full key, as a stored copy,
// unless one is recorded already. It must be called with idx.mu held.
func (idx *dedupIndex) put(row *Entry) bool {
	k, ok := keyOf(row.MD5, row.Size)
	if !ok {
		return false
	}
	if _, ok := idx.bySum[k]; ok {
		return false
	}
	idx.bySum[k] = dedupCopy{
		location:     idx.intern(row.Location),
		codec:        idx.intern(row.Codec),
		storedMD5:    strings.Clone(row.StoredMD5),
		versionID:    strings.Clone(row.VersionID),
		offset:       row.Offset,
		storedOffset: row.StoredOffset,
		storedLength: row.StoredLength,
		storedStart:  row.StoredStart,
		storedSize:   row.StoredSize,
	}
	idx.sizes[row.Size] = struct{}{}
	return true
}

func newDedupIndex() *dedupIndex {
	return &dedupIndex{bySum: map[dedupKey]dedupCopy{}, sizes: map[int64]struct{}{}, strings: map[string]string{}}
}

// dedupName is the key of a copy in the dedup index.
func dedupName(md5sum string, size int64) string {
	return md5sum + "-" + strconv.FormatInt(size, 10)
}

// loadDedup reads the dedup index of the destination.
func loadDedup(ctx context.Context, d *dest) (*dedupIndex, error) {
	idx := newDedupIndex()
	entries, err := d.f.List(ctx, dedupDir)
	if errors.Is(err, fs.ErrorDirNotFound) {
		return idx, nil
	}
	if err != nil {
		return nil, fmt.Errorf("list dedup index: %w", err)
	}
	for _, entry := range entries {
		o, ok := entry.(fs.Object)
		if !ok || path.Ext(o.Remote()) != ".csv" {
			continue
		}
		if err := idx.load(ctx, o); err != nil {
			return nil, fmt.Errorf("read dedup index %q: %w", o.Remote(), err)
		}
	}
	return idx, nil
}

// load adds the copies listed in the dedup index file o, reading it as
// a stream, as it can be large.
func (idx *dedupIndex) load(ctx context.Context, o fs.Object) (err error) {
	in, err := o.Open(ctx)
	if err != nil {
		return err
	}
	defer fs.CheckClose(in, &err)
	idx.mu.Lock()
	defer idx.mu.Unlock()
	return readEntriesFunc(in, func(row *Entry) error {
		idx.put(row)
		return nil
	})
}

// mayHave returns true if a copy of this size is stored, so a file of
// this size is worth hashing to look for its copy.
func (idx *dedupIndex) mayHave(size int64) bool {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	_, ok := idx.sizes[size]
	return ok
}

// find returns the stored copy of content with md5sum and size.
func (idx *dedupIndex) find(md5sum string, size int64) (Entry, bool) {
	k, ok := keyOf(md5sum, size)
	if !ok {
		return Entry{}, false
	}
	idx.mu.Lock()
	c, ok := idx.bySum[k]
	idx.mu.Unlock()
	if !ok {
		return Entry{}, false
	}
	return copyRow(k, c), true
}

// copyRow returns the dedup index row of the copy c of content k.
func copyRow(k dedupKey, c dedupCopy) Entry {
	sum := hex.EncodeToString(k.md5[:])
	row := NewEntry(dedupName(sum, k.size), TypeFile)
	row.Size, row.MD5 = k.size, sum
	row.Location, row.Codec, row.StoredMD5, row.VersionID = c.location, c.codec, c.storedMD5, c.versionID
	row.Offset, row.StoredOffset, row.StoredLength, row.StoredStart = c.offset, c.storedOffset, c.storedLength, c.storedStart
	row.StoredSize = c.storedSize
	return row
}

// write uploads every copy in the index to remote, through a temporary
// file, as it can be large.
func (idx *dedupIndex) write(ctx context.Context, d *dest, remote string) (err error) {
	tmp, err := os.CreateTemp(d.tempDir, "gda-dedup-*.csv")
	if err != nil {
		return err
	}
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
	}()
	h := md5.New()
	cw := csv.NewWriter(io.MultiWriter(tmp, h))
	if err := cw.Write(columns); err != nil {
		return err
	}
	record := make([]string, len(columns))
	idx.mu.Lock()
	for k, c := range idx.bySum {
		row := copyRow(k, c)
		for i, col := range columns {
			record[i] = row.value(col)
		}
		if err = cw.Write(record); err != nil {
			break
		}
	}
	idx.mu.Unlock()
	if err != nil {
		return err
	}
	cw.Flush()
	if err := cw.Error(); err != nil {
		return err
	}
	size, err := tmp.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return d.putFile(ctx, remote, tmp.Name(), size, hex.EncodeToString(h.Sum(nil)), d.metaTier)
}

// add records the copy in row, stored in the directory at key.
func (idx *dedupIndex) add(key string, row Entry) {
	if row.MD5 == "" || row.Location == "" {
		return
	}
	copyRow := row
	copyRow.Name = dedupName(row.MD5, row.Size)
	copyRow.Location = joinRemote(key, row.Location)
	copyRow.Action, copyRow.Target, copyRow.DedupOf = "", "", ""
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if idx.put(&copyRow) {
		idx.added = append(idx.added, copyRow)
	}
}

// save writes the copies stored by this run.
func (idx *dedupIndex) save(ctx context.Context, d *dest, runID, worker string) error {
	idx.mu.Lock()
	added := append([]Entry(nil), idx.added...)
	idx.mu.Unlock()
	if len(added) == 0 {
		return nil
	}
	return d.writeEntries(ctx, joinRemote(dedupDir, runID+"-"+worker+".csv"), added, columns)
}

// dedupRow returns the index row for e, in the index at key, which
// refers to the stored copy in copyRow. dedup_of is relative to the
// index's directory, like location, so it resolves wherever the tree is
// read from.
func dedupRow(key string, e *sourceEntry, copyRow Entry, runID string) Entry {
	row := e.Entry
	row.MD5 = copyRow.MD5
	row.Location = ""
	row.DedupOf = relKey(key, copyRow.Location)
	row.Offset = copyRow.Offset
	row.Codec = copyRow.Codec
	row.StoredOffset = copyRow.StoredOffset
	row.StoredLength = copyRow.StoredLength
	row.StoredStart = copyRow.StoredStart
	row.StoredSize = copyRow.StoredSize
	row.StoredMD5 = copyRow.StoredMD5
	row.VersionID = copyRow.VersionID
	row.Run = runID
	return row
}

// dedupEntries takes the files in entries which are copies of stored
// content out of entries, adding their index rows to stored. Only files
// of a size some stored copy has are hashed.
func (b *backup) dedupEntries(key string, entries []*sourceEntry, stored map[string]Entry) []*sourceEntry {
	if b.dedup == nil || b.d.dryRun {
		return entries
	}
	var rest []*sourceEntry
	for _, e := range entries {
		// A rebased file would find its own stored copy.
		if e.Type != TypeFile || e.rebase || e.Size < b.opt.DedupMin || !b.dedup.mayHave(e.Size) {
			rest = append(rest, e)
			continue
		}
		sum, err := hashFile(e.path)
		if err != nil {
			b.errorf("hash %q: %v", e.path, err)
			rest = append(rest, e)
			continue
		}
		copyRow, ok := b.dedup.find(sum, e.Size)
		if ok {
			// As for packing, a file which changed while being read
			// isn't recorded with what was read.
			info, err := os.Lstat(e.path)
			ok = err == nil && info.Size() == e.Size && info.ModTime().Equal(e.ModTime)
		}
		if !ok {
			rest = append(rest, e)
			continue
		}
		stored[e.Name] = dedupRow(key, e, copyRow, b.runID)
		b.count(func(s *Stats) *int64 { return &s.Deduplicated }, 1)
		b.count(func(s *Stats) *int64 { return &s.DeduplicatedBytes }, e.Size)
	}
	return rest
}

// recordCopies adds the files stored in the directory at key to the
// dedup index.
func (b *backup) recordCopies(key string, rows map[string]Entry) {
	if b.dedup == nil {
		return
	}
	for _, row := range rows {
		if row.Type == TypeFile && row.Size >= b.opt.DedupMin && row.DedupOf == "" && !strings.Contains(row.Location, "/") {
			b.dedup.add(key, row)
		}
	}
}

// dedupCompactAt is the number of dedup index files above which a run
// merges them into one. Tests lower it.
var dedupCompactAt = 50

// compactDedup merges the dedup index files into one when there are more
// than dedupCompactAt, then removes the merged ones. It must be called
// while holding the destination lock. Readers take the union of the
// files, so a copy listed twice while this runs does no harm.
func compactDedup(ctx context.Context, d *dest, runID string) error {
	if d.dryRun {
		return nil
	}
	entries, err := d.f.List(ctx, dedupDir)
	if errors.Is(err, fs.ErrorDirNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	var files []fs.Object
	for _, entry := range entries {
		if o, ok := entry.(fs.Object); ok && path.Ext(o.Remote()) == ".csv" {
			files = append(files, o)
		}
	}
	if len(files) <= dedupCompactAt {
		return nil
	}
	idx := newDedupIndex()
	for _, o := range files {
		if err := idx.load(ctx, o); err != nil {
			return fmt.Errorf("read dedup index %q: %w", o.Remote(), err)
		}
	}
	target := joinRemote(dedupDir, "compact-"+runID+".csv")
	if err := idx.write(ctx, d, target); err != nil {
		return err
	}
	for _, o := range files {
		if o.Remote() == target {
			continue
		}
		if err := o.Remove(ctx); err != nil {
			return fmt.Errorf("remove merged dedup index %q: %w", o.Remote(), err)
		}
	}
	fs.Infof(nil, "gda: merged %d dedup index files into %q", len(files), target)
	return nil
}
