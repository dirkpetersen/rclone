//go:build unix

package gda

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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
// concurrent use.
type dedupIndex struct {
	mu    sync.Mutex
	bySum map[string]Entry // by dedupName
	sizes map[int64]bool   // sizes of stored copies
	added []Entry          // copies stored by this run
}

// dedupName is the key of a copy in the dedup index.
func dedupName(md5sum string, size int64) string {
	return md5sum + "-" + strconv.FormatInt(size, 10)
}

// loadDedup reads the dedup index of the destination.
func loadDedup(ctx context.Context, d *dest) (*dedupIndex, error) {
	idx := &dedupIndex{bySum: map[string]Entry{}, sizes: map[int64]bool{}}
	entries, err := d.f.List(ctx, dedupDir)
	if errors.Is(err, fs.ErrorDirNotFound) {
		return idx, nil
	}
	if err != nil {
		return nil, fmt.Errorf("list dedup index: %w", err)
	}
	for _, entry := range entries {
		if _, ok := entry.(fs.Object); !ok || path.Ext(entry.Remote()) != ".csv" {
			continue
		}
		data, err := d.get(ctx, entry.Remote())
		if err != nil {
			return nil, fmt.Errorf("read dedup index: %w", err)
		}
		rows, err := ReadEntries(bytes.NewReader(data))
		if err != nil {
			return nil, fmt.Errorf("parse dedup index %q: %w", entry.Remote(), err)
		}
		for _, row := range rows {
			idx.bySum[row.Name] = row
			idx.sizes[row.Size] = true
		}
	}
	return idx, nil
}

// mayHave returns true if a copy of this size is stored, so a file of
// this size is worth hashing to look for its copy.
func (idx *dedupIndex) mayHave(size int64) bool {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	return idx.sizes[size]
}

// find returns the stored copy of content with md5sum and size.
func (idx *dedupIndex) find(md5sum string, size int64) (Entry, bool) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	row, ok := idx.bySum[dedupName(md5sum, size)]
	return row, ok
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
	if _, ok := idx.bySum[copyRow.Name]; ok {
		return
	}
	idx.bySum[copyRow.Name] = copyRow
	idx.sizes[copyRow.Size] = true
	idx.added = append(idx.added, copyRow)
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
	merged := map[string]Entry{}
	for _, o := range files {
		data, err := d.get(ctx, o.Remote())
		if err != nil {
			return err
		}
		rows, err := ReadEntries(bytes.NewReader(data))
		if err != nil {
			return fmt.Errorf("parse dedup index %q: %w", o.Remote(), err)
		}
		for _, row := range rows {
			if _, ok := merged[row.Name]; !ok {
				merged[row.Name] = row
			}
		}
	}
	rows := make([]Entry, 0, len(merged))
	for _, row := range merged {
		rows = append(rows, row)
	}
	sortEntries(rows)
	target := joinRemote(dedupDir, "compact-"+runID+".csv")
	if err := d.writeEntries(ctx, target, rows, columns); err != nil {
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
