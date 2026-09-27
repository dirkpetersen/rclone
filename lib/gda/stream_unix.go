//go:build unix

package gda

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/csv"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/rclone/rclone/fs"
)

// A directory with more than streamMin entries is committed in chunks of
// streamChunk entries, in name order, so that only a chunk of it is held
// in memory rather than the whole directory. The previous index is read
// a part at a time alongside, and the changeset and the new index are
// written to temporary files and uploaded at the end, so the commit
// protocol is the same as for any other directory. Such a directory is
// too big to be rolled up, and gets no checkpoints.

// Tests lower these.
var (
	streamMin   = 200000
	streamChunk = 50000
)

// streamItem is an entry of a directory committed in chunks.
type streamItem struct {
	name  string       // name in the index
	raw   string       // name in the source, for a file or directory of this directory
	dir   *scanned     // a subdirectory, already read
	entry *sourceEntry // an entry of a subdirectory packed with this one
}

// streamTree backs up the directory at rel, whose names are names, in
// chunks. Its subdirectories are read first, as scanTree reads them.
func (b *backup) streamTree(ctx context.Context, d *scanned, rel string, names []string, sem chan struct{}, ids chan string) {
	isDir, err := readDirDirs(sourcePath(b.srcRoot, rel))
	if err != nil {
		b.errorf("read directory %q: %v", rel, err)
		d.failed = true
		return
	}
	keep := map[string]bool{}
	var items []streamItem
	var dirs []*scanned
	for _, name := range names {
		childRel := joinRemote(rel, name)
		encName, encoding := encodeName(name)
		if encoding != "" {
			d.summary.badName = true
		}
		if isReserved(name, rel == "") {
			fs.Logf(nil, "gda: skipping %q: name is reserved for GDA's own objects", childRel)
			b.count(func(s *Stats) *int64 { return &s.Skipped }, 1)
			continue
		}
		if isDir[name] {
			e, err := statEntry(sourcePath(b.srcRoot, childRel), encName, b.names)
			if err == nil && b.excluded(childRel, e.IsDir(), e.Size, e.ModTime) {
				continue
			}
			if err == nil && b.opt.Xattrs {
				e.Xattrs, err = readXattrs(e.path)
			}
			if err != nil {
				b.errorf("stat %q: %v", childRel, err)
				d.summary.unreadable = true
				keep[encName] = true
				continue
			}
			e.NameEncoding, e.rel = encoding, childRel
			childKey := joinRemote(d.key, encName)
			dirs = append(dirs, &scanned{encName: encName, encoding: encoding, row: e, key: childKey, mustRoll: b.dirKeysTooLong(childKey)})
			continue
		}
		// Files are read chunk by chunk.
		items = append(items, streamItem{name: encName, raw: name})
	}
	var wg sync.WaitGroup
	for _, child := range dirs {
		scan := func() { b.scanTree(ctx, child, child.row.rel, sem, ids) }
		select {
		case sem <- struct{}{}:
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				scan()
			}()
		default:
			scan()
		}
	}
	wg.Wait()
	for _, child := range dirs {
		if child.failed {
			d.summary.unreadable = true
			keep[child.encName] = true
			continue
		}
		s := &child.summary
		d.summary.treeSize += s.treeSize
		d.summary.treeFiles += s.treeFiles
		d.summary.standalone = d.summary.standalone || s.standalone
		d.summary.unreadable = d.summary.unreadable || s.unreadable
		d.summary.badName = d.summary.badName || s.badName
		items = append(items, streamItem{name: child.encName, dir: child})
		if child.pending && child.mustRoll && child.encoding == "" {
			for i := range child.entries {
				e := &child.entries[i]
				e.Name = child.encName + "/" + e.Name
				items = append(items, streamItem{name: e.Name, entry: e})
			}
			for name := range child.keep {
				keep[child.encName+"/"+name] = true
			}
		}
	}
	for _, child := range dirs {
		if child.pending && !child.mustRoll {
			// The top of a rolled up subtree, with its own index.
			b.commitRollup(ctx, takeRollup(child), ids)
		}
	}
	slices.SortFunc(items, func(a, b streamItem) int { return strings.Compare(a.name, b.name) })
	w := <-ids
	defer func() { ids <- w }()
	b.commitStream(ctx, w, rel, d, items, keep)
}

// commitStream commits the directory d at rel, whose entries in name
// order are items, a chunk at a time, as worker w.
func (b *backup) commitStream(ctx context.Context, w, rel string, d *scanned, items []streamItem, keep map[string]bool) {
	if ctx.Err() != nil || b.isStopped() {
		return
	}
	key := d.key
	label := dirLabel(key, b.opt.RootLabel)
	b.count(func(s *Stats) *int64 { return &s.IndexedDirs }, 1)
	prev, err := newPrevIter(ctx, b.d, key)
	if err != nil {
		b.errorf("%v", err)
		return
	}
	changesets, err := newRowFile(b.opt.TempDir, columns)
	if err != nil {
		b.errorf("%v", err)
		return
	}
	defer changesets.remove()
	index := newIndexWriter(b.opt.TempDir, b.runID)
	defer index.remove()
	var retire []string
	changed := !prev.exists
	part := 0
	for start := 0; start < len(items) || start == 0; start += streamChunk {
		end := min(start+streamChunk, len(items))
		chunk := items[start:end]
		var cur []sourceEntry
		for _, item := range chunk {
			switch {
			case item.entry != nil:
				cur = append(cur, *item.entry)
			case item.dir != nil:
				row := item.dir.row
				row.TreeSize, row.TreeFiles = item.dir.summary.treeSize, item.dir.summary.treeFiles
				switch {
				case item.dir.pending && item.dir.mustRoll && item.dir.encoding == "":
					row.Listing = ListingRollup
					b.count(func(s *Stats) *int64 { return &s.RollupDirs }, 1)
				case item.dir.mustRoll:
					b.errorf("skipping directory %q: its keys would be longer than %d bytes", item.dir.row.rel, maxKeyLength)
					keep[item.name] = true
					continue
				default:
					row.Listing = ListingIndex
				}
				cur = append(cur, row)
			default:
				childRel := joinRemote(rel, item.raw)
				e, err := statEntry(sourcePath(b.srcRoot, childRel), item.name, b.names)
				if err == nil && b.excluded(childRel, e.IsDir(), e.Size, e.ModTime) {
					continue
				}
				if err == nil && b.opt.Xattrs {
					e.Xattrs, err = readXattrs(e.path)
				}
				if err != nil {
					b.errorf("stat %q: %v", childRel, err)
					d.summary.unreadable = true
					keep[item.name] = true
					continue
				}
				_, e.NameEncoding = encodeName(item.raw)
				e.rel = childRel
				d.summary.treeFiles++
				if e.Type == TypeFile {
					d.summary.treeSize += e.Size
					if e.Size >= b.opt.StandaloneMin {
						d.summary.standalone = true
					}
				}
				cur = append(cur, e)
			}
		}
		// The previous rows up to the last name of this chunk, or all
		// that are left for the last chunk.
		last := ""
		if end < len(items) {
			last = items[end-1].name
		}
		prevChunk, err := prev.until(ctx, last, end >= len(items))
		if err != nil {
			b.errorf("%v", err)
			return
		}
		prevMap := make(map[string]*Entry, len(prevChunk))
		for i := range prevChunk {
			prevMap[prevChunk[i].Name] = &prevChunk[i]
		}
		c := b.compare(key, prevMap, cur, keep, false)
		changes, rows, ok := b.resolve(ctx, w, key, label, prevChunk, c, &part)
		if !ok {
			return
		}
		chunkChanged, err := indexChanged(prevChunk, rows)
		if err != nil {
			b.errorf("compare index of %q: %v", rel, err)
			return
		}
		changed = changed || chunkChanged
		// Chunks follow each other in name order, so sorting each keeps
		// the whole index and changeset in order, as readers expect.
		sortEntries(rows)
		sortEntries(changes)
		for i := range changes {
			if err := changesets.add(&changes[i]); err != nil {
				b.errorf("write changeset of %q: %v", rel, err)
				return
			}
		}
		for i := range rows {
			if err := index.add(&rows[i]); err != nil {
				b.errorf("write index of %q: %v", rel, err)
				return
			}
		}
		if b.catalog != nil && len(changes) > 0 {
			if err := b.catalog.add(key, changes); err != nil {
				b.errorf("catalog: %v", err)
			}
		}
		retire = append(retire, c.retire...)
		b.countChanges(changes, len(rows))
	}
	if changesets.rows > 0 {
		if err := changesets.upload(ctx, b.d, joinRemote(key, changesetName(label, b.runID, w))); err != nil {
			b.errorf("write changeset of %q: %v", rel, err)
			return
		}
		b.count(func(s *Stats) *int64 { return &s.MetaObjects }, 1)
	}
	for _, childKey := range retire {
		if !b.retireIndex(ctx, w, childKey) {
			return
		}
	}
	if !changed {
		return
	}
	if b.d.cache != nil {
		// Too big to keep a copy of.
		b.d.cache.drop(key)
	}
	if err := index.upload(ctx, b.d, key); err != nil {
		b.errorf("write index of %q: %v", rel, err)
		return
	}
	b.count(func(s *Stats) *int64 { return &s.MetaObjects }, 1)
}

// prevIter reads a directory's previous index a part at a time, in name
// order.
type prevIter struct {
	d      *dest
	key    string
	exists bool     // whether there is a previous index
	parts  []string // parts still to read, of a split index
	rows   []Entry  // rows read and not yet returned
}

func newPrevIter(ctx context.Context, d *dest, key string) (*prevIter, error) {
	it := &prevIter{d: d, key: key}
	data, err := d.get(ctx, joinRemote(key, IndexName))
	if errors.Is(err, fs.ErrorObjectNotFound) {
		return it, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read index of %q: %w", key, err)
	}
	switch {
	case isTOC(data):
		parts, err := readTOC(bytes.NewReader(data))
		if err != nil {
			return nil, fmt.Errorf("index of %q: %w", key, err)
		}
		for _, part := range parts {
			it.parts = append(it.parts, part.name)
		}
	case looksLikeIndex(data):
		if it.rows, err = ReadEntries(bytes.NewReader(data)); err != nil {
			return nil, fmt.Errorf("parse index of %q: %w", key, err)
		}
	default:
		// Not one of ours, so the directory isn't a GDA one.
		return it, nil
	}
	it.exists = true
	return it, nil
}

// until returns the rows whose names are at most last, or all that are
// left if all is set.
func (it *prevIter) until(ctx context.Context, last string, all bool) ([]Entry, error) {
	var out []Entry
	for {
		n := 0
		for n < len(it.rows) && (all || it.rows[n].Name <= last) {
			n++
		}
		out = append(out, it.rows[:n]...)
		it.rows = it.rows[n:]
		if len(it.rows) > 0 || len(it.parts) == 0 {
			return out, nil
		}
		part := it.parts[0]
		it.parts = it.parts[1:]
		data, err := it.d.get(ctx, joinRemote(it.key, part))
		if err != nil {
			return nil, fmt.Errorf("read index part %q of %q: %w", part, it.key, err)
		}
		if it.rows, err = ReadEntries(bytes.NewReader(data)); err != nil {
			return nil, fmt.Errorf("parse index part %q of %q: %w", part, it.key, err)
		}
	}
}

// rowFile is a CSV file of rows written a row at a time.
type rowFile struct {
	file   *os.File
	hash   hash.Hash
	w      *csv.Writer
	cols   []string
	record []string
	rows   int
	first  string
	last   string
}

func newRowFile(tempDir string, cols []string) (*rowFile, error) {
	f, err := os.CreateTemp(tempDir, "gda-rows-*.csv")
	if err != nil {
		return nil, err
	}
	r := &rowFile{file: f, hash: md5.New(), cols: cols, record: make([]string, len(cols))}
	r.w = csv.NewWriter(io.MultiWriter(f, r.hash))
	if err := r.w.Write(cols); err != nil {
		r.remove()
		return nil, err
	}
	return r, nil
}

// add writes e as a row.
func (r *rowFile) add(e *Entry) error {
	for i, col := range r.cols {
		r.record[i] = e.value(col)
	}
	if r.rows == 0 {
		r.first = e.Name
	}
	r.last = e.Name
	r.rows++
	return r.w.Write(r.record)
}

// upload uploads the rows written as remote.
func (r *rowFile) upload(ctx context.Context, d *dest, remote string) error {
	r.w.Flush()
	if err := r.w.Error(); err != nil {
		return err
	}
	size, err := r.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	if err := r.file.Sync(); err != nil {
		return err
	}
	return d.putFile(ctx, remote, r.file.Name(), size, hex.EncodeToString(r.hash.Sum(nil)), d.metaTier)
}

// remove removes the file.
func (r *rowFile) remove() {
	_ = r.file.Close()
	_ = os.Remove(r.file.Name())
}

// indexWriter writes an index a row at a time, split into parts as
// encodeIndex splits it.
type indexWriter struct {
	tempDir string
	runID   string
	parts   []*rowFile
}

func newIndexWriter(tempDir, runID string) *indexWriter {
	return &indexWriter{tempDir: tempDir, runID: runID}
}

// add writes e, which must come after the rows written before it.
func (x *indexWriter) add(e *Entry) error {
	if n := len(x.parts); n == 0 || x.parts[n-1].rows >= maxIndexRows {
		part, err := newRowFile(x.tempDir, columns)
		if err != nil {
			return err
		}
		x.parts = append(x.parts, part)
	}
	return x.parts[len(x.parts)-1].add(e)
}

// upload writes the index of the directory at key: the parts first, then
// the table of contents that refers to them, or just the index if it
// fits in one part.
func (x *indexWriter) upload(ctx context.Context, d *dest, key string) error {
	if len(x.parts) == 0 {
		return d.writeRemoteIndex(ctx, key, []Entry{}, x.runID)
	}
	if len(x.parts) == 1 {
		return x.parts[0].upload(ctx, d, joinRemote(key, IndexName))
	}
	var toc bytes.Buffer
	cw := csv.NewWriter(&toc)
	_ = cw.Write(tocColumns)
	for n, part := range x.parts {
		name := indexPartName(x.runID, n+1)
		if err := part.upload(ctx, d, joinRemote(key, name)); err != nil {
			return err
		}
		_ = cw.Write([]string{name, part.first, part.last, strconv.Itoa(part.rows)})
	}
	cw.Flush()
	if err := cw.Error(); err != nil {
		return err
	}
	return d.putBytes(ctx, joinRemote(key, IndexName), toc.Bytes(), d.metaTier)
}

// remove removes the parts' files.
func (x *indexWriter) remove() {
	for _, part := range x.parts {
		part.remove()
	}
}
