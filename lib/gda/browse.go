package gda

import (
	"context"
	"errors"
	"io"
	"path"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/rclone/rclone/fs"
	"golang.org/x/sync/singleflight"
)

// browseCacheTime is how long a Browser keeps indexes and listings, so
// that long running users such as mount see new runs.
const browseCacheTime = time.Minute

// Browser presents the GDA trees below the root of an Fs as the files
// they hold, reading only indexes. It is safe for concurrent use.
type Browser struct {
	f  fs.Fs
	at string

	mu      sync.Mutex
	indexes map[string][]Entry           // index rows by key, nil for no index
	tiers   map[string]map[string]string // storage class by object key, by directory
	expires time.Time
	reads   singleflight.Group // index reads in progress, by key
}

// NewBrowser returns a Browser for the GDA trees below the root of f, as
// they are now or, if at isn't "", as they were at the end of that run.
func NewBrowser(f fs.Fs, at string) (*Browser, error) {
	runID, err := ParseAt(at)
	if err != nil {
		return nil, err
	}
	return &Browser{f: f, at: runID}, nil
}

// resetExpired starts fresh caches when the old ones have expired. It
// must be called with b.mu held.
func (b *Browser) resetExpired() {
	if b.indexes == nil || time.Now().After(b.expires) {
		b.indexes = map[string][]Entry{}
		b.tiers = map[string]map[string]string{}
		b.expires = time.Now().Add(browseCacheTime)
	}
}

// entries returns the rows of the index at key, or nil if there is none.
// Concurrent reads of the same index are done once.
func (b *Browser) entries(ctx context.Context, key string) ([]Entry, error) {
	if key == "" && b.f.Features().BucketBased {
		// Above the buckets there are no objects, so no index.
		return nil, nil
	}
	b.mu.Lock()
	b.resetExpired()
	rows, ok := b.indexes[key]
	b.mu.Unlock()
	if ok {
		return rows, nil
	}
	v, err, _ := b.reads.Do(key, func() (any, error) {
		return newTree(&dest{f: b.f, retries: 1}, b.at).entries(ctx, key)
	})
	if err != nil {
		return nil, err
	}
	rows = v.([]Entry)
	b.mu.Lock()
	b.indexes[key] = rows
	b.mu.Unlock()
	return rows, nil
}

// List returns the entries directly in dir, which is relative to the
// root of the Fs. ok is false if dir isn't part of a GDA tree, in which
// case the caller should list the Fs itself. It returns
// fs.ErrorDirNotFound if dir is in a GDA tree but doesn't exist in it.
func (b *Browser) List(ctx context.Context, dir string) (entries []Located, ok bool, err error) {
	dir = strings.Trim(dir, "/")
	own, err := b.entries(ctx, dir)
	if err != nil {
		return nil, false, err
	}
	if len(own) > 0 {
		return children(own, dir, ""), true, nil
	}
	// With no index, or the empty one left when a directory is deleted,
	// rolled up or replaced, the parent's row says what dir is. A rolled
	// up directory is listed in the index at the top of its rollup.
	for ancestor := dir; ancestor != ""; {
		ancestor = parentRel(ancestor)
		rows, err := b.entries(ctx, ancestor)
		if err != nil {
			return nil, false, err
		}
		if rows == nil {
			continue
		}
		rel := strings.TrimPrefix(dir, ancestor+"/")
		if ancestor == "" {
			rel = dir
		}
		row, found := findEntry(rows, rel)
		switch {
		case !found || !row.IsDir():
			return nil, true, fs.ErrorDirNotFound
		case row.Listing == ListingRollup:
			return children(rows, ancestor, rel), true, nil
		case own != nil:
			// It has its own index, which is empty.
			return nil, true, nil
		default:
			fs.Debugf(nil, "gda: %q should have an index but has none", dir)
			return nil, false, nil
		}
	}
	if own != nil {
		// An empty index with nothing above it.
		return nil, true, nil
	}
	return nil, false, nil
}

// children returns the rows of the index at key directly below the
// rolled up directory prefix, or directly in the index if prefix is "".
//
// Path is the name to show: the original name where it is valid UTF-8,
// otherwise the encoded one, which is also used for names which would
// show the same.
func children(rows []Entry, key, prefix string) []Located {
	var out []Located
	shown := map[string]int{}
	for _, e := range rows {
		name := e.Name
		if prefix != "" {
			if !strings.HasPrefix(name, prefix+"/") {
				continue
			}
			name = strings.TrimPrefix(name, prefix+"/")
		}
		if strings.Contains(name, "/") {
			continue
		}
		local := name
		if e.NameEncoding == NameEncodingPercent {
			local = localName(&e)
		}
		show := name
		if utf8.ValidString(local) {
			show = local
		}
		shown[show]++
		out = append(out, Located{Entry: e, IndexKey: key, Path: show, LocalPath: local})
	}
	for i := range out {
		if shown[out[i].Path] > 1 {
			out[i].Path = path.Base(out[i].Name)
		}
	}
	return out
}

// Tiers returns the storage class of each object in the directory key of
// the Fs, by object key, from one listing. It returns nil if the Fs
// doesn't report storage classes.
func (b *Browser) Tiers(ctx context.Context, key string) (map[string]string, error) {
	if !b.f.Features().GetTier {
		return nil, nil
	}
	b.mu.Lock()
	b.resetExpired()
	tiers, ok := b.tiers[key]
	b.mu.Unlock()
	if ok {
		return tiers, nil
	}
	listing, err := b.f.List(ctx, key)
	if err != nil && !errors.Is(err, fs.ErrorDirNotFound) {
		return nil, err
	}
	tiers = map[string]string{}
	for _, entry := range listing {
		if t, ok := entry.(fs.GetTierer); ok {
			tiers[entry.Remote()] = t.GetTier()
		}
	}
	b.mu.Lock()
	b.tiers[key] = tiers
	b.mu.Unlock()
	return tiers, nil
}

// Open opens the file l, honouring fs.RangeOption and fs.SeekOption.
// For a file in a pack it reads only the frames or bytes holding it.
// Data which must be restored first gives the underlying backend's error.
func (b *Browser) Open(ctx context.Context, l *Located, options ...fs.OpenOption) (io.ReadCloser, error) {
	if l.Type != TypeFile {
		return nil, fs.ErrorNotAFile
	}
	start, end, others := fileRange(l.Size, options)
	if l.Size == 0 || start >= l.Size || start > end {
		return io.NopCloser(strings.NewReader("")), nil
	}
	if l.outsideRoot() {
		return nil, l.errOutsideRoot()
	}
	o, err := b.f.NewObject(ctx, l.ObjectKey())
	if err != nil {
		return nil, err
	}
	switch {
	case l.Offset < 0 && l.Codec != CodecZstd:
		// A standalone object holds exactly the file.
		return o.Open(ctx, options...)
	case l.Offset < 0:
		// Compressed frames can't be entered in the middle.
		in, err := o.Open(ctx, others...)
		if err != nil {
			return nil, err
		}
		return decompressRange(in, start, end-start+1)
	case l.Codec == CodecZstd:
		in, err := o.Open(ctx, append(others, &fs.RangeOption{Start: l.StoredOffset, End: l.StoredOffset + l.StoredLength - 1})...)
		if err != nil {
			return nil, err
		}
		return decompressRange(in, l.Offset-l.StoredStart+start, end-start+1)
	default:
		return o.Open(ctx, append(others, &fs.RangeOption{Start: l.StoredOffset + start, End: l.StoredOffset + end})...)
	}
}

// fileRange returns the first and last byte of a file of size bytes
// which options ask for, and the options other than ranges and seeks.
func fileRange(size int64, options []fs.OpenOption) (start, end int64, others []fs.OpenOption) {
	start, end = 0, size-1
	for _, option := range options {
		switch x := option.(type) {
		case *fs.RangeOption:
			offset, limit := x.Decode(size)
			start, end = offset, size-1
			if limit >= 0 {
				end = min(offset+limit-1, size-1)
			}
		case *fs.SeekOption:
			start, end = x.Offset, size-1
		default:
			others = append(others, option)
		}
	}
	return start, end, others
}

// Find returns the entry at remote, which is relative to the root of the
// Fs. ok is false if remote isn't in a GDA tree.
func (b *Browser) Find(ctx context.Context, remote string) (entry *Located, ok bool, err error) {
	dir := path.Dir(remote)
	if dir == "." {
		dir = ""
	}
	entries, ok, err := b.List(ctx, dir)
	if err != nil || !ok {
		if errors.Is(err, fs.ErrorDirNotFound) {
			return nil, true, fs.ErrorObjectNotFound
		}
		return nil, ok, err
	}
	name := path.Base(remote)
	for i := range entries {
		if entries[i].Path == name {
			return &entries[i], true, nil
		}
	}
	return nil, true, fs.ErrorObjectNotFound
}
