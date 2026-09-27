package gda

import (
	"context"
	"errors"
	"io"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/rclone/rclone/fs"
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
	t       *tree
	tiers   map[string]map[string]string // storage class by object key, by directory
	expires time.Time
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

// tree returns the tree to read, starting a fresh cache when the old
// one has expired.
func (b *Browser) tree() *tree {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.t == nil || time.Now().After(b.expires) {
		b.t = newTree(&dest{f: b.f, retries: 1}, b.at)
		b.tiers = map[string]map[string]string{}
		b.expires = time.Now().Add(browseCacheTime)
	}
	return b.t
}

// entries returns the rows of the index at key, or nil if there is none.
func (b *Browser) entries(ctx context.Context, t *tree, key string) ([]Entry, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return t.entries(ctx, key)
}

// List returns the entries directly in dir, which is relative to the
// root of the Fs. ok is false if dir isn't part of a GDA tree, in which
// case the caller should list the Fs itself. It returns
// fs.ErrorDirNotFound if dir is in a GDA tree but doesn't exist in it.
func (b *Browser) List(ctx context.Context, dir string) (entries []Located, ok bool, err error) {
	t := b.tree()
	dir = strings.Trim(dir, "/")
	rows, err := b.entries(ctx, t, dir)
	if err != nil {
		return nil, false, err
	}
	if rows != nil {
		return children(rows, dir, ""), true, nil
	}
	// A rolled up directory has no index of its own: it is listed in the
	// index of the directory at the top of the rollup.
	for ancestor := dir; ancestor != ""; {
		ancestor = parentRel(ancestor)
		rows, err := b.entries(ctx, t, ancestor)
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
		case !found:
			return nil, true, fs.ErrorDirNotFound
		case !row.IsDir() || row.Listing != ListingRollup:
			// Either not a directory, or a directory whose own index
			// is missing: not something to present as GDA.
			return nil, false, nil
		}
		return children(rows, ancestor, rel), true, nil
	}
	return nil, false, nil
}

// children returns the rows of the index at key directly below the
// rolled up directory prefix, or directly in the index if prefix is "".
func children(rows []Entry, key, prefix string) []Located {
	var out []Located
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
		out = append(out, Located{Entry: e, IndexKey: key, Path: name, LocalPath: local})
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
	b.tree()
	b.mu.Lock()
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
// For a file in a pack it reads only the file's bytes. Data which must be
// restored first gives the underlying backend's error.
func (b *Browser) Open(ctx context.Context, l *Located, options ...fs.OpenOption) (io.ReadCloser, error) {
	if l.Type != TypeFile {
		return nil, fs.ErrorNotAFile
	}
	if l.Size == 0 {
		return io.NopCloser(strings.NewReader("")), nil
	}
	o, err := b.f.NewObject(ctx, l.ObjectKey())
	if err != nil {
		return nil, err
	}
	if l.Offset < 0 {
		// A standalone object holds exactly the file.
		return o.Open(ctx, options...)
	}
	start, end := int64(0), l.Size-1
	var others []fs.OpenOption
	for _, option := range options {
		switch x := option.(type) {
		case *fs.RangeOption:
			offset, limit := x.Decode(l.Size)
			start, end = offset, l.Size-1
			if limit >= 0 {
				end = min(offset+limit-1, l.Size-1)
			}
		case *fs.SeekOption:
			start, end = x.Offset, l.Size-1
		default:
			others = append(others, option)
		}
	}
	if start >= l.Size || start > end {
		return io.NopCloser(strings.NewReader("")), nil
	}
	others = append(others, &fs.RangeOption{Start: l.StoredOffset + start, End: l.StoredOffset + end})
	return o.Open(ctx, others...)
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
