package gda

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/rclone/rclone/fs"
)

// changesetRe matches changeset names and captures their run ID.
var changesetRe = regexp.MustCompile(`\.gda\.(\d{8}T\d{6}Z)\.[^./]+\.csv$`)

// tree reads the entries of a GDA destination, as they are now or as
// they were at the end of a past run.
type tree struct {
	d  *dest
	at string // run ID to read the tree at, or "" for the latest indexes

	cache map[string][]Entry // entries by index key
}

func newTree(d *dest, at string) *tree {
	return &tree{d: d, at: at, cache: map[string][]Entry{}}
}

// ParseAt converts a run ID or an RFC 3339 time to the run ID used to
// read a tree at that point.
func ParseAt(at string) (string, error) {
	if at == "" {
		return "", nil
	}
	if _, err := ParseRunID(at); err == nil {
		return at, nil
	}
	t, err := time.Parse(time.RFC3339, at)
	if err != nil {
		return "", fmt.Errorf("%q is neither a run ID like 20260926T120000Z nor an RFC 3339 time", at)
	}
	return NewRunID(t), nil
}

// entries returns the rows of the index at key. It returns nil if the
// directory has no index.
func (t *tree) entries(ctx context.Context, key string) ([]Entry, error) {
	if e, ok := t.cache[key]; ok {
		return e, nil
	}
	var entries []Entry
	var err error
	if t.at == "" {
		entries, err = t.d.readIndex(ctx, key)
	} else {
		entries, err = t.replay(ctx, key)
	}
	if err != nil {
		return nil, err
	}
	t.cache[key] = entries
	return entries, nil
}

// replay rebuilds the index at key as it was at the end of run t.at by
// applying its changesets in run order.
func (t *tree) replay(ctx context.Context, key string) ([]Entry, error) {
	dirEntries, err := t.d.f.List(ctx, key)
	if errors.Is(err, fs.ErrorDirNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("list %q: %w", key, err)
	}
	type changeset struct {
		run    string
		remote string
	}
	var changesets []changeset
	for _, de := range dirEntries {
		if _, ok := de.(fs.Object); !ok {
			continue
		}
		m := changesetRe.FindStringSubmatch(path.Base(de.Remote()))
		if m == nil || m[1] > t.at {
			continue
		}
		changesets = append(changesets, changeset{run: m[1], remote: de.Remote()})
	}
	if len(changesets) == 0 {
		return nil, nil
	}
	sort.Slice(changesets, func(i, j int) bool {
		if changesets[i].run != changesets[j].run {
			return changesets[i].run < changesets[j].run
		}
		return changesets[i].remote < changesets[j].remote
	})
	state := map[string]Entry{}
	for _, c := range changesets {
		data, err := t.d.get(ctx, c.remote)
		if err != nil {
			return nil, fmt.Errorf("read changeset %q: %w", c.remote, err)
		}
		rows, err := ReadEntries(bytes.NewReader(data))
		if err != nil {
			return nil, fmt.Errorf("parse changeset %q: %w", c.remote, err)
		}
		for _, row := range rows {
			if row.Action == ActionDelete {
				delete(state, row.Name)
				continue
			}
			row.Action = ""
			state[row.Name] = row
		}
	}
	entries := make([]Entry, 0, len(state))
	for _, e := range state {
		entries = append(entries, e)
	}
	sortEntries(entries)
	return entries, nil
}

// Located is an entry together with the key of the index it is in.
type Located struct {
	Entry
	IndexKey  string // key of the directory whose index holds the entry
	Path      string // path relative to the walk's start, as named in indexes
	LocalPath string // Path with names decoded to their original bytes
}

// ObjectKey returns the key of the object holding the entry's data.
func (l *Located) ObjectKey() string {
	return joinRemote(l.IndexKey, l.Location)
}

// errNotFound is returned when a requested path isn't in the tree.
var errNotFound = errors.New("not found")

// encodePath encodes each element of a "/" separated path as names are
// encoded in indexes.
func encodePath(p string) string {
	p = strings.Trim(p, "/")
	if p == "" {
		return ""
	}
	parts := strings.Split(p, "/")
	for i, part := range parts {
		parts[i], _ = encodeName(part)
	}
	return strings.Join(parts, "/")
}

// resolve finds the entry at p relative to the root. key and name say
// where the entry's contents are listed: the index at key, below the
// rolled up directory name ("" for the whole index). row is the entry
// itself, from the index at rowKey, or nil for the root. It returns
// errNotFound if there is no such entry.
func (t *tree) resolve(ctx context.Context, p string) (key, name string, row *Entry, rowKey string, err error) {
	p = encodePath(p)
	if p == "" {
		return "", "", nil, "", nil
	}
	for _, part := range strings.Split(p, "/") {
		entries, err := t.entries(ctx, key)
		if err != nil {
			return "", "", nil, "", err
		}
		candidate := joinRemote(name, part)
		e, ok := findEntry(entries, candidate)
		if !ok || (row != nil && !row.IsDir()) {
			return "", "", nil, "", fmt.Errorf("%q: %w", p, errNotFound)
		}
		row, rowKey = &e, key
		if e.IsDir() && e.Listing == ListingIndex && name == "" {
			key, name = joinRemote(key, part), ""
			continue
		}
		name = candidate
	}
	return key, name, row, rowKey, nil
}

func findEntry(entries []Entry, name string) (Entry, bool) {
	i := sort.Search(len(entries), func(i int) bool { return entries[i].Name >= name })
	if i < len(entries) && entries[i].Name == name {
		return entries[i], true
	}
	return Entry{}, false
}

// localName returns the original bytes of the last element of e's name.
func localName(e *Entry) string {
	name, err := e.DecodeName()
	if err != nil {
		name = e.Name
	}
	return path.Base(name)
}

// walk calls fn for the entry at p and everything below it, parents
// before their contents. A directory at p is passed with Path "", and
// the paths of its contents are relative to it; a file at p is passed
// with its own name as Path.
func (t *tree) walk(ctx context.Context, p string, fn func(*Located) error) error {
	key, name, row, rowKey, err := t.resolve(ctx, p)
	if err != nil {
		return err
	}
	if row != nil {
		if !row.IsDir() {
			return fn(&Located{Entry: *row, IndexKey: rowKey, Path: path.Base(row.Name), LocalPath: localName(row)})
		}
		if err := fn(&Located{Entry: *row, IndexKey: rowKey}); err != nil {
			return err
		}
	}
	return t.walkIndex(ctx, key, name, "", "", fn)
}

// walkIndex calls fn for the entries of the index at key, only those
// below the rolled up directory prefix if it isn't "", naming them below
// out and localOut.
func (t *tree) walkIndex(ctx context.Context, key, prefix, out, localOut string, fn func(*Located) error) error {
	entries, err := t.entries(ctx, key)
	if err != nil {
		return err
	}
	for _, e := range entries {
		rel := e.Name
		if prefix != "" {
			if !strings.HasPrefix(e.Name, prefix+"/") {
				continue
			}
			rel = strings.TrimPrefix(e.Name, prefix+"/")
		}
		// Only direct children of an index can have encoded names, as
		// subtrees holding such names are never rolled up.
		localRel := rel
		if e.NameEncoding == NameEncodingPercent {
			localRel = localName(&e)
		}
		l := &Located{Entry: e, IndexKey: key, Path: joinRemote(out, rel), LocalPath: joinRemote(localOut, localRel)}
		if err := fn(l); err != nil {
			return err
		}
		if e.IsDir() && e.Listing == ListingIndex && isDirectChild(e.Name) && prefix == "" {
			if err := t.walkIndex(ctx, joinRemote(key, e.Name), "", l.Path, l.LocalPath, fn); err != nil {
				return err
			}
		}
	}
	return nil
}

// List returns the entries directly in the directory at p below the root
// of dst, as it is now or, if at is set, as it was at the end of that run.
func List(ctx context.Context, dst fs.Fs, p, at string) ([]Located, error) {
	t := newTree(&dest{f: dst, retries: 1}, at)
	var out []Located
	err := t.walk(ctx, p, func(l *Located) error {
		if l.Path != "" && !strings.Contains(l.Path, "/") {
			out = append(out, *l)
		}
		return nil
	})
	return out, err
}
