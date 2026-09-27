// Package gda implements a read only backend which shows GDA backups
// and archives as the files they hold.
package gda

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/cache"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/fspath"
	"github.com/rclone/rclone/fs/hash"
	libgda "github.com/rclone/rclone/lib/gda"
)

// Register with Fs
func init() {
	fs.Register(&fs.RegInfo{
		Name:        "gda",
		Description: "Browse GDA backups and archives (read only)",
		NewFs:       NewFs,
		MetadataInfo: &fs.MetadataInfo{
			System: map[string]fs.MetadataHelp{
				"mode":         {Help: "File type and mode", Type: "octal, unix style", Example: "0100664", ReadOnly: true},
				"uid":          {Help: "User ID of owner", Type: "decimal number", Example: "500", ReadOnly: true},
				"gid":          {Help: "Group ID of owner", Type: "decimal number", Example: "500", ReadOnly: true},
				"mtime":        {Help: "Time of last modification", Type: "RFC 3339", Example: "2006-01-02T15:04:05.999999999Z07:00", ReadOnly: true},
				"gda-location": {Help: "Pack or object holding the data", Type: "string", Example: "results.gda.20260926T120000Z.w01.002.tar", ReadOnly: true},
				"gda-run":      {Help: "Backup run which stored this version", Type: "string", Example: "20260926T120000Z", ReadOnly: true},
			},
			Help: `The metadata recorded when the file was backed up.`,
		},
		Options: []fs.Option{{
			Name: "remote",
			Help: `Remote holding GDA backups, e.g. "myremote:bucket/path".

Directories with a gda-index.csv are shown as the files they hold, and
everything else is shown as it is, so this can point at a GDA root or
anything above it.

If this is left empty, the root is used as the remote, so that
:gda:remote:path is the same as setting remote="remote:path".`,
		}, {
			Name: "at",
			Help: `Show the backups as they were at the end of this run.

A run ID like 20260926T120000Z or an RFC 3339 time. In a connection
string, quote a time as it contains ":", e.g.
:gda,at="2026-09-01T00:00:00Z":remote:path.`,
		}, {
			Name:     "show_internals",
			Help:     `Show the objects GDA stores rather than the files they hold.`,
			Default:  false,
			Advanced: true,
		}},
	})
}

// Options defines the configuration for this backend
type Options struct {
	Remote        string `config:"remote"`
	At            string `config:"at"`
	ShowInternals bool   `config:"show_internals"`
}

// Fs shows GDA trees on a remote as the files they hold
//
// It wraps the top of the remote and keeps the path it shows as a
// prefix, so that the indexes of directories above that path, which a
// rolled up directory is listed in, can always be read.
type Fs struct {
	name     string
	root     string
	opt      Options
	outer    fs.Fs  // the top of the remote being shown
	prefix   string // path of this Fs's root in outer
	browser  *libgda.Browser
	features *fs.Features
}

// passObject is an object shown as it is, outside any GDA tree.
type passObject struct {
	fs.Object
	fs     *Fs
	remote string
}

// Object is a file in a GDA tree
type Object struct {
	fs     *Fs
	remote string
	entry  libgda.Located
	tier   string
}

// errReadOnly is returned for anything which would change the remote.
var errReadOnly = errors.New("gda backend is read only")

// NewFs constructs an Fs from the path
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	opt := new(Options)
	if err := configstruct.Set(m, opt); err != nil {
		return nil, err
	}
	remote, origRoot := opt.Remote, root
	if remote == "" {
		remote, root = root, ""
	}
	if strings.HasPrefix(remote, name+":") {
		return nil, errors.New("can't point gda remote at itself - check the value of the remote setting")
	}
	outerName, prefix, err := fspath.SplitFs(fspath.JoinRootPath(remote, root))
	if err != nil {
		return nil, err
	}
	switch {
	case outerName == "":
		// A local path: wrap the root of its volume.
		abs, err := filepath.Abs(prefix)
		if err != nil {
			return nil, err
		}
		volume := filepath.VolumeName(abs)
		outerName, prefix = volume+"/", filepath.ToSlash(abs[len(volume):])
	case strings.HasPrefix(prefix, "/"):
		// An absolute path on a remote with a home directory.
		outerName += "/"
	}
	outer, err := cache.Get(ctx, outerName)
	if err != nil {
		return nil, fmt.Errorf("failed to make remote %q to wrap: %w", outerName, err)
	}
	browser, err := libgda.NewBrowser(outer, opt.At)
	if err != nil {
		return nil, err
	}
	// Root is the path as given, so that different paths are different
	// remotes to the Fs cache.
	f := &Fs{name: name, root: origRoot, opt: *opt, outer: outer, prefix: strings.Trim(prefix, "/"), browser: browser}
	f.features = (&fs.Features{
		CanHaveEmptyDirectories: true,
		ReadMetadata:            true,
		ReadMimeType:            true,
		GetTier:                 outer.Features().GetTier,
	}).Fill(ctx, f)
	// A root naming a file makes the parent the root, as with other
	// backends.
	if f.prefix != "" {
		if _, err := f.NewObject(ctx, ""); err == nil {
			f.prefix = parentOf(f.prefix)
			if f.root = path.Dir(strings.TrimRight(f.root, "/")); f.root == "." {
				f.root = ""
			}
			return f, fs.ErrorIsFile
		}
	}
	return f, nil
}

// parentOf returns the parent of the "/" separated path p, or "".
func parentOf(p string) string {
	if i := strings.LastIndexByte(p, '/'); i >= 0 {
		return p[:i]
	}
	return ""
}

// full returns the path in outer of remote.
func (f *Fs) full(remote string) string {
	return strings.Trim(path.Join(f.prefix, remote), "/")
}

// relative returns the path relative to this Fs of full, a path in outer.
func (f *Fs) relative(full string) string {
	if f.prefix == "" {
		return full
	}
	return strings.TrimPrefix(strings.TrimPrefix(full, f.prefix), "/")
}

// listOuter lists dir as it is in the wrapped remote.
func (f *Fs) listOuter(ctx context.Context, dir string) (fs.DirEntries, error) {
	entries, err := f.outer.List(ctx, f.full(dir))
	if err != nil {
		return nil, err
	}
	for i, e := range entries {
		switch x := e.(type) {
		case fs.Directory:
			entries[i] = fs.NewDirWrapper(f.relative(x.Remote()), x)
		case fs.Object:
			entries[i] = &passObject{Object: x, fs: f, remote: f.relative(x.Remote())}
		}
	}
	return entries, nil
}

// Name of the remote (as passed into NewFs)
func (f *Fs) Name() string { return f.name }

// Root of the remote (as passed into NewFs)
func (f *Fs) Root() string { return f.root }

// String converts this Fs to a string
func (f *Fs) String() string { return fmt.Sprintf("GDA view of %v%s", f.outer, f.prefix) }

// Precision of the modification times
func (f *Fs) Precision() time.Duration { return time.Nanosecond }

// Hashes returns the supported hash types
func (f *Fs) Hashes() hash.Set { return hash.Set(hash.MD5) }

// Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features { return f.features }

// List the objects and directories in dir into entries
func (f *Fs) List(ctx context.Context, dir string) (fs.DirEntries, error) {
	if f.opt.ShowInternals {
		return f.listOuter(ctx, dir)
	}
	located, ok, err := f.browser.List(ctx, f.full(dir))
	if err != nil {
		return nil, err
	}
	if !ok {
		return f.listOuter(ctx, dir)
	}
	var entries fs.DirEntries
	for _, l := range located {
		remote := path.Join(dir, l.Path)
		switch l.Type {
		case libgda.TypeDir:
			d := fs.NewDir(remote, l.ModTime)
			if l.TreeSize >= 0 {
				d.SetSize(l.TreeSize)
			}
			if l.TreeFiles >= 0 {
				d.SetItems(l.TreeFiles)
			}
			entries = append(entries, d)
		case libgda.TypeFile:
			o := &Object{fs: f, remote: remote, entry: l}
			if o.tier, err = f.tier(ctx, &l); err != nil {
				return nil, err
			}
			entries = append(entries, o)
		}
		// Symlinks and special files have no content to show.
	}
	return entries, nil
}

// tier returns the storage class of the object holding l's data.
func (f *Fs) tier(ctx context.Context, l *libgda.Located) (string, error) {
	if l.Location == "" || !f.features.GetTier {
		return "", nil
	}
	key := l.ObjectKey()
	dir := path.Dir(key)
	if dir == "." {
		dir = ""
	}
	tiers, err := f.browser.Tiers(ctx, dir)
	if err != nil {
		return "", err
	}
	return tiers[key], nil
}

// NewObject finds the Object at remote.
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	passThrough := func() (fs.Object, error) {
		o, err := f.outer.NewObject(ctx, f.full(remote))
		if err != nil {
			return nil, err
		}
		return &passObject{Object: o, fs: f, remote: remote}, nil
	}
	if f.opt.ShowInternals {
		return passThrough()
	}
	l, ok, err := f.browser.Find(ctx, f.full(remote))
	if !ok && err == nil {
		return passThrough()
	}
	if err != nil {
		return nil, err
	}
	switch l.Type {
	case libgda.TypeFile:
	case libgda.TypeDir:
		return nil, fs.ErrorIsDir
	default:
		// Symlinks and special files aren't shown.
		return nil, fs.ErrorObjectNotFound
	}
	o := &Object{fs: f, remote: remote, entry: *l}
	if o.tier, err = f.tier(ctx, l); err != nil {
		return nil, err
	}
	return o, nil
}

// Put is not supported
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	return nil, errReadOnly
}

// Mkdir is not supported
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	return errReadOnly
}

// Rmdir is not supported
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	return errReadOnly
}

// Fs returns the parent Fs
func (o *Object) Fs() fs.Info { return o.fs }

// Remote returns the remote path
func (o *Object) Remote() string { return o.remote }

// String returns a description of the Object
func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.remote
}

// ModTime returns the modification time recorded at backup
func (o *Object) ModTime(ctx context.Context) time.Time { return o.entry.ModTime }

// Size returns the size of the file
func (o *Object) Size() int64 { return o.entry.Size }

// Storable returns whether the object is storable
func (o *Object) Storable() bool { return true }

// Hash returns the MD5 recorded at backup
func (o *Object) Hash(ctx context.Context, t hash.Type) (string, error) {
	if t != hash.MD5 {
		return "", hash.ErrUnsupported
	}
	return o.entry.MD5, nil
}

// SetModTime is not supported
func (o *Object) SetModTime(ctx context.Context, t time.Time) error {
	return errReadOnly
}

// Open opens the file for read, reading only its own bytes of a pack.
// Data in an archive storage class must be restored first.
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	return o.fs.browser.Open(ctx, &o.entry, options...)
}

// Update is not supported
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	return errReadOnly
}

// Remove is not supported
func (o *Object) Remove(ctx context.Context) error {
	return errReadOnly
}

// MimeType returns the MIME type from the file name
func (o *Object) MimeType(ctx context.Context) string {
	return fs.MimeTypeFromName(o.remote)
}

// GetTier returns the storage class of the object holding the data
func (o *Object) GetTier() string { return o.tier }

// Metadata returns the metadata recorded at backup
func (o *Object) Metadata(ctx context.Context) (fs.Metadata, error) {
	e := &o.entry
	m := fs.Metadata{
		"mode":         fmt.Sprintf("%o", 0o100000|e.Mode),
		"mtime":        e.ModTime.Format(time.RFC3339Nano),
		"gda-location": e.Location,
		"gda-run":      e.Run,
	}
	if e.UID >= 0 {
		m["uid"] = strconv.FormatInt(e.UID, 10)
	}
	if e.GID >= 0 {
		m["gid"] = strconv.FormatInt(e.GID, 10)
	}
	return m, nil
}

// Fs returns the parent Fs
func (o *passObject) Fs() fs.Info { return o.fs }

// Remote returns the remote path
func (o *passObject) Remote() string { return o.remote }

// String returns a description of the Object
func (o *passObject) String() string { return o.remote }

// SetModTime is not supported
func (o *passObject) SetModTime(ctx context.Context, t time.Time) error {
	return errReadOnly
}

// Update is not supported
func (o *passObject) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	return errReadOnly
}

// Remove is not supported
func (o *passObject) Remove(ctx context.Context) error {
	return errReadOnly
}

// GetTier returns the storage class of the wrapped object
func (o *passObject) GetTier() string {
	if t, ok := o.Object.(fs.GetTierer); ok {
		return t.GetTier()
	}
	return ""
}

// Metadata returns the metadata of the wrapped object
func (o *passObject) Metadata(ctx context.Context) (fs.Metadata, error) {
	if m, ok := o.Object.(fs.Metadataer); ok {
		return m.Metadata(ctx)
	}
	return nil, nil
}

// ID returns the ID of the wrapped object
func (o *passObject) ID() string {
	if i, ok := o.Object.(fs.IDer); ok {
		return i.ID()
	}
	return ""
}

// MimeType returns the MIME type of the wrapped object
func (o *passObject) MimeType(ctx context.Context) string {
	return fs.MimeType(ctx, o.Object)
}

// UnWrap returns the wrapped object
func (o *passObject) UnWrap() fs.Object { return o.Object }

// Check the interfaces are satisfied
var (
	_ fs.Fs              = (*Fs)(nil)
	_ fs.Object          = (*Object)(nil)
	_ fs.MimeTyper       = (*Object)(nil)
	_ fs.GetTierer       = (*Object)(nil)
	_ fs.Metadataer      = (*Object)(nil)
	_ fs.Object          = (*passObject)(nil)
	_ fs.GetTierer       = (*passObject)(nil)
	_ fs.Metadataer      = (*passObject)(nil)
	_ fs.IDer            = (*passObject)(nil)
	_ fs.MimeTyper       = (*passObject)(nil)
	_ fs.ObjectUnWrapper = (*passObject)(nil)
)
