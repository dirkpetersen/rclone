//go:build unix

package gda

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/user"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// idNames caches user and group name lookups.
type idNames struct {
	mu     sync.Mutex
	users  map[int64]string
	groups map[int64]string
}

func newIDNames() *idNames {
	return &idNames{users: map[int64]string{}, groups: map[int64]string{}}
}

// user returns the user name for uid, or "" if it can't be found.
func (n *idNames) user(uid int64) string {
	n.mu.Lock()
	defer n.mu.Unlock()
	name, ok := n.users[uid]
	if !ok {
		if u, err := user.LookupId(strconv.FormatInt(uid, 10)); err == nil {
			name = u.Username
		}
		n.users[uid] = name
	}
	return name
}

// group returns the group name for gid, or "" if it can't be found.
func (n *idNames) group(gid int64) string {
	n.mu.Lock()
	defer n.mu.Unlock()
	name, ok := n.groups[gid]
	if !ok {
		if g, err := user.LookupGroupId(strconv.FormatInt(gid, 10)); err == nil {
			name = g.Name
		}
		n.groups[gid] = name
	}
	return name
}

// unixMode returns the permission bits of mode including setuid, setgid
// and sticky in their Unix positions.
func unixMode(mode fs.FileMode) uint32 {
	m := uint32(mode.Perm())
	if mode&fs.ModeSetuid != 0 {
		m |= syscall.S_ISUID
	}
	if mode&fs.ModeSetgid != 0 {
		m |= syscall.S_ISGID
	}
	if mode&fs.ModeSticky != 0 {
		m |= syscall.S_ISVTX
	}
	return m
}

// entryType returns the GDA type for mode.
func entryType(mode fs.FileMode) string {
	switch {
	case mode.IsRegular():
		return TypeFile
	case mode.IsDir():
		return TypeDir
	case mode&fs.ModeSymlink != 0:
		return TypeSymlink
	case mode&fs.ModeNamedPipe != 0:
		return TypeFifo
	case mode&fs.ModeSocket != 0:
		return TypeSocket
	case mode&fs.ModeCharDevice != 0:
		return TypeCharDev
	default:
		return TypeBlockDev
	}
}

// statEntry reads the metadata of the file at p with lstat. name is the
// entry's name relative to its index.
func statEntry(p, name string, names *idNames) (sourceEntry, error) {
	info, err := os.Lstat(p)
	if err != nil {
		return sourceEntry{}, err
	}
	return entryFromInfo(p, name, info, names)
}

func entryFromInfo(p, name string, info fs.FileInfo, names *idNames) (sourceEntry, error) {
	e := sourceEntry{Entry: NewEntry(name, entryType(info.Mode())), path: p}
	e.ModTime = info.ModTime().UTC()
	e.Mode = unixMode(info.Mode())
	if e.Type == TypeFile {
		e.Size = info.Size()
	}
	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		e.UID = int64(st.Uid)
		e.GID = int64(st.Gid)
		e.Owner = names.user(e.UID)
		e.Group = names.group(e.GID)
		if e.Type == TypeFile && st.Nlink > 1 {
			e.HardLink = fmt.Sprintf("%x:%x", uint64(st.Dev), uint64(st.Ino)) //nolint:unconvert // not uint64 on every platform
		}
		if e.Type == TypeCharDev || e.Type == TypeBlockDev {
			rdev := uint64(st.Rdev) //nolint:unconvert // Rdev isn't uint64 on every platform
			e.DevMajor, e.DevMinor = int64(unix.Major(rdev)), int64(unix.Minor(rdev))
		}
	}
	if e.Type == TypeSymlink {
		target, err := os.Readlink(p)
		if err != nil {
			return e, err
		}
		e.LinkTarget = target
	}
	return e, nil
}

// readDir returns the names in the directory at p, sorted.
func readDir(p string) ([]string, error) {
	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	names, err := f.Readdirnames(-1)
	closeErr := f.Close()
	if err != nil {
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
	}
	sort.Strings(names)
	return names, nil
}

// readDirDirs returns the names of the subdirectories of the directory
// at p, from the directory's own entry types where the file system
// gives them, so without reading every entry's metadata.
func readDirDirs(p string) (map[string]bool, error) {
	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	dirs := map[string]bool{}
	for {
		entries, err := f.ReadDir(10000)
		for _, e := range entries {
			if e.IsDir() {
				dirs[e.Name()] = true
			}
		}
		if errors.Is(err, io.EOF) {
			return dirs, nil
		}
		if err != nil {
			return nil, err
		}
	}
}

// excluded returns true if rclone's filters leave out the entry at rel,
// relative to the source root, which is a directory if isDir.
func (b *backup) excluded(rel string, isDir bool, size int64, modTime time.Time) bool {
	if b.filter == nil {
		return false
	}
	if isDir {
		include, err := b.filter.IncludeDirectory(context.Background(), b.filterFs)(rel)
		if err != nil {
			b.errorf("filter %q: %v", rel, err)
			return true
		}
		return !include
	}
	return !b.filter.Include(rel, size, modTime, nil)
}

// sourcePath returns the source path of rel below root.
func sourcePath(root, rel string) string {
	if rel == "" {
		return root
	}
	return filepath.Join(root, filepath.FromSlash(rel))
}
