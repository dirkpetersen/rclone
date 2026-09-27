//go:build unix

package gda

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Change list formats.
const (
	// ChangesLines is one changed path per line, absolute or relative to
	// the source root. Lists from GPFS policies or Lustre changelogs can
	// be turned into this.
	ChangesLines = "lines"
	// ChangesZFS is the output of zfs diff -H, optionally with -F.
	ChangesZFS = "zfs"
)

// ParseChanges reads a change list in format and returns the changed
// paths below srcRoot, relative to it. Paths outside srcRoot are left out.
//
// zfs diff names paths below where the file system is mounted, so for a
// source in a snapshot, such as /pool/data/.zfs/snapshot/today, they are
// taken as below /pool/data.
func ParseChanges(r io.Reader, format, srcRoot string) ([]string, error) {
	srcRoot = filepath.Clean(srcRoot)
	pathRoot := srcRoot
	if format == ChangesZFS {
		if i := strings.Index(srcRoot, "/.zfs/snapshot/"); i >= 0 {
			mount := srcRoot[:i]
			rest := strings.TrimPrefix(srcRoot[i:], "/.zfs/snapshot/")
			_, below, _ := strings.Cut(rest, "/")
			pathRoot = filepath.Join(mount, below)
		}
	}
	var out []string
	add := func(p string) {
		if !filepath.IsAbs(p) {
			p = filepath.Join(pathRoot, p)
		}
		rel, err := filepath.Rel(pathRoot, filepath.Clean(p))
		if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, "../") {
			return
		}
		out = append(out, filepath.ToSlash(rel))
	}
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		switch format {
		case ChangesLines:
			if strings.TrimSpace(line) != "" {
				add(line)
			}
		case ChangesZFS:
			// Change type, an optional file type with -F, then the path,
			// and for renames the new path.
			for _, field := range strings.Split(line, "\t") {
				if strings.HasPrefix(field, "/") {
					p, err := unescapeZFS(field)
					if err != nil {
						return nil, fmt.Errorf("zfs diff line %q: %w", line, err)
					}
					add(p)
				}
			}
		default:
			return nil, fmt.Errorf("unknown change list format %q: use %s or %s", format, ChangesLines, ChangesZFS)
		}
	}
	return out, scanner.Err()
}

// unescapeZFS undoes the \NNNN octal escapes (a backslash and four octal
// digits) which zfs diff uses for bytes such as spaces and newlines in
// paths.
func unescapeZFS(s string) (string, error) {
	if !strings.Contains(s, `\`) {
		return s, nil
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+5 <= len(s) {
			if n, err := strconv.ParseUint(s[i+1:i+5], 8, 8); err == nil {
				b.WriteByte(byte(n))
				i += 4
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String(), nil
}

// dirtyDirs returns the directories a change run must look at: the
// parent of every changed path, the path itself if it is a directory,
// and every ancestor of those up to the root, whose subtree totals change.
func (b *backup) dirtyDirs(changes []string) map[string]bool {
	dirty := map[string]bool{"": true}
	mark := func(rel string) {
		for !dirty[rel] {
			dirty[rel] = true
			rel = parentRel(rel)
		}
	}
	for _, rel := range changes {
		mark(parentRel(rel))
		if info, err := os.Lstat(sourcePath(b.srcRoot, rel)); err == nil && info.IsDir() {
			mark(rel)
		}
	}
	return dirty
}

// summarizeChanged computes the summaries of the dirty directories below
// rel, reading only dirty directories from the source. The subtree
// totals of other directories come from their rows in the previous
// indexes; a directory which has none is new, so is scanned in full and
// processed in full.
func (b *backup) summarizeChanged(ctx context.Context, rel string, t *tree) (*dirSummary, error) {
	s := &dirSummary{}
	b.setSummary(rel, s)
	names, err := readDir(sourcePath(b.srcRoot, rel))
	if err != nil {
		b.errorf("read directory %q: %v", rel, err)
		s.unreadable = true
		return s, nil
	}
	for _, name := range names {
		childRel := joinRemote(rel, name)
		if _, encoding := encodeName(name); encoding != "" {
			s.badName = true
		}
		info, err := os.Lstat(sourcePath(b.srcRoot, childRel))
		if err != nil {
			b.errorf("stat %q: %v", childRel, err)
			s.unreadable = true
			continue
		}
		if b.excluded(childRel, info.IsDir(), info.Size(), info.ModTime()) {
			continue
		}
		if !info.IsDir() {
			s.treeFiles++
			if info.Mode().IsRegular() {
				s.treeSize += info.Size()
				if info.Size() >= b.opt.StandaloneMin {
					s.standalone = true
				}
			}
			continue
		}
		var child *dirSummary
		switch row, err := b.prevRow(ctx, t, childRel); {
		case err != nil:
			return nil, err
		case b.dirty[childRel]:
			if child, err = b.summarizeChanged(ctx, childRel, t); err != nil {
				return nil, err
			}
		case row == nil || !row.IsDir() || row.TreeSize < 0:
			child = b.summarize(childRel, nil)
			b.markNew(childRel)
		default:
			child = &dirSummary{
				treeSize:   row.TreeSize,
				treeFiles:  row.TreeFiles,
				standalone: row.TreeSize >= b.opt.StandaloneMin,
				// An indexed subtree stays indexed until a full run
				// decides to roll it up.
				noRollup: row.Listing == ListingIndex,
			}
			b.setSummary(childRel, child)
		}
		s.treeSize += child.treeSize
		s.treeFiles += child.treeFiles
		s.standalone = s.standalone || child.standalone
		s.unreadable = s.unreadable || child.unreadable
		s.badName = s.badName || child.badName
		s.noRollup = s.noRollup || child.noRollup
	}
	return s, nil
}

// prevRow returns the previous index row of the directory at rel, or nil.
func (b *backup) prevRow(ctx context.Context, t *tree, rel string) (*Entry, error) {
	_, _, row, _, err := t.resolve(ctx, rel)
	if errors.Is(err, errNotFound) {
		return nil, nil
	}
	return row, err
}

// markNew marks the directory at rel and everything below it as new, so
// processing goes into all of it.
func (b *backup) markNew(rel string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.newDirs[rel] = true
}

// wanted returns true if a change run must process the directory at rel:
// it is dirty, or it or a directory above it is new.
func (b *backup) wanted(rel string) bool {
	if b.dirty == nil {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.dirty[rel] {
		return true
	}
	for p := rel; ; p = parentRel(p) {
		if b.newDirs[p] {
			return true
		}
		if p == "" {
			return false
		}
	}
}

// backupChanges runs a change run: only directories affected by changes
// are read and backed up.
func (b *backup) backupChanges(ctx context.Context, changes []string) error {
	t := newTree(b.d, "")
	root, err := t.entries(ctx, "")
	if err != nil {
		return err
	}
	if root == nil {
		return errors.New("a change run needs a full backup first")
	}
	b.dirty = b.dirtyDirs(changes)
	b.newDirs = map[string]bool{}
	if _, err := b.summarizeChanged(ctx, "", t); err != nil {
		return err
	}
	b.processItems(ctx, []childDir{{}})
	return nil
}
