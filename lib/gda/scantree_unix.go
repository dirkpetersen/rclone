//go:build unix

package gda

import (
	"context"
	"sync"

	"github.com/rclone/rclone/fs"
)

// A full run reads the source once, depth first. Each directory is
// committed after its subdirectories have been read, so their totals are
// known, and the decision to roll a subtree up can be made from them:
//
//   - a subtree which may be rolled up is handed to its parent without
//     being committed, as the parent may be rolled up too;
//   - a directory which may not be rolled up is committed with its own
//     index, and so is each subtree handed to it, as the top of a
//     rolled up subtree, as soon as it is clear the directory itself
//     can't be rolled up.
//
// Only the entries of the directories being read and of subtrees which
// may still be rolled up are held in memory, rather than totals for the
// whole tree. Subdirectories are committed before their parents, so a
// parent's index never refers to an index which isn't written yet.

// scanned is a directory read by scanTree.
type scanned struct {
	encName  string          // name in its parent's index
	encoding string          // how the name is encoded, if it is
	row      sourceEntry     // its row in its parent's index
	key      string          // its destination key
	mustRoll bool            // its keys would be too long, so it must be packed with its parent
	summary  dirSummary      // totals of its subtree
	pending  bool            // may be rolled up, so hasn't been committed
	done     bool            // has been read
	entries  []sourceEntry   // if pending, the subtree's entries named relative to it
	keep     map[string]bool // if pending, entries whose previous rows are kept
	failed   bool            // couldn't be read, so its previous row is kept
}

// scanAll backs up the whole source in a single pass.
func (b *backup) scanAll(ctx context.Context) {
	ids := make(chan string, b.opt.Workers)
	for i := range b.opt.Workers {
		ids <- b.workerID(i)
	}
	var sem chan struct{}
	if b.opt.Workers > 1 {
		sem = make(chan struct{}, b.opt.Workers-1)
	}
	root := &scanned{}
	b.scanTree(ctx, root, "", sem, ids)
	if root.pending {
		// The whole tree is small enough to be packed as one unit.
		b.commitWith(ctx, ids, "", "", root.entries, root.keep)
	}
}

// commitWith commits a directory as whichever worker is free.
func (b *backup) commitWith(ctx context.Context, ids chan string, rel, key string, cur []sourceEntry, keep map[string]bool) {
	w := <-ids
	defer func() { ids <- w }()
	b.commitEntries(ctx, w, rel, key, cur, keep)
}

// takeRollup marks the subtree d, which may be rolled up, as committed
// with its own index as the top of a rolled up subtree, and returns what
// to commit, letting go of its entries.
func takeRollup(d *scanned) (rollup scanned) {
	rollup = *d
	d.entries, d.keep = nil, nil
	d.pending = false
	return rollup
}

// commitRollup commits a subtree taken by takeRollup.
func (b *backup) commitRollup(ctx context.Context, d scanned, ids chan string) {
	b.commitWith(ctx, ids, d.row.rel, d.key, d.entries, d.keep)
}

// scanTree reads the directory at rel into d, whose key is set, and
// everything below it. It commits what must have its own index, leaving
// d's totals and, if it may still be rolled up, its entries.
func (b *backup) scanTree(ctx context.Context, d *scanned, rel string, sem chan struct{}, ids chan string) {
	if ctx.Err() != nil || b.isStopped() {
		d.failed = true
		return
	}
	names, err := readDir(sourcePath(b.srcRoot, rel))
	if err != nil {
		b.errorf("read directory %q: %v", rel, err)
		d.failed = true
		return
	}
	var (
		cur  []sourceEntry
		keep = map[string]bool{}
		dirs []*scanned
	)
	for _, name := range names {
		childRel := joinRemote(rel, name)
		encName, encoding := encodeName(name)
		if encoding != "" {
			d.summary.badName = true
		}
		// Checked inside rollups too, so that the same files are skipped
		// whether or not their subtree is rolled up.
		if isReserved(name, rel == "") {
			fs.Logf(nil, "gda: skipping %q: name is reserved for GDA's own objects", childRel)
			b.count(func(s *Stats) *int64 { return &s.Skipped }, 1)
			continue
		}
		e, err := statEntry(sourcePath(b.srcRoot, childRel), encName, b.names)
		if err == nil && b.opt.Xattrs {
			e.Xattrs, err = readXattrs(e.path)
		}
		if err != nil {
			b.errorf("stat %q: %v", childRel, err)
			d.summary.unreadable = true
			keep[encName] = true
			continue
		}
		e.NameEncoding = encoding
		e.rel = childRel
		if e.IsDir() {
			childKey := joinRemote(d.key, encName)
			dirs = append(dirs, &scanned{
				encName:  encName,
				encoding: encoding,
				row:      e,
				key:      childKey,
				mustRoll: b.dirKeysTooLong(childKey),
			})
			continue
		}
		d.summary.treeFiles++
		if e.Type == TypeFile {
			d.summary.treeSize += e.Size
			if e.Size >= b.opt.StandaloneMin {
				d.summary.standalone = true
			}
		}
		cur = append(cur, e)
	}

	// Once this directory can't be rolled up, a subtree which may is
	// committed as soon as it is read, rather than held until the rest
	// are.
	var (
		mu sync.Mutex
		wg sync.WaitGroup
	)
	finish := func(child *scanned) {
		mu.Lock()
		child.done = true
		if child.failed {
			d.summary.unreadable = true
		} else {
			s := &child.summary
			d.summary.treeSize += s.treeSize
			d.summary.treeFiles += s.treeFiles
			d.summary.standalone = d.summary.standalone || s.standalone
			d.summary.unreadable = d.summary.unreadable || s.unreadable
			d.summary.badName = d.summary.badName || s.badName
		}
		var early []scanned
		if !b.eligible(&d.summary) && !d.mustRoll {
			for _, c := range dirs {
				if c.done && c.pending && !c.mustRoll {
					early = append(early, takeRollup(c))
				}
			}
		}
		mu.Unlock()
		for _, c := range early {
			b.commitRollup(ctx, c, ids)
		}
	}
	for _, child := range dirs {
		scan := func() {
			b.scanTree(ctx, child, child.row.rel, sem, ids)
			finish(child)
		}
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

	d.pending = b.eligible(&d.summary) || d.mustRoll && !d.summary.badName
	for _, child := range dirs {
		row := child.row
		row.TreeSize, row.TreeFiles = child.summary.treeSize, child.summary.treeFiles
		switch {
		case child.failed:
			keep[child.encName] = true
		case child.pending && (d.pending || child.mustRoll && child.encoding == ""):
			// Packed with this directory.
			row.Listing = ListingRollup
			cur = append(cur, row)
			for _, e := range child.entries {
				e.Name = child.encName + "/" + e.Name
				cur = append(cur, e)
			}
			for name := range child.keep {
				keep[child.encName+"/"+name] = true
			}
			child.entries, child.keep = nil, nil
			b.count(func(s *Stats) *int64 { return &s.RollupDirs }, 1)
		case child.mustRoll:
			b.errorf("skipping directory %q: its keys would be longer than %d bytes", child.row.rel, maxKeyLength)
			keep[child.encName] = true
		default:
			if child.pending {
				b.commitRollup(ctx, takeRollup(child), ids)
			}
			row.Listing = ListingIndex
			cur = append(cur, row)
		}
	}
	switch {
	case d.pending:
		d.entries, d.keep = cur, keep
	case d.mustRoll:
		// Can't be packed with its parent, which reports it.
	default:
		b.commitWith(ctx, ids, rel, d.key, cur, keep)
	}
}
