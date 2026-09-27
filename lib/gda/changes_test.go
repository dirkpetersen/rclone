//go:build unix

package gda

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseChanges(t *testing.T) {
	root := "/pool/src"
	got, err := ParseChanges(strings.NewReader(
		"M\t/pool/src/d07/f1.dat\n"+
			"+\tF\t/pool/src/d19/new file.dat\n"+
			"R\t/pool/src/a/old\t/pool/src/b/new\n"+
			"M\t/pool/src/with\\0040space\n"+
			"-\t/pool/other/x\n"+
			"M\t/pool/src/\n",
	), ChangesZFS, root)
	require.NoError(t, err)
	assert.Equal(t, []string{"d07/f1.dat", "d19/new file.dat", "a/old", "b/new", "with space"}, got)

	got, err = ParseChanges(strings.NewReader("d01/x\n/pool/src/d02/y\n\n/elsewhere/z\n"), ChangesLines, root)
	require.NoError(t, err)
	assert.Equal(t, []string{"d01/x", "d02/y"}, got)

	// A source in a snapshot, and one below the snapshot's root.
	got, err = ParseChanges(strings.NewReader("M\t/pool/src/d07/f1.dat\n"), ChangesZFS, "/pool/src/.zfs/snapshot/today")
	require.NoError(t, err)
	assert.Equal(t, []string{"d07/f1.dat"}, got)
	got, err = ParseChanges(strings.NewReader("M\t/pool/src/d07/f1.dat\n"), ChangesZFS, "/pool/src/.zfs/snapshot/today/d07")
	require.NoError(t, err)
	assert.Equal(t, []string{"f1.dat"}, got)

	_, err = ParseChanges(strings.NewReader("x"), "gpfs", root)
	assert.Error(t, err)
}

func TestBackupChanges(t *testing.T) {
	fakeClock(t)
	src := parallelTree(t)
	dst := filepath.Join(t.TempDir(), "lab")
	opt := testOptions()
	opt.RollupMax = 16
	runBackup(t, src, dst, opt)

	// Change a file, add one, delete one, change a file in a rolled up
	// subtree, and add a new directory tree listed only by its top.
	writeFile(t, src, "d07/f1.dat", 99)
	writeFile(t, src, "d19/new.dat", 42)
	require.NoError(t, os.Remove(filepath.Join(src, "d03/f0.dat")))
	writeFile(t, src, "d11/sub/deep/x11.txt", 6)
	writeFile(t, src, "fresh/a/b/c.txt", 7)
	writeFile(t, src, "fresh/a/d.txt", 8)
	changes := []string{"d07/f1.dat", "d19/new.dat", "d03/f0.dat", "d11/sub/deep/x11.txt", "fresh"}

	changed := opt
	changed.Changes = changes
	l := runBackup(t, src, dst, changed)
	assert.Equal(t, int64(0), l.Stats.Errors)
	// The ledger counts the change list rather than holding it.
	assert.Equal(t, len(changes), l.Changes)
	assert.Nil(t, l.Options.Changes)
	// Only the affected directories were backed up.
	assert.Less(t, l.Stats.IndexedDirs, int64(15))

	// The result is what a full backup of the new source gives.
	full := filepath.Join(t.TempDir(), "lab")
	runBackup(t, src, full, opt)
	assert.Equal(t, indexRows(t, full), indexRows(t, dst))

	target := t.TempDir()
	st, err := StartRestore(context.Background(), newDst(t, dst), target, DefaultRestoreOptions())
	require.NoError(t, err)
	assert.Equal(t, StateDone, st.State)
	assertSameTree(t, src, target)

	// A change run needs a full backup first.
	empty := filepath.Join(t.TempDir(), "lab")
	f := newDst(t, empty)
	_, err = Backup(context.Background(), src, f, changed)
	assert.ErrorContains(t, err, "errors")
}

func TestBackupChangesUnrollsSubtree(t *testing.T) {
	fakeClock(t)
	src := parallelTree(t)
	dst := filepath.Join(t.TempDir(), "lab")
	opt := testOptions()
	opt.RollupMax = 16
	runBackup(t, src, dst, opt)
	require.Equal(t, ListingRollup, readIndexFile(t, dst, "d11/sub")["deep"].Listing)

	// The rolled up subtree grows too big to roll up, so the directory
	// below it, which the change list doesn't name, needs its own index.
	writeFile(t, src, "d11/sub/new.dat", 100)
	changed := opt
	changed.Changes = []string{"d11/sub/new.dat"}
	l := runBackup(t, src, dst, changed)
	assert.Equal(t, int64(0), l.Stats.Errors)

	full := filepath.Join(t.TempDir(), "lab")
	runBackup(t, src, full, opt)
	assert.Equal(t, indexRows(t, full), indexRows(t, dst))
	target := t.TempDir()
	st, err := StartRestore(context.Background(), newDst(t, dst), target, DefaultRestoreOptions())
	require.NoError(t, err)
	assert.Equal(t, StateDone, st.State)
	assertSameTree(t, src, target)
}

func TestBackupChangesUnrollParallel(t *testing.T) {
	fakeClock(t)
	src := parallelTree(t)
	dst := filepath.Join(t.TempDir(), "lab")
	opt := testOptions()
	opt.RollupMax = 16
	opt.Workers = 4
	runBackup(t, src, dst, opt)
	var changes []string
	for i := range 10 {
		rel := fmt.Sprintf("d%02d/sub/new.dat", i)
		writeFile(t, src, rel, 100)
		changes = append(changes, rel)
	}
	changed := opt
	changed.Changes = changes
	l := runBackup(t, src, dst, changed)
	assert.Equal(t, int64(0), l.Stats.Errors)
	full := filepath.Join(t.TempDir(), "lab")
	runBackup(t, src, full, opt)
	assert.Equal(t, indexRows(t, full), indexRows(t, dst))
}
