package gda

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestXattrs(t *testing.T) {
	fakeClock(t)
	src, dst := t.TempDir(), filepath.Join(t.TempDir(), "lab")
	writeFile(t, src, "d/a.txt", 100)
	if err := unix.Lsetxattr(filepath.Join(src, "d/a.txt"), "user.gda.test", []byte("one"), 0); err != nil {
		t.Skipf("no user extended attributes here: %v", err)
	}
	// Read only entries, and a second link to one, get theirs too.
	writeFile(t, src, "ro/b.txt", 100)
	require.NoError(t, unix.Lsetxattr(filepath.Join(src, "ro/b.txt"), "user.gda.test", []byte("ro"), 0))
	require.NoError(t, unix.Lsetxattr(filepath.Join(src, "ro"), "user.gda.test", []byte("dir"), 0))
	require.NoError(t, os.Link(filepath.Join(src, "ro/b.txt"), filepath.Join(src, "d/b-link.txt")))
	require.NoError(t, os.Chmod(filepath.Join(src, "ro/b.txt"), 0o444))
	require.NoError(t, os.Chmod(filepath.Join(src, "ro"), 0o555))
	t.Cleanup(func() { _ = os.Chmod(filepath.Join(src, "ro"), 0o755) })
	opt := testOptions()
	opt.RollupMax = 0
	opt.Xattrs = true
	runBackup(t, src, dst, opt)
	assert.NotEmpty(t, readIndexFile(t, dst, "d")["a.txt"].Xattrs)

	// A changed attribute is a metadata change.
	require.NoError(t, unix.Lsetxattr(filepath.Join(src, "d/a.txt"), "user.gda.test", []byte("two"), 0))
	l := runBackup(t, src, dst, opt)
	assert.Equal(t, int64(1), l.Stats.MetaOnly)

	target := t.TempDir()
	st, err := StartRestore(context.Background(), newDst(t, dst), target, DefaultRestoreOptions())
	require.NoError(t, err)
	assert.Equal(t, StateDone, st.State)
	buf := make([]byte, 16)
	n, err := unix.Lgetxattr(filepath.Join(target, "d/a.txt"), "user.gda.test", buf)
	require.NoError(t, err)
	assert.Equal(t, "two", string(buf[:n]))
	assert.Empty(t, st.Errors)
	for p, want := range map[string]string{"ro/b.txt": "ro", "ro": "dir", "d/b-link.txt": "ro"} {
		n, err := unix.Lgetxattr(filepath.Join(target, p), "user.gda.test", buf)
		require.NoError(t, err, p)
		assert.Equal(t, want, string(buf[:n]), p)
	}
	require.NoError(t, os.Chmod(filepath.Join(target, "ro"), 0o755))
}
