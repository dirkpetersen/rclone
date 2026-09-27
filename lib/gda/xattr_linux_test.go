package gda

import (
	"context"
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
}
