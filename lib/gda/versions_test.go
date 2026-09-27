//go:build unix

package gda

import (
	"context"
	"testing"

	"github.com/rclone/rclone/fs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVersionKey(t *testing.T) {
	assert.Equal(t, "a/b", versionKey("a/b", ""))
	key, version := splitVersionKey(versionKey("a/b", "v1"))
	assert.Equal(t, "a/b", key)
	assert.Equal(t, "v1", version)
	key, version = splitVersionKey("a/b")
	assert.Equal(t, "a/b", key)
	assert.Equal(t, "", version)
}

// versionFs reports every object as being at version.
type versionFs struct {
	fs.Fs
	version string
}

type versionObject struct {
	fs.Object
	version string
}

func (o versionObject) VersionID() string { return o.version }

func (f *versionFs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	o, err := f.Fs.NewObject(ctx, remote)
	if err != nil {
		return nil, err
	}
	return versionObject{Object: o, version: f.version}, nil
}

func TestNameTakenVersioned(t *testing.T) {
	dst := t.TempDir()
	writeFile(t, dst, "big.bin", 10)
	f := &versionFs{Fs: newDst(t, dst), version: "v2"}
	b := &backup{d: &dest{f: f}, versioned: true}
	ctx := context.Background()

	// A new name is free.
	taken, err := b.nameTaken(ctx, "new.bin", "new.bin", &sourceEntry{})
	require.NoError(t, err)
	assert.False(t, taken)
	// The version the previous row refers to by ID is replaced.
	taken, err = b.nameTaken(ctx, "big.bin", "big.bin", &sourceEntry{prev: &Entry{Location: "big.bin", VersionID: "v2"}})
	require.NoError(t, err)
	assert.False(t, taken)
	// Any other object keeps the name: one no row refers to by version,
	// or which isn't the previous version.
	for _, prev := range []*Entry{nil, {Location: "big.bin"}, {Location: "big.bin", VersionID: "v1"}, {Location: "big.bin.gda.x.w01", VersionID: "v2"}} {
		taken, err = b.nameTaken(ctx, "big.bin", "big.bin", &sourceEntry{prev: prev})
		require.NoError(t, err)
		assert.True(t, taken, "%+v", prev)
	}
}
