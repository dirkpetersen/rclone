//go:build unix

package gda

import (
	"context"
	"slices"
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

// versionStore is a versioned bucket holding versions of keys, which
// records what is removed.
type versionStore struct {
	fs.Fs
	versions map[string][]string
	removed  []string
	markers  []string
}

type storedVersion struct {
	fs.Object
	s       *versionStore
	key     string
	version string
}

func (o storedVersion) Remove(ctx context.Context) error {
	if o.version == "" {
		o.s.markers = append(o.s.markers, o.key)
	} else {
		o.s.removed = append(o.s.removed, versionKey(o.key, o.version))
	}
	return nil
}

func (s *versionStore) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	if len(s.versions[remote]) == 0 {
		return nil, fs.ErrorObjectNotFound
	}
	return storedVersion{s: s, key: remote}, nil
}

func (s *versionStore) NewObjectVersion(ctx context.Context, remote, versionID string) (fs.Object, error) {
	if !slices.Contains(s.versions[remote], versionID) {
		return nil, fs.ErrorObjectNotFound
	}
	return storedVersion{s: s, key: remote, version: versionID}, nil
}

func (s *versionStore) IsVersioned(ctx context.Context) (bool, error) { return true, nil }

func (s *versionStore) ObjectVersionIDs(ctx context.Context, remote string) ([]string, error) {
	return s.versions[remote], nil
}

func TestRemoveData(t *testing.T) {
	ctx := context.Background()
	s := &versionStore{versions: map[string][]string{
		"pack.tar":  {"v1"},
		"old.bin":   {"v3", "v2", "null"},
		"new.bin":   {"v5", "v4"},
		"moved.bin": {"v6"},
	}}

	// Without versioning a key is deleted.
	require.NoError(t, removeData(ctx, s, false, "pack.tar", nil))
	assert.Equal(t, []string{"pack.tar"}, s.markers)
	s.markers = nil

	// With versioning the version is deleted: the one named, the one
	// stored before versioning, or the only one.
	require.NoError(t, removeData(ctx, s, true, versionKey("new.bin", "v4"), nil))
	require.NoError(t, removeData(ctx, s, true, "pack.tar", nil))
	require.NoError(t, removeData(ctx, s, true, "old.bin", map[string]bool{"v3": true}))
	assert.Equal(t, []string{versionKey("new.bin", "v4"), versionKey("pack.tar", "v1"), versionKey("old.bin", "null")}, s.removed)

	// Versions rows refer to are kept, and so is a key with several
	// versions, none from before versioning.
	assert.ErrorIs(t, removeData(ctx, s, true, "moved.bin", map[string]bool{"v6": true}), errVersionsInUse)
	assert.ErrorIs(t, removeData(ctx, s, true, versionKey("new.bin", "v5"), map[string]bool{"v5": true}), errVersionsInUse)
	assert.ErrorContains(t, removeData(ctx, s, true, "new.bin", nil), "2 versions")
	assert.ErrorIs(t, removeData(ctx, s, true, "gone.bin", nil), fs.ErrorObjectNotFound)
	assert.Len(t, s.removed, 3)
	assert.Empty(t, s.markers)
}
