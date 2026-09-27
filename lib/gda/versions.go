package gda

import (
	"context"
	"errors"
	"strings"

	"github.com/rclone/rclone/fs"
)

// In a bucket with versioning enabled, a changed standalone file is
// stored under the same key, and its row records the version ID, so
// the version it had at any run can be read. Backends support this by
// implementing these interfaces, as the fork's S3 backend does.

// versionedFs is a backend which can read a given version of an object.
type versionedFs interface {
	// NewObjectVersion returns the version versionID of the object at
	// remote.
	NewObjectVersion(ctx context.Context, remote, versionID string) (fs.Object, error)
	// IsVersioned returns true if versioning is enabled.
	IsVersioned(ctx context.Context) (bool, error)
}

// versionedObject is an object which knows its version.
type versionedObject interface {
	// VersionID returns the ID of the object's version, or "".
	VersionID() string
}

// restorableObject is an archived object whose restore can be requested
// on its own, whichever version it is.
type restorableObject interface {
	// RequestRestore asks for the object to be restored for days days
	// with tier.
	RequestRestore(ctx context.Context, tier string, days int32) error
}

// versionSep separates a key from a version ID in the object references
// restores use. Keys never hold control characters, as names which do
// are encoded.
const versionSep = "\x00"

// versionKey returns a reference to version of the object at key, or
// just key if version is "".
func versionKey(key, version string) string {
	if version == "" {
		return key
	}
	return key + versionSep + version
}

// splitVersionKey returns the key and version a reference is made of.
func splitVersionKey(ref string) (key, version string) {
	key, version, _ = strings.Cut(ref, versionSep)
	return key, version
}

// newDataObject returns the object a reference made by versionKey
// refers to.
func newDataObject(ctx context.Context, f fs.Fs, ref string) (fs.Object, error) {
	key, version := splitVersionKey(ref)
	if version == "" {
		return f.NewObject(ctx, key)
	}
	vf, ok := f.(versionedFs)
	if !ok {
		return nil, errors.New("the destination can't read object versions")
	}
	return vf.NewObjectVersion(ctx, key, version)
}

// isVersioned returns true if f stores new versions of an object under
// the same key.
func isVersioned(ctx context.Context, f fs.Fs) (bool, error) {
	vf, ok := f.(versionedFs)
	if !ok {
		return false, nil
	}
	return vf.IsVersioned(ctx)
}
