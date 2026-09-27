package s3

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/rclone/rclone/fs"
)

// VersionID returns the ID of the version of the object o refers to, as
// set when it was listed or opened by version, or as reported by the
// last HEAD or upload. It is "" if the bucket isn't versioned.
func (o *Object) VersionID() string {
	if o.versionID != nil {
		return *o.versionID
	}
	return deref(o.headVersion)
}

// NewObjectVersion returns the version versionID of the object at
// remote, or fs.ErrorObjectNotFound if there is no such version.
func (f *Fs) NewObjectVersion(ctx context.Context, remote, versionID string) (fs.Object, error) {
	o := &Object{fs: f, remote: remote, versionID: &versionID}
	if err := o.readMetaData(ctx); err != nil {
		return nil, err
	}
	return o, nil
}

// RequestRestore asks for the archived object o, or the version of it
// o refers to, to be restored for days days with tier. It returns
// nothing to do if o isn't archived, as the restore backend command
// does.
func (o *Object) RequestRestore(ctx context.Context, tier string, days int32) error {
	if o.storageClass == nil || (*o.storageClass != "GLACIER" && *o.storageClass != "DEEP_ARCHIVE" && *o.storageClass != "INTELLIGENT_TIERING") {
		return nil
	}
	bucket, bucketPath := o.split()
	req := s3.RestoreObjectInput{
		Bucket:    &bucket,
		Key:       &bucketPath,
		VersionId: o.versionID,
		RestoreRequest: &types.RestoreRequest{
			GlacierJobParameters: &types.GlacierJobParameters{Tier: types.Tier(tier)},
		},
	}
	if *o.storageClass != "INTELLIGENT_TIERING" {
		req.RestoreRequest.Days = &days
	}
	err := o.fs.pacer.Call(func() (bool, error) {
		_, err := o.fs.c.RestoreObject(ctx, &req)
		return o.fs.shouldRetry(ctx, err)
	})
	if err != nil {
		return fmt.Errorf("restore %q: %w", o.remote, err)
	}
	return nil
}

// IsVersioned returns true if versioning is enabled on the bucket of
// the Fs's root.
func (f *Fs) IsVersioned(ctx context.Context) (bool, error) {
	status, err := f.setGetVersioning(ctx)
	if err != nil {
		return false, err
	}
	return status == "Enabled", nil
}
