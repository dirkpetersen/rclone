//go:build unix

package backup

import (
	"fmt"
	"strings"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/fspath"
)

// s3ChunkSize is the part size used for multipart uploads to S3 unless
// the user sets one: big enough that large and compressed standalone
// files are uploaded in few requests, small enough that the parts in
// flight for each worker fit in memory.
const s3ChunkSize = "64Mi"

// s3MaxCutoff is the largest upload_cutoff S3 accepts. Sizes without a
// suffix are in KiB, so they are given with B.
const s3MaxCutoff = 5 * 1024 * 1024 * 1024

// tuneS3 returns remote with the S3 upload settings a backup needs added
// where the user hasn't set them: a larger chunk_size, and an
// upload_cutoff above packSize so each pack is uploaded in one request
// checked against its MD5. Other remotes are returned as they are.
func tuneS3(remote string, packSize int64) string {
	info, _, _, config, err := fs.ConfigFs(remote)
	if err != nil || info.Name != "s3" {
		return remote
	}
	var add []string
	if _, ok := config.GetPriority("chunk_size", configmap.PriorityConfig); !ok {
		add = append(add, "chunk_size="+s3ChunkSize)
	}
	if _, ok := config.GetPriority("upload_cutoff", configmap.PriorityConfig); !ok {
		add = append(add, fmt.Sprintf("upload_cutoff=%dB", min(packSize+1, s3MaxCutoff)))
	}
	parsed, err := fspath.Parse(remote)
	if err != nil || len(add) == 0 {
		return remote
	}
	fs.Infof(nil, "gda: uploading to S3 with %s", strings.Join(add, ", "))
	return strings.TrimSuffix(parsed.ConfigString, ":") + "," + strings.Join(add, ",") + ":" + parsed.Path
}
