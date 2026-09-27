//go:build unix

package backup

import (
	"testing"

	_ "github.com/rclone/rclone/backend/local"
	_ "github.com/rclone/rclone/backend/s3"
	"github.com/stretchr/testify/assert"
)

func TestTuneS3(t *testing.T) {
	assert.Equal(t, ":s3,provider=AWS,chunk_size=64Mi,upload_cutoff=268435457B:bucket/lab",
		tuneS3(":s3,provider=AWS:bucket/lab", 256<<20))
	// Settings the user gave are kept.
	assert.Equal(t, ":s3,chunk_size=16Mi,upload_cutoff=5368709120B:bucket",
		tuneS3(":s3,chunk_size=16Mi:bucket", 10<<30))
	assert.Equal(t, ":s3,chunk_size=16Mi,upload_cutoff=1Gi:bucket",
		tuneS3(":s3,chunk_size=16Mi,upload_cutoff=1Gi:bucket", 256<<20))
	// Other remotes are left alone.
	assert.Equal(t, "/tmp/lab", tuneS3("/tmp/lab", 256<<20))
}
