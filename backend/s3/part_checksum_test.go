package s3

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/fs/object"
	"github.com/rclone/rclone/lib/random"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeMultipartServer is just enough of S3 to do a multipart upload.
// It checks the part checksums like S3 does and records what it got.
type fakeMultipartServer struct {
	t  *testing.T
	mu sync.Mutex
	// from CreateMultipartUpload
	checksumAlgorithm string
	// per part number
	parts      map[int][]byte
	contentMD5 map[int]string
	crc32c     map[int]string
	// from CompleteMultipartUpload
	completed []fakeCompletedPart
	// unexpected requests
	unexpected []string
}

type fakeCompletedPart struct {
	PartNumber     int
	ETag           string
	ChecksumCRC32C string
}

func newFakeMultipartServer(t *testing.T) *fakeMultipartServer {
	return &fakeMultipartServer{
		t:          t,
		parts:      map[int][]byte{},
		contentMD5: map[int]string{},
		crc32c:     map[int]string{},
	}
}

func crc32cBase64(data []byte) string {
	var sum [4]byte
	binary.BigEndian.PutUint32(sum[:], crc32.Checksum(data, crc32cTable))
	return base64.StdEncoding.EncodeToString(sum[:])
}

func (s *fakeMultipartServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q := r.URL.Query()
	body, err := io.ReadAll(r.Body)
	require.NoError(s.t, err)
	switch {
	case r.Method == http.MethodPost && q.Has("uploads"):
		s.checksumAlgorithm = r.Header.Get("X-Amz-Checksum-Algorithm")
		_, _ = io.WriteString(w, `<InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>file</Key><UploadId>upload-1</UploadId></InitiateMultipartUploadResult>`)
	case r.Method == http.MethodPut && q.Has("partNumber"):
		var part int
		_, err := fmt.Sscan(q.Get("partNumber"), &part)
		require.NoError(s.t, err)
		if md5sum := r.Header.Get("Content-Md5"); md5sum != "" {
			s.contentMD5[part] = md5sum
			sum := md5.Sum(body)
			if md5sum != base64.StdEncoding.EncodeToString(sum[:]) {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = io.WriteString(w, `<Error><Code>BadDigest</Code></Error>`)
				return
			}
		}
		if crc := r.Header.Get("X-Amz-Checksum-Crc32c"); crc != "" {
			s.crc32c[part] = crc
			if crc != crc32cBase64(body) {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = io.WriteString(w, `<Error><Code>BadDigest</Code></Error>`)
				return
			}
			w.Header().Set("X-Amz-Checksum-Crc32c", crc)
		}
		s.parts[part] = body
		sum := md5.Sum(body)
		w.Header().Set("ETag", `"`+hex.EncodeToString(sum[:])+`"`)
	case r.Method == http.MethodPost && q.Has("uploadId"):
		var req struct {
			Parts []fakeCompletedPart `xml:"Part"`
		}
		require.NoError(s.t, xml.Unmarshal(body, &req))
		s.completed = req.Parts
		var md5s []byte
		for i := 1; i <= len(s.parts); i++ {
			sum := md5.Sum(s.parts[i])
			md5s = append(md5s, sum[:]...)
		}
		etag := md5.Sum(md5s)
		_, _ = fmt.Fprintf(w, `<CompleteMultipartUploadResult><Bucket>bucket</Bucket><Key>file</Key><ETag>"%s-%d"</ETag></CompleteMultipartUploadResult>`, hex.EncodeToString(etag[:]), len(s.parts))
	default:
		s.unexpected = append(s.unexpected, r.Method+" "+r.URL.String())
		w.WriteHeader(http.StatusNotImplemented)
	}
}

// uploadWithPartChecksum uploads data as a multipart upload of 5 MiB
// parts to a fake server and returns what the server saw
func uploadWithPartChecksum(t *testing.T, checksum string, data []byte) *fakeMultipartServer {
	ctx := context.Background()
	fake := newFakeMultipartServer(t)
	srv := httptest.NewServer(fake)
	defer srv.Close()

	f, err := fs.NewFs(ctx, fmt.Sprintf(":s3,provider=Other,endpoint='%s',access_key_id=key,secret_access_key=secret,force_path_style,no_check_bucket,no_head,chunk_size=5Mi,upload_cutoff=5Mi,upload_concurrency=2,part_checksum=%s:bucket", srv.URL, checksum))
	require.NoError(t, err)

	src := object.NewStaticObjectInfo("file", time.Now(), int64(len(data)), true, nil, nil)
	_, err = f.Put(ctx, bytes.NewReader(data), src)
	require.NoError(t, err)
	require.Empty(t, fake.unexpected)
	require.Len(t, fake.parts, 3)
	var got []byte
	for i := 1; i <= len(fake.parts); i++ {
		got = append(got, fake.parts[i]...)
	}
	require.Equal(t, data, got, "the parts make up the file")
	return fake
}

func TestPartChecksum(t *testing.T) {
	data := []byte(random.String(12 << 20))
	partData := func(i int) []byte {
		start := (i - 1) * 5 << 20
		return data[start:min(start+5<<20, len(data))]
	}

	t.Run("md5", func(t *testing.T) {
		fake := uploadWithPartChecksum(t, "md5", data)
		assert.Equal(t, "", fake.checksumAlgorithm)
		assert.Len(t, fake.contentMD5, 3)
		assert.Empty(t, fake.crc32c)
		for _, p := range fake.completed {
			assert.Equal(t, "", p.ChecksumCRC32C)
		}
	})

	t.Run("md5-simd", func(t *testing.T) {
		require.NoError(t, hash.SetMD5SIMD(2))
		defer func() { require.NoError(t, hash.SetMD5SIMD(0)) }()
		fake := uploadWithPartChecksum(t, "md5", data)
		require.Len(t, fake.contentMD5, 3)
		for i := 1; i <= 3; i++ {
			sum := md5.Sum(partData(i))
			assert.Equal(t, base64.StdEncoding.EncodeToString(sum[:]), fake.contentMD5[i], "part %d", i)
		}
	})

	t.Run("crc32c", func(t *testing.T) {
		fake := uploadWithPartChecksum(t, "crc32c", data)
		assert.Equal(t, "CRC32C", fake.checksumAlgorithm)
		assert.Empty(t, fake.contentMD5)
		require.Len(t, fake.crc32c, 3)
		require.Len(t, fake.completed, 3)
		for _, p := range fake.completed {
			assert.Equal(t, crc32cBase64(partData(p.PartNumber)), p.ChecksumCRC32C, "part %d", p.PartNumber)
		}
	})

	t.Run("none", func(t *testing.T) {
		fake := uploadWithPartChecksum(t, "none", data)
		assert.Equal(t, "", fake.checksumAlgorithm)
		assert.Empty(t, fake.contentMD5)
		assert.Empty(t, fake.crc32c)
		assert.Len(t, fake.completed, 3)
	})
}
