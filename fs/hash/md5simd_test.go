package hash

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sizes which cross the MD5 block size (64) and md5-simd's internal
// block size (32 KiB)
var md5SIMDTestSizes = []int{0, 1, 55, 56, 63, 64, 65, 127, 128, 1000, 32<<10 - 1, 32 << 10, 32<<10 + 1, 64<<10 + 63, 1<<20 + 7}

// withMD5SIMD enables md5-simd for the test, also on CPUs without
// AVX2 where md5-simd's fallback server exercises the same code here
func withMD5SIMD(t *testing.T, servers int) {
	t.Helper()
	supported := md5SIMDSupported
	md5SIMDSupported = true
	require.NoError(t, SetMD5SIMD(servers))
	t.Cleanup(func() {
		require.NoError(t, SetMD5SIMD(0))
		md5SIMDSupported = supported
	})
}

func randomData(seed int64, n int) []byte {
	data := make([]byte, n)
	rand.New(rand.NewSource(seed)).Read(data)
	return data
}

// writeInPieces writes data to h in pieces of random size
func writeInPieces(t *testing.T, h interface{ Write([]byte) (int, error) }, data []byte, seed int64) {
	r := rand.New(rand.NewSource(seed))
	for len(data) > 0 {
		n := min(1+r.Intn(70<<10), len(data))
		written, err := h.Write(data[:n])
		require.NoError(t, err)
		require.Equal(t, n, written)
		data = data[n:]
	}
}

func TestMD5SIMDSelection(t *testing.T) {
	assert.Equal(t, 0, MD5SIMDServers())
	_, isSIMD := NewMD5().(*md5SIMDHasher)
	assert.False(t, isSIMD, "crypto/md5 by default")

	withMD5SIMD(t, 2)
	assert.Equal(t, 2, MD5SIMDServers())
	h := NewMD5()
	_, isSIMD = h.(*md5SIMDHasher)
	assert.True(t, isSIMD)
	ReleaseHasher(h)

	require.NoError(t, SetMD5SIMD(0))
	assert.Equal(t, 0, MD5SIMDServers())
	_, isSIMD = NewMD5().(*md5SIMDHasher)
	assert.False(t, isSIMD)

	assert.Error(t, SetMD5SIMD(65))
	ReleaseHasher(md5.New()) // no-op for other hashers
}

func TestMD5SIMDUnsupportedCPU(t *testing.T) {
	supported := md5SIMDSupported
	md5SIMDSupported = false
	defer func() { md5SIMDSupported = supported }()
	require.NoError(t, SetMD5SIMD(2))
	assert.False(t, MD5SIMDSupported())
	assert.Equal(t, 0, MD5SIMDServers())
	_, isSIMD := NewMD5().(*md5SIMDHasher)
	assert.False(t, isSIMD, "crypto/md5 without AVX2")
}

func TestMD5SIMDMatchesCryptoMD5(t *testing.T) {
	for _, servers := range []int{1, 3} {
		t.Run(fmt.Sprintf("servers=%d", servers), func(t *testing.T) {
			withMD5SIMD(t, servers)
			for i, size := range md5SIMDTestSizes {
				data := randomData(int64(i), size)
				want := md5.Sum(data)

				// one write
				h := NewMD5()
				_, err := h.Write(data)
				require.NoError(t, err)
				assert.Equal(t, want[:], h.Sum(nil), "size %d", size)
				ReleaseHasher(h)

				// many writes of odd sizes
				h = NewMD5()
				writeInPieces(t, h, data, int64(size))
				assert.Equal(t, want[:], h.Sum(nil), "size %d in pieces", size)
				ReleaseHasher(h)
			}
		})
	}
}

func TestMD5SIMDHashInterface(t *testing.T) {
	withMD5SIMD(t, 1)
	h := NewMD5()
	defer ReleaseHasher(h)
	assert.Equal(t, md5.Size, h.Size())
	assert.Equal(t, md5.BlockSize, h.BlockSize())

	// Sum appends and doesn't change the state
	data := randomData(1, 100_000)
	_, _ = h.Write(data[:40_000])
	first := md5.Sum(data[:40_000])
	assert.Equal(t, append([]byte("x"), first[:]...), h.Sum([]byte("x")))
	_, _ = h.Write(data[40_000:])
	all := md5.Sum(data)
	assert.Equal(t, all[:], h.Sum(nil))

	// Reset starts again
	h.Reset()
	_, _ = h.Write(data[:123])
	part := md5.Sum(data[:123])
	assert.Equal(t, part[:], h.Sum(nil))
}

func TestMD5SIMDConcurrent(t *testing.T) {
	withMD5SIMD(t, 2)
	const streams = 48
	var wg sync.WaitGroup
	errs := make(chan error, streams)
	for i := range streams {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range 5 {
				data := randomData(int64(i*100+j), 100_000+i*1009+j*7)
				want := md5.Sum(data)
				h := NewMD5()
				r := rand.New(rand.NewSource(int64(i)))
				rest := data
				for len(rest) > 0 {
					n := min(1+r.Intn(40<<10), len(rest))
					_, _ = h.Write(rest[:n])
					rest = rest[n:]
				}
				got := h.Sum(nil)
				ReleaseHasher(h)
				if !bytes.Equal(want[:], got) {
					errs <- fmt.Errorf("stream %d file %d: got %x want %x", i, j, got, want)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// freeHashers returns the number of hashers kept by the servers
func freeHashers() (n int) {
	md5SIMDMu.Lock()
	defer md5SIMDMu.Unlock()
	for _, s := range md5SIMDServers {
		s.mu.Lock()
		n += len(s.free)
		s.mu.Unlock()
	}
	return n
}

func TestMD5SIMDReuse(t *testing.T) {
	withMD5SIMD(t, 1)
	before := freeHashers()
	data := randomData(7, 5000)
	want := md5.Sum(data)
	for range 1000 {
		h := NewMD5()
		_, _ = h.Write(data)
		require.Equal(t, want[:], h.Sum(nil))
		ReleaseHasher(h)
		ReleaseHasher(h) // twice is harmless
	}
	// Used one at a time, so at most one more hasher exists
	assert.LessOrEqual(t, freeHashers(), before+1)
}

func TestMD5SIMDGarbageCollected(t *testing.T) {
	withMD5SIMD(t, 1)
	var inUse int
	func() {
		h := NewMD5()
		_, _ = h.Write([]byte("not released"))
		_ = h.Sum(nil)
		inUse = freeHashers()
	}()
	deadline := time.Now().Add(10 * time.Second)
	for freeHashers() <= inUse && time.Now().Before(deadline) {
		runtime.GC()
		time.Sleep(10 * time.Millisecond)
	}
	assert.Greater(t, freeHashers(), inUse, "an unreleased hasher goes back to its server when collected")

	// and it is clean when reused
	h := NewMD5()
	defer ReleaseHasher(h)
	data := randomData(9, 777)
	_, _ = h.Write(data)
	want := md5.Sum(data)
	assert.Equal(t, want[:], h.Sum(nil))
}

func TestMD5SIMDThroughRegistry(t *testing.T) {
	withMD5SIMD(t, 1)
	data := randomData(3, 300_000)
	want := md5.Sum(data)
	sums, err := StreamTypes(bytes.NewReader(data), NewHashSet(MD5, SHA1))
	require.NoError(t, err)
	assert.Equal(t, fmt.Sprintf("%x", want), sums[MD5])
}

func benchmarkMD5Streams(b *testing.B, servers, streams int) {
	require.NoError(b, SetMD5SIMD(servers))
	defer func() { require.NoError(b, SetMD5SIMD(0)) }()
	const size = 16 << 20
	data := randomData(1, size)
	b.SetBytes(int64(size) * int64(streams))
	b.ResetTimer()
	for b.Loop() {
		var wg sync.WaitGroup
		for range streams {
			wg.Add(1)
			go func() {
				defer wg.Done()
				h := NewMD5()
				for off := 0; off < size; off += 32 << 10 {
					_, _ = h.Write(data[off : off+32<<10])
				}
				_ = h.Sum(nil)
				ReleaseHasher(h)
			}()
		}
		wg.Wait()
	}
}

func BenchmarkMD5Streams(b *testing.B) {
	for _, streams := range []int{1, 8, 32} {
		for _, servers := range []int{0, 1, 2, 4} {
			b.Run(fmt.Sprintf("streams=%d/servers=%d", streams, servers), func(b *testing.B) {
				benchmarkMD5Streams(b, servers, streams)
			})
		}
	}
}
