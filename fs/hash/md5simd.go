package hash

import (
	"crypto/md5"
	"fmt"
	"hash"
	"runtime"
	"sync"
	"sync/atomic"

	md5simd "github.com/minio/md5-simd"
)

// MD5 hashing through github.com/minio/md5-simd.
//
// md5-simd hashes up to 16 independent streams at once per server
// with AVX512 (8 with AVX2), so it is much faster than crypto/md5
// when many files or chunks are hashed concurrently, and a bit slower
// for a single stream. Each server is driven by one goroutine, so
// more servers spread the work over more cores.
//
// Its hashers must not be closed: md5-simd v1.1.2 never removes a
// closed hasher from its server, which then slows down and grows for
// ever. So hashers are reset and kept in a free list per server
// instead, and the number of hashers is bounded by the peak number
// in use at once.

// md5SIMDServer is an md5-simd server with its free hashers
type md5SIMDServer struct {
	server md5simd.Server
	mu     sync.Mutex
	free   []md5simd.Hasher
}

// get returns a hasher which is ready to use
func (s *md5SIMDServer) get() md5simd.Hasher {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n := len(s.free); n > 0 {
		h := s.free[n-1]
		s.free = s.free[:n-1]
		return h
	}
	return s.server.NewHash()
}

// put resets h and keeps it for reuse
func (s *md5SIMDServer) put(h md5simd.Hasher) {
	h.Reset()
	s.mu.Lock()
	s.free = append(s.free, h)
	s.mu.Unlock()
}

// md5SIMDHasher is the hash.Hash handed out by NewMD5 when md5-simd
// is in use.
type md5SIMDHasher struct {
	md5simd.Hasher
	releaseFn func()
}

// releaser is implemented by hashers which can give their resources
// back before they are garbage collected
type releaser interface {
	release()
}

// release gives the hasher back to its server. It must not be used
// afterwards.
func (h *md5SIMDHasher) release() {
	if h.releaseFn != nil {
		h.releaseFn()
	}
}

var (
	md5SIMDMu      sync.Mutex
	md5SIMDServers []*md5SIMDServer // nil if crypto/md5 is used
	md5SIMDState   atomic.Pointer[[]*md5SIMDServer]
	md5SIMDNext    atomic.Uint64
)

// SetMD5SIMD makes MD5 use md5-simd with the given number of servers,
// or crypto/md5 if servers is 0 or less.
//
// On CPUs without AVX2 md5-simd falls back to crypto/md5 by itself.
// Hashers which are in use keep their server, so calling it again is
// safe at any time.
func SetMD5SIMD(servers int) error {
	if servers > 64 {
		return fmt.Errorf("too many md5-simd servers %d: the maximum is 64", servers)
	}
	md5SIMDMu.Lock()
	defer md5SIMDMu.Unlock()
	if servers <= 0 {
		md5SIMDState.Store(nil)
		return nil
	}
	// Reuse existing servers, so changing the number keeps their
	// free hashers. Servers are never closed as hashers handed out
	// earlier may still use them.
	for len(md5SIMDServers) < servers {
		md5SIMDServers = append(md5SIMDServers, &md5SIMDServer{server: md5simd.NewServer()})
	}
	active := md5SIMDServers[:servers:servers]
	md5SIMDState.Store(&active)
	return nil
}

// MD5SIMDServers returns the number of md5-simd servers in use, or 0
// if crypto/md5 is used.
func MD5SIMDServers() int {
	if p := md5SIMDState.Load(); p != nil {
		return len(*p)
	}
	return 0
}

// NewMD5 returns a new MD5 hasher.
//
// This uses md5-simd if enabled with SetMD5SIMD, otherwise
// crypto/md5. Call ReleaseHasher when finished with it to make its
// resources available again straight away, otherwise this happens
// when it is garbage collected.
func NewMD5() hash.Hash {
	p := md5SIMDState.Load()
	if p == nil {
		return md5.New()
	}
	servers := *p
	server := servers[md5SIMDNext.Add(1)%uint64(len(servers))]
	inner := server.get()
	h := &md5SIMDHasher{Hasher: inner}
	var once sync.Once
	cleanup := runtime.AddCleanup(h, func(inner md5simd.Hasher) {
		once.Do(func() { server.put(inner) })
	}, inner)
	h.releaseFn = func() {
		cleanup.Stop()
		once.Do(func() { server.put(inner) })
		h.Hasher = nil
	}
	return h
}

// ReleaseHasher gives the resources of a hasher made by NewMD5 back
// straight away. The hasher must not be used afterwards.
//
// It does nothing for other hashers, so it is safe to call on any
// hash.Hash.
func ReleaseHasher(h hash.Hash) {
	if r, ok := h.(releaser); ok {
		r.release()
	}
}
