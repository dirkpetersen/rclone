//go:build unix && !aix

package gda

import (
	"errors"

	"golang.org/x/sys/unix"
)

// errLocked is returned by tryLock if another holds the lock.
var errLocked = errors.New("locked")

// tryLock takes an exclusive lock on the open file fd, returning
// errLocked if another holds it.
func tryLock(fd uintptr) error {
	err := unix.Flock(int(fd), unix.LOCK_EX|unix.LOCK_NB)
	if errors.Is(err, unix.EWOULDBLOCK) {
		return errLocked
	}
	return err
}
