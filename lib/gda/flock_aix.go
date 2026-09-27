package gda

import (
	"errors"

	"golang.org/x/sys/unix"
)

// errLocked is returned by tryLock if another holds the lock.
var errLocked = errors.New("locked")

// tryLock takes an exclusive lock on the open file fd, returning
// errLocked if another process holds it. AIX has no flock, and fcntl
// locks don't exclude other users in the same process.
func tryLock(fd uintptr) error {
	lk := unix.Flock_t{Type: unix.F_WRLCK}
	err := unix.FcntlFlock(fd, unix.F_SETLK, &lk)
	if errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EACCES) {
		return errLocked
	}
	return err
}
