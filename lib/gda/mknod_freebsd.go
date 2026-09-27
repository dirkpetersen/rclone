package gda

import "golang.org/x/sys/unix"

// mknod creates a device file.
func mknod(p string, mode uint32, major, minor int64) error {
	return unix.Mknod(p, mode, unix.Mkdev(uint32(major), uint32(minor)))
}
