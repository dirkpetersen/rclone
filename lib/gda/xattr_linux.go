package gda

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"sort"
	"strings"

	"golang.org/x/sys/unix"
)

// readXattrs returns the extended attributes of p, without following a
// symlink, encoded for the xattrs column: base64 name and value pairs
// joined by ":", separated by spaces and sorted, or "" if there are
// none.
func readXattrs(p string) (string, error) {
	var buf []byte
	var size int
	for {
		n, err := unix.Llistxattr(p, nil)
		if errors.Is(err, unix.ENOTSUP) {
			return "", nil
		}
		if err != nil {
			return "", fmt.Errorf("list extended attributes: %w", err)
		}
		if n == 0 {
			return "", nil
		}
		buf = make([]byte, n)
		size, err = unix.Llistxattr(p, buf)
		if errors.Is(err, unix.ERANGE) {
			// More were added since the size was read.
			continue
		}
		if err != nil {
			return "", fmt.Errorf("list extended attributes: %w", err)
		}
		break
	}
	var pairs []string
	for _, name := range strings.Split(strings.TrimRight(string(buf[:size]), "\x00"), "\x00") {
		if name == "" {
			continue
		}
		value, err := getXattr(p, name)
		if errors.Is(err, unix.ENODATA) {
			// Removed since it was listed.
			continue
		}
		if err != nil {
			return "", fmt.Errorf("read extended attribute %q: %w", name, err)
		}
		pairs = append(pairs, base64.StdEncoding.EncodeToString([]byte(name))+":"+base64.StdEncoding.EncodeToString(value))
	}
	sort.Strings(pairs)
	return strings.Join(pairs, " "), nil
}

// writeXattrs sets the extended attributes encoded in xattrs on p,
// without following a symlink. Unless isRoot, the trusted and security
// namespaces, which only root may write, are left out.
func writeXattrs(p, xattrs string, isRoot bool) error {
	var errs []error
	for _, pair := range strings.Fields(xattrs) {
		encName, encValue, ok := strings.Cut(pair, ":")
		name, err1 := base64.StdEncoding.DecodeString(encName)
		value, err2 := base64.StdEncoding.DecodeString(encValue)
		if !ok || err1 != nil || err2 != nil {
			errs = append(errs, fmt.Errorf("bad extended attribute %q", pair))
			continue
		}
		if !isRoot && (strings.HasPrefix(string(name), "trusted.") || strings.HasPrefix(string(name), "security.")) {
			continue
		}
		if current, err := getXattr(p, string(name)); err == nil && bytes.Equal(current, value) {
			// Already set, as through another link to the same file.
			continue
		}
		if err := unix.Lsetxattr(p, string(name), value, 0); err != nil {
			errs = append(errs, fmt.Errorf("set extended attribute %q: %w", name, err))
		}
	}
	return errors.Join(errs...)
}

// getXattr returns the value of the extended attribute name of p.
func getXattr(p, name string) ([]byte, error) {
	for {
		n, err := unix.Lgetxattr(p, name, nil)
		if err != nil {
			return nil, err
		}
		value := make([]byte, n)
		if n == 0 {
			return value, nil
		}
		n, err = unix.Lgetxattr(p, name, value)
		if errors.Is(err, unix.ERANGE) {
			// It grew since its size was read.
			continue
		}
		if err != nil {
			return nil, err
		}
		return value[:n], nil
	}
}

// xattrsSupported is whether extended attributes can be kept here.
const xattrsSupported = true
