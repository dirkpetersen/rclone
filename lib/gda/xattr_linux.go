package gda

import (
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
	size, err := unix.Llistxattr(p, nil)
	if err != nil {
		if errors.Is(err, unix.ENOTSUP) {
			return "", nil
		}
		return "", fmt.Errorf("list extended attributes: %w", err)
	}
	if size == 0 {
		return "", nil
	}
	buf := make([]byte, size)
	size, err = unix.Llistxattr(p, buf)
	if err != nil {
		return "", fmt.Errorf("list extended attributes: %w", err)
	}
	var pairs []string
	for _, name := range strings.Split(strings.TrimRight(string(buf[:size]), "\x00"), "\x00") {
		if name == "" {
			continue
		}
		n, err := unix.Lgetxattr(p, name, nil)
		if err != nil {
			return "", fmt.Errorf("read extended attribute %q: %w", name, err)
		}
		value := make([]byte, n)
		if n > 0 {
			if n, err = unix.Lgetxattr(p, name, value); err != nil {
				return "", fmt.Errorf("read extended attribute %q: %w", name, err)
			}
		}
		pairs = append(pairs, base64.StdEncoding.EncodeToString([]byte(name))+":"+base64.StdEncoding.EncodeToString(value[:n]))
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
		if err := unix.Lsetxattr(p, string(name), value, 0); err != nil {
			errs = append(errs, fmt.Errorf("set extended attribute %q: %w", name, err))
		}
	}
	return errors.Join(errs...)
}

// xattrsSupported is whether extended attributes can be kept here.
const xattrsSupported = true
