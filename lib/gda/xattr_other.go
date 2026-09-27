//go:build unix && !linux

package gda

import "errors"

var errNoXattrs = errors.New("extended attributes are only kept on Linux")

func readXattrs(p string) (string, error) {
	return "", errNoXattrs
}

func writeXattrs(p, xattrs string) error {
	return errNoXattrs
}

// xattrsSupported is whether extended attributes can be kept here.
const xattrsSupported = false
