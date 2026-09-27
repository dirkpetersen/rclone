// Build for unsupported platforms to stop go complaining
// about "no buildable Go source files"

//go:build !unix

// Package restore implements 'rclone gda restore'.
package restore
