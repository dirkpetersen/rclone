// Build for unsupported platforms to stop go complaining
// about "no buildable Go source files"

//go:build !unix

// Package gc implements 'rclone gda gc'.
package gc
