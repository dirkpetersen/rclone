// Build for unsupported platforms to stop go complaining
// about "no buildable Go source files"

//go:build !unix

// Package backup implements 'rclone gda backup'.
package backup
