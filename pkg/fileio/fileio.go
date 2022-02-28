package fileio

import (
	"context"
	"io"
	"os"
)

type FileIO interface {
	// Close for close the connect.
	Close() error

	// MkdirAll for create directory recursively.
	MkdirAll(ctx context.Context, dirname string, perm os.FileMode) error

	// IsExists for check a file whether exists.
	IsExists(ctx context.Context, name string) (bool, error)

	// CreateAndWrite for create a file and writing data into.
	// Return eTag(md5 as hex), error
	CreateAndWrite(ctx context.Context, name string, reader io.ReadCloser) (string, error)

	// OpenForRead for open a file to read data.
	OpenForRead(ctx context.Context, name string) (io.ReadCloser, error)

	// Remove for delete a file.
	Remove(ctx context.Context, name string) error

	// RemoveAll removes path and any children it contains. It removes everything it
	// can but returns the first error it encounters. If the path does not exist,
	// RemoveAll returns nil (no error).
	RemoveAll(ctx context.Context, name string) error

	// Rename for rename a file name to `newName` from `oldName`.
	Rename(ctx context.Context, oldName string, newName string) error
}
