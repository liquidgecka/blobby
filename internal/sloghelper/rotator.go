package sloghelper

import (
	"bufio"
	"context"
	"log/slog"
	"os"
	"sync"
)

// This is partial implementation of a slog.Handler that will handle output
// to a file/console while also supporting concepts like rotation. This
// functionality is not present in the default slog Handlers.
//
// Note that this does not perform moves on the underlying file. It is
// assumed that something like logrotate will be handling that.
type Rotator struct {
	// Logs issues with rotation.
	log *slog.Logger

	// The file name that should be re-opened/written to when
	// rotation happens.
	fileName string

	// The current file descriptor that is being used to write.
	fd *os.File

	// The output buffered writer that is pointing at the fd
	// above.
	buffer *bufio.Writer

	// A lock that protects writes to the file, this will be locked
	// during write operations to ensure that multiple calls to
	// Handle() do not overwrite each other.
	lock sync.Mutex
}

func NewRotator(ctx context.Context, file string) (*Rotator, error) {
	r := &Rotator{
		fileName: file,
	}
	if err := r.Rotate(ctx); err != nil {
		return nil, err
	}
	return r, nil
}

// Opens the file on disk.
func (r *Rotator) Rotate(ctx context.Context) error {
	newfd, err := os.OpenFile(
		r.fileName,
		os.O_WRONLY|os.O_CREATE|os.O_APPEND,
		0644)
	if err != nil {
		return err
	}
	newbuffer := bufio.NewWriter(newfd)
	oldfd, oldbuffer := func() (*os.File, *bufio.Writer) {
		r.lock.Lock()
		defer r.lock.Unlock()
		oldfd, oldbuffer := r.fd, r.buffer
		r.fd, r.buffer = newfd, newbuffer
		return oldfd, oldbuffer
	}()
	if oldbuffer != nil {
		if err := oldbuffer.Flush(); err != nil {
			r.log.LogAttrs(
				ctx,
				slog.LevelError,
				"Error flushing logs to disk.",
				Error("error", err))
		}
	}
	if oldfd != nil {
		if err := oldfd.Close(); err != nil {
			r.log.LogAttrs(
				ctx,
				slog.LevelError,
				"Error closing the old file.",
				Error("error", err))
		}
	}

	return nil
}

// Acts like a io.Writter, allowing raw data to be written to the
// current log buffer. Note that this does not lock as its assumed
// that this will be called from within something that is locking
// propertly.
func (r *Rotator) Write(data []byte) (int, error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.buffer.Write(data)
}
