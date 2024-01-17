package logging

import (
	"bufio"
	"fmt"
	"os"
	"sync"
)

type Rotator struct {
	file   string
	fd     *os.File
	output *Output
	lock   sync.Mutex
}

func NewANSIRotator(file string) (*Rotator, *Output, error) {
	fd, err := os.OpenFile(file, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, nil, err
	}
	o := NewANSIOutput(bufio.NewWriter(fd))
	return &Rotator{
		file:   file,
		fd:     fd,
		output: o,
	}, o, nil
}

func NewJSONRotator(file string) (*Rotator, *Output, error) {
	fd, err := os.OpenFile(file, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, nil, err
	}
	o := NewJSONOutput(bufio.NewWriter(fd))
	return &Rotator{
		file:   file,
		fd:     fd,
		output: o,
	}, o, nil
}

func NewPlainRotator(file string) (*Rotator, *Output, error) {
	fd, err := os.OpenFile(file, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, nil, err
	}
	o := NewPlainOutput(bufio.NewWriter(fd))
	return &Rotator{
		file:   file,
		fd:     fd,
		output: o,
	}, o, nil
}

// Rotates the log file by reopening the filename and replacing the
// file descriptor in the output device.
func (r *Rotator) Rotate() error {
	r.lock.Lock()
	defer r.lock.Unlock()

	// Re-open the file.
	fd, err := os.OpenFile(r.file, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	buffer := bufio.NewWriter(fd)

	// Reset the buffer of the given Output object.
	oldBuffer := func() (old *bufio.Writer) {
		r.output.lock.Lock()
		defer r.output.lock.Unlock()
		old, r.output.buffer = r.output.buffer, buffer
		return
	}()

	// Flush the old buffer and close the file.
	ferr := oldBuffer.Flush()
	cerr := r.fd.Close()
	r.fd = fd
	if ferr != nil && cerr != nil {
		return fmt.Errorf(""+
			"Rotation was successful, but there was an error flushing the "+
			"buffer `%s` and closing the file `%s`. Old log lines might be "+
			"lost.",
			ferr.Error(),
			cerr.Error())
	} else if ferr != nil {
		return fmt.Errorf(""+
			"Rotation was successful but there was an error flushing the old "+
			"buffer `%s`. Old log lines might be lost.",
			ferr.Error())
	} else if cerr != nil {
		return fmt.Errorf(""+
			"Rotation was successful but there was an error closing the old "+
			"log file `%s`.",
			cerr.Error())
	} else {
		return nil
	}
}
