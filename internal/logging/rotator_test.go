package logging

import (
	"bufio"
	"os"
	"path/filepath"
	"testing"

	"github.com/liquidgecka/testlib"
)

func TestNewANSIRotator(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// Test the error condition first.
	r, o, err := NewANSIRotator("/should/not/exist")
	T.ExpectErrorMessage(
		err,
		"open /should/not/exist: no such file or directory")
	T.Equal(r, nil)
	T.Equal(o, nil)

	// Make sure that a valid creation does the right thing.
	file := filepath.Join(T.TempDir(), "logfile")
	r, o, err = NewANSIRotator(file)
	T.ExpectSuccess(err)
	T.NotEqual(o, nil)
	T.NotEqual(r, nil)
	defer r.fd.Close()

	// Make sure that logging to this output produces output in the
	// file above.
	o.Write(&renderData{})
	stat, err := os.Stat(file)
	T.ExpectSuccess(err)
	T.Equal(stat.Size(), int64(44))
}

func TestNewJSONRotator(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// Test the error condition first.
	r, o, err := NewJSONRotator("/should/not/exist")
	T.ExpectErrorMessage(
		err,
		"open /should/not/exist: no such file or directory")
	T.Equal(r, nil)
	T.Equal(o, nil)

	// Make sure that a valid creation does the right thing.
	file := filepath.Join(T.TempDir(), "logfile")
	r, o, err = NewJSONRotator(file)
	T.ExpectSuccess(err)
	T.NotEqual(o, nil)
	T.NotEqual(r, nil)
	defer r.fd.Close()

	// Make sure that logging to this output produces output in the
	// file above.
	o.Write(&renderData{})
	stat, err := os.Stat(file)
	T.ExpectSuccess(err)
	T.Equal(stat.Size(), int64(66))
}

func TestNewPlainRotator(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// Test the error condition first.
	r, o, err := NewPlainRotator("/should/not/exist")
	T.ExpectErrorMessage(
		err,
		"open /should/not/exist: no such file or directory")
	T.Equal(r, nil)
	T.Equal(o, nil)

	// Make sure that a valid creation does the right thing.
	file := filepath.Join(T.TempDir(), "logfile")
	r, o, err = NewPlainRotator(file)
	T.ExpectSuccess(err)
	T.NotEqual(o, nil)
	T.NotEqual(r, nil)
	defer r.fd.Close()

	// Make sure that logging to this output produces output in the
	// file above.
	o.Write(&renderData{})
	stat, err := os.Stat(file)
	T.ExpectSuccess(err)
	T.Equal(stat.Size(), int64(27))
}

func TestRotator_Rotate(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// Setup a simple logger.
	fd := T.TempFile()
	output := Output{
		buffer: bufio.NewWriter(fd),
	}
	r := Rotator{
		file:   fd.Name(),
		fd:     fd,
		output: &output,
	}
	defer r.fd.Close()

	// Simulate an underlying file rotation by deleting the initial file.
	T.ExpectSuccess(os.Remove(fd.Name()))

	// Force a rotation.
	T.ExpectSuccess(r.Rotate())

	// Make sure that the two files are not the same.
	T.NotEqual(fd, r.fd)

	// Ensure that the file exists.
	_, err := os.Stat(fd.Name())
	T.ExpectSuccess(err)
}

func TestRotator_Rotate_OpenError(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// Setup a simple logger.
	r := Rotator{
		file: "/should/not/exist",
	}
	defer r.fd.Close()

	// Simulate an underlying file rotation by deleting the initial file.
	T.ExpectErrorMessage(
		r.Rotate(),
		"open /should/not/exist: no such file or directory")
}

func TestRotator_Rotate_CloseError(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// Setup a simple logger.
	fd := T.TempFile()
	output := Output{
		buffer: bufio.NewWriter(fd),
	}
	r := Rotator{
		file:   fd.Name(),
		fd:     fd,
		output: &output,
	}
	defer r.fd.Close()

	// If we close the file then the second close should error.
	fd.Close()
	T.ExpectErrorMessage(r.Rotate(), ""+
		"Rotation was successful but there was an error closing the "+
		"old log file `close "+fd.Name()+": file already closed`.")
}

func TestRotator_Rotate_CloseAndFlushError(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// Setup a simple logger with a file that is not in write mode.
	fdMaster := T.TempFile()
	fd, err := os.Open(fdMaster.Name())
	T.ExpectSuccess(err)
	output := Output{
		buffer: bufio.NewWriter(fd),
	}
	r := Rotator{
		file:   fd.Name(),
		fd:     fd,
		output: &output,
	}
	output.buffer.WriteString("test")
	defer r.fd.Close()
	defer fdMaster.Close()
	fd.Close()

	// If we close the file then the second close should error.
	T.ExpectErrorMessage(r.Rotate(), ""+
		"Rotation was successful, but there was an error flushing the "+
		"buffer `write "+fd.Name()+": file already closed` and closing the "+
		"file `close "+fd.Name()+": file already closed`. Old log lines "+
		"might be lost.")
}

func TestRotator_Rotate_FlushError(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// Setup a simple logger with a file that is not in write mode.
	fdMaster := T.TempFile()
	fd, err := os.Open(fdMaster.Name())
	T.ExpectSuccess(err)
	output := Output{
		buffer: bufio.NewWriter(fd),
	}
	r := Rotator{
		file:   fd.Name(),
		fd:     fd,
		output: &output,
	}
	output.buffer.WriteString("test")
	defer r.fd.Close()
	defer fdMaster.Close()
	defer fd.Close()

	// If we close the file then the second close should error.
	T.ExpectErrorMessage(r.Rotate(), ""+
		"Rotation was successful but there was an error flushing the "+
		"old buffer `write "+fd.Name()+": bad file descriptor`. Old log "+
		"lines might be lost.")
}
