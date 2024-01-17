package iohelp

import (
	"io"
)

// The io.CopyBuffer call doesn't give us clear enough details about what
// side the error was on so we have to implement a version of it ourselves.
// This returns TWO errors, one that is an error received from the reader
// and the other is the error returned from the writer. This allows us
// to break apart the different error types.
func CopyBuffer(
	dest io.Writer,
	source io.Reader,
	buff []byte,
) (
	written int64,
	desterr error,
	sourceerr error,
) {
	rn := 0
	wn := 0
	cont := true
	for cont {
		rn, sourceerr = source.Read(buff)
		if sourceerr != nil {
			if sourceerr == io.EOF {
				cont = false
				sourceerr = nil
			} else {
				return
			}
		}
		wn, desterr = dest.Write(buff[0:rn])
		written += int64(wn)
		if desterr != nil {
			// Nothing much we can do here except abort and return the
			// errors to the caller.
			return
		}
		if rn != wn {
			desterr = io.ErrShortWrite
			return
		}
	}
	return
}
