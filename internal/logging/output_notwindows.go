// build !windows

package logging

import (
	"bufio"
)

func writeReturn(out *bufio.Writer) {
	// There is no need to do anything on non windows machines, however leaving
	// this function empty shows it as 0% test coverage so we simply return
	// from this nil function.
	return
}
