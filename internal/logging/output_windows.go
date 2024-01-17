// build windows

package logging

import (
	"bufio"
)

func writeReturn(out *bufio.Writer) {
	out.WriteByte('\r')
}
