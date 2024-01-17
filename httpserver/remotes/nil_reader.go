package remotes

import (
	"io"
)

type nilReader struct{}

func (n nilReader) Close() error {
	return nil
}

func (n nilReader) Read(data []byte) (int, error) {
	return 0, io.EOF
}
