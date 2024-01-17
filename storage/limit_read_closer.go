package storage

import "io"

type limitReadCloser struct {
	RC io.ReadCloser
	N  int64
}

func (l *limitReadCloser) Close() error {
	return l.RC.Close()
}

func (l *limitReadCloser) Read(p []byte) (n int, err error) {
	if l.N <= 0 {
		return 0, io.EOF
	}

	if int64(len(p)) > l.N {
		p = p[0:l.N]
	}
	n, err = l.RC.Read(p)
	l.N -= int64(n)
	return
}
