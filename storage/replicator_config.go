package storage

import (
	"io"
	"os"
)

type replicatorConfig struct {
	end       uint64
	fd        *os.File
	fid       string
	hash      string
	namespace string
	start     uint64
}

func (r *replicatorConfig) FileName() string {
	return r.fid
}

func (r *replicatorConfig) GetBody() io.ReadCloser {
	return &reader{
		source: r.fd,
		offset: r.start,
		end:    r.end,
	}
}

func (r *replicatorConfig) Hash() string {
	return r.hash
}

func (r *replicatorConfig) NameSpace() string {
	return r.namespace
}

func (r *replicatorConfig) Offset() uint64 {
	return r.start
}

func (r *replicatorConfig) Size() uint64 {
	return r.end - r.start
}

type reader struct {
	source *os.File
	offset uint64
	end    uint64
}

func (r *reader) Close() error {
	r.offset = r.end
	return nil
}

func (r *reader) Read(data []byte) (n int, err error) {
	if remaining := r.end - r.offset; remaining == 0 {
		return 0, io.EOF
	} else if remaining < uint64(len(data)) {
		data = data[:int(remaining)]
	}
	n, err = r.source.ReadAt(data, int64(r.offset))
	r.offset += uint64(n)
	return
}
