package httpserver

import (
	"io"
)

type remoteReplicatorConfig struct {
	body      io.ReadCloser
	end       uint64
	fid       string
	hash      string
	namespace string
	start     uint64
}

func (r *remoteReplicatorConfig) FileName() string {
	return r.fid
}

func (r *remoteReplicatorConfig) GetBody() io.ReadCloser {
	return r.body
}

func (r *remoteReplicatorConfig) Hash() string {
	return r.hash
}

func (r *remoteReplicatorConfig) NameSpace() string {
	return r.namespace
}

func (r *remoteReplicatorConfig) Offset() uint64 {
	return r.start
}

func (r *remoteReplicatorConfig) Size() uint64 {
	return r.end - r.start
}
