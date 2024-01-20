package storage

import (
	"context"
	"io"
	"log/slog"

	"github.com/liquidgecka/testlib"

	"github.com/liquidgecka/blobby/internal/sloghelper"
)

func NewTestLogger() *slog.Logger {
	return slog.New(sloghelper.DiscardHandler{})
}

func InitializeTestableStorage(T *testlib.T) *Storage {
	settings := &Settings{
		BaseDirectory: T.TempDir(),
	}
	s := New(settings)
	s.Start(context.Background())
	return s
}

type testReader struct {
	read func([]byte) (int, error)
}

func (t *testReader) Read(d []byte) (n int, err error) {
	return t.read(d)
}

type testRemote struct {
	del        func(namespace, fn string) error
	heartBeat  func(namespace, fn string) (bool, error)
	initialize func(namespace, fn string) error
	name       string
	read       func(rc ReadConfig) (io.ReadCloser, error)
	replicate  func(rc RemoteReplicateConfig) (bool, error)
}

func (t *testRemote) Delete(namespace, fn string) error {
	if t.del != nil {
		return t.del(namespace, fn)
	} else {
		panic("NOT IMPLEMTNED")
	}
}

func (t *testRemote) HeartBeat(namespace, fn string) (bool, error) {
	if t.heartBeat != nil {
		return t.heartBeat(namespace, fn)
	} else {
		panic("NOT IMPLEMTNED")
	}
}

func (t *testRemote) Initialize(namespace, fn string) error {
	if t.initialize != nil {
		return t.initialize(namespace, fn)
	} else {
		panic("NOT IMPLEMTNED")
	}
}

func (t *testRemote) Read(rc ReadConfig) (io.ReadCloser, error) {
	if t.read != nil {
		return t.read(rc)
	} else {
		panic("NOT IMPLEMTNED")
	}
}

func (t *testRemote) Replicate(rc RemoteReplicateConfig) (bool, error) {
	if t.replicate != nil {
		return t.replicate(rc)
	} else {
		panic("NOT IMPLEMTNED")
	}
}

func (t *testRemote) String() string {
	return t.name
}
