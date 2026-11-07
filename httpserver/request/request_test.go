package request

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/liquidgecka/testlib"
)

func TestRequest_Basic(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	req := httptest.NewRequest("GET", "/test/123", nil)
	w := httptest.NewRecorder()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	r := New(w, req, logger)
	T.Equal(r.statusCode, 0)
	r.WriteHeader(http.StatusOK)
	T.Equal(r.statusCode, http.StatusOK)
	_, err := r.Write([]byte("hello"))
	T.Equal(err, nil)
	T.Equal(r.replyBytes, int64(5))

	r.AccessLog(logger)
}

func BenchmarkAccessLog(b *testing.B) {
	req := httptest.NewRequest("GET", "/namespace/id123456789", nil)
	w := httptest.NewRecorder()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	r := New(w, req, logger)
	r.statusCode = 200

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.AccessLog(logger)
	}
}

func BenchmarkRequestNew(b *testing.B) {
	w := httptest.NewRecorder()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest("GET", "/namespace/id123456789", nil)
		r := New(w, req, logger)
		_ = r
	}
}

func BenchmarkRequestPool(b *testing.B) {
	w := httptest.NewRecorder()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	req := httptest.NewRequest("GET", "/namespace/id123456789", nil)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := Get(w, req, logger)
		Put(r)
	}
}
