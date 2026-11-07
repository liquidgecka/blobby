package httpserver

import (
	"reflect"
	"strings"
	"testing"

	"github.com/liquidgecka/testlib"
)

func TestSplitPath(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	testPaths := []string{
		"",
		"/",
		"/namespace",
		"/namespace/fid",
		"/_id/namespace/fid",
		"/namespace/file/10/20",
		"/_saml/auth/acs",
		"/a/b/c/d/e/f/g/h/i/j",
	}

	for _, p := range testPaths {
		var buf [8]string
		got := splitPath(p, &buf)
		expected := strings.Split(p, "/")
		T.Equal(reflect.DeepEqual(got, expected), true, p)
	}
}

func BenchmarkStringsSplit(b *testing.B) {
	path := "/my-namespace/fid123456789"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		parts := strings.Split(path, "/")
		_ = parts
	}
}

func BenchmarkSplitPath(b *testing.B) {
	path := "/my-namespace/fid123456789"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var buf [8]string
		parts := splitPath(path, &buf)
		_ = parts
	}
}
