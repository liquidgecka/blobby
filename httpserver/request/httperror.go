package request

import (
	"fmt"
	"net/http"
	"unsafe"
)

type HTTPError struct {
	Status   int
	Response string
	Err      error
}

func (h *HTTPError) Error() string {
	if h.Err == nil {
		return h.Err.Error()
	} else {
		return fmt.Sprintf("no error")
	}
}

func (h *HTTPError) ServeError(w http.ResponseWriter) {
	w.Header().Add("Content-Type", "text/plain")
	w.WriteHeader(h.Status)
	w.Write(*(*[]byte)(unsafe.Pointer(&h.Response)))
	w.Write([]byte{'\n'})
}
