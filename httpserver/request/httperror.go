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
	if h.Err != nil {
		return h.Err.Error()
	} else if h.Response != "" {
		return h.Response
	} else {
		return fmt.Sprintf("HTTP error: %d", h.Status)
	}
}

func (h *HTTPError) ServeError(w http.ResponseWriter) {
	w.Header().Add("Content-Type", "text/plain")
	w.WriteHeader(h.Status)
	w.Write(*(*[]byte)(unsafe.Pointer(&h.Response)))
	w.Write([]byte{'\n'})
}
