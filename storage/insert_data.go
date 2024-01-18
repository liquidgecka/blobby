package storage

import (
	"io"

	"github.com/liquidgecka/blobby/internal/tracing"
)

// When Calling Insert there are many different values that can be provided
// which are all bundled up here. This makes it a little cleaner for passing
// the data between server -> storage -> primary.
type InsertData struct {
	// The actual data written to disk will be read from this Reader.
	Source io.Reader

	// If provided then this will ensure that the data received from the
	// client is at least this long. If this is zero then it is assumed
	// that the expected length of the data is unknown and therefor should
	// be read until EOF.
	Length int64

	// If this is defined then tracing will be used at various points during
	// the insertion process. If this is nil then no tracing will be performed.
	Tracer *tracing.Trace
}
