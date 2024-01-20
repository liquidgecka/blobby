//go:build !go1.10
// +build !go1.10

package compat

import (
	"bytes"
)

// For go releases prior to 1.10 we need to use a bytes.Buffer.
type Builder struct {
	bytes.Buffer
}
