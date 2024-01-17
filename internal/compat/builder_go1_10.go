// +build go1.10

// Implements and wraps functionality that was added in later releases
// of golang in order to ensure that this package will still build
// with older releases.
package compat

import (
	"strings"
)

// In go 1.10+ we can just use the golang strings.Builder implementation.
type Builder = strings.Builder
