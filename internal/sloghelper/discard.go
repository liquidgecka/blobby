package sloghelper

import (
	"context"
	"log/slog"
)

// log/slog currently doesn't have a cheap and fast way to implement a
// discard logger, though it does appear to be coming soon
// (see: https://github.com/golang/go/issues/62005). This implements this
// function in the short term until it can be officially added.
type DiscardHandler struct{}

func (DiscardHandler) Enabled(context.Context, slog.Level) bool {
	return false
}

func (DiscardHandler) Handle(context.Context, slog.Record) error {
	return nil
}

func (d DiscardHandler) WithAttrs([]slog.Attr) slog.Handler {
	return d
}

func (d DiscardHandler) WithGroup(string) slog.Handler {
	return d
}
