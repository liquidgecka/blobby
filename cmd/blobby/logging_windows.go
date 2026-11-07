//go:build windows

package main

import (
	"context"
	"log/slog"
)

func SetupRotation(ctx context.Context) {
	log.LogAttrs(
		ctx,
		slog.LevelInfo,
		"Log rotation does not currently work on windows.")
}
