// build: windows

package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

func SetupRotation(ctx context.Context) {
	log.LogAttrs(
		ctx,
		slog.LevelInfo,
		"Log rotation does not currently work on windows.")
}
