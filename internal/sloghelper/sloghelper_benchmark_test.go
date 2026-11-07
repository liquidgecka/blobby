package sloghelper

import (
	"context"
	"fmt"
	"io"
	"log"
	"log/slog"
	"testing"
)

type nonOptimizedDiscard struct{}

func (nonOptimizedDiscard) Write(p []byte) (int, error) {
	return len(p), nil
}

// Traditional map-based structured logging (logrus / zap sugared style)
func traditionalMapLog(w io.Writer, msg string, fields map[string]interface{}) {
	fmt.Fprintf(w, "%s: ", msg)
	for k, v := range fields {
		fmt.Fprintf(w, "%s=%v ", k, v)
	}
	fmt.Fprintln(w)
}

// BenchmarkBlobbyLogAttrs measures Blobby's approach: slog.LogAttrs with sloghelper typed attributes
func BenchmarkBlobbyLogAttrs(b *testing.B) {
	w := nonOptimizedDiscard{}
	handler := slog.NewTextHandler(w, &slog.HandlerOptions{Level: slog.LevelInfo})
	logger := slog.New(handler)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		logger.LogAttrs(
			ctx,
			slog.LevelInfo,
			"client request received",
			String("method", "GET"),
			String("path", "/api/v1/blobs/12345"),
			Int64("status", 200),
			Uint64("bytes", 65536),
		)
	}
}

// BenchmarkStandardSlogVariadic measures typical slog with variadic ...any key-value pairs
func BenchmarkStandardSlogVariadic(b *testing.B) {
	w := nonOptimizedDiscard{}
	handler := slog.NewTextHandler(w, &slog.HandlerOptions{Level: slog.LevelInfo})
	logger := slog.New(handler)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		logger.Info(
			"client request received",
			"method", "GET",
			"path", "/api/v1/blobs/12345",
			"status", int64(200),
			"bytes", uint64(65536),
		)
	}
}

// BenchmarkTraditionalStdlibLogPrintf measures Go's traditional standard library log.Printf
func BenchmarkTraditionalStdlibLogPrintf(b *testing.B) {
	w := nonOptimizedDiscard{}
	stdLogger := log.New(w, "", log.LstdFlags)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stdLogger.Printf("client request received method=%s path=%s status=%d bytes=%d",
			"GET", "/api/v1/blobs/12345", 200, 65536)
	}
}

// BenchmarkTraditionalMapStructuredLog measures traditional interface/map-based structured logging
func BenchmarkTraditionalMapStructuredLog(b *testing.B) {
	w := nonOptimizedDiscard{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fields := map[string]interface{}{
			"method": "GET",
			"path":   "/api/v1/blobs/12345",
			"status": 200,
			"bytes":  65536,
		}
		traditionalMapLog(w, "client request received", fields)
	}
}

// --- Disabled Logging Benchmarks (Level filtering) ---

// BenchmarkBlobbyDisabledLogAttrs measures Blobby's approach when the level is disabled (LevelDebug called on LevelInfo logger)
func BenchmarkBlobbyDisabledLogAttrs(b *testing.B) {
	w := nonOptimizedDiscard{}
	handler := slog.NewTextHandler(w, &slog.HandlerOptions{Level: slog.LevelInfo})
	logger := slog.New(handler)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		logger.LogAttrs(
			ctx,
			slog.LevelDebug,
			"client request received",
			String("method", "GET"),
			String("path", "/api/v1/blobs/12345"),
			Int64("status", 200),
			Uint64("bytes", 65536),
		)
	}
}

// BenchmarkBlobbyDisabledWithCheck measures Blobby's idiom: if log.Enabled(...) { log.Debug(...) }
func BenchmarkBlobbyDisabledWithCheck(b *testing.B) {
	w := nonOptimizedDiscard{}
	handler := slog.NewTextHandler(w, &slog.HandlerOptions{Level: slog.LevelInfo})
	logger := slog.New(handler)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if logger.Enabled(ctx, slog.LevelDebug) {
			logger.LogAttrs(
				ctx,
				slog.LevelDebug,
				"client request received",
				String("method", "GET"),
				String("path", "/api/v1/blobs/12345"),
				Int64("status", 200),
				Uint64("bytes", 65536),
			)
		}
	}
}

// BenchmarkStandardSlogDisabledVariadic measures variadic slog when disabled
func BenchmarkStandardSlogDisabledVariadic(b *testing.B) {
	w := nonOptimizedDiscard{}
	handler := slog.NewTextHandler(w, &slog.HandlerOptions{Level: slog.LevelInfo})
	logger := slog.New(handler)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		logger.Debug(
			"client request received",
			"method", "GET",
			"path", "/api/v1/blobs/12345",
			"status", int64(200),
			"bytes", uint64(65536),
		)
	}
}

// BenchmarkTraditionalMapDisabledLog measures traditional map-based logging when disabled without check
func BenchmarkTraditionalMapDisabledLog(b *testing.B) {
	enabled := false
	w := nonOptimizedDiscard{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fields := map[string]interface{}{
			"method": "GET",
			"path":   "/api/v1/blobs/12345",
			"status": 200,
			"bytes":  65536,
		}
		if enabled {
			traditionalMapLog(w, "client request received", fields)
		}
	}
}

// BenchmarkBlobbyDiscardHandler measures Blobby's DiscardHandler
func BenchmarkBlobbyDiscardHandler(b *testing.B) {
	logger := slog.New(DiscardHandler{})
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		logger.LogAttrs(
			ctx,
			slog.LevelInfo,
			"client request received",
			String("method", "GET"),
			String("path", "/api/v1/blobs/12345"),
			Int64("status", 200),
			Uint64("bytes", 65536),
		)
	}
}
