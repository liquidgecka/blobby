package sloghelper

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestAttrHelpers(t *testing.T) {
	// Nil error should not panic and return <nil>
	errAttrNil := Error("err", nil)
	if errAttrNil.Key != "err" || errAttrNil.Value.String() != "<nil>" {
		t.Fatalf("unexpected nil error attr: %+v", errAttrNil)
	}

	// Non-nil error
	testErr := errors.New("something went wrong")
	errAttr := Error("err", testErr)
	if errAttr.Key != "err" || errAttr.Value.String() != testErr.Error() {
		t.Fatalf("unexpected error attr: %+v", errAttr)
	}

	// Duration
	dur := Duration("dur", 500*time.Millisecond)
	if dur.Key != "dur" || dur.Value.Duration() != 500*time.Millisecond {
		t.Fatalf("unexpected duration attr: %+v", dur)
	}

	// Interface
	if iface := Interface("iface", "custom"); iface.Key != "iface" || iface.Value.Any() != "custom" {
		t.Fatalf("unexpected interface attr: %+v", iface)
	}

	// Int, Int32, Int64
	if i := Int("int", 10); i.Key != "int" || i.Value.Int64() != 10 {
		t.Fatalf("unexpected int attr: %+v", i)
	}
	if i32 := Int32("int32", 20); i32.Key != "int32" || i32.Value.Int64() != 20 {
		t.Fatalf("unexpected int32 attr: %+v", i32)
	}
	if i64 := Int64("int64", 30); i64.Key != "int64" || i64.Value.Int64() != 30 {
		t.Fatalf("unexpected int64 attr: %+v", i64)
	}

	// String
	if s := String("str", "val"); s.Key != "str" || s.Value.String() != "val" {
		t.Fatalf("unexpected string attr: %+v", s)
	}

	// Uint32, Uint64
	if u32 := Uint32("u32", 40); u32.Key != "u32" || u32.Value.Uint64() != 40 {
		t.Fatalf("unexpected uint32 attr: %+v", u32)
	}
	if u64 := Uint64("u64", 50); u64.Key != "u64" || u64.Value.Uint64() != 50 {
		t.Fatalf("unexpected uint64 attr: %+v", u64)
	}
}

func TestLeveler(t *testing.T) {
	var l Leveler
	if l.Level() != slog.LevelInfo {
		t.Fatalf("expected default LevelInfo, got: %v", l.Level())
	}

	l.SetLevel(slog.LevelDebug)
	if l.Level() != slog.LevelDebug {
		t.Fatalf("expected LevelDebug, got: %v", l.Level())
	}

	l.SetLevel(slog.LevelWarn)
	if l.Level() != slog.LevelWarn {
		t.Fatalf("expected LevelWarn, got: %v", l.Level())
	}
}

func TestDiscardHandler(t *testing.T) {
	d := DiscardHandler{}
	ctx := context.Background()

	if d.Enabled(ctx, slog.LevelDebug) || d.Enabled(ctx, slog.LevelError) {
		t.Fatalf("expected DiscardHandler.Enabled to return false")
	}

	if err := d.Handle(ctx, slog.Record{}); err != nil {
		t.Fatalf("expected Handle to return nil, got: %v", err)
	}

	if d.WithAttrs([]slog.Attr{String("k", "v")}) != d {
		t.Fatalf("expected WithAttrs to return same handler")
	}

	if d.WithGroup("grp") != d {
		t.Fatalf("expected WithGroup to return same handler")
	}
}

func TestRotator(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "test.log")
	ctx := context.Background()

	r, err := NewRotator(ctx, logFile)
	if err != nil {
		t.Fatalf("NewRotator failed: %v", err)
	}
	defer r.Close()

	r.SetLogger(slog.Default())

	// Write log message
	msg1 := []byte("first log message\n")
	n, err := r.Write(msg1)
	if err != nil || n != len(msg1) {
		t.Fatalf("Write failed: n=%d err=%v", n, err)
	}

	// Verify content on disk
	content, err := os.ReadFile(logFile)
	if err != nil || string(content) != string(msg1) {
		t.Fatalf("unexpected file content: %s, err: %v", string(content), err)
	}

	// Simulate log rotation (move old file away, call Rotate)
	rotatedFile := filepath.Join(tmpDir, "test.log.1")
	if err := os.Rename(logFile, rotatedFile); err != nil {
		t.Fatalf("Rename failed: %v", err)
	}

	if err := r.Rotate(ctx); err != nil {
		t.Fatalf("Rotate failed: %v", err)
	}

	msg2 := []byte("second log message\n")
	n, err = r.Write(msg2)
	if err != nil || n != len(msg2) {
		t.Fatalf("Write after rotate failed: n=%d err=%v", n, err)
	}

	// Check new file content
	contentNew, err := os.ReadFile(logFile)
	if err != nil || string(contentNew) != string(msg2) {
		t.Fatalf("unexpected new file content: %s, err: %v", string(contentNew), err)
	}

	// Check old file preserved
	contentOld, err := os.ReadFile(rotatedFile)
	if err != nil || string(contentOld) != string(msg1) {
		t.Fatalf("unexpected old file content: %s, err: %v", string(contentOld), err)
	}

	// Close rotator and verify write fails
	if err := r.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	_, err = r.Write([]byte("should fail\n"))
	if !errors.Is(err, os.ErrClosed) {
		t.Fatalf("expected ErrClosed after close, got: %v", err)
	}
}
