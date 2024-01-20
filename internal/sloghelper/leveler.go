package sloghelper

import (
	"log/slog"
	"sync/atomic"
)

type Leveler struct {
	level int32
}

func (l *Leveler) Level() slog.Level {
	return slog.Level(atomic.LoadInt32(&l.level))
}

func (l *Leveler) SetLevel(level slog.Level) {
	atomic.StoreInt32(&l.level, int32(level))
}
