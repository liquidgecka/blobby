package sloghelper

import (
	"log/slog"
	"time"
)

func Duration(key string, value time.Duration) slog.Attr {
	return slog.Attr{
		Key:   key,
		Value: slog.DurationValue(value),
	}
}

func Error(key string, value error) slog.Attr {
	return slog.Attr{
		Key:   key,
		Value: slog.StringValue(value.Error()),
	}
}

func Interface(key string, value interface{}) slog.Attr {
	return slog.Attr{
		Key:   key,
		Value: slog.AnyValue(value),
	}
}

func Int(key string, value int) slog.Attr {
	return slog.Attr{
		Key:   key,
		Value: slog.Int64Value(int64(value)),
	}
}

func Int32(key string, value int32) slog.Attr {
	return slog.Attr{
		Key:   key,
		Value: slog.Int64Value(int64(value)),
	}
}

func Int64(key string, value int64) slog.Attr {
	return slog.Attr{
		Key:   key,
		Value: slog.Int64Value(value),
	}
}

func String(key, value string) slog.Attr {
	return slog.Attr{
		Key:   key,
		Value: slog.StringValue(value),
	}
}

func Uint32(key string, value uint32) slog.Attr {
	return slog.Attr{
		Key:   key,
		Value: slog.Uint64Value(uint64(value)),
	}
}

func Uint64(key string, value uint64) slog.Attr {
	return slog.Attr{
		Key:   key,
		Value: slog.Uint64Value(value),
	}
}
