package mkqd

import (
	"fmt"
	"log/slog"
	"os"
)

// newLogger builds the process logger from configuration. It writes to
// stderr so that job output on stdout (if an executor ever produces
// any) stays separable in a container log pipeline.
func newLogger(cfg LogConfig) (*slog.Logger, error) {
	var level slog.Level
	switch cfg.Level {
	case "debug":
		level = slog.LevelDebug
	case "info", "":
		level = slog.LevelInfo
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		return nil, fmt.Errorf("mkqd: log.level %q is not one of debug, info, warn, error", cfg.Level)
	}

	opts := &slog.HandlerOptions{Level: level}
	var h slog.Handler
	switch cfg.Format {
	case "json":
		h = slog.NewJSONHandler(os.Stderr, opts)
	case "text", "":
		h = slog.NewTextHandler(os.Stderr, opts)
	default:
		return nil, fmt.Errorf("mkqd: log.format %q is not one of text, json", cfg.Format)
	}

	return slog.New(h), nil
}
