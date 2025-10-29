package fredb

// Logger interface matches the implementation of slog.
// See pkg logger for adapters implementations for common logger libraries.
type Logger interface {
	Error(msg string, args ...any)
	Warn(msg string, args ...any)
	Info(msg string, args ...any)
}

// DiscardLogger is the default logger that compiles to a no-op
type DiscardLogger struct{}

func (d DiscardLogger) Error(string, ...any) {}

func (d DiscardLogger) Warn(string, ...any) {}

func (d DiscardLogger) Info(string, ...any) {}
