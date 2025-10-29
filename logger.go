package fredb

type Logger interface {
	Error(msg string, args ...any)
	Warn(msg string, args ...any)
	Info(msg string, args ...any)
}

type DiscardLogger struct{}

func (d DiscardLogger) Error(string, ...any) {}

func (d DiscardLogger) Warn(string, ...any) {}

func (d DiscardLogger) Info(string, ...any) {}
