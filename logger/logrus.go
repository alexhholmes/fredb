package logger

import (
	"github.com/sirupsen/logrus"

	"github.com/alexhholmes/fredb"
)

// Logrus wraps a logrus.Logger to implement fredb.Logger.
type Logrus struct {
	logger *logrus.Logger
}

// NewLogrus creates a fredb.Logger from a logrus.Logger.
func NewLogrus(logger *logrus.Logger) fredb.Logger {
	return &Logrus{logger: logger}
}

// Error logs an error message with key-value pairs.
func (l *Logrus) Error(msg string, args ...any) {
	logrus.WithFields(argsToFields(args)).Error(msg)
}

// Warn logs a warning message with key-value pairs.
func (l *Logrus) Warn(msg string, args ...any) {
	logrus.WithFields(argsToFields(args)).Warn(msg)
}

// Info logs an info message with key-value pairs.
func (l *Logrus) Info(msg string, args ...any) {
	logrus.WithFields(argsToFields(args)).Info(msg)
}

func argsToFields(args []any) logrus.Fields {
	fields := logrus.Fields{}
	for i := 0; i < len(args)-1; i += 2 {
		if key, ok := args[i].(string); ok {
			fields[key] = args[i+1]
		}
	}
	return fields
}
