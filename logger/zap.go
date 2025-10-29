package logger

import (
	"go.uber.org/zap"

	"github.com/alexhholmes/fredb"
)

// Zap wraps a zap.Logger to implement fredb.Logger.
type Zap struct {
	logger *zap.Logger
}

// NewZap creates a fredb.Logger from a zap.Logger.
func NewZap(logger *zap.Logger) fredb.Logger {
	return &Zap{logger: logger}
}

// Error logs an error message with key-value pairs.
func (z *Zap) Error(msg string, args ...any) {
	z.logger.Sugar().Errorw(msg, args...)
}

// Warn logs a warning message with key-value pairs.
func (z *Zap) Warn(msg string, args ...any) {
	z.logger.Sugar().Warnw(msg, args...)
}

// Info logs an info message with key-value pairs.
func (z *Zap) Info(msg string, args ...any) {
	z.logger.Sugar().Infow(msg, args...)
}
