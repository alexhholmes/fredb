// Package logger provides adapters for popular logger libraries to work with fredb's Logger interface.
//
// The adapters allow you to use your existing logger with fredb without writing boilerplate.
// Note that the standard library's slog.Logger already implements fredb.Logger directly.
//
// Example with zap:
//
//	import (
//	    "github.com/alexhholmes/fredb"
//	    "github.com/alexhholmes/fredb/logger"
//	    "go.uber.org/zap"
//	)
//
//	func main() {
//	    zapLogger, _ := zap.NewProduction()
//
//	    db, err := fredb.Open("data.db", &fredb.Options{
//	        Logger: logger.NewZap(zapLogger),
//	    })
//	    if err != nil {
//	        panic(err)
//	    }
//	    defer db.Close()
//	}
//
package logger
