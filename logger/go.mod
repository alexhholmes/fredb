module logger

go 1.25

replace github.com/alexhholmes/fredb => ../

require (
	github.com/alexhholmes/fredb v0.0.0-00010101000000-000000000000
	github.com/sirupsen/logrus v1.9.3
	go.uber.org/zap v1.27.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/elastic/go-freelru v0.16.0 // indirect
	github.com/google/btree v1.1.3 // indirect
	go.uber.org/multierr v1.10.0 // indirect
	golang.org/x/sys v0.12.0 // indirect
)
