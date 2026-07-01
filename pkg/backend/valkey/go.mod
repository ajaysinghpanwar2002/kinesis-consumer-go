module github.com/pratilipi/kinesis-consumer-go/pkg/backend/valkey

go 1.26.3

require (
	github.com/alicebob/miniredis/v2 v2.35.0
	github.com/pratilipi/kinesis-consumer-go v0.0.0
	github.com/valkey-io/valkey-go v1.0.65
)

require (
	github.com/yuin/gopher-lua v1.1.1 // indirect
	golang.org/x/sys v0.31.0 // indirect
)

// The core module is not published; resolve it from the local checkout.
replace github.com/pratilipi/kinesis-consumer-go => ../../..
