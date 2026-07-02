module github.com/pratilipi/kinesis-consumer-go/test/integration

go 1.26.3

require (
	github.com/aws/aws-sdk-go-v2 v1.37.1
	github.com/aws/aws-sdk-go-v2/service/kinesis v1.36.1
	github.com/pratilipi/kinesis-consumer-go v0.0.0
	github.com/pratilipi/kinesis-consumer-go/pkg/backend/valkey v0.0.0
)

require (
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.7.0 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.1 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.1 // indirect
	github.com/aws/smithy-go v1.22.5 // indirect
	github.com/valkey-io/valkey-go v1.0.65 // indirect
	golang.org/x/sys v0.31.0 // indirect
)

// The core module and the Valkey backend module are not published; resolve them
// from the local checkout.
replace github.com/pratilipi/kinesis-consumer-go => ../..

replace github.com/pratilipi/kinesis-consumer-go/pkg/backend/valkey => ../../pkg/backend/valkey
