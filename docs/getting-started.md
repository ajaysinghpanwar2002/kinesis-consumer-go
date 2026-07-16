# Getting Started

This guide takes you from zero to a running Kinesis consumer backed by Valkey
for checkpoints and shard leasing. For the full capability list see
[features.md](features.md); for handler/retry/DLQ detail see
[handler-behavior.md](handler-behavior.md).

## Install

The core library and the Valkey backend are separate modules — add both:

```bash
go get github.com/ajaysinghpanwar2002/kinesis-consumer-go
go get github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/backend/valkey
```

You also need the AWS SDK v2 Kinesis client (pulled in transitively, but you
import it directly):

```bash
go get github.com/aws/aws-sdk-go-v2/service/kinesis
go get github.com/aws/aws-sdk-go-v2/config
```

## The five steps

A consumer is assembled from five pieces:

1. a **Kinesis client** (`*kinesis.Client`),
2. a **checkpoint store** (the Valkey store, which also provides leasing),
3. a **`Config`** naming the stream and start position,
4. a **handler** for your records,
5. `consumer.New(...)` then `Start(ctx)`.

## Minimal consumer

This program compiles and runs as-is. It consumes a stream and logs each record;
`Ctrl-C` triggers a graceful shutdown.

```go
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"

	valkeycheckpoint "github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/backend/valkey/checkpoint"
	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/consumer"
)

func main() {
	// Cancel the context on Ctrl-C so graceful drain can run before Start returns.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	// 1. Kinesis client from the standard AWS credential chain (env vars, shared
	//    config, IAM roles). See the "LocalStack" note below for local testing.
	awsCfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("load aws config: %v", err)
	}
	kc := kinesis.NewFromConfig(awsCfg)

	// 2. Valkey-backed checkpoint store. Because *Store also implements
	//    lease.Provider, passing it as the store is all that is needed to turn
	//    on shard leasing — no WithLeaseManager call.
	store, err := valkeycheckpoint.New("localhost:6379",
		valkeycheckpoint.WithKeyPrefix("my-app"),
		valkeycheckpoint.WithPingTimeout(5*time.Second),
	)
	if err != nil {
		log.Fatalf("connect to valkey: %v", err)
	}
	defer store.Close()

	// 3. Which stream, and where to start when there is no checkpoint yet.
	cfg := consumer.Config{
		StreamName:    "my-stream",
		StartPosition: consumer.StartTrimHorizon,
	}

	// 4. Your record handler. Return nil on success; a non-nil error triggers
	//    retries and then the configured failure policy (default: fail-fast,
	//    which returns the error without checkpointing the failed page).
	handler := func(_ context.Context, rec consumer.Record) error {
		log.Printf("seq=%s pk=%s bytes=%d",
			aws.ToString(rec.SequenceNumber), aws.ToString(rec.PartitionKey), len(rec.Data))
		return nil
	}

	// 5. Build and run. Start blocks until ctx is cancelled or a fatal error.
	cons, err := consumer.New(cfg, kc, store, handler,
		consumer.WithGracefulDrain(30*time.Second),
	)
	if err != nil {
		log.Fatalf("create consumer: %v", err)
	}

	log.Println("starting consumer; press Ctrl-C to stop")
	if err := cons.Start(ctx); err != nil {
		log.Fatalf("consumer stopped: %v", err)
	}
	log.Println("consumer stopped cleanly")
}
```

That is the whole happy path. Every option beyond `WithGracefulDrain` has a
working default (see the defaults table in [features.md](features.md)), so the
consumer runs with no options at all.

## Multi-worker coordination

Leasing is automatic: run **more than one copy** of this program against the
same stream and the same Valkey, and shards spread across the workers. Each
worker holds a lease per shard (exclusive in steady state; ownership
transfers open short, bounded dual-processing windows — see the ownership
transfer windows in [features.md](features.md)) and the pool fair-share
rebalances as workers join or leave. Nothing extra to configure — it falls
out of passing the Valkey store (which satisfies `lease.Provider`).

Lease keys live under `<checkpointPrefix>-lease` (here `my-app-lease`), adjacent
to the checkpoint keys under `my-app`.

## Graceful shutdown

`Start(ctx)` blocks until `ctx` is cancelled. The `signal.NotifyContext` above
cancels on `Ctrl-C`; with `WithGracefulDrain`, workers then finish in-flight
records, checkpoint, and release their leases before `Start` returns. Without
`WithGracefulDrain`, shutdown is prompt and relies on at-least-once redelivery +
failover for any in-flight page.

## Running against LocalStack

For local testing, point the Kinesis client at LocalStack instead of the default
credential chain by building the client from static options:

```go
kc := kinesis.New(kinesis.Options{
	Region:       "us-east-1",
	BaseEndpoint: aws.String("http://localhost:4566"),
	Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		return aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test"}, nil
	}),
})
```

Then run Valkey locally (`docker run -p 6379:6379 valkey/valkey:8-alpine`) and
start the program.

## Next steps

- [`examples/valkey`](../examples/valkey) — a fuller runnable version with every
  tuning flag wired up.
- [features.md](features.md) — the complete capability list and defaults table.
- [handler-behavior.md](handler-behavior.md) — batch handlers, retries, failure
  policies, DLQ, and shard concurrency in depth.
