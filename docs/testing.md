# Integration Test Suite

This is a ledger, not a marketing page. The library ships with an integration
suite that runs against **real LocalStack Kinesis + real Valkey** (no mocks of
the AWS or Valkey APIs) via `make integration`. Every row below maps to a real
`func Test…` in [`test/integration`](../test/integration) that you can read and
run.

## How to run

```bash
# Compile-check the tagged tests without any infrastructure:
make integration-build

# Bring up LocalStack + Valkey, run the suite, tear the infra down:
make integration
```

`make integration` composes up the services with a health-gate (`up -d --wait`),
runs `go test -tags integration -count=1 -timeout 600s ./...`, and always tears
the infra down (`down -v`) — even when a test fails. It needs Docker.

The suite is behind the `integration` build tag and lives in its own module, so
it is deliberately **excluded** from `make test`; the unit path stays infra-free.

## What "verified" means here

Each hardening scenario (the `IT-*` rows) was held to the same standard during
development — this is the confidence argument:

- **Load-bearing assertion.** Each test has at least one assertion that fails if
  the behavior under test regresses — not just a smoke check that records flow.
- **Mutation-verified.** A deliberate bug (a flipped option, a disabled code
  path, a cleared checkpoint/store) was introduced to confirm the load-bearing
  assertion actually fails, then reverted. A test that can't fail proves
  nothing; these were shown to fail for the right reason.
- **Non-flaky.** Each was run repeatedly (typically 5×) to rule out timing
  flakiness before being accepted.

Note the distinction: mutation-verification and the 5× repeat were a
development-time gate recorded per scenario; a normal `make integration` run
executes each test **once** (`-count=1`).

## Foundation tests

| Test | Proves |
| --- | --- |
| `TestSingleConsumerEndToEnd` | End-to-end single consumer: produce → consume → checkpoint → resume. |
| `TestMultiConsumerRebalancesLeases` | Two consumers redistribute shard leases across ≥2 owners. |

## Scenario ledger

Groups A–I map to the integration-hardening plan; `#n` is the original scenario
number.

### Group A — Attribution & exclusivity

| IT | Test | Proves |
| --- | --- | --- |
| IT-1 (#1) | `TestConsumerBProcessesAfterJoining` | A late-joining consumer processes records after joining the group. |
| IT-2 (#3) | `TestShardExclusivitySteadyState` | In steady state each shard is processed by exactly one owner. |

### Group B — Checkpoint / resume correctness

| IT | Test | Proves |
| --- | --- | --- |
| IT-3 (#2) | `TestCheckpointResumeAfterRebalance` | After a rebalance, a fresh consumer resumes from checkpoints (`AFTER_SEQUENCE_NUMBER`) with bounded replay. |
| IT-4 (#7) | `TestCheckpointFrequencyBoundsReplayAfterCrash` | Under real paging, `checkpointEvery` bounds replay after a hard (non-drain) crash. |
| IT-5 (#8) | `TestGracefulDrainCheckpointsInFlightProgress` | Graceful drain flushes in-flight progress so a resume replays nothing. |
| IT-5b (#4) | `TestGracefulDrainHandsOffToLiveSuccessor` | On graceful stop, a released lease is picked up by a live peer that resumes from the drained frontier. |

### Group C — Uncooperative failover

| IT | Test | Proves |
| --- | --- | --- |
| IT-6 (#6) | `TestStaleLeaseReclaimedAfterTTLExpiry` | A stale lease (no heartbeat) blocks takeover until its TTL lapses, then is reclaimed. |
| IT-7 (#5) | `TestUngracefulCrashFailoverResumesFromCheckpoint` | After an ungraceful crash (lease not released), a successor reclaims post-TTL and resumes from the checkpoint with bounded replay. |

### Group D — Failure handling

| IT | Test | Proves |
| --- | --- | --- |
| IT-8 (#14) | `TestDefaultFailurePolicyReturnsHandlerError` | The default `FailFast` policy surfaces the handler error through `Start` and does not checkpoint. |
| IT-9a (#15) | `TestSkipPoisonContinuesAndCheckpoints` | `Skip` drops a poison record, keeps delivering later records, and checkpoints past it. |
| IT-9b (#18) | `TestRetryEventuallySucceeds` | A flaky record is retried, succeeds once (no double-delivery), and stops retrying early. |
| IT-10a (#16) | `TestDLQCapturesPoisonAndContinues` | `SendToDLQ` publishes a poison record once with full metadata, then continues and checkpoints. |
| IT-10b (#17) | `TestDLQPublishFailureFailsConsumer` | A failing DLQ publish fails the consumer instead of silently checkpointing. |

### Group E — Batch & concurrency

| IT | Test | Proves |
| --- | --- | --- |
| IT-11a (#19) | `TestBatchHandlerDeliversPagesAndResumes` | The batch handler is called once per GetRecords page; per-page checkpoint + resume works. |
| IT-11b (#19) | `TestBatchDLQFansOutWholePage` | In batch mode one poison record fails the whole page; every record fans out to the DLQ. |
| IT-11c (#19) | `TestBatchSkipDropsWholePage` | In batch mode `Skip` drops the whole page (durable loss of collateral) and checkpoints past it. |
| IT-12 (#20) | `TestShardConcurrencyDeliversAllAndCheckpointsPerPage` | With per-shard concurrency > 1, records still deliver exactly once and the page checkpoint holds. |

### Group F — Start positions & stream identity

| IT | Test | Proves |
| --- | --- | --- |
| IT-13a (#9) | `TestStartLatestSkipsBacklogDeliversNew` | `StartLatest` skips the pre-existing backlog and delivers only new records. |
| IT-13b (#9) | `TestStartAtTimestampSkipsBeforeTimestamp` | `StartAtTimestamp` skips before-boundary records and delivers after-boundary ones. |
| IT-14 (#10) | `TestStreamARNConsumesAndResumes` | A consumer configured by `StreamARN` (name empty) consumes and resumes. |
| IT-15 (#13) | `TestClosedShardCompletionCheckpointSkipsParentOnRestart` | A closed shard is marked `SHARD_END`; a restart skips the completed parent. |

### Group G — Resharding

| IT | Test | Proves |
| --- | --- | --- |
| IT-16 (#11) | `TestLiveConsumerDiscoversChildrenOnReshard` | A live consumer discovers child shards after a split and processes them without restart. |
| IT-17 (#12) | `TestParentChildGatingHoldsChildrenUntilParentCompletes` | A child shard is gated until its parent reaches `SHARD_END`, then delivered. |

### Group H — Rebalance fairness

| IT | Test | Proves |
| --- | --- | --- |
| IT-18 (#21) | `TestRebalanceMoreConsumersThanShards` | With more consumers than shards, only enough own shards; idle workers still heartbeat; ownership is stable. |
| IT-19 (#22) | `TestRebalanceUnevenShardCount` | An uneven shard count (5 shards, 2 consumers) redistributes to the fair split. |
| IT-20 (#23) | `TestRebalanceCooldownPreventsImmediateReacquire` | After shedding a shard, a worker respects the cooldown before reacquiring it. |
| IT-21 (#24) | `TestRebalanceMaxMovesPerTick` | Rebalancing moves at most `maxMoves` shards per tick (a stepwise waypoint, not a jump). |

### Group I — Key isolation & restart

| IT | Test | Proves |
| --- | --- | --- |
| IT-22 (#25) | `TestKeyPrefixIsolationAcrossStreams` | Distinct key prefixes isolate checkpoint/lease/worker keys across streams (foreign-prefix reads return empty). |
| IT-23 (#26) | `TestStoreDerivedLeasePrefix` | A store-provided lease manager leases under the derived `<checkpointPrefix>-lease` prefix. |
| IT-24 (#27) | `TestRestartOverReleasedLeaseResumesFromCheckpoint` | After a clean stop releases the lease, a fresh consumer resumes over the leftover checkpoint. |
