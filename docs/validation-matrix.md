# Explicit processing validation

The tests below cover acknowledgment, recovery, admission, and lifecycle
interactions. Unit tests inject failures and control concurrency; infrastructure
tests exercise Kinesis through LocalStack and real Valkey. The load and heap
experiments are described in [performance results](performance.md).

| Contract | Coverage |
| --- | --- |
| Copied handles, duplicate/concurrent/out-of-order Ack, atomic acceptance versus retry and invalidation, compact suffix metadata | `pkg/consumer/ack_tracker_test.go`; `TestAckCapacityReleaseRacesRetryAndInvalidation` |
| Delayed Ack after return, gaps across pages, partial retries, panic, skip, DLQ success/failure | `pkg/consumer/explicit_processor_test.go`, including `TestExplicitDLQCannotCrossEarlierAsyncGap` |
| Initialization before admission and oversized rejection, competing writes, successor reuse, checkpoint supersession | `pkg/consumer/shard_session_test.go`, `fenced_recovery_test.go`, `admission_page_test.go`; memory and Valkey fenced store tests |
| Crash before initialization and complete state loss boundaries | `TestMemoryRecoveryStartupAndTotalLossBoundary`; `TestFencedConsumerRecordsNothingBeforeTheFirstRecord` |
| Accepted suffix behind an unfinished gap survives application process death by replay | `TestExplicitProcessCrashReplaysAcceptedSuffix`: a child test process is killed without shutdown cleanup; its successor reads the persisted inclusive anchor |
| Persisted progress after graceful drain and exclusive successor resume | `TestExplicitAcknowledgmentDrainFlushesAndResumes` against LocalStack/Valkey |
| Expired iterators during pauses, empty anchor reads, missing/trimmed anchors, transient failures and verification timeout | `pkg/consumer/shard_anchor_test.go`, `fenced_recovery_test.go`; `TestExplicitExpiredIteratorResumesFromLastFetchedSequence`; `TestBoundedRecoverySlotWaitFlushesCompletedProgress` |
| Independent metadata lifetimes across expiry, release and cleanup of all leases/workers | `TestMemoryRecoverySurvivesLeaseAndWorkerExpiry`; `TestFencedMetadataOutlivesEveryLeaseAndWorker` |
| Transfer, expiry, random generations, stale writes and permanent invalidation | `pkg/lease/memory_fenced_test.go`; `pkg/checkpoint/memory_fenced_test.go`; Valkey lease/checkpoint `fenced_test.go` |
| Partial loss, observed rollback, total-loss limits and unobserved historical live leases | `TestMemoryRecoveryCorruption`, `TestFencedMetadataLossAndRollback`, `TestUnobservedHistoricalLeaseIsOutsideGuarantee` |
| Count/byte budgets at both scopes, zero-byte payloads, ordered batch splitting, fairness, slot retention, cancellation | `pkg/consumer/in_flight_test.go`, `admission_page_test.go`; `test/integration/in_flight_test.go` |
| Heartbeats and checkpoints while admission is paused | `TestBoundedWorkerRenewsWhileAdmissionPausedAndCancellationReleasesSlot`; `TestExplicitCapacityWaitAllowsAckAndCheckpoint`; recovery-slot tests |
| Graceful timer completion, live completion contexts, timeout, immediate stop, lease loss, unadmitted staging | `pkg/consumer/explicit_lifecycle_test.go`; infrastructure drain test and application shutdown example |
| Closed shards and split/merge parent gating on persisted completion | `TestExplicitShardCompletionWaitsForAcknowledgments`; `TestParentChildGatingHoldsChildrenUntilParentCompletes`; `TestExplicitMergeWaitsForBothPersistedParents` |
| Pressure separation, periodic blocked ages, accepted versus persisted sequences, checkpoint failure visibility and gauge cleanup | `pkg/consumer/observations_test.go`; infrastructure explicit drain health assertions |
| Real cluster scripts, incompatible layout rejection, AOF process termination/restart | `TestRealValkeyClusterFencing`, `TestRealValkeyAOFRestart` via `make valkey-integration` |
| Retained payloads across long gaps, partial retries, staging and stopped workers | `pkg/consumer/retained_heap_test.go`; retained heap and pprof measurements |

Run the normal gates across all modules:

```sh
make fmt-check build vet test lint integration-build test-race tidy-check
make vulncheck
make integration
make valkey-integration
```

The process-kill and merge tests run with `make integration`. Performance tests
are opt-in and skipped by that command. Infrastructure tests validate controlled
application crashes and backend restarts, not arbitrary replica promotion. See
[recovery boundaries](fenced-recovery.md) and
[Valkey durability assumptions](fenced-valkey.md#durability-assumptions).
