# Measured processing results

Measured on 2026-09-12: Apple M4 Pro, 24 GiB RAM, macOS arm64,
Go 1.26.3, Docker 28.5.1, and standalone Valkey 8.1.8 in Docker.
Valkey used `noeviction`, AOF, and `appendfsync always`. Each offered-load
window lasted five seconds; test suites did not run concurrently.

All 96 cases without injected latency sustained at least 99% of offered load
across the baseline (1,157 records/second), 2× and 5×, with one/four shards and
1/16 KiB payloads. Their small end-of-window backlogs do not indicate a sustained
capacity limit within these short runs. Longer production workloads and AWS
network costs are outside this measurement.

The injected 1 ms cases show the limit of serialized Ack calls. One asynchronous
completion worker cannot sustain the baseline with that added delay. Four
synchronous or batch shard workers sustain more load, but accumulate backlog
at the higher offered rates. No ownership checks were removed.

See [method and reproduction](performance.md). [Full CSV](performance-data/load.csv)
contains all 120 cases, including midpoint/final backlog, Ack p50/p95/p99,
coordination API calls, process CPU, allocations and heap bytes.

## Throughput without injected latency

1 KiB payloads. Values are achieved records/second, rounded to the nearest record.
The last column is the backlog after five seconds at 5× load.

| Backend | Mode | Shards | 1× | 2× | 5× | 5× backlog |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| memory | automatic | 1 | 1157 | 2314 | 5784 | 4 |
| memory | automatic | 4 | 1157 | 2313 | 5785 | 1 |
| memory | sync | 1 | 1157 | 2314 | 5784 | 6 |
| memory | sync | 4 | 1157 | 2314 | 5785 | 0 |
| memory | async | 1 | 1157 | 2313 | 5784 | 6 |
| memory | async | 4 | 1156 | 2313 | 5783 | 10 |
| memory | batch | 1 | 1157 | 2314 | 5785 | 0 |
| memory | batch | 4 | 1157 | 2314 | 5785 | 1 |
| valkey | automatic | 1 | 1157 | 2314 | 5784 | 5 |
| valkey | automatic | 4 | 1157 | 2314 | 5784 | 3 |
| valkey | sync | 1 | 1157 | 2313 | 5784 | 6 |
| valkey | sync | 4 | 1157 | 2313 | 5783 | 9 |
| valkey | async | 1 | 1156 | 2313 | 5781 | 21 |
| valkey | async | 4 | 1157 | 2312 | 5781 | 20 |
| valkey | batch | 1 | 1156 | 2313 | 5784 | 7 |
| valkey | batch | 4 | 1157 | 2313 | 5783 | 8 |

## Added 1 ms per backend operation

Four shards, 1 KiB payloads. Backlog growth is final minus midpoint backlog,
divided by the 2.5-second half-window, at 5× load.

| Backend | Mode | 1× throughput | 2× throughput | 5× throughput | 5× backlog growth/s |
| --- | --- | ---: | ---: | ---: | ---: |
| memory | automatic | 1157 | 2314 | 5785 | 0 |
| memory | sync | 1157 | 2313 | 3471 | 2316 |
| memory | async | 865 | 863 | 865 | 4920 |
| memory | batch | 1156 | 2313 | 3494 | 2289 |
| valkey | automatic | 1157 | 2314 | 5784 | -3 |
| valkey | sync | 1156 | 2198 | 2204 | 3595 |
| valkey | async | 599 | 559 | 539 | 5256 |
| valkey | batch | 1156 | 2190 | 2218 | 3574 |

## Baseline costs

One shard, 1 KiB payloads, no injected delay. Ack values are microseconds;
automatic handling has no Ack call. CPU is process CPU seconds per wall second,
so 1.0 means one CPU core. Heap is the whole process heap before drain and GC,
not retained payload size. Coordination counts have the exclusions described
in the method.

| Backend | Mode | Ack p50 / p95 / p99 µs | Coordination calls/record | CPU cores | Allocations/record | Allocated bytes/record | Heap MiB |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| memory | automatic | — | 0.010 | 0.045 | 25.9 | 2422 | 1.50 |
| memory | sync | 5.8 / 14.9 / 27.0 | 1.011 | 0.098 | 41.5 | 3551 | 2.61 |
| memory | async | 1.7 / 5.2 / 12.0 | 1.011 | 0.091 | 42.8 | 3626 | 2.51 |
| memory | batch | 4.7 / 11.5 / 18.4 | 1.011 | 0.094 | 38.8 | 3415 | 2.71 |
| valkey | automatic | — | 0.010 | 0.054 | 28.3 | 3377 | 11.57 |
| valkey | sync | 454.3 / 723.5 / 1392.6 | 1.011 | 0.231 | 50.0 | 4939 | 10.90 |
| valkey | async | 421.8 / 735.1 / 1403.7 | 1.011 | 0.260 | 51.8 | 4845 | 14.85 |
| valkey | batch | 432.5 / 635.3 / 1345.4 | 1.011 | 0.234 | 49.7 | 4930 | 10.54 |

Two saturated Valkey asynchronous cases were rerun with a 30-second drain
allowance after their original five-second allowance expired. Measured windows
remain five seconds and end before drain. The CSV includes only the successful
rerun for each of those cases. Other rows completed with the earlier allowance;
that difference does not affect their pre-drain measurements.
