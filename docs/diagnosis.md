# Diagnosis of the Rust client gaps in `full-1` and `full-2`

`full-1` showed two results that looked like client defects rather than language differences: the Rust producer was about 0.43x Java at the baseline (1 KB, `batch.size` 16 KB, 6 partitions), and the Rust consumer was about 0.06x Java at 100 B messages. This document records how each was diagnosed and what fixes it. The rerun `full-2` (Rust `native` profile, clients `java`, `rust`, `rust-tuned`) then exposed two latency regressions, covered in the last two sections. Raw outputs are in `results/diag-1/` (not committed; regenerate with the commands below).

All experiments ran on the same host, broker and pinning as `full-1` (broker on 7 physical cores, client on 8 physical cores, Kafka 4.3.1 single broker on tmpfs), with a fresh 6-partition topic per run and a 200k-message warmup. Throughputs are single runs, so treat differences under about 3% as noise.

## Producer: the per-request ceiling

### Symptom

At the baseline, Rust stayed at about 240k msg/s whatever else changed: 1, 6 or 12 partitions, `linger.ms` 0, 5 or 50, `acks` 0, 1 or all. Java reached 570k msg/s at 6 partitions and 653k at 12, but dropped to exactly Rust's number (245k) with 1 partition.

### Hypotheses ruled out

| Experiment | Rust msg/s | Conclusion |
|---|---|---|
| `full-1` settings (`max.in.flight=5` and other Java-like values) | 245k | reference |
| `native` profile: `max.in.flight=1000000` and the other librdkafka defaults | 236k | in-flight window is not the limit |
| `native` + `socket.nagle.disable=true` | 235k | Nagle is not the limit |
| `native` + `queue.buffering.backpressure.threshold=10` | 237k | request-creation backpressure is not the limit |
| both of the above | 239k | no interaction |

Per-thread CPU during a baseline run (from `/proc/1/task/*/stat`): the librdkafka broker thread used 0.64 cores and the application sending thread 0.28 cores. The client is not CPU-bound.

### Mechanism

`debug=protocol` shows every ProduceRequest at 15,621 bytes (one 16 KB batch) with a 62-byte response (one partition), RTT about 0.4 ms:

```
Sent ProduceRequest (v10, 15621 bytes @ 0, CorrId 4)
Received ProduceResponse (v10, 62 bytes, CorrId 4, rtt 0.39ms)
```

librdkafka 2.12.1 puts exactly one partition's MessageSet in each ProduceRequest; its configuration reference and changelog have no setting to change this. The Kafka broker processes the requests of one connection strictly in order, one at a time, and with one broker the client has one connection. The client is therefore bounded by the broker's per-request cost: 240 MB/s over 15.6 KB requests is about 15.4k requests/s, or about 65 us of broker time per request, independent of partition count and in-flight window. The Java client packs the batches of all partitions bound for a broker into one ProduceRequest, amortizing that cost; with one partition it has nothing to pack and lands on the same ceiling.

### Confirmation: bytes per request is the lever

| `batch.size` | Java msg/s | Rust msg/s (`native`) | Rust/Java |
|---|---|---|---|
| 16 KB | 552k | 233k | 0.42x |
| 32 KB | 769k | 390k | 0.51x |
| 64 KB | 956k | 577k | 0.60x |
| 128 KB | 1,042k | 768k | 0.74x |
| 256 KB | 1,000k | 903k | 0.90x |
| 512 KB | 890k | 991k | 1.11x |
| 1 MB | 622k | 995k | 1.60x |

Both clients converge on about 1 GB/s, which is what one connection to this broker carries. Java gets there at 128 KB because each of its requests already carries 6 batches; Rust needs 512 KB to 1 MB batches. Java declines above 128 KB, which the diagnosis did not investigate further.

### Fix

- Use librdkafka's own default `batch.size` (1,000,000 bytes). The `full-1` baseline imposed Java's 16 KB default on both clients, which is close to librdkafka's worst case.
- More connections scale the ceiling: each producer instance has its own connection, and `full-1` scaling already showed Rust nearly doubling from K=1 to K=2.
- `max.in.flight`, Nagle and backpressure settings do not matter for this bottleneck.

## Consumer: the 1-second fetch backoff

### Symptom

At 100 B, the Rust consumer reached about 390k msg/s against Java's 5.5M, using only 0.45 cores. Its per-second counts arrived in multiples of about 57k messages with idle gaps.

### Mechanism

`debug=fetch` shows:

```
Fetch backoff for 1000ms: Local: Queue full
```

When a partition's local queue reaches `queued.min.messages` (default 100,000) or `queued.max.messages.kbytes`, librdkafka stops fetching that partition for `fetch.queue.backoff.ms`, default 1000 ms. At 100 B, 100k messages is about 10 MB, which the application drains in milliseconds, then waits for the rest of the second. The Java consumer has no timed backoff: it fetches a partition again as soon as its buffered records are consumed. The same stall reappears for Rust at 10 KB when `max.partition.fetch.bytes` is raised to 8 MB (157k msg/s against 331k at 1 MB), because larger fetches hit the kilobyte threshold sooner.

### Fixes measured (100 B, 6 partitions, 10M messages)

| Rust configuration | msg/s | Rust cores |
|---|---|---|
| `poll()` (rust-rdkafka `BaseConsumer`), defaults | 389k | 0.46 |
| `poll()`, `fetch.queue.backoff.ms=100` | 1,338k | 1.54 |
| `poll()`, `fetch.queue.backoff.ms=10` | 1,480k | 1.85 |
| `poll()`, `fetch.queue.backoff.ms=0` | 948k | 1.51 |
| `poll()`, `queued.min.messages=1000000` | 1,103k | 1.37 |
| batch API (`rd_kafka_consume_batch_queue`, 500 per call), defaults | 567k | 0.48 |
| batch API, `fetch.queue.backoff.ms=10` | 2,022k | 1.81 |
| Java `KafkaConsumer.poll()` | 5,612k | 2.62 |

`fetch.queue.backoff.ms=0` is worse than 10 ms because the fetcher then spins on the full queue. With the backoff fixed, the remaining limits are per-message CPU costs: `poll()` returns one message per call (the polling thread saturates at about 1.5M msg/s), and with the batch API the librdkafka broker thread saturates (0.94 cores at 2.07M msg/s) building one queue entry per fetched message. Beyond that the only lever for one client instance is more instances, which means more broker threads and connections.

### Effect at larger messages

| Size | Java | Rust `poll()` defaults | Rust `poll()` + backoff 10 ms | Rust batch + backoff 10 ms |
|---|---|---|---|---|
| 1 KB | 1,250k | 1,271k | 1,280k | 1,511k |
| 10 KB | 133k | 331k | 334k | 348k |

The fixes cost nothing at larger sizes, and the batch API adds 19% at 1 KB.

### Why Java trails at 10 KB

Java stays at about 1.3 to 1.4 GB/s per consumer instance at both 1 KB and 10 KB. Disabling CRC checks (`check.crcs=false`: 1,400 MB/s) and raising `max.partition.fetch.bytes` to 8 MB (1,374 MB/s) do not change it, so it is neither CRC nor fetch round trips; it is per-byte processing on the single polling thread. librdkafka does its fetch work on a separate broker thread and reaches 3.4 to 3.8 GB/s.

## Latency: the batch API waits for a full batch (`full-2`)

### Symptom

In the rate-limited `e2e-*` scenarios of `full-2`, the `rust-tuned` consumer (batch API, 500 per call, `fetch.queue.backoff.ms=10`) had end-to-end latencies one to two orders of magnitude above both other clients, while the producer side of the same runs matched `rust` exactly. Medians of 3 reps, 1 KB, 6 partitions, `batch.size` 16 KB:

| Scenario | Java e2e p50 / p99 | `rust` e2e p50 / p99 | `rust-tuned` e2e p50 / p99 |
|---|---|---|---|
| linger 0, 1k/s | 0.16 / 0.42 ms | 0.13 / 0.21 ms | 50.2 / 99.2 ms |
| linger 0, 10k/s | 0.14 / 0.40 ms | 0.12 / 0.23 ms | 25.2 / 49.6 ms |
| linger 5, 1k/s | 3.3 / 5.6 ms | 5.1 / 5.2 ms | 55.2 / 104.2 ms |
| linger 5, 10k/s | 3.0 / 6.0 ms | 3.1 / 5.8 ms | 28.0 / 54.3 ms |
| linger 5, 50k/s | 1.4 / 2.3 ms | 1.4 / 2.4 ms | 6.4 / 12.1 ms |

The linger 0, 50k/s row is left out here because it is dominated by the producer effect in the next section.

### Mechanism (proven arithmetically)

The harness called `rd_kafka_consume_batch_queue(queue, 100, buf, 500)` once per iteration. That call returns only when 500 messages have been collected or the 100 ms timeout expires; it does not return early when fewer messages are already queued. The wait each message sees therefore follows from the arrival rate:

| Rate | Time to collect 500 | Call returns after | Expected e2e p50 / p99 (linger 0) | Measured |
|---|---|---|---|---|
| 1k/s | 500 ms | 100 ms timeout, every call | 50 / 99 ms (uniform over 0 to 100 ms) | 50.2 / 99.2 ms |
| 10k/s | 50 ms | 50 ms, batch full | 25 / 49.5 ms (uniform over 0 to 50 ms) | 25.2 / 49.6 ms |
| 50k/s | 10 ms | 10 ms, batch full | 5 / 9.9 ms plus producer latency | 6.4 / 12.1 ms at linger 5 (producer adds 1.3 / 2.3 ms) |

At linger 5 the producer's own send-to-ack latency adds on top (about 5 ms at 1k/s and 10k/s), which accounts for the linger 5 rows. Java's `KafkaConsumer.poll(timeout)` returns as soon as any record is available, and `rust`'s `BaseConsumer::poll` returns one message as soon as it is queued, so neither waits for a batch to fill. The throughput numbers in the previous sections are unaffected, because at full speed the 500-message buffer fills in well under a millisecond.

This is a harness artifact, not a librdkafka limitation: the batch API is being asked to wait.

### Fix (measured)

`BatchSource::next` in `rust-bench/src/consume.rs` now goes through `fill`, which mirrors `poll()`: it first calls `rd_kafka_consume_batch_queue` with timeout 0 and takes whatever is queued, up to 500; only when that returns nothing does it block with the 100 ms timeout for a single message, and then drains whatever else is queued with timeout 0.

Throughput from `diag-1/e2efix-thr100-r*.out` (3 runs), latency from `diag-1/e2efix-1` (one rep per scenario, same scenarios and pinning as `full-2`):

| Check | Before (`full-2`) | After |
|---|---|---|
| Throughput, 100 B, 6 partitions, 10M messages, batch API + `fetch.queue.backoff.ms=10` | 2,097k msg/s | 2,124k, 2,139k, 2,118k msg/s |
| e2e p50 / p99, linger 0, 1k/s | 50.2 / 99.2 ms | 0.125 / 0.202 ms |
| e2e p50 / p99, linger 0, 10k/s | 25.2 / 49.6 ms | 0.118 / 0.232 ms |
| e2e p50 / p99, linger 5, 1k/s | 55.2 / 104.2 ms | 5.13 / 5.21 ms |
| e2e p50 / p99, linger 5, 50k/s | 6.4 / 12.1 ms | 1.42 / 2.42 ms |

After the fix `rust-tuned` matches the `rust` poll loop in every e2e scenario. The remaining high values at linger 0 (p99.9 5.18 ms at 10k/s, p99 25.7 ms at 50k/s) come from the producer, whose send-to-ack p99.9 and p99 in the same runs were 5.04 and 25.6 ms; that is the in-flight effect below. The `full-2` e2e numbers for `rust-tuned` are superseded by `e2efix-1` and by `e2e-2`.

**Confirmed in `e2e-2` (3 reps, `results/e2e-2/summary.csv`).** `rust-tuned` e2e p50 / p99 is 0.13 / 0.21 ms at linger 0, 1k/s and 0.12 / 0.23 ms at 10k/s, and matches `rust` within 0.01 ms in every scenario except linger 0 at 50k/s, where both are dominated by the producer's in-flight window (next section). The `full-2` e2e numbers for `rust-tuned` are superseded by `e2e-2`.

## Latency: librdkafka's unlimited in-flight window at linger 0 (`full-2`)

### Symptom

Moving the Rust producer from the `matched` profile (`full-1`) to the `native` profile (`full-2`) left throughput unchanged but raised latency at linger 0 and 50k msg/s by more than an order of magnitude. Medians of 3 reps, 1 KB, 6 partitions, `batch.size` 16 KB in both runs:

| Metric (linger 0, 50k/s) | `full-1` (`matched`) | `full-2` (`native`) |
|---|---|---|
| Rust producer send-to-ack p50 / p99 | 0.54 / 1.81 ms | 13.6 / 26.2 ms |
| Rust consumer e2e p50 / p99 | 0.66 / 1.94 ms | 13.7 / 26.3 ms |
| Java consumer e2e p50 / p99 (reference) | 0.64 / 0.92 ms | 0.65 / 0.94 ms |

The effect is limited to this corner. At linger 0 and 1k/s the numbers are unchanged (send-to-ack p99 0.14 vs 0.15 ms); at 10k/s only the tail moves (send-to-ack p99.9 0.26 vs 2.5 ms); at linger 5 and 50k/s they match (p50 1.34 ms both).

### Single-knob experiment

The two profiles differ in several producer settings (`max.in.flight`, `batch.num.messages`, `queue.buffering.max.kbytes`, among others). One run each, linger 0, 50k msg/s for 30 s, 1 KB, 6 partitions, `batch.size` 16 KB (`results/diag-1/e2e50k-*.out`):

| Producer configuration | msg/s | send-to-ack p50 | p99 | p99.9 | max | Rust cores |
|---|---|---|---|---|---|---|
| `native` (`max.in.flight=1000000`) | 49,958 | 14.1 ms | 26.4 ms | 28.9 ms | 32.2 ms | 1.68 |
| `native` + `max.in.flight=5` | 49,997 | 0.47 ms | 1.28 ms | 2.05 ms | 7.6 ms | 1.77 |
| `matched` | 49,990 | 0.48 ms | 1.34 ms | 2.20 ms | 7.7 ms | 1.77 |

Setting only `max.in.flight.requests.per.connection=5` on top of `native` reproduces `matched` to within noise, so the in-flight window is the knob; the other `matched` settings do not contribute. Throughput is identical in all three.

**Confirmed in `e2e-2` (3 reps).** The `rust-lowlat` variant is `native` plus `max.in.flight.requests.per.connection=5` on the producer, with the `rust` consumer. At linger 0 and 50k/s, consumer e2e p50 / p99 is 17.6 / 32.9 ms for `rust` and 0.66 / 1.93 ms for `rust-lowlat`, against 0.64 / 0.93 ms for Java; producer send-to-ack p99 is 32.8 ms and 1.80 ms. In the other five e2e scenarios `rust-lowlat` and `rust` agree within 0.01 ms at p50 and p99. The `rust` number at this cell is higher than in `full-2` (13.7 / 26.3 ms), so the size of the unlimited-window queue varies between runs; the low-latency result does not.

### Mechanism (inferred, not directly observed)

A `debug=protocol` run to count messages per ProduceRequest slowed the client to 12k msg/s with p50 5.6 s (`e2e50k-native-dbg.out`), so it could not show the steady state. The likely mechanism, consistent with the producer section above: with linger 0, librdkafka sends as soon as a partition has data. With a window of 5, once 5 requests are outstanding new messages wait in the partition queues and coalesce, so each request carries many messages and the request rate stays low. With a window of 1,000,000, nothing ever waits for a slot, so each message or two becomes its own request; the broker processes one connection's requests serially, so these small requests queue behind each other on the broker and latency grows until the backlog is large enough to be batched. At 1k/s the request rate is low enough that the broker keeps up either way, which fits the rate dependence above. The per-request broker cost for small requests was not measured.

### Practical settings

- For throughput, raise `batch.size` (librdkafka default 1 MB); `max.in.flight` does not affect it.
- For latency at low linger and moderate to high rates, set `max.in.flight.requests.per.connection` to about 5; this costs no throughput here.

## Reproducing

The experiments use `results/diag-1/lib.sh`, which only issues `docker` and `infra/` commands against the benchmark host:

```
source results/diag-1/lib.sh
run rust r-native 3000000 --config-profile native
BATCH=1048576 run rust r-b1048576 3000000 --config-profile native
prefill kbench-diag-c100 6 12000000 100
consume rust c100-batch-fqb10 kbench-diag-c100 6 10000000 1000000 --consume-api batch --extra fetch.queue.backoff.ms=10
consume rust c100-dbg kbench-diag-c100 6 500000 0 --extra debug=fetch
```
