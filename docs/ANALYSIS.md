# Java vs Rust Kafka clients: full analysis

This document is the complete analysis of the benchmark, in the order the work happened: how the bench was made fair, what the first run showed, how each gap was diagnosed on both clients, what the rerun showed, what the latency trade-offs are, and what is still open. The detailed diagnoses are in [`diagnosis.md`](diagnosis.md) (Rust side) and [`java-diagnosis.md`](java-diagnosis.md) (Java side); this document summarizes them and ties them to the benchmark numbers.

Clients: Java `org.apache.kafka:kafka-clients` 4.3.1 on Temurin JDK 25, and Rust `rdkafka` 0.39.0 wrapping librdkafka 2.12.1. Broker: a single Kafka 4.3.1 broker (KRaft) with log dirs on a 16 GiB tmpfs. Host: one EC2 c6i.8xlarge (16 physical cores, 32 vCPUs). Unless stated otherwise, numbers are medians of 3 reps with 1 KB messages, 6 partitions, `acks=1` and `linger.ms=5`. Result directories: `full-1` (first run), `full-2` (rerun), `diag-1` (Rust diagnosis and fixes), `diag-java` (Java diagnosis). Each finding is labelled **proven** (measured directly or read from source) or **inferred** (consistent with the measurements, not observed directly).

## 1. A fair bench

The comparison is only meaningful if both clients get the same hardware, broker, bytes and configuration. [`CONTRACT.md`](../CONTRACT.md) specifies this in full; the main points:

- **Isolation.** Everything runs in Docker on one host. The broker is pinned to cores 0 to 6, clients to cores 8 to 15 and their hyperthreads (8 physical cores, 16 vCPUs), and the orchestrator and analysis containers to core 7. Rate-limited end-to-end scenarios split the client cores between one producer and one consumer container.
- **Identical payloads.** Both harnesses generate the same SplitMix64 payload pool (seed 42); its SHA-256 is checked against a Python reference for every payload type and size before any run, and the run aborts on a mismatch.
- **Explicit partitioning.** Record `i` goes to partition `i % partitions` in both clients, so the Java sticky partitioner and librdkafka's `consistent_random` play no role.
- **Configuration mapping.** In `full-1` every producer and consumer setting was mapped to its counterpart and set explicitly in both clients (`acks`, `linger.ms`, `batch.size`, `max.in.flight`, buffer sizes, retries, timeouts, fetch sizes, CRC checks). Where librdkafka has a knob Java lacks, it was set so it does not interfere (for example `batch.num.messages=1000000`, so the byte limit governs as in Java).
- **Measurement.** Warmup of 10% of the measured count (at least 200k messages), identical windows, monotonic clocks, microsecond HdrHistograms for latency, process CPU from `/proc/self/stat`, RSS and threads sampled every 100 ms, broker CPU from the broker cgroup. Every producer run uses a fresh topic. Consumers of all variants read the same prefilled topic within a rep. Scenario order is shuffled per rep and client order rotates, so slow drift spreads over all clients.
- **Detached and reproducible.** The orchestrator runs in a container on the benchmark host, and every result directory records the exact `docker run` command, effective client configuration, image IDs and host details.

## 2. The first run (`full-1`)

`full-1` ran both clients with Java-equivalent librdkafka settings. Two results stood out because they looked like client defects rather than language differences:

| Scenario | Java | Rust | Rust/Java |
|---|---|---|---|
| produce-baseline (`batch.size` 16 KB) | 569,676 msg/s | 245,245 msg/s | 0.43x |
| produce, 12 partitions | 653,308 | 244,092 | 0.37x |
| produce, 1 partition | 244,848 | 246,764 | 1.01x |
| consume 100 B | 5,765,761 | 343,178 | 0.06x |
| consume 1 KB | 1,220,190 | 1,265,139 | 1.04x |
| consume 10 KB | 138,518 | 296,066 | 2.14x |

The Rust producer stayed at about 245k msg/s regardless of partitions, `linger.ms` (0, 5, 50) or `acks` (0, 1, all), while Java scaled with partitions and dropped to exactly Rust's number with one partition. The Rust consumer was 16x slower than Java at 100 B but 2.14x faster at 10 KB.

## 3. Gap 1: the Rust producer's per-request ceiling

Full write-up: [`diagnosis.md`, Producer](diagnosis.md#producer-the-per-request-ceiling).

**Ruled out (proven).** librdkafka's own `max.in.flight=1000000` (236k), `socket.nagle.disable=true` (235k), `queue.buffering.backpressure.threshold=10` (237k), both together (239k). Per-thread CPU: the librdkafka broker thread at 0.64 cores and the sending thread at 0.28 cores, so the client is not CPU-bound.

**Mechanism (proven).** `debug=protocol` shows every ProduceRequest at 15,621 bytes with a 62-byte, single-partition response: librdkafka 2.12.1 puts one partition's batch in each ProduceRequest, and has no setting to change that. The broker processes one connection's requests strictly in order, and one broker means one connection. 240 MB/s over 15.6 KB requests is about 15.4k requests/s, about 65 us of broker time per request, independent of partitions and in-flight window. Java packs the batches of all partitions for a broker into one request; with one partition it has nothing to pack and lands on the same ceiling.

**Lever (proven).** Bytes per request. The `batch.size` sweep (`full-2`, both clients):

| `batch.size` | Java msg/s | Rust msg/s | Rust/Java |
|---|---|---|---|
| 16 KB | 556,293 | 234,395 | 0.42x |
| 32 KB | 763,452 | 389,263 | 0.51x |
| 64 KB | 973,025 | 584,332 | 0.60x |
| 128 KB | 1,040,683 | 772,578 | 0.74x |
| 256 KB | 993,562 | 919,128 | 0.93x |
| 512 KB | 866,346 | 977,481 | 1.13x |
| 1 MB | 633,585 | 1,011,437 | 1.60x |

Both clients approach about 1 GB/s, roughly what one connection to this broker carries. Java gets there at 128 KB because each of its requests already carries 6 batches; Rust needs 512 KB to 1 MB. The `full-1` baseline imposed Java's 16 KB default on both clients, which is close to librdkafka's worst case; librdkafka's own default is 1,000,000 bytes. Java's decline above 128 KB is explained in section 7.

## 4. Gap 2: the Rust consumer's 1-second fetch backoff

Full write-up: [`diagnosis.md`, Consumer](diagnosis.md#consumer-the-1-second-fetch-backoff).

**Symptom.** At 100 B the Rust consumer used 0.45 cores and its per-second counts came in multiples of about 57k messages with idle gaps.

**Mechanism (proven).** `debug=fetch` logs `Fetch backoff for 1000ms: Local: Queue full`. When a partition's local queue reaches `queued.min.messages` (default 100,000) or `queued.max.messages.kbytes`, librdkafka stops fetching that partition for `fetch.queue.backoff.ms` (default 1000 ms). At 100 B, 100k messages is about 10 MB, which the application drains in milliseconds and then waits for the rest of the second. Java fetches a partition again as soon as its buffered records are consumed.

**Fixes (proven, `diag-1`, 100 B, 6 partitions, 10M messages, single runs):**

| Rust configuration | msg/s | Rust cores |
|---|---|---|
| `poll()`, defaults | 389k | 0.46 |
| `poll()`, `fetch.queue.backoff.ms=10` | 1,480k | 1.85 |
| `poll()`, `fetch.queue.backoff.ms=0` | 948k | 1.51 |
| batch API (`rd_kafka_consume_batch_queue`, 500 per call), defaults | 567k | 0.48 |
| batch API, `fetch.queue.backoff.ms=10` | 2,022k | 1.81 |
| Java `KafkaConsumer.poll()` | 5,612k | 2.62 |

With the backoff fixed, the limits are per-message CPU: `poll()` returns one message per call and saturates the polling thread at about 1.5M msg/s; with the batch API the librdkafka broker thread saturates (0.94 cores at 2.07M msg/s) building one queue entry per fetched message. The batch API plus the shorter backoff became the `rust-tuned` client variant for the rerun.

## 5. The rerun (`full-2`)

`full-2` changed two things: the Rust producer used the `native` profile (only the swept settings and `message.max.bytes` are set; everything else is librdkafka's default), and a third client, `rust-tuned`, ran the consumer with the batch API and `fetch.queue.backoff.ms=10` (its producer is identical to `rust`). It added scenarios that run each client at its own settings: `produce-defaults` (Java 16 KB `batch.size` and 32 MB `buffer.memory`, Rust 1,000,000 bytes and librdkafka defaults) and `produce-best` (Java 128 KB, Rust 1 MB).

### Producer

| Scenario | Java | Rust | Rust/Java |
|---|---|---|---|
| produce-baseline (both 16 KB) | 556,293 | 234,395 | 0.42x |
| produce-defaults (each at its library defaults) | 560,705 | 999,937 | 1.78x |
| produce-best (each at its best `batch.size`) | 1,049,784 | 1,002,387 | 0.95x |
| produce-max-throughput (100 B text, lz4, linger 50, 1 MB) | 2,078,357 | 1,762,507 | 0.85x |

At the same configuration Java wins most producer sweeps (18 of 22 beyond noise, geometric mean Rust/Java 0.60x), because most sweeps keep the 16 KB `batch.size`. At each client's defaults Rust is 1.78x Java; at each client's best `batch.size` they are close, Java slightly ahead (0.95x), with similar CPU per million messages (1.91 s Java, 1.96 s Rust). Section 7 shows that Java's best rises further with OS-sized socket buffers.

### Consumer

| Scenario | Java | Rust | rust-tuned | rust-tuned/Java |
|---|---|---|---|---|
| consume 100 B | 5,753,542 | 369,009 | 2,097,402 | 0.36x |
| consume 1 KB | 1,212,338 | 1,267,157 | 1,348,045 | 1.11x |
| consume 10 KB | 136,514 | 298,846 | 305,385 | 2.24x |
| consume 1 KB lz4 | 930,624 | 1,103,546 | 1,287,634 | 1.38x |
| consume 1 KB zstd | 595,121 | 561,319 | 576,626 | 0.97x (within noise) |
| consume 1 KB, 1 partition | 1,108,748 | 2,006,547 | 2,148,675 | 1.94x |

`rust-tuned` is 5.68x `rust` at 100 B and costs nothing at larger sizes. At 100 B it still trails Java (0.36x) because of the per-message cost on the librdkafka broker thread (section 4). From 1 KB up, Rust leads, most at 10 KB and on a single partition; section 7 explains the Java ceiling.

### Scaling (K client instances in one process, 16 partitions)

| Mode, profile | Java peak | Rust peak | rust-tuned peak |
|---|---|---|---|
| produce 100 B | 14.24M at K=8 | 11.10M at K=16 | - |
| produce 1 KB | 1.85M at K=8 | 1.34M at K=16 | - |
| produce 1 KB text, lz4, linger 20, 1 MB | 4.72M at K=16 | 3.77M at K=16 | - |
| consume 100 B | 41.5M at K=16 | 5.58M at K=16 | 27.4M at K=16 |
| consume 1 KB | 6.58M at K=16 | 11.39M at K=16 | 12.75M at K=16 (13.1 GB/s) |
| consume 1 KB text, lz4 | 6.29M at K=16 | 8.51M at K=16 | 9.26M at K=16 |

The Java producer at 100 B peaks at K=8 and falls to 6.58M at K=16, with p99 514 ms and GC pauses taking 36.9% of the measured window (1.0% at K=8); section 7 explains this. The Rust K=16 100 B producer has 21% CV across reps (7.5M, 11.1M, 11.2M). The fastest consume scaling runs have windows under 2 s (see caveats).

### Resources

- **CPU.** Rust uses a median 1.22x Java's CPU seconds per million messages across K=1 scenarios (from 0.45x at consume 10 KB to 4.15x at consume 100 B).
- **Memory.** Java peak RSS is about 2.4 GB in every scenario, because the harness pre-touches a 2 GiB heap (`-Xms2g -XX:+AlwaysPreTouch`); it reflects the configured heap, not live data. Rust ranges from 9 MB to 1.2 GB in single-instance runs, depending on how much librdkafka queues. In scaling runs Rust RSS grows with K: 4.0 GB at K=16 for 100 B produce and 2.1 GB for 1 KB produce.
- **GC.** Java GC pauses take a median 0.61% of the window in single-instance runs (max 1.11%). The K=16 producer is the exception (36.9%).
- **Threads.** Java 35 to 43, Rust 6.
- **Startup.** Process start to client ready: Java 260 ms, Rust 0.8 ms. Not part of any throughput window.

## 6. Latency trade-offs

The rate-limited `e2e-*` scenarios (1 KB, 6 partitions, `batch.size` 16 KB, fixed rates of 1k, 10k and 50k msg/s, `linger.ms` 0 and 5) measure consumer wall clock minus the producer's embedded wall clock. `full-2` exposed two latency effects, neither of which affects throughput.

### The batch API waited for a full batch (harness artifact, fixed)

Full write-up: [`diagnosis.md`, batch API latency](diagnosis.md#latency-the-batch-api-waits-for-a-full-batch-full-2).

**Symptom.** In `full-2` the `rust-tuned` consumer had e2e p99 of 99.2 ms at 1k/s and 49.6 ms at 10k/s (linger 0), against Java's 0.41 and 0.40 ms and `rust`'s 0.21 and 0.23 ms; its producer side matched `rust` exactly.

**Mechanism (proven arithmetically).** The harness called `rd_kafka_consume_batch_queue(queue, 100 ms, buf, 500)`, which returns only when 500 messages are collected or the 100 ms timeout expires. At 1k/s it always times out, so the wait is uniform over 0 to 100 ms (expected p50/p99 50/99 ms, measured 50.2/99.2); at 10k/s 500 messages take 50 ms (expected 25/49.5, measured 25.2/49.6). Java's `poll()` returns as soon as any record is available.

**Fix (proven).** `BatchSource::next` in `rust-bench/src/consume.rs` now drains whatever is queued with timeout 0, blocks for at most 100 ms for a single message only when nothing is queued, then drains again with timeout 0. Throughput at 100 B is unchanged: 2,124k, 2,139k and 2,118k msg/s over three runs (`diag-1/e2efix-thr100-r*.out`), against 2,097k in `full-2`. Latency, one rep per scenario (`diag-1/e2efix-1`), consumer e2e p50/p99/p99.9 in ms:

| Scenario | `rust-tuned` before (`full-2`) | `rust-tuned` after (`e2efix-1`) | `rust` (`full-2`) |
|---|---|---|---|
| linger 0, 1k/s | 50.2 / 99.2 / 100 | 0.125 / 0.202 / 0.251 | 0.13 / 0.21 / 0.25 |
| linger 0, 10k/s | 25.2 / 49.6 / 50.05 | 0.118 / 0.232 / 5.18 | 0.12 / 0.23 / 2.67 |
| linger 0, 50k/s | 18.4 / 32.6 / 36.0 | 13.5 / 25.7 / 29.6 | 13.7 / 26.3 / 29.1 |
| linger 5, 1k/s | 55.2 / 104 / 105 | 5.13 / 5.21 / 5.26 | 5.13 / 5.21 / 5.25 |
| linger 5, 10k/s | 28.1 / 54.3 / 55.2 | 3.10 / 5.79 / 5.97 | 3.11 / 5.80 / 5.97 |
| linger 5, 50k/s | 6.37 / 12.1 / 12.9 | 1.42 / 2.42 / 2.53 | 1.43 / 2.43 / 2.55 |

After the fix `rust-tuned` matches `rust` everywhere. The remaining outliers are on the producer: at linger 0, 10k/s the producer's send-to-ack p99.9 in the same run was 5.04 ms, and at linger 0, 50k/s its send-to-ack p99 was 25.6 ms, which is the next effect. **The `full-2` e2e numbers for `rust-tuned` are superseded** by `e2efix-1` and by `e2e-2` below.

### librdkafka's unlimited in-flight window at linger 0

Full write-up: [`diagnosis.md`, in-flight window](diagnosis.md#latency-librdkafkas-unlimited-in-flight-window-at-linger-0-full-2).

**Symptom (proven).** Moving the Rust producer from `full-1`'s Java-equivalent settings (`max.in.flight=5`) to the `native` profile (librdkafka default `max.in.flight=1000000`) left throughput unchanged but, at linger 0 and 50k/s, raised Rust producer send-to-ack from 0.54/1.81 ms (p50/p99) to 13.6/26.2 ms and consumer e2e from 0.66/1.94 ms to 13.7/26.3 ms. Java stayed at 0.65/0.94 ms e2e. At 1k/s nothing changed; at 10k/s only the send-to-ack p99.9 moved (0.26 to 2.5 ms); at linger 5 and 50k/s the profiles match.

**Single-knob experiment (proven, `diag-1/e2e50k-*.out`, single runs, linger 0, 50k/s for 30 s):**

| Rust producer configuration | msg/s | send-to-ack p50 | p99 | p99.9 |
|---|---|---|---|---|
| `native` (`max.in.flight=1000000`) | 49,958 | 14.1 ms | 26.4 ms | 28.9 ms |
| `native` + `max.in.flight=5` | 49,997 | 0.47 ms | 1.28 ms | 2.05 ms |
| Java-equivalent settings | 49,990 | 0.48 ms | 1.34 ms | 2.20 ms |

`max.in.flight=5` alone reproduces the Java-equivalent profile, so the in-flight window is the knob.

**Mechanism (inferred).** With linger 0, librdkafka sends as soon as a partition has data. With a window of 5, new messages wait for a slot and coalesce, so requests carry many messages. With an unlimited window each message or two becomes its own request, and those small requests queue behind each other on the broker's serial per-connection processing (the same per-request cost as in section 3). A `debug=protocol` run slowed the client too much to observe the steady state, so this is not directly confirmed.

**Practical settings.** Large `batch.size` for throughput; `max.in.flight` around 5 when latency at low linger and moderate to high rates matters. Here the two do not conflict.

### The rerun of the latency scenarios (`e2e-2`)

`e2e-2` reruns all `e2e-*` scenarios, 3 reps, with four clients: `java`, `rust` (`native` profile), `rust-tuned` (with the fixed batch API) and `rust-lowlat` (`native` profile plus `max.in.flight=5` on the producer, consumer identical to `rust`). It replaces the `full-2` e2e table for all clients. Consumer e2e p50 / p99 / p99.9 in ms, medians of 3 reps; every run reached its target rate and none was truncated:

| linger.ms | rate | Java | `rust` | `rust-tuned` | `rust-lowlat` | producer send-to-ack p99 (Java / `rust` / `rust-lowlat`) |
|---|---|---|---|---|---|---|
| 0 | 1,000 | 0.16 / 0.39 / 0.69 | 0.13 / 0.20 / 0.24 | 0.13 / 0.21 / 0.25 | 0.13 / 0.21 / 0.25 | 0.22 / 0.14 / 0.15 |
| 0 | 10,000 | 0.14 / 0.35 / 2.93 | 0.12 / 0.23 / 1.95 | 0.12 / 0.23 / 3.63 | 0.12 / 0.23 / 0.76 | 0.18 / 0.12 / 0.12 |
| 0 | 50,000 | 0.64 / 0.93 / 3.64 | 17.6 / 32.9 / 36.1 | 15.2 / 28.9 / 32.4 | 0.66 / 1.93 / 2.97 | 0.55 / 32.8 / 1.80 |
| 5 | 1,000 | 3.32 / 5.57 / 5.86 | 5.13 / 5.20 / 5.25 | 5.13 / 5.21 / 5.26 | 5.13 / 5.21 / 5.25 | 5.29 / 5.15 / 5.15 |
| 5 | 10,000 | 2.99 / 5.97 / 6.12 | 3.10 / 5.79 / 5.93 | 3.10 / 5.79 / 5.96 | 3.10 / 5.79 / 5.95 | 5.75 / 5.67 / 5.67 |
| 5 | 50,000 | 1.36 / 2.25 / 2.40 | 1.43 / 2.42 / 2.53 | 1.42 / 2.42 / 2.53 | 1.43 / 2.43 / 2.54 | 2.04 / 2.32 / 2.32 |

- **The batch API fix holds over 3 reps.** `rust-tuned` matches `rust` within 0.01 ms at p50 and p99 in every scenario except linger 0 at 50k/s, where both sit in the unlimited-window regime below.
- **The in-flight window is the only large gap.** At linger 0 and 50k/s, `rust` is at 17.6 / 32.9 ms and its producer's send-to-ack p99 is 32.8 ms; `rust-lowlat` is at 0.66 / 1.93 ms, next to Java's 0.64 / 0.93 ms. The remaining p99 difference against Java (1.93 vs 0.93 ms) matches the producer side (1.80 vs 0.55 ms).
- **Everywhere else the clients are close.** At 1k/s and 10k/s with linger 0, all Rust variants have a lower p99 than Java (0.20 to 0.23 ms against 0.35 to 0.39 ms). At linger 5, p99 is within 8% between Java and every Rust variant (5.57 vs 5.20 ms at 1k/s, 5.97 vs 5.79 at 10k/s, 2.25 vs 2.42 at 50k/s). At linger 5 and 1k/s Java's p50 is lower (3.32 vs 5.13 ms): Java's median send-to-ack is 2.79 ms against librdkafka's 5.10 ms, which suggests Java sends a lone record before `linger.ms` has fully elapsed more often than librdkafka (inferred, not investigated).
- **Drift against `full-2` (proven, same configuration).** Most cells agree within a few percent. The unlimited-window cell moved: `rust` at linger 0 and 50k/s went from 13.7 / 26.3 ms in `full-2` to 17.6 / 32.9 ms in `e2e-2`. That regime is a queue that builds on the broker's serial per-connection processing, so it is sensitive to small changes in request timing; `rust-lowlat` in the same runs is stable. The p99.9 values at 10k/s (Java 1.88 to 2.93 ms, `rust` 2.67 to 1.95 ms) also vary between runs, as tail percentiles do with a few reps.

## 7. The Java side

Full write-up: [`java-diagnosis.md`](java-diagnosis.md). Raw outputs in `results/diag-java/`. Throughputs there are single runs unless several values are listed.

### Producer slows down as `batch.size` grows past 128 KB

**Ruled out (proven).** `buffer.memory` 1 GB (623k at 1 MB; the pool is exhausted because the sender drains slowly), buffer pooling (every batch buffer is exactly `batch.size`, so allocation is pooled), GC (2 young collections per run), broker CPU per byte (1.02 s/GB at 1 MB vs 1.08 at 128 KB), the in-flight window (1 of 5 requests in flight).

**Mechanism (proven).** The Java sender packs one batch per partition into a request, so with 6 partitions a request is 6 x `batch.size` (6.29 MB at 1 MB). Batch buffers are heap `ByteBuffer`s, and for heap buffers the JDK's `IOUtil.write` copies all remaining bytes into a temporary direct buffer on every write, then discards what the kernel did not accept. The socket send buffer is fixed by `send.buffer.bytes` (default 128 KB), so each non-blocking write accepts only about 298 KB (JFR `jdk.SocketWrite` p50 and p90). A 6.29 MB request takes about 22 writes and copies 69.6 MB, an 11.1x amplification (1.9x at 128 KB). The measured 296 us per write matches the predicted copy time, and the network thread's user CPU per GB rises from 0.44 s at 128 KB to 1.02 s at 1 MB while kernel time per byte stays the same.

**Why that caps throughput (inferred).** A second request only starts once the first is fully written, and the broker processes one connection's requests in order, so the broker waits while the sender re-copies the tail of a large request.

**Fix (proven).**

| `batch.size` | Java default | Java `send.buffer.bytes=-1` | Rust `native` (`diag-1`) |
|---|---|---|---|
| 16 KB | 546k | 556k | 233k |
| 128 KB | 1,038k | 1,122k | 768k |
| 256 KB | 1,009k | 1,118k | 903k |
| 512 KB | 857k | 1,104k | 991k |
| 1 MB | 630k | 875k (888k, 875k, 862k) | 995k |

`max.request.size=1100000` (one batch per request, like librdkafka) gives 922k at 1 MB, and both fixes together 984k. librdkafka's default `socket.send.buffer.bytes=0` already leaves the socket to OS autotuning, so with both clients on OS-sized buffers Java's best producer result is 1.12M msg/s against Rust's 1.00M.

### One `KafkaConsumer` stops at about 1.3 to 1.6 GB/s

**Ruled out (proven).** `max.poll.records=5000` (1,227k), `check.crcs=false` (1,400 MB/s at 10 KB), `max.partition.fetch.bytes` 8 MB (1,374 MB/s at 10 KB), the new consumer protocol `group.protocol=consumer` (1,195k and 1,175k), GC (6 young collections, about 20 ms per run).

**Mechanism (proven).** With the classic consumer, the thread that calls `poll()` also does the socket reads, and it runs at 0.95 to 0.98 cores in every configuration. Its JFR CPU profile: 51% (1 KB) and 58% (10 KB) in the socket read path, of which 39% and 43% kernel reads and 10% and 13% the JDK's copy from its temporary direct buffer into the heap; 30% and 29% record parsing and value copying. The receive buffer is fixed by `receive.buffer.bytes` (default 64 KB), so a fetch response arrives in about 56 KB reads. librdkafka reads on its broker thread with an OS-autotuned buffer and hands the application pointers into its own buffers.

**Why the new consumer does not help (inferred from source and thread CPU).** With `group.protocol=consumer` the background thread takes 0.66 cores and the application thread 0.37, still about one core in total. Both implementations fetch only partitions with no buffered data and keep one fetch in flight per broker, so fetching the next data does not overlap with parsing the current data.

**Fix (proven).**

| Case | Java default | Java `receive.buffer.bytes=-1` | Rust |
|---|---|---|---|
| 1 KB, 6 partitions | 1,229k | 1,427k | 1,445k |
| 10 KB, 6 partitions | 1,359 MB/s | 1,595 MB/s | 3,414 MB/s |
| 1 KB, 1 partition | 1,080k | 1,418k (plus 8 MB `max.partition.fetch.bytes`) | 2,087k |

At 1 KB on 6 partitions this brings Java level with Rust. What remains is architectural: one polling thread does all the per-byte work, so at 10 KB Rust stays 2.1x ahead per instance. The lever is more consumer instances.

### 16 producer instances in one JVM deliver less than 8

**Mechanism (proven).** GC collapse. At K=16 a class histogram shows 2.27M records waiting in the accumulators, each with about 190 B of per-record objects (`AppendCallbacks`, `FutureRecordMetadata`, `RecordHeaders` and its `ArrayList`, the harness callback, `ProducerBatch$Thunk`) for a 100 B record: 436 MB of objects next to 273 MB of `byte[]`, in a 2 GB heap. They live for 300 ms or more, survive young collections and fill the old generation; the GC log shows evacuation failures and 6 full GCs of 105 to 117 ms each, and the GC threads use 4.79 cores.

**Trigger (inferred).** 32 busy threads (one sending thread and one network thread per instance) on 16 vCPUs. Once the senders fall behind, the accumulators fill to `buffer.memory` and GC takes about a third of the CPU, which makes the senders fall further behind.

**Fix (proven, 50M messages, 100 B, 16 partitions):**

| Configuration | msg/s | send-to-ack p50 / p99 | GC time |
|---|---|---|---|
| K=8, harness settings | 12.9M | 0.3 / 22 ms | 65 ms |
| K=16, harness settings | 6.4M, 6.8M | 337 / 540 ms | 2,781 ms |
| K=16, 8 GB heap | 9.5M | 183 / 364 ms | 499 ms |
| K=16, `buffer.memory` 64 MB total | 9.9M | 43 / 99 ms | 308 ms |
| K=16, `buffer.memory` 32 MB total | 10.1M | 16 / 54 ms | 161 ms |

Bounding the backlog brings Java to 91% of Rust's 11.1M at K=16. The harness splits its 256 MB `buffer.memory` across instances (16 MB each at K=16); that is a harness setting, and Java's own default is 32 MB per producer, so the effective fix is well below Java's default.

### gzip

**Mechanism (proven).** The clients interpret `batch.size` differently for compressed data: Java closes a batch when its estimated compressed size reaches `batch.size`, librdkafka limits the uncompressed bytes. At 16 KB a Java batch holds about 43 KB of input and a librdkafka batch 16 KB; zlib is slower per byte on the longer input and compresses better. Both clients use zlib's default level.

| Client, `batch.size` | input per batch | msg/s | compression ratio on the broker |
|---|---|---|---|
| Java 16 KB | about 43 KB | 33.9k | 5.39 |
| Rust 16 KB | 16 KB | 46.4k | 4.70 |
| Rust 88 KB | 88 KB | 27.5k | 5.71 |
| Java 3,000 B | about 8 KB | 48.2k | 4.15 |

Matching the input per batch reverses the gap in both directions, so neither zlib binding is faster. At level 1, Java reaches 145k and Rust 109k msg/s. The `full-2` gzip result (Rust 1.34x, Java runs truncated at the 120 s cap) is a speed-versus-ratio trade-off, not a client efficiency result: Java wrote 13% fewer bytes to the broker.

## 8. What is left, and why

- **Rust consumer at 100 B (0.36x Java per instance, 0.66x at the K=16 peak).** librdkafka creates one queue entry per fetched message on its broker thread, which saturates at about 2.1M msg/s per instance; the batch API only removes the per-call overhead on the application side. Java parses records straight out of the fetch buffer on the polling thread. This is a design difference in librdkafka, not a setting. More instances (more broker threads and connections) is the only lever measured.
- **Java consumer at 10 KB and single-partition (Rust 2.1x and 1.5x after the Java fix).** One polling thread does all the socket reads, parsing and value copies; neither the classic nor the new consumer protocol spreads that work over more cores. More instances is the lever.
- **Rust producer at small `batch.size`.** librdkafka's one-partition-per-request design bounds it by the broker's per-request cost; the fix is its own default `batch.size`, which it already ships with.
- **Java producer at large `batch.size`.** The heap-buffer copy on partial writes is reduced by OS-sized socket buffers but not removed (5.1x amplification remains at 1 MB); keeping requests near `batch.size` via `max.request.size` or keeping `batch.size` at or below 256 KB avoids it.
- **Java producer scaling beyond the core count.** Per-record heap objects make a large backlog expensive; bounding `buffer.memory` fixes the collapse but K=16 still stays below K=8's 12.9M to 14.2M on this host.
- **Rust producer K=16 at 100 B** has 21% CV across reps (7.5M, 11.1M, 11.2M); not investigated.
- **Latency.** Nothing is left that is not a setting. The `rust-tuned` wait was a harness artifact and is fixed (confirmed over 3 reps in `e2e-2`). librdkafka's default unlimited in-flight window costs 17.6 / 32.9 ms at linger 0 and 50k/s; `max.in.flight=5` brings Rust to 0.66 / 1.93 ms against Java's 0.64 / 0.93 ms. The remaining factor of about 2 at p99 in that one cell is on the producer side and was not investigated.

## 9. Takeaways

- Most of the large gaps in the first run came from configuration, not language: a Java default imposed on librdkafka (`batch.size` 16 KB), a librdkafka default that stalls fast consumers (`fetch.queue.backoff.ms=1000`), Java's fixed socket buffer sizes, a large per-instance backlog, and a setting (`batch.size` under compression) that means different things in the two clients.
- Mapping settings one to one is necessary for a fair bench but not sufficient: the same property name can drive different internal behaviour. Each client should also be measured at its own defaults and at its own best settings.
- With both clients tuned, single-instance producer throughput is close (Java 1.12M with OS-sized socket buffers, Rust 1.00M). Rust leads on consumer throughput from 1 KB up and at 10 KB by 2.1x per instance; Java leads on 100 B consumption by about 2.7x per instance and on producer scaling up to the core count.
- Rust's advantages are memory footprint (tens to hundreds of MB against a 2 GB heap), thread count, startup time and the absence of GC; its median CPU per million messages is 1.22x Java's.
- For latency, `max.in.flight` around 5 matters more than the language at linger 0: with it, Rust's worst e2e p99 across all six scenarios is 5.8 ms against Java's 6.0 ms; without it, Rust reaches 32.9 ms at linger 0 and 50k/s. At linger 5 the two clients' e2e p99 are within 8% of each other.

## 10. Caveats

- **Single broker, replication factor 1, log dirs on tmpfs.** `acks=all` means one replica, and there is no disk or replication cost. Results describe client overhead against a fast local broker, not production cluster behaviour. The per-connection serial processing that shapes both producers' results is amplified by having one broker, hence one connection per client instance.
- **No real network.** Clients and broker share one host and communicate over a Docker bridge: no network latency or bandwidth limit, which magnifies client-side CPU differences.
- **Unpinned host processes.** The host also runs unrelated containers and non-container processes (security and monitoring agents, interactive sessions). They are not pinned, so the kernel may schedule them on broker or client cores; that time is not attributed to any measured cgroup.
- **Scaling is host-bound.** Scaling runs share one broker with 7 physical cores; peak numbers describe this host, not the clients' limits on a cluster.
- **Short windows.** The 16 GiB tmpfs caps topic size, so the fastest consume runs (10 KB K=1 Rust, and K=8 and K=16 consume scaling) have windows under 2 s, some under 1 s. Startup transients and librdkafka read-ahead weigh more in such windows; treat those numbers as upper bounds.
- **Client API differences that cannot be equalized.** Java batches per partition in its `RecordAccumulator` and sends from its own network thread; librdkafka queues messages centrally and uses per-broker threads, so `linger.ms`, `batch.size` and `max.in.flight` do not map to identical internal behaviour. librdkafka copies payloads on produce; Java serializes into the batch buffer. Java's `buffer.memory` blocks the caller, while the Rust harness polls and retries on `QueueFull`.
- **`buffer.memory` 256 MB is a harness setting.** Java's default is 32 MB per producer. Single-instance runs give one producer 256 MB; scaling runs split it across instances. `produce-defaults` uses Java's 32 MB default.
- **`batch.size` under compression is not like for like.** Java bounds the compressed size of a batch, librdkafka the uncompressed size, so equal settings compress different amounts of data per call (section 7, gzip).
- **`produce-best` in `full-2` used Java's default socket buffers.** librdkafka leaves socket buffers to the OS by default; Java with `send.buffer.bytes=-1` reaches 1.12M msg/s at 128 KB, above the `produce-best` figure of 1.05M. That result is a single-run diagnosis number, not part of a 3-rep matrix run.
- **Memory numbers.** Java RSS includes the pre-touched 2 GiB heap, so it reflects the configured heap. Rust's 9 MB to 1.2 GB range is for single-instance runs; in scaling runs Rust reached 4.0 GB at K=16 (100 B produce).
- **JIT and warmup.** Java numbers include JIT and GC behaviour after a warmup of at least 200k messages; JIT compilation can continue inside the window. Rust has no warmup effects beyond connection setup.
- **acks=0 latency** is time to local completion, not broker receipt.
- **Consumers read hot data.** Topics are written moments before being read, so all data is in memory; fetch cost is purely broker CPU and memory copy.
- **Significance.** Min/max range overlap with 3 reps is a coarse test; small real differences can be reported as within noise. Diagnosis experiments are mostly single runs; treat differences under about 3% as noise.
