# Diagnosis of the Java client's weak spots

`full-2` left four results where the Java client (`kafka-clients` 4.3.1, Temurin 25) trails the Rust client or its own best case: the producer gets slower as `batch.size` grows past 128 KB, one `KafkaConsumer` stops at about 1.3 to 1.4 GB/s, 16 producer instances in one JVM deliver less than half of what 8 deliver, and gzip is slower than librdkafka's. This document records how each was diagnosed and what fixes it, in the same style as `docs/diagnosis.md`. Raw outputs are in `results/diag-java/` (not committed; regenerate with the commands at the end).

All experiments ran on the same host, broker and pinning as `full-2` (broker on cores 0-6, client on cores 8-15,24-31, analysis sidecars on 7,23), with a fresh topic per run. Client metrics come from the client's own JMX MBeans, sampled once per second by a sidecar container with `metrics.sample.window.ms=1000`; per-thread CPU comes from `/proc/1/task/*/stat`; profiles come from JDK Flight Recorder (`settings=profile`, plus the JDK 25 CPU-time sampler `jdk.CPUTimeSample` where noted). Throughputs are single runs unless several values are listed, so treat differences under about 3% as noise. Each finding is labelled **proven** (measured directly or read from source) or **inferred** (consistent with the measurements, not observed directly).

## Producer: throughput falls as `batch.size` grows past 128 KB

### Symptom

1 KB random payload, 6 partitions, `acks=1`, `linger.ms=5`, `max.in.flight=5`, `buffer.memory=256 MB`: Java peaks at about 1.04M msg/s with 128 KB batches and falls to about 630k msg/s at 1 MB, while librdkafka keeps climbing to 1.0M msg/s at 1 MB.

### Hypotheses ruled out

| Experiment (1 MB batches unless stated) | Java msg/s | Conclusion |
|---|---|---|
| Default settings | 626k, 630k, 601k, 630k | reference |
| `buffer.memory` 1 GB (6 GB heap) | 623k | not accumulator memory pressure: the pool is exhausted (`buffer-available-bytes` 0, `bufferpool-wait-ratio` 0.66 to 0.70) because the sender drains slowly, so blocking in `send()` is a symptom |
| Buffer pooling | n/a | every batch buffer is exactly `batch.size` (`RecordAccumulator.java:329` allocates `max(batchSize, estimate)`), so `BufferPool.allocate` takes the pooled path (`BufferPool.java:124`); no non-pooled large allocations |
| GC | 2 young collections, about 30 ms per run | not GC |
| Broker cost per byte | 1.02 s broker CPU per GB at 1 MB, 1.08 s at 128 KB | the broker does not work harder for large requests |
| In-flight window | `requests-in-flight` 1 of 5 allowed | the window is not the limit; something keeps it from filling |

### Mechanism

**Request size (proven).** The sender packs one batch per partition into each ProduceRequest, up to `max.request.size` (`Sender.java:418` passes `maxRequestSize` to `RecordAccumulator.drain`). With 6 partitions a request is 6 x `batch.size`:

| `batch.size` | msg/s | request size | requests/s | request latency | `requests-in-flight` | sender `io-ratio` |
|---|---|---|---|---|---|---|
| 16 KB | 546k | 93 KB | 6,026 | 0.76 ms | 5 | 0.14 |
| 128 KB | 1,038k | 782 KB | 1,390 | 1.16 ms | 2 | 0.33 |
| 256 KB | 1,009k | 1.57 MB | 669 | 2.25 ms | 2 | 0.43 |
| 512 KB | 857k | 3.15 MB | 285 | 4.91 ms | 1 | 0.52 |
| 1 MB | 630k | 6.29 MB | 104 | 12.6 ms | 1 | 0.64 |

Capping the request at one batch (`max.request.size=1100000`, so every request carries a single 1 MB batch like librdkafka's) raises 1 MB batches from 630k to 929k and 915k msg/s with the same batch size and the same bytes on the wire.

**Why large requests are expensive (proven from source, cost measured).** A request is written with a gathering write of the batch buffers (`ByteBufferSend.java:57`, `PlaintextTransportLayer.java:150`), and those buffers are heap buffers (`BufferPool.java:240`, `ByteBuffer.allocate`). For a heap buffer, the JDK's `IOUtil.write` (JDK 25 `sun/nio/ch/IOUtil.java:168-235`) first copies *all remaining bytes* of every buffer into temporary direct buffers, then calls `writev`, and discards whatever the kernel did not accept. The socket is non-blocking and its send buffer is fixed by `send.buffer.bytes` (default 131,072, `ProducerConfig.java:408`), so each write accepts only about 298 KB (JFR `jdk.SocketWrite` events: p50 and p90 exactly 297,866 bytes). A 6.29 MB request therefore takes about 22 writes, and each one re-copies the whole unsent tail:

| Case | request | bytes per write | writes per request | bytes copied per request | copy amplification | measured time per write |
|---|---|---|---|---|---|---|
| 128 KB batches | 0.78 MB | 298 KB | 3 | 1.45 MB | 1.9x | 73 us |
| 1 MB batches | 6.29 MB | 298 KB | 22 | 69.6 MB | 11.1x | 296 us |
| 1 MB, `send.buffer.bytes=-1` | 6.29 MB | 692 KB | 10 | 31.8 MB | 5.1x | 349 us |
| 1 MB, `max.request.size=1100000` | 1.05 MB | 298 KB | 4 | 2.41 MB | 2.3x | 91 us |

At 1 MB batches the predicted copy per write (69.6 MB / 22 = 3.2 MB, about 290 us at 11 GB/s) matches the measured 296 us. Per-thread CPU agrees: the `kafka-producer-network-thread` spends 0.66 cores in user space and 0.15 in the kernel at 645 MB/s (1.02 user-seconds per GB), against 0.47 user and 0.25 kernel at 1,080 MB/s with 128 KB batches (0.44 user-seconds per GB). Kernel time per byte is the same; the extra cost is user-space copying. (JFR's CPU-time sampler attributes these samples to `SocketDispatcher.writev0`; the user/kernel split and the per-write timing are the more reliable evidence.)

**Why that caps throughput (inferred).** `InFlightRequests.canSendMore` (`InFlightRequests.java:96-99`) only lets a second request start once the first has been written completely, and the broker processes one connection's requests one at a time. While the sender thread is re-copying the tail of a 6 MB request, the next request cannot start and the broker waits for bytes. `requests-in-flight` stays at 1 and request latency grows faster than request size (1.49 ms/MB at 128 KB, 2.0 ms/MB at 1 MB).

### Fixes measured (1 KB, 6 partitions)

| `batch.size` | default | `send.buffer.bytes=-1` (OS autotuning, up to `tcp_wmem` max 4 MB) |
|---|---|---|
| 16 KB | 546k | 556k |
| 128 KB | 1,038k | 1,122k |
| 256 KB | 1,009k | 1,118k |
| 512 KB | 857k | 1,104k |
| 1 MB | 630k | 875k (888k, 875k, 862k) |
| 1 MB + `max.request.size=1100000` | 922k (929k, 915k) | 984k |

- `send.buffer.bytes=-1` flattens the curve: Java stays at 1.10M to 1.12M msg/s from 128 KB to 512 KB and its best result rises from 1.04M to 1.12M msg/s. librdkafka's default `socket.send.buffer.bytes=0` already leaves the socket to OS autotuning, so this is also a fairness note on the `produce-best` comparison.
- Keeping requests small also works: `max.request.size` close to `batch.size`, or `batch.size` at or below 256 KB with 6 partitions.
- `buffer.memory`, `max.in.flight` and heap size do not matter for this bottleneck.

## Consumer: one `KafkaConsumer` stops at about 1.3 to 1.6 GB/s

### Symptom

Java consumes 1.22M msg/s at 1 KB (1.25 GB/s) and 133k msg/s at 10 KB (1.36 GB/s), and 1.08M msg/s from a single partition; Rust reaches 1.44M, 333k (3.4 GB/s) and 2.09M on the same topics.

### Hypotheses ruled out

| Experiment | Java msg/s | Conclusion |
|---|---|---|
| 1 KB, 6 partitions, defaults | 1,218k, 1,233k, 1,229k | reference |
| `max.poll.records=5000` | 1,227k | not the per-`poll()` batch size |
| `check.crcs=false` (`diag-1`) | 1,400 MB/s at 10 KB | not CRC |
| `max.partition.fetch.bytes` 8 MB (`diag-1`, 10 KB) | 1,374 MB/s | not fetch round trips on 6 partitions |
| `group.protocol=consumer` (the new consumer, network I/O on a background thread) | 1,195k, 1,175k | moving I/O off the polling thread does not help (see below) |
| GC | 6 young collections, about 20 ms per run | not GC |

### Mechanism

**One thread does all the per-byte work (proven).** With the classic consumer, the thread that calls `poll()` also performs the socket reads. Its CPU is 0.95 to 0.98 cores in every configuration (1 KB, 10 KB, 1 partition, with or without the fixes below); the process total is about 1.5 cores, the rest being JIT and GC threads. JFR CPU-time profile of that thread:

| Share of the polling thread | 1 KB | 10 KB |
|---|---|---|
| Socket read path (`NetworkReceive.readFrom`), total | 51% | 58% |
| of which `SocketDispatcher.read0` (kernel read) | 39% | 43% |
| of which `IOUtil.read` (JDK copy from its temporary direct buffer into the heap receive buffer) | 10% | 13% |
| Record parsing and value copy (`CompletedFetch.fetchRecords`, `parseRecord`, `nextFetchedRecord`, `DefaultRecord.readFrom`) | 30% | 29% |
| Harness loop | 6% | 1% |

`io-ratio` from the client's own metrics agrees (0.56 at 1 KB, 0.59 at 10 KB). The socket receive buffer is fixed at `receive.buffer.bytes` (default 65,536, `ConsumerConfig.java:491-493`), so a 6 MB fetch response arrives in about 56 KB reads (`select-rate` 22.5k/s at 1.25 GB/s). librdkafka reads on its broker thread with an OS-autotuned buffer and hands the application pointers into its own buffers, so its application thread does neither the socket reads nor a value copy.

**Why the new consumer does not help (inferred from source and thread CPU).** With `group.protocol=consumer`, the background thread takes 0.66 cores and the application thread 0.37 cores, together about the same one core, and throughput does not change. Both implementations only fetch partitions that have no buffered data (`AbstractFetch.java:346-352, 429-436`) and keep at most one fetch in flight per broker (`AbstractFetch.java:458`), so the fetch of the next data does not overlap with parsing the current data; the work is split across two threads but still runs in sequence.

### Fixes measured

| Case | defaults | `receive.buffer.bytes=-1` | other | Rust |
|---|---|---|---|---|
| 1 KB, 6 partitions | 1,229k (1.26 GB/s) | 1,427k (1,413k, 1,427k, 1,432k) | 4 MB receive buffer: 1,411k; new consumer + autotuned buffer: 1,384k | 1,445k |
| 10 KB, 6 partitions | 133k (1,359 MB/s) | 155k (1,595 MB/s) | new consumer + autotuned buffer: 155k | 333k (3,414 MB/s) |
| 1 KB, 1 partition | 1,080k | 1,211k | 8 MB `max.partition.fetch.bytes`: 1,200k; both: 1,418k | 2,087k |

- `receive.buffer.bytes=-1` halves the number of reads (`select-rate` 22.5k/s to 10.3k/s) and adds 16 to 17%. At 1 KB with 6 partitions it brings Java level with Rust (1.43M vs 1.44M).
- With a single partition, each 1 MB fetch is one round trip (1,076 fetches/s); an 8 MB `max.partition.fetch.bytes` together with the autotuned buffer adds 31% (1.08M to 1.42M).
- What remains is architectural: the polling thread is saturated at about 1.4 to 1.6 GB/s, and a single `KafkaConsumer` has no way to spread per-byte work over more cores. At 10 KB Rust stays 2.1x ahead. The lever is more consumer instances (`full-2` peak consume 1 KB: Java 6.6M msg/s with 16 instances).

## Producer scaling: 16 instances in one JVM deliver less than 8

### Symptom

100 B messages, 16 partitions, 16 KB batches, `buffer.memory` 256 MB split across instances (a harness setting; the Java default is 32 MB per producer), default 2 GB heap: 14.2M msg/s at K=8 falls to 6.6M at K=16 with p99 514 ms, while Rust keeps scaling to 11.1M. `full-2` records 6.1 s of GC time in a 16.6 s window at K=16, against 60 ms at K=8.

### Mechanism

**GC collapse (proven).**

| Configuration (50M messages) | msg/s | send-to-ack p50 / p99 | young GCs | full GCs | GC time |
|---|---|---|---|---|---|
| K=8, harness defaults | 12.9M | 0.3 / 22 ms | 21 | 0 | 65 ms |
| K=12, harness defaults | 7.9M | 131 / 467 ms | 69 | 1 | 1,740 ms |
| K=16, harness defaults | 6.4M, 6.8M | 337 / 540 ms | 82 (plus 3 concurrent cycles) | 6 | 2,781 ms |
| K=16, 8 GB heap | 9.5M | 183 / 364 ms | 9 | 0 | 499 ms |
| K=16, `buffer.memory` 64 MB total | 9.9M | 43 / 99 ms | 23 | 0 | 308 ms |
| K=16, `buffer.memory` 32 MB total | 10.1M | 16 / 54 ms | 22 | 0 | 161 ms |

At K=16 with defaults, the GC log shows the heap full after every young collection (for example 1913M->1846M of 2048M), evacuation failures, and a full GC about once a second, each 105 to 117 ms, compacting from 2045M to about 700M. The GC worker threads use 4.79 cores, more than the 16 application threads (4.50) or the 16 sender threads (3.92). At K=8 young collections go from 1440M to 214M in 1.3 ms.

**What is live (proven).** A class histogram taken during the K=16 run shows 2.27M records waiting in the accumulator, each carrying five or six objects: `KafkaProducer$AppendCallbacks` (48 B), `FutureRecordMetadata` (48 B), `RecordHeaders` (24 B) and its `ArrayList` (24 B), the harness's `LatencyCallback` (24 B) and `ProducerBatch$Thunk` (24 B). That is about 190 B of per-record objects for every 100 B record: 436 MB, next to 273 MB of `byte[]` (the 256 MB of batch buffers and the 64 MB payload pool). These objects live for the 300 ms or more a record waits, so they survive young collections and are copied and promoted, and the old generation fills.

**Trigger (inferred).** At K=16 the JVM has 32 busy threads (one sending thread and one `kafka-producer-network-thread` per instance) on 16 vCPUs and uses 14.3 to 15.5 cores. Once the sender threads fall behind, the accumulators fill to `buffer.memory`, the live set grows to about 700 MB, GC takes about a third of the CPU, and the senders fall further behind. At K=8 (12.4 cores used) the queues stay nearly empty and almost nothing survives a young collection.

### Fixes

- Bound the backlog: `buffer.memory` 32 MB total (2 MB per instance) gives 10.1M msg/s at K=16 with p99 54 ms, 1.5x the default and 91% of Rust's 11.1M. A larger heap (8 GB) gives 9.5M but keeps p50 at 183 ms because the queues stay full.
- Do not oversubscribe: K=8 on this host (12.9M to 14.2M msg/s) is the Java peak. Each Java producer instance needs two busy threads; each librdkafka instance keeps its per-message work on its broker thread and allocates nothing per record on a garbage-collected heap.

## Producer: gzip

### Symptom

1 KB text, 6 partitions, `batch.size` 16 KB, `linger.ms` 5, default level: Java 33.9k msg/s vs Rust 46.4k (0.73x). Java's application thread is at 1.00 core, 96% of it in `java.util.zip.Deflater.deflateBytesBytes` (native zlib); librdkafka's broker thread is at 1.00 core. Both clients use zlib's default level (6): Kafka's `GzipCompression` defaults to `Deflater.DEFAULT_COMPRESSION` and wraps it in a 16 KB `BufferedOutputStream` over an 8 KB deflater buffer (`GzipCompression.java:54`), librdkafka's `compression.level=-1` maps to the same zlib default.

### Mechanism (proven)

The two clients interpret `batch.size` differently. Java closes a batch when its *estimated compressed* size reaches `batch.size` (`MemoryRecordsBuilder.hasRoomFor`, `MemoryRecordsBuilder.java:849-868`, with `estimatedBytesWritten()` scaled by the observed compression ratio); librdkafka limits the *uncompressed* message bytes. At the same 16 KB setting, a Java batch holds about 43 KB of input (`batch-size-avg` 7.9 KB compressed at `compression-rate-avg` 0.18), a librdkafka batch about 16 KB. zlib is slower per byte on the longer input (more history to search) and compresses better:

| Client, `batch.size` | uncompressed input per batch | msg/s | compression ratio on the broker |
|---|---|---|---|
| Java 16 KB (default) | about 43 KB | 33.9k | 5.39 |
| Rust 16 KB | 16 KB | 46.4k | 4.70 |
| Rust 88 KB | 88 KB | 27.5k | 5.71 |
| Java 3,000 B | about 8 KB | 48.2k | 4.15 |

Matching the input per batch reverses the gap in both directions, so neither zlib binding is faster; the difference is how much data each client compresses per call. At level 1 (`compression.gzip.level=1` and `compression.level=1`) Java reaches 145k and Rust 109k msg/s, with ratios 4.27 and 4.05.

### Fixes

- To trade ratio for speed on Java, lower `compression.gzip.level` (level 1: 4.3x throughput, ratio 5.39 to 4.27) or `batch.size`.
- The `full-2` gzip comparison is not like for like at equal `batch.size`: Java produced 13% fewer bytes on the broker. It is better read as a compression speed/ratio trade-off than as a client efficiency result.

## Reproducing

The experiments use `results/diag-java/tools/jlib.sh`, which only issues `docker` and `infra/` commands against the benchmark host (sidecars and analysis containers run on cores 7,23 from `kbench-java:latest`). Knobs: `JMX=1` samples client MBeans (`JmxDump.java`, summarized by `jmxsum.py`), `THREADS=1` samples per-thread CPU (`summ.py`, `thr2.py` for the user/kernel split), `JFR=1` records a profile (`JFR_CPU=2ms` adds the CPU-time sampler, `JFR_EXTRA` adds event settings), `GCLOG=1` writes a GC log, `HISTO_AT=s` takes a class histogram, `LOGDIRS=1` records topic size on the broker, `JVM_ARGS` replaces the image's JVM flags.

```
source results/diag-java/tools/jlib.sh
P="--payload random --acks 1 --compression none --linger-ms 5 --extra metrics.sample.window.ms=1000"
JMX=1 jprod java pb-1m-b 6 5000000 300000 1024 $P --batch-size 1048576
JMX=1 jprod java pb-1m-sb-1 6 5000000 300000 1024 $P --batch-size 1048576 --extra send.buffer.bytes=-1
JMX=1 jprod java pb-1m-mrs1m 6 5000000 300000 1024 $P --batch-size 1048576 --extra max.request.size=1100000
JFR=1 JFR_DELAY=3 JFR_EXTRA=",jdk.SocketWrite#enabled=true,jdk.SocketWrite#threshold=0ms" jprod java pb-1m-sock 6 5000000 300000 1024 $P --batch-size 1048576
prefill kbench-jdiag-c1k 6 6000000 1024
JMX=1 jcons java c1k-rb-1 kbench-jdiag-c1k 6 5000000 500000 --extra receive.buffer.bytes=-1
JFR=1 JFR_DELAY=3 JFR_CPU=2ms jcons java c1k-cpu kbench-jdiag-c1k 6 5000000 500000; jfrana c1k-cpu
bash infra/topic.sh delete kbench-jdiag-c1k
P="--payload random --acks 1 --compression none --linger-ms 5 --batch-size 16384"
GCLOG=1 THREADS=1 HISTO_AT=6 jprod java k16-base 16 50000000 5000000 100 $P --instances 16
GCLOG=1 jprod java k16-bm32m 16 50000000 5000000 100 $P --instances 16 --buffer-memory 33554432
P="--payload text --acks 1 --compression gzip --linger-ms 5"
LOGDIRS=1 JMX=1 jprod java gz-java-b16k 6 400000 40000 1024 $P --batch-size 16384
LOGDIRS=1 jprod rust gz-rust-b88k 6 400000 40000 1024 $P --batch-size 88000 --config-profile native
```

Run these from `bash` (the helpers rely on word splitting of `$P`).
