# Kafka client benchmark: shared contract

Every component (broker infra, Java harness, Rust harness, orchestrator) MUST follow this contract exactly so that results are comparable. If something here is impossible, stop and report instead of silently deviating.

## Style rules (apply to all files)

- No em dashes or en dashes anywhere (code, comments, docs). Use plain hyphens, commas, or colons.
- No trailing whitespace, including on blank lines.
- No comments that restate what the code says. Comment only non-obvious reasoning (fairness decisions, client API quirks).
- Markdown: never hard-wrap; one paragraph or list item per line.
- Use inclusive terminology (leader/follower, allowlist/denylist).

## Environment

- Docker daemon is REMOTE, reached through the active docker context pointing at the benchmark host (EC2 c6i.8xlarge, x86_64, Intel Xeon Platinum 8375C, 16 physical cores / 32 vCPUs, 62 GiB RAM, Amazon Linux 2, kernel 5.10, cgroup v1). The local machine has no docker daemon.
- Consequence: bind mounts of LOCAL paths do not work. Never use `-v ./local:/x`. Everything a container needs is baked into its image via `docker build` (the build context is uploaded to the remote daemon). Results leave containers via stdout only.
- The host already runs unrelated containers `excalidraw`, `excalidraw-mcp`, `plantuml_server`. NEVER stop, remove, restart, or modify them, and never run `docker system prune`, `docker container prune`, `docker volume prune`, `docker network prune`, or `docker rmi` on images not created by this project. Only touch resources whose name starts with `kbench`.
- EVERYTHING runs in docker on that host. The local machine is only a docker CLI client: it may run `docker build`, `docker run`, `docker logs`, `docker cp`, and nothing else for the benchmark. No local python, java, or cargo is used to run, orchestrate, analyze, or report. The orchestrator and report generator run inside the `kbench-runner` container (see Names), which talks to the host daemon through the mounted `/var/run/docker.sock` and writes results to the docker volume `kbench-results` mounted at `/results`. Final results are copied to the local machine with `docker cp` only so the user can read them.

## Pinned versions

| Component | Version |
|---|---|
| Kafka broker image | `apache/kafka:4.3.1` (KRaft, single combined broker+controller) |
| Java client | `org.apache.kafka:kafka-clients:4.3.1` |
| Java runtime | Eclipse Temurin 25 JDK (`eclipse-temurin:25-jdk-noble` runtime, `maven:3-eclipse-temurin-25` builder; verify the builder tag exists, fall back to installing maven on the temurin image) |
| Java histogram | `org.hdrhistogram:HdrHistogram:2.2.2` |
| Rust client | `rdkafka = 0.39.0` (bundles librdkafka 2.12.1 via `rdkafka-sys 4.10.0+2.12.1`), features `cmake-build` (static librdkafka) plus whatever is needed for gzip, snappy, lz4, zstd support (verify all five codecs work) |
| Rust toolchain | `rust:1.98.1-bookworm` builder, `debian:bookworm-slim` runtime |
| Rust histogram | `hdrhistogram = 7.6.0` |

## Names

| Thing | Name |
|---|---|
| Compose project | `kbench` |
| Docker network | `kbench-net` (bridge) |
| Broker container / hostname | `kbench-kafka` |
| Bootstrap address (from other containers on `kbench-net`) | `kbench-kafka:9092` |
| Java harness image | `kbench-java:latest` |
| Rust harness image | `kbench-rust:latest` |
| Client container names | `kbench-client-<run_id>` (consumer side of e2e: `kbench-client-<run_id>-c`) |
| Runner image / container | `kbench-runner:latest` / `kbench-runner` |
| Results volume | `kbench-results` (mounted at `/results` in the runner) |

## CPU and memory pinning (cgroup cpusets on the remote host)

Hyperthread siblings are `n` and `n+16`. Whole physical cores are assigned so no two roles share a core. The goal is to use the full host: the broker and the client under test get all cores except one reserved for the runner, docker daemon, and the idle unrelated containers.

| Role | Physical cores | `--cpuset-cpus` | Memory limit |
|---|---|---|---|
| Broker | 0-6 | `0-6,16-22` | 24g (heap `-Xms4g -Xmx4g`; tmpfs pages count toward this cgroup) |
| Runner, host, docker daemon, other containers | 7 | runner pinned to `7,23`; others untouched | runner 2g |
| Client under test (produce and consume modes) | 8-15 | `8-15,24-31` | 12g |
| E2E runs only: producer | 8-11 | `8-11,24-27` | 8g |
| E2E runs only: consumer | 12-15 | `12-15,28-31` | 8g |

Broker log dirs live on tmpfs (`/var/lib/kafka/data`, size 16g) so disk I/O on EBS is not a variable. Topics are deleted after every run.

Broker tuning for throughput (identical for both clients, recorded in the report): `num.network.threads=8`, `num.io.threads=16`. Everything else default.

## Multiple client instances (`--instances K`, both modes)

To saturate the host, one harness process can run K fully independent client instances, each with its own client object (Java `KafkaProducer`/`KafkaConsumer`, Rust `ThreadedProducer`/`BaseConsumer`) and its own dedicated sending or polling thread. Default K=1, which must behave exactly as before.

- Partitions: instance k (0-based) owns partitions `{p : p % K == k}`. Requires `partitions >= K`. A producer instance sends its j-th record (0-based) to its owned partitions round-robin (`owned[j % len(owned)]`). A consumer instance assigns only its owned partitions at the beginning offset.
- Message split: `--num-messages N` and `--warmup-messages W` are TOTALS across instances. Instance k handles `N/K` (+1 for the first `N % K` instances), same for W.
- Payload: instance k's j-th record (warmup and measured counted together, warmup first) has global index `i = j*K + k` and uses `pool[i % pool_count]`. The pool is generated once and shared read-only.
- Buffering: `--buffer-memory` is a TOTAL; each producer instance gets `buffer_memory / K` (Java `buffer.memory`, librdkafka `queue.buffering.max.kbytes = buffer_memory / K / 1024`).
- Rate: `--rate R` is a total; each instance paces at `R/K` with its own schedule.
- Producer synchronization: all instances construct their clients and complete their warmup (warmup sent and flushed), then wait on a common barrier. The measured window starts when the barrier releases (single `t0` for the whole process) and ends when the LAST instance has all its records acked and flushed. Throughput = total measured messages / (t_end - t0). `window_kind` = `barrier`.
- Consumer synchronization: NO barrier after warmup. A consumer instance that idles while its client keeps prefetching would start the window with data already buffered, and librdkafka prefetches far more than the Java classic consumer (which only fetches inside `poll()`), so a barrier biases the result. Each instance polls continuously from assignment to its last measured record. Instance k's measured window starts at receipt of its own warmup-boundary record (its record index `W_k`, 0-based) and ends at receipt of its last measured record. The process window is `[min_k start_k, max_k end_k]` and throughput = total measured messages / that window. `window_kind` = `union` for K>1; for K=1 this equals the old definition and is recorded as `first_to_last`. Clients are all constructed before a start gate; each instance calls `assign` only AFTER the gate releases, inside its own thread, because librdkafka starts fetching on `assign` even without `poll`.
- Latency: one histogram per instance, merged into a single histogram for `latency_us`. Also emit `instances_detail`: a list of per-instance `{"k", "messages", "errors", "duration_s", "throughput_msgs_per_s", "latency_us": {"p50","p99","max"}}`.
- Resources are process-wide (all instances), as before. Timeseries buckets are summed across instances.
- `--max-duration-s` applies to the whole measured window: every instance stops issuing sends once it expires.
- Record `instances` in `params` and at the top level of the RESULT JSON (`"instances": K`).

## Payload generation (MUST be byte-identical in Java and Rust)

PRNG: SplitMix64 with 64-bit wrapping arithmetic:

```
state += 0x9E3779B97F4A7C15
z = state
z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9
z = (z ^ (z >>> 27)) * 0x94D049BB133111EB
return z ^ (z >>> 31)
```

Initial `state = seed` (default seed 42).

Corpus: `pool_count = min(16384, floor(67108864 / message_size))` distinct messages, each exactly `message_size` bytes, generated in order message 0, 1, 2, ... from ONE PRNG stream:

- `--payload random`: fill each message with bytes taken from successive `next()` outputs, little-endian, 8 bytes per output. If `message_size` is not a multiple of 8, take the low-order bytes of the last output and discard the rest. The next message starts with a fresh `next()` call.
- `--payload text` (compressible): word list `W = ["kafka","stream","broker","topic","partition","offset","consumer","producer","record","batch","leader","follower","replica","commit","segment","index","latency","throughput","cluster","message","payload","header","key","value","timestamp","schema","event","log","queue","fetch","poll","ack"]` (32 words). Build the message by repeatedly appending `W[next() % 32]` followed by one ASCII space, until length >= message_size, then truncate to exactly `message_size` bytes.

`payload_sha256` = lowercase hex SHA-256 over the concatenation of all pool messages in order (before any timestamp embedding). The orchestrator verifies Java and Rust report the same value for the same (payload, size, seed).

Measured message `i` (0-based over warmup+measured) uses `pool[i % pool_count]`. Keys are always null. No headers.

Partition assignment: the harness sets the partition EXPLICITLY on every record: `partition = i % partitions`. This removes differences between the Java built-in partitioner and librdkafka's sticky/consistent_random partitioner.

Timestamp embedding (`--embed-timestamp`, used in e2e): copy the pool message into a fresh buffer and overwrite bytes 0..8 with the send wall-clock time as big-endian signed 64-bit nanoseconds since Unix epoch (Java: `Instant.now()` to nanos; Rust: `SystemTime::now()`). Requires `message_size >= 8`. Without this flag, pool buffers are reused directly (Java: pass the same `byte[]`, which is safe because `send()` copies into the batch synchronously; Rust: pass `&pool[j]`, librdkafka copies).

## CLI (identical flags in both harnesses)

Entry point: the image ENTRYPOINT runs the harness, so `docker run ... kbench-java:latest produce --topic t ...` works.

### `produce`

| Flag | Default | Meaning |
|---|---|---|
| `--bootstrap` | `kbench-kafka:9092` | |
| `--topic` | required | |
| `--partitions` | required | number of partitions of the topic, used for explicit round-robin |
| `--num-messages` | required | measured messages |
| `--warmup-messages` | 0 | sent before the measured phase, followed by a full flush; excluded from all stats |
| `--max-duration-s` | 120 | stop issuing sends after this many seconds of the measured phase; flush; report actual counts and set `truncated: true` |
| `--message-size` | required | bytes |
| `--payload` | `random` | `random` or `text` |
| `--seed` | 42 | |
| `--acks` | `1` | `0`, `1`, `all` |
| `--compression` | `none` | `none`, `gzip`, `snappy`, `lz4`, `zstd` |
| `--linger-ms` | 5 | |
| `--batch-size` | 16384 | bytes |
| `--max-in-flight` | 5 | |
| `--idempotence` | `false` | `true` requires acks=all |
| `--buffer-memory` | 268435456 | bytes of client-side buffering |
| `--rate` | 0 | target msgs/s for the measured phase, 0 = unlimited. Pace by schedule: message k is due at `t0 + k/rate`; spin/park until due. Never burst to catch up by more than 1 ms of backlog; record `rate_lag_max_us` |
| `--embed-timestamp` | off | see above |
| `--run-id` | required | |
| `--extra k=v` | none, repeatable | raw client property passed through and recorded |

### `consume`

| Flag | Default | Meaning |
|---|---|---|
| `--bootstrap` | `kbench-kafka:9092` | |
| `--topic` | required | |
| `--partitions` | required | assigns partitions 0..P-1 manually at the beginning offset. No subscribe, no group rebalance, no commits |
| `--num-messages` | required | measured messages |
| `--warmup-messages` | 0 | the first W received messages are excluded; the measured window starts at receipt of message W (0-based) |
| `--fetch-min-bytes` | 1 | |
| `--fetch-max-wait-ms` | 500 | |
| `--max-partition-fetch-bytes` | 1048576 | |
| `--fetch-max-bytes` | 52428800 | |
| `--check-crcs` | `true` | |
| `--measure-e2e` | off | parse the first 8 bytes as the embedded send timestamp and record `now_wall_ns - ts` into the latency histogram |
| `--timeout-s` | 180 | abort with `"status":"timeout"` if not done |
| `--run-id` | required | |
| `--extra k=v` | none, repeatable | |

## Kafka configuration mapping (fairness)

Both harnesses set every property below explicitly, even when equal to the default, and record the full map they passed in `effective_config`.

Producer:

| Concept | Java property | librdkafka property |
|---|---|---|
| acks | `acks` | `acks` |
| linger | `linger.ms` | `linger.ms` |
| batch bytes | `batch.size` | `batch.size` |
| batch message cap | n/a (Java has none) | `batch.num.messages=1000000` (so the byte limit governs, like Java) |
| compression | `compression.type` | `compression.type` |
| in-flight | `max.in.flight.requests.per.connection` | `max.in.flight.requests.per.connection` |
| idempotence | `enable.idempotence` | `enable.idempotence` |
| buffer | `buffer.memory` (bytes), `max.block.ms=60000` | `queue.buffering.max.kbytes=buffer/1024`, `queue.buffering.max.messages=2147483647` |
| retries | `retries=2147483647`, `delivery.timeout.ms=120000`, `request.timeout.ms=30000` | `message.send.max.retries=2147483647`, `message.timeout.ms=120000`, `request.timeout.ms=30000` |
| request size | `max.request.size=10485760` | `message.max.bytes=10485760` |
| client id | `client.id=<run_id>` | `client.id=<run_id>` |

Producer buffer-full handling: Java `send()` blocks internally. Rust: on `QueueFull`, poll the producer and retry the same record; the latency start timestamp is taken ONCE before the first attempt in both clients.

Rust producer API: use `BaseProducer` (or `ThreadedProducer`) with a custom `ProducerContext` whose `delivery()` callback records latency; pass the send timestamp through `DeliveryOpaque` as `usize` (nanoseconds since harness start from a monotonic clock) to avoid a heap allocation per message. Do not use `FutureProducer` (extra allocation and channel per message that Java does not pay).

Consumer:

| Concept | Java property | librdkafka property |
|---|---|---|
| group id | `group.id=<run_id>` (required by librdkafka, unused) | `group.id=<run_id>` |
| commits | `enable.auto.commit=false` | `enable.auto.commit=false`, `enable.auto.offset.store=false` |
| reset | `auto.offset.reset=earliest` | `auto.offset.reset=earliest` |
| fetch min | `fetch.min.bytes` | `fetch.min.bytes` |
| fetch wait | `fetch.max.wait.ms` | `fetch.wait.max.ms` |
| per-partition fetch | `max.partition.fetch.bytes` | `max.partition.fetch.bytes` |
| fetch max | `fetch.max.bytes` | `fetch.max.bytes` |
| crc | `check.crcs` | `check.crcs` |
| client id | `client.id` | `client.id` |

Rust consumer API: `BaseConsumer` poll loop (not `StreamConsumer`). Java: `KafkaConsumer.poll(Duration.ofMillis(100))`. Both touch every payload byte count (sum value lengths) so work is not optimized away.

## Measurement definitions

- Monotonic clock for durations and producer latency: Java `System.nanoTime()`, Rust `std::time::Instant`.
- Producer throughput window: from immediately before the first measured send to the moment the final flush returns (all measured records acked or failed).
- Producer latency: per-record time from before the first send attempt to the delivery callback, recorded in microseconds into an HdrHistogram (3 significant digits, highest trackable 120 s). Failed deliveries count in `errors`, not in the histogram.
- Consumer throughput window: receipt of message index W to receipt of message index W+N-1.
- E2E latency: consumer wall clock at receipt minus embedded producer wall clock, microseconds, same HdrHistogram settings. Both containers share the host clock.
- MB means 10^6 bytes. Payload bytes only (no keys, headers, protocol overhead).
- Resource sampling: a background thread samples every 100 ms during the measured window: RSS (`VmRSS` from `/proc/self/status`) and thread count (`Threads`). CPU: `utime`+`stime` from `/proc/self/stat` at window start and end, converted with the real clock tick rate (Rust: `libc::sysconf(_SC_CLK_TCK)`; Java: read `getconf CLK_TCK` once at startup or assume 100 and record the assumption). Also read cgroup totals at window start and end if present: cgroup v1 `/sys/fs/cgroup/cpuacct/cpuacct.usage` (ns) and `/sys/fs/cgroup/memory/memory.max_usage_in_bytes`, cgroup v2 `/sys/fs/cgroup/cpu.stat` (`usage_usec`) and `/sys/fs/cgroup/memory.peak`; null if absent.
- Per-second timeseries: messages completed (acked for producer, received for consumer) per 1 s bucket of the measured window, plus RSS at the bucket end.

## Output

Human-readable logs go to STDERR only. At exit, print exactly ONE line to STDOUT: the literal prefix `RESULT ` followed by a single-line JSON object. Exit code 0 when `status` is `ok`, non-zero otherwise (still print the RESULT line if at all possible).

```json
{
  "schema_version": 1,
  "status": "ok | error | timeout",
  "error": null,
  "client": "java | rust",
  "client_lib": "kafka-clients | rust-rdkafka",
  "client_version": "4.3.1 | 0.39.0",
  "native_lib_version": null,
  "runtime": "Temurin 25.x.y ... | rustc 1.98.1",
  "mode": "produce | consume",
  "run_id": "...",
  "params": {"every CLI flag": "resolved value"},
  "effective_config": {"kafka property": "value as passed"},
  "payload_sha256": "hex (produce only; null for consume)",
  "pool_count": 16384,
  "messages": 0,
  "bytes": 0,
  "errors": 0,
  "truncated": false,
  "duration_s": 0.0,
  "throughput_msgs_per_s": 0.0,
  "throughput_mb_per_s": 0.0,
  "rate_lag_max_us": null,
  "latency_kind": "send_to_ack | e2e | null",
  "latency_us": {"count": 0, "min": 0, "mean": 0.0, "stddev": 0.0, "p50": 0, "p90": 0, "p99": 0, "p99_9": 0, "p99_99": 0, "max": 0},
  "startup_ms": 0.0,
  "first_message_ms": null,
  "resources": {
    "clk_tck": 100,
    "cpu_user_s": 0.0,
    "cpu_sys_s": 0.0,
    "cpu_total_s": 0.0,
    "cpu_cores_avg": 0.0,
    "cpu_s_per_million_msgs": 0.0,
    "rss_start_bytes": 0,
    "rss_avg_bytes": 0,
    "rss_peak_bytes": 0,
    "threads_peak": 0,
    "cgroup_cpu_s": null,
    "cgroup_mem_peak_bytes": null
  },
  "jvm": null,
  "timeseries": [{"t": 1, "msgs": 0, "rss_bytes": 0}]
}
```

- `native_lib_version`: Rust reports the librdkafka version string from `rdkafka::util::get_rdkafka_version()`; Java null.
- `startup_ms`: process start (as early as possible in main) to client constructed and ready (producer: all instances constructed; consumer: all instances constructed, before the start gate and before `assign`).
- `first_message_ms`: consume only, from start-gate release (immediately before `assign`) to the earliest first received record across instances.
- `jvm` (Java only, measured window deltas): `{"vm": "...", "flags": [...input args...], "gc": [{"name": "G1 Young Generation", "count": 0, "time_ms": 0}], "gc_total_count": 0, "gc_total_time_ms": 0, "jit_compile_time_ms": 0, "heap_used_peak_bytes": 0, "heap_committed_bytes": 0}`.

## JVM and build settings

- Java: JVM flags in the ENTRYPOINT: `-Xms2g -Xmx2g -XX:+AlwaysPreTouch -XX:+UseG1GC -XX:+ExitOnOutOfMemoryError`. Nothing else tuned. Record the flags in `jvm.flags`.
- Rust: `[profile.release] opt-level=3, lto="fat", codegen-units=1, panic="abort"`. Default target CPU (generic x86-64), default system allocator (glibc malloc). Record these facts in the result under `params.build` or equivalent.
- Neither harness does anything the other cannot (no batching tricks, no extra threads for sending). Exactly one sending thread per producer instance and one polling thread per consumer instance, in both (plus the client library's own internal threads, and the Rust ThreadedProducer's callback poll thread per instance).

## Smoke testing by component agents

Do not run heavy load. Smoke tests must stay under about 200k messages and 60 s. To avoid colliding with other agents, a harness agent that needs a broker starts its OWN throwaway broker named `kbench-smoke-<java|rust>` on its OWN network `kbench-smoke-<java|rust>-net` using `apache/kafka:4.3.1`, and removes both when done. Never touch `kbench-kafka` or `kbench-net` unless you are the infra agent.
