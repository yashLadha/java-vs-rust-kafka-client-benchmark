# Java vs Rust Kafka clients: a benchmark

![Java and Rust producer throughput against batch.size: Java peaks at 128 KB, Rust at 1 MB](docs/images/hero.svg)

A benchmark of the Java Kafka client (`kafka-clients` 4.3.1, Temurin 25) against the Rust client (`rdkafka` 0.39.0, librdkafka 2.12.1): same host, same single Kafka 4.3.1 broker, byte-identical payloads, everything in Docker.

- **The story** (what the first run showed, how each gap was diagnosed, what fixed it): [the blog post](https://yashladha.in/blog/java-vs-rust-kafka-client-performance/)
- **The numbers and caveats:** [`docs/ANALYSIS.md`](docs/ANALYSIS.md), with the diagnoses in [`docs/diagnosis.md`](docs/diagnosis.md) (Rust) and [`docs/java-diagnosis.md`](docs/java-diagnosis.md) (Java)
- **The fairness rules** both harnesses implement: [`CONTRACT.md`](CONTRACT.md)

## Results

Each client tuned, 1 KB messages, 6 partitions, one client instance, medians of 3 reps on an EC2 c6i.8xlarge.

| | Java | Rust |
|---|---|---|
| Producer, each at its best `batch.size` | 1.05M msg/s (1.12M with `send.buffer.bytes=-1`) | 1.00M msg/s |
| Consumer, 1 KB / 10 KB / 100 B | 1.21M / 137k / 5.75M msg/s | 1.35M / 305k / 2.10M msg/s |
| End-to-end p99 at 50k msg/s, `linger.ms=0` | 0.93 ms | 1.93 ms (`max.in.flight=5`) |
| Peak RSS / startup | about 2.4 GB / 260 ms | 9 MB to 1.2 GB / 0.8 ms |

Most of the first run's gaps were defaults, not languages:

![Rust to Java ratio in the first run and after the fixes](docs/images/09-before-after.svg)

## Quick start

Needs only a `docker` CLI whose active context points at the benchmark host (`docker context use <your-context>`). Builds, runs and analysis happen in containers on that host; runs are detached, so the local machine can disconnect.

```
./kbench.sh build                     # build kbench-java, kbench-rust, kbench-runner
./kbench.sh run --dry-run             # plan, run count and ETA
./kbench.sh run --reps 3 --out full-1 # run everything; the report is generated at the end
./kbench.sh status                    # or: logs, ls
./kbench.sh fetch full-1              # copy results to ./results/full-1
./kbench.sh stop                      # stop the runner, remove its clients and topics
bash infra/down.sh                    # remove the broker and kbench-net
```

Useful options: `--reps N`, `--scale F`, `--only REGEX`, `--clients LIST`, `--rust-config-profile matched|native`, `--skip-scaling`, `--resume`. Regenerate a report with `./kbench.sh report <dir>`.

| `--clients` variant | What it is |
|---|---|
| `java` | Java client, contract configuration |
| `rust` | Rust client, `BaseConsumer` poll loop |
| `rust-tuned` | Rust consumer with the batch API and `fetch.queue.backoff.ms=10` |
| `rust-lowlat` | Rust producer with `max.in.flight=5`; e2e only, not in the default list |

`--rust-config-profile matched` (default) forces Java-like librdkafka producer settings; `native` leaves librdkafka defaults. The published runs used `native`.

## Layout

| Path | Contents |
|---|---|
| `java-bench/`, `rust-bench/` | The two harnesses |
| `bench/` | Orchestrator (`run.py`), scenario matrix (`matrix.py`), report (`report.py`), charts (`story_plots.py`, `hero_svg.py`) |
| `infra/` | Broker compose file and helper scripts |
| `runner/`, `kbench.sh` | Runner image and the local launcher |
| `docs/` | Analysis and diagnosis write-ups |

Each result directory holds `report.md` (tables, charts, findings, caveats), `summary.csv` (one row per scenario, client and role), `raw.jsonl` (every run with its exact command and effective config), and host and environment details.
