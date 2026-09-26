mod consume;
mod instances;
mod payload;
mod produce;
mod resources;

use clap::{ArgAction, Args, Parser, Subcommand};
use hdrhistogram::Histogram;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::error::KafkaError;
use serde_json::{Map, Value, json};
use std::process::ExitCode;
use std::time::Instant;

pub const CLIENT_VERSION: &str = "0.39.0";
pub const HIST_HIGHEST_US: u64 = 120_000_000;

#[derive(Parser)]
#[command(name = "kbench-rust", about = "Kafka client benchmark harness (rust-rdkafka)")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    Produce(ProduceArgs),
    Consume(ConsumeArgs),
    PayloadHash(PayloadHashArgs),
}

fn parse_kv(s: &str) -> Result<(String, String), String> {
    s.split_once('=')
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .ok_or_else(|| format!("expected k=v, got '{s}'"))
}

#[derive(Args)]
pub struct ProduceArgs {
    #[arg(long, default_value = "kbench-kafka:9092")]
    pub bootstrap: String,
    #[arg(long)]
    pub topic: String,
    #[arg(long)]
    pub partitions: u32,
    #[arg(long)]
    pub num_messages: u64,
    #[arg(long, default_value_t = 0)]
    pub warmup_messages: u64,
    #[arg(long, default_value_t = 120)]
    pub max_duration_s: u64,
    #[arg(long)]
    pub message_size: usize,
    #[arg(long, default_value = "random", value_parser = ["random", "text"])]
    pub payload: String,
    #[arg(long, default_value_t = 42)]
    pub seed: u64,
    #[arg(long, default_value = "1", value_parser = ["0", "1", "all"])]
    pub acks: String,
    #[arg(long, default_value = "none", value_parser = ["none", "gzip", "snappy", "lz4", "zstd"])]
    pub compression: String,
    #[arg(long, default_value_t = 5)]
    pub linger_ms: u64,
    #[arg(long, default_value_t = 16384)]
    pub batch_size: u64,
    #[arg(long, default_value_t = 5)]
    pub max_in_flight: u64,
    #[arg(long, default_value_t = false, action = ArgAction::Set)]
    pub idempotence: bool,
    #[arg(long, default_value_t = 268_435_456)]
    pub buffer_memory: u64,
    #[arg(long, default_value_t = 0)]
    pub rate: u64,
    #[arg(long)]
    pub embed_timestamp: bool,
    #[arg(long)]
    pub run_id: String,
    #[arg(long = "extra", value_parser = parse_kv)]
    pub extra: Vec<(String, String)>,
    #[arg(long, default_value_t = 1)]
    pub instances: u32,
    #[arg(long, default_value = "matched", value_parser = ["matched", "native"])]
    pub config_profile: String,
}

impl ProduceArgs {
    fn params(&self) -> Value {
        json!({
            "bootstrap": self.bootstrap,
            "topic": self.topic,
            "partitions": self.partitions,
            "num-messages": self.num_messages,
            "warmup-messages": self.warmup_messages,
            "max-duration-s": self.max_duration_s,
            "message-size": self.message_size,
            "payload": self.payload,
            "seed": self.seed,
            "acks": self.acks,
            "compression": self.compression,
            "linger-ms": self.linger_ms,
            "batch-size": self.batch_size,
            "max-in-flight": self.max_in_flight,
            "idempotence": self.idempotence,
            "buffer-memory": self.buffer_memory,
            "rate": self.rate,
            "embed-timestamp": self.embed_timestamp,
            "run-id": self.run_id,
            "extra": extras_json(&self.extra),
            "instances": self.instances,
            "config-profile": self.config_profile,
            "build": build_info("ThreadedProducer"),
        })
    }
}

#[derive(Args)]
pub struct ConsumeArgs {
    #[arg(long, default_value = "kbench-kafka:9092")]
    pub bootstrap: String,
    #[arg(long)]
    pub topic: String,
    #[arg(long)]
    pub partitions: u32,
    #[arg(long)]
    pub num_messages: u64,
    #[arg(long, default_value_t = 0)]
    pub warmup_messages: u64,
    #[arg(long, default_value_t = 1)]
    pub fetch_min_bytes: u64,
    #[arg(long, default_value_t = 500)]
    pub fetch_max_wait_ms: u64,
    #[arg(long, default_value_t = 1_048_576)]
    pub max_partition_fetch_bytes: u64,
    #[arg(long, default_value_t = 52_428_800)]
    pub fetch_max_bytes: u64,
    #[arg(long, default_value_t = true, action = ArgAction::Set)]
    pub check_crcs: bool,
    #[arg(long)]
    pub measure_e2e: bool,
    #[arg(long, default_value_t = 180)]
    pub timeout_s: u64,
    #[arg(long)]
    pub run_id: String,
    #[arg(long = "extra", value_parser = parse_kv)]
    pub extra: Vec<(String, String)>,
    #[arg(long, default_value_t = 1)]
    pub instances: u32,
    #[arg(long, default_value = "poll", value_parser = ["poll", "batch"])]
    pub consume_api: String,
    #[arg(long, default_value_t = 500)]
    pub batch_max: usize,
}

impl ConsumeArgs {
    fn params(&self) -> Value {
        json!({
            "bootstrap": self.bootstrap,
            "topic": self.topic,
            "partitions": self.partitions,
            "num-messages": self.num_messages,
            "warmup-messages": self.warmup_messages,
            "fetch-min-bytes": self.fetch_min_bytes,
            "fetch-max-wait-ms": self.fetch_max_wait_ms,
            "max-partition-fetch-bytes": self.max_partition_fetch_bytes,
            "fetch-max-bytes": self.fetch_max_bytes,
            "check-crcs": self.check_crcs,
            "measure-e2e": self.measure_e2e,
            "timeout-s": self.timeout_s,
            "run-id": self.run_id,
            "extra": extras_json(&self.extra),
            "instances": self.instances,
            "consume-api": self.consume_api,
            "batch-max": self.batch_max,
            "build": build_info(if self.consume_api == "batch" { "BaseConsumer + rd_kafka_consume_batch_queue" } else { "BaseConsumer" }),
        })
    }
}

#[derive(Args)]
struct PayloadHashArgs {
    #[arg(long, default_value = "random", value_parser = ["random", "text"])]
    payload: String,
    #[arg(long)]
    message_size: usize,
    #[arg(long, default_value_t = 42)]
    seed: u64,
}

fn extras_json(extra: &[(String, String)]) -> Value {
    Value::Object(extra.iter().map(|(k, v)| (k.clone(), Value::String(v.clone()))).collect())
}

fn build_info(client_api: &str) -> Value {
    let features = rdkafka::config::ClientConfig::new()
        .create_native_config()
        .and_then(|c| c.get("builtin.features"))
        .unwrap_or_default();
    json!({
        "opt_level": 3,
        "lto": "fat",
        "codegen_units": 1,
        "panic": "abort",
        "target_cpu": "generic x86-64 (default)",
        "allocator": "system (glibc malloc)",
        "rdkafka_features": ["cmake-build", "libz", "zstd"],
        "librdkafka_builtin_features": features,
        "client_api": client_api,
    })
}

/// Result object pre-filled with every schema key so partial failures still produce a full line.
pub fn base_result(mode: &str, run_id: &str, params: Value) -> Map<String, Value> {
    let v = json!({
        "schema_version": 1,
        "status": "error",
        "error": null,
        "client": "rust",
        "client_lib": "rust-rdkafka",
        "client_version": CLIENT_VERSION,
        "native_lib_version": rdkafka::util::get_rdkafka_version().1,
        "runtime": env!("KBENCH_RUSTC_VERSION"),
        "mode": mode,
        "run_id": run_id,
        "instances": null,
        "window_kind": null,
        "params": params,
        "effective_config": {},
        "payload_sha256": null,
        "pool_count": null,
        "messages": 0,
        "bytes": 0,
        "errors": 0,
        "truncated": false,
        "duration_s": 0.0,
        "throughput_msgs_per_s": 0.0,
        "throughput_mb_per_s": 0.0,
        "rate_lag_max_us": null,
        "latency_kind": null,
        "latency_us": null,
        "startup_ms": 0.0,
        "first_message_ms": null,
        "resources": null,
        "jvm": null,
        "instances_detail": [],
        "timeseries": [],
    });
    match v {
        Value::Object(m) => m,
        _ => unreachable!(),
    }
}

pub fn new_histogram() -> Histogram<u64> {
    Histogram::new_with_bounds(1, HIST_HIGHEST_US, 3).expect("histogram bounds")
}

pub fn latency_summary(h: &Histogram<u64>) -> Value {
    if h.is_empty() {
        return json!({"count": 0, "min": 0, "mean": 0.0, "stddev": 0.0, "p50": 0, "p90": 0,
            "p99": 0, "p99_9": 0, "p99_99": 0, "max": 0});
    }
    json!({
        "count": h.len(),
        "min": h.min(),
        "mean": h.mean(),
        "stddev": h.stdev(),
        "p50": h.value_at_quantile(0.50),
        "p90": h.value_at_quantile(0.90),
        "p99": h.value_at_quantile(0.99),
        "p99_9": h.value_at_quantile(0.999),
        "p99_99": h.value_at_quantile(0.9999),
        "max": h.max(),
    })
}

pub fn ms_since(t: Instant) -> f64 {
    t.elapsed().as_secs_f64() * 1000.0
}

pub fn fill_throughput(r: &mut Map<String, Value>, messages: u64, bytes: u64, duration_s: f64) {
    r.insert("messages".into(), json!(messages));
    r.insert("bytes".into(), json!(bytes));
    r.insert("duration_s".into(), json!(duration_s));
    let (mps, mbps) = if duration_s > 0.0 {
        (messages as f64 / duration_s, bytes as f64 / 1e6 / duration_s)
    } else {
        (0.0, 0.0)
    };
    r.insert("throughput_msgs_per_s".into(), json!(mps));
    r.insert("throughput_mb_per_s".into(), json!(mbps));
}

pub fn set_error(r: &mut Map<String, Value>, msg: impl Into<String>) {
    let msg = msg.into();
    eprintln!("error: {msg}");
    r.insert("status".into(), json!("error"));
    r.insert("error".into(), json!(msg));
}

/// rust-rdkafka overrides librdkafka's `log_level` after creation, so debug
/// output requested via `--extra debug=...` needs the Debug level set here.
pub fn log_level(cfg: &[(String, String)]) -> RDKafkaLogLevel {
    if cfg.iter().any(|(k, _)| k == "debug") {
        RDKafkaLogLevel::Debug
    } else {
        RDKafkaLogLevel::Warning
    }
}

/// Routes librdkafka logs and global errors to stderr; the default context
/// forwards them to the `log` crate, which has no logger installed here.
pub fn log_to_stderr(level: RDKafkaLogLevel, fac: &str, msg: &str) {
    eprintln!("librdkafka {level:?} {fac}: {msg}");
}

pub fn error_to_stderr(error: KafkaError, reason: &str) {
    eprintln!("librdkafka error: {error}: {reason}");
}

fn payload_hash(a: &PayloadHashArgs) -> Value {
    let kind = payload::PayloadKind::parse(&a.payload).expect("validated by clap");
    if a.message_size == 0 {
        return json!({"status": "error", "error": "--message-size must be > 0"});
    }
    let pool = payload::build_pool(kind, a.message_size, a.seed);
    json!({
        "status": "ok",
        "payload": a.payload,
        "message_size": a.message_size,
        "seed": a.seed,
        "payload_sha256": pool.sha256,
        "pool_count": pool.messages.len(),
    })
}

fn main() -> ExitCode {
    let process_start = Instant::now();
    let cli = match Cli::try_parse() {
        Ok(c) => c,
        Err(e) => {
            use clap::error::ErrorKind;
            if matches!(e.kind(), ErrorKind::DisplayHelp | ErrorKind::DisplayVersion) {
                let _ = e.print();
                return ExitCode::SUCCESS;
            }
            eprintln!("{e}");
            let mode = std::env::args().nth(1).unwrap_or_default();
            let mut r = base_result(&mode, "", Value::Null);
            r.insert("error".into(), json!(format!("argument error: {}", e.kind())));
            println!("RESULT {}", Value::Object(r));
            return ExitCode::from(2);
        }
    };
    let result = match &cli.cmd {
        Cmd::Produce(a) => Value::Object(produce::run(a, a.params(), process_start)),
        Cmd::Consume(a) => Value::Object(consume::run(a, a.params(), process_start)),
        Cmd::PayloadHash(a) => payload_hash(a),
    };
    let ok = result.get("status").and_then(Value::as_str) == Some("ok");
    println!("RESULT {result}");
    if ok { ExitCode::SUCCESS } else { ExitCode::from(1) }
}
