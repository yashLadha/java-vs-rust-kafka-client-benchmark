use crate::instances::{self, Gate};
use crate::payload::{self, PayloadKind};
use crate::resources;
use crate::{ProduceArgs, base_result, fill_throughput, latency_summary, ms_since, new_histogram, set_error};
use hdrhistogram::Histogram;
use rdkafka::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::producer::{BaseRecord, DeliveryResult, ProducerContext, ThreadedProducer};
use serde_json::{Map, Value, json};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// Producer API choice: ThreadedProducer (a BaseProducer plus one thread that
// blocks in poll() and runs delivery callbacks as soon as librdkafka queues
// the DR event). This mirrors the Java client, whose callbacks run on its
// sender thread, and keeps the send loop free of callback work. Polling a
// BaseProducer from the send loop instead would delay acks by up to one
// pacing interval in --rate runs (inflating latency) and would require busy
// polling during flush, because rust-rdkafka's poll(timeout) always blocks for
// the full timeout. The extra thread never sends: there is still exactly one
// sending thread.

// rust-rdkafka's Producer::flush polls in 100 ms slices after each
// rd_kafka_flush(0) check, which would round the throughput window up by up to
// 100 ms. Completion is therefore tracked by counting delivery callbacks.
const FLUSH_TIMEOUT: Duration = Duration::from_secs(150);

struct State {
    measuring: bool,
    measure_start_ns: u64,
    completed: u64,
    wait_target: u64,
    acked: u64,
    errors: u64,
    warmup_errors: u64,
    first_error: Option<String>,
    hist: Histogram<u64>,
    buckets: Vec<u64>,
}

// Each instance's Shared is allocated back to back on the main thread; the
// alignment keeps one instance's delivery-callback mutex off the cache lines
// of another's (128 bytes covers the adjacent-line prefetcher).
#[repr(align(128))]
struct Shared {
    base: Instant,
    state: Mutex<State>,
    cv: Condvar,
}

struct Ctx {
    shared: Arc<Shared>,
}

impl ClientContext for Ctx {
    fn log(&self, level: RDKafkaLogLevel, fac: &str, msg: &str) {
        crate::log_to_stderr(level, fac, msg);
    }

    fn error(&self, error: KafkaError, reason: &str) {
        crate::error_to_stderr(error, reason);
    }
}

impl ProducerContext for Ctx {
    type DeliveryOpaque = usize;

    fn delivery(&self, result: &DeliveryResult<'_>, sent_ns: usize) {
        let now_ns = self.shared.base.elapsed().as_nanos() as u64;
        let mut st = self.shared.state.lock().unwrap();
        st.completed += 1;
        match result {
            Ok(_) if st.measuring => {
                st.acked += 1;
                st.hist.saturating_record(now_ns.saturating_sub(sent_ns as u64) / 1000);
                let bucket = (now_ns.saturating_sub(st.measure_start_ns) / 1_000_000_000) as usize;
                if bucket >= st.buckets.len() {
                    st.buckets.resize(bucket + 1, 0);
                }
                st.buckets[bucket] += 1;
            }
            Ok(_) => {}
            Err((e, _)) => {
                if st.measuring {
                    st.errors += 1;
                } else {
                    st.warmup_errors += 1;
                }
                if st.first_error.is_none() {
                    eprintln!("delivery error: {e}");
                    st.first_error = Some(e.to_string());
                }
            }
        }
        if st.completed >= st.wait_target {
            self.shared.cv.notify_all();
        }
    }
}

fn wait_completed(shared: &Shared, target: u64) -> bool {
    let deadline = Instant::now() + FLUSH_TIMEOUT;
    let mut st = shared.state.lock().unwrap();
    st.wait_target = target;
    while st.completed < target {
        let now = Instant::now();
        if now >= deadline {
            st.wait_target = u64::MAX;
            return false;
        }
        st = shared.cv.wait_timeout(st, deadline - now).unwrap().0;
    }
    st.wait_target = u64::MAX;
    true
}

// Park for most of the gap and spin only the tail: Linux timer slack makes a
// sleep overshoot by roughly 50-60 us, so the tail is spun. The thresholds
// (park above 200 us, wake 100 us early) match the Java harness so pacing CPU
// cost is identical.
fn wait_until(due: Instant) {
    loop {
        let now = Instant::now();
        if now >= due {
            return;
        }
        let rem = due - now;
        if rem > Duration::from_micros(200) {
            thread::sleep(rem - Duration::from_micros(100));
        } else {
            std::hint::spin_loop();
        }
    }
}

fn wall_ns() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos() as i64).unwrap_or(0)
}

// Keys the `matched` profile forces to Java-like values. The `native` profile
// leaves them at librdkafka defaults, which is the equivalent of Java running
// on its own defaults for the same settings.
const NATIVE_DEFAULTED: [&str; 7] = [
    "batch.num.messages",
    "max.in.flight.requests.per.connection",
    "queue.buffering.max.kbytes",
    "queue.buffering.max.messages",
    "message.send.max.retries",
    "message.timeout.ms",
    "request.timeout.ms",
];

fn config(a: &ProduceArgs, kk: u64) -> Vec<(String, String)> {
    let mut cfg: Vec<(String, String)> = [
        ("bootstrap.servers", a.bootstrap.clone()),
        ("client.id", a.run_id.clone()),
        ("acks", a.acks.clone()),
        ("linger.ms", a.linger_ms.to_string()),
        ("batch.size", a.batch_size.to_string()),
        ("batch.num.messages", "1000000".to_string()),
        ("compression.type", a.compression.clone()),
        ("max.in.flight.requests.per.connection", a.max_in_flight.to_string()),
        ("enable.idempotence", a.idempotence.to_string()),
        ("queue.buffering.max.kbytes", (a.buffer_memory / kk / 1024).to_string()),
        ("queue.buffering.max.messages", "2147483647".to_string()),
        ("message.send.max.retries", "2147483647".to_string()),
        ("message.timeout.ms", "120000".to_string()),
        ("request.timeout.ms", "30000".to_string()),
        ("message.max.bytes", "10485760".to_string()),
    ]
    .into_iter()
    .filter(|(k, _)| a.config_profile != "native" || !NATIVE_DEFAULTED.contains(k))
    .map(|(k, v)| (k.to_string(), v))
    .collect();
    for (k, v) in &a.extra {
        match cfg.iter_mut().find(|(ek, _)| ek == k) {
            Some(e) => e.1 = v.clone(),
            None => cfg.push((k.clone(), v.clone())),
        }
    }
    cfg
}

pub fn run(a: &ProduceArgs, params: Value, process_start: Instant) -> Map<String, Value> {
    let mut r = base_result("produce", &a.run_id, params);
    r.insert("instances".into(), json!(a.instances));
    r.insert("window_kind".into(), json!("barrier"));
    r.insert("latency_kind".into(), json!("send_to_ack"));
    if let Err(e) = run_inner(a, &mut r, process_start) {
        set_error(&mut r, e);
    }
    r
}

struct Instance {
    k: u64,
    producer: ThreadedProducer<Ctx>,
    shared: Arc<Shared>,
}

struct Plan<'a> {
    a: &'a ProduceArgs,
    pool: &'a [Vec<u8>],
    kk: u64,
    gate: &'a Gate,
}

enum PreFail {
    Failed(String),
    Aborted,
}

struct InstanceOut {
    t0: Instant,
    end: Instant,
    sent: u64,
    lag_max: Duration,
    truncated: bool,
    send_err: Option<String>,
    flushed: bool,
}

// The instance is moved onto its thread's stack. Reading the client handle
// from a shared heap Vec put it on a cache line next to the delivery
// callback's Mutex<State>, and that false sharing cost K=1 about 5% throughput.
fn run_instance(inst: Instance, plan: &Plan) -> (Instance, Result<InstanceOut, PreFail>) {
    let res = drive_instance(&inst, plan);
    (inst, res)
}

fn drive_instance(inst: &Instance, plan: &Plan) -> Result<InstanceOut, PreFail> {
    let a = plan.a;
    let kk = plan.kk;
    let k = inst.k;
    let producer = &inst.producer;
    let shared = &*inst.shared;
    let base = shared.base;
    let pool = plan.pool;
    let pool_count = pool.len() as u64;
    // Owned partitions are {k, k+K, k+2K, ...}; owned[j % n] is computed
    // arithmetically so K=1 does no extra memory access per record.
    let owned = (a.partitions as u64 - k).div_ceil(kk);
    let warmup = instances::share(a.warmup_messages, k, kk);
    let measured = instances::share(a.num_messages, k, kk);

    let send_one = |j: u64, sent_ns: u64, scratch: Option<&[u8]>| -> Result<(), String> {
        let payload: &[u8] = match scratch {
            Some(p) => p,
            None => &pool[((j * kk + k) % pool_count) as usize],
        };
        let mut record = BaseRecord::<(), [u8], usize>::with_opaque_to(&a.topic, sent_ns as usize)
            .payload(payload)
            .partition((k + (j % owned) * kk) as i32);
        loop {
            match producer.send(record) {
                Ok(()) => return Ok(()),
                Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), rec)) => {
                    record = rec;
                    producer.poll(Duration::ZERO);
                    thread::sleep(Duration::from_micros(50));
                }
                Err((e, _)) => return Err(format!("send failed: {e}")),
            }
        }
    };

    let embed = |j: u64| -> Vec<u8> {
        let mut buf = pool[((j * kk + k) % pool_count) as usize].clone();
        buf[..8].copy_from_slice(&wall_ns().to_be_bytes());
        buf
    };

    let fail = |e: String| {
        plan.gate.abort();
        PreFail::Failed(e)
    };
    for j in 0..warmup {
        let sent_ns = base.elapsed().as_nanos() as u64;
        let buf = if a.embed_timestamp { Some(embed(j)) } else { None };
        send_one(j, sent_ns, buf.as_deref()).map_err(|e| fail(format!("instance {k}: {e}")))?;
    }
    if warmup > 0 {
        if !wait_completed(shared, warmup) {
            return Err(fail(format!("instance {k}: warmup flush timed out")));
        }
        let st = shared.state.lock().unwrap();
        eprintln!("instance {k}: warmup done: {warmup} messages, {} errors", st.warmup_errors);
    }

    let t0 = plan.gate.arrive(None).ok_or(PreFail::Aborted)?;
    {
        let mut st = shared.state.lock().unwrap();
        st.measuring = true;
        st.measure_start_ns = t0.duration_since(base).as_nanos() as u64;
    }
    let max_duration = Duration::from_secs(a.max_duration_s);
    let interval_ns = if a.rate > 0 { 1e9 * kk as f64 / a.rate as f64 } else { 0.0 };
    let max_backlog = Duration::from_millis(1);
    let mut sched_base = t0;
    let mut sched_m0: u64 = 0;
    let mut lag_max = Duration::ZERO;
    let mut sent: u64 = 0;
    let mut truncated = false;
    let mut send_err = None;

    for m in 0..measured {
        if a.rate > 0 {
            let due = sched_base + Duration::from_nanos(((m - sched_m0) as f64 * interval_ns) as u64);
            let now = Instant::now();
            if now < due {
                wait_until(due);
            } else {
                let lag = now - due;
                lag_max = lag_max.max(lag);
                if lag > max_backlog {
                    sched_base = now - max_backlog;
                    sched_m0 = m;
                }
            }
        }
        let now = Instant::now();
        if now.duration_since(t0) >= max_duration {
            truncated = true;
            break;
        }
        let j = warmup + m;
        let sent_ns = now.duration_since(base).as_nanos() as u64;
        let buf = if a.embed_timestamp { Some(embed(j)) } else { None };
        if let Err(e) = send_one(j, sent_ns, buf.as_deref()) {
            send_err = Some(format!("instance {k}: {e}"));
            break;
        }
        sent += 1;
    }
    let flushed = wait_completed(shared, warmup + sent);
    let end = Instant::now();
    Ok(InstanceOut { t0, end, sent, lag_max, truncated, send_err, flushed })
}

fn run_inner(a: &ProduceArgs, r: &mut Map<String, Value>, process_start: Instant) -> Result<(), String> {
    let kind = PayloadKind::parse(&a.payload).ok_or("invalid --payload")?;
    if a.partitions == 0 {
        return Err("--partitions must be > 0".into());
    }
    instances::validate(a.instances, a.partitions)?;
    if a.message_size == 0 {
        return Err("--message-size must be > 0".into());
    }
    if a.embed_timestamp && a.message_size < 8 {
        return Err("--embed-timestamp requires --message-size >= 8".into());
    }
    if a.idempotence && a.acks != "all" {
        return Err("--idempotence true requires --acks all".into());
    }
    let kk = a.instances as u64;

    let cfg_pairs = config(a, kk);
    r.insert(
        "effective_config".into(),
        Value::Object(cfg_pairs.iter().map(|(k, v)| (k.clone(), json!(v))).collect()),
    );
    let mut cfg = ClientConfig::new();
    for (k, v) in &cfg_pairs {
        cfg.set(k, v);
    }
    cfg.set_log_level(crate::log_level(&cfg_pairs));
    if a.config_profile == "native" {
        let native = match cfg.create_native_config() {
            Ok(n) => n,
            Err(e) => return Err(format!("invalid producer config: {}", e)),
        };
        let defaults: Map<String, Value> = NATIVE_DEFAULTED
            .iter()
            .map(|k| (k.to_string(), native.get(k).map(|v| json!(v)).unwrap_or(Value::Null)))
            .collect();
        r.insert("library_defaults".into(), Value::Object(defaults));
    }

    let base = Instant::now();
    let mut insts = Vec::with_capacity(kk as usize);
    for k in 0..kk {
        let shared = Arc::new(Shared {
            base,
            state: Mutex::new(State {
                measuring: false,
                measure_start_ns: 0,
                completed: 0,
                wait_target: u64::MAX,
                acked: 0,
                errors: 0,
                warmup_errors: 0,
                first_error: None,
                hist: new_histogram(),
                buckets: Vec::new(),
            }),
            cv: Condvar::new(),
        });
        let producer: ThreadedProducer<Ctx> = cfg
            .create_with_context(Ctx { shared: Arc::clone(&shared) })
            .map_err(|e| format!("producer {k} creation failed: {e}"))?;
        insts.push(Instance { k, producer, shared });
    }
    r.insert("startup_ms".into(), json!(ms_since(process_start)));

    let pool = payload::build_pool(kind, a.message_size, a.seed);
    r.insert("payload_sha256".into(), json!(pool.sha256));
    r.insert("pool_count".into(), json!(pool.messages.len()));
    eprintln!("pool: {} messages of {} bytes, sha256 {}", pool.messages.len(), a.message_size, pool.sha256);

    let gate = Gate::new(kk as usize);
    let plan = Plan { a, pool: &pool.messages, kk, gate: &gate };
    // Instance 0 runs on the calling thread so K=1 has the same threads as
    // before; instances 1..K each get one dedicated sending thread.
    let mut rest = insts.into_iter();
    let first = rest.next().expect("at least one instance");
    let (insts, outs): (Vec<Instance>, Vec<Result<InstanceOut, PreFail>>) = thread::scope(|s| {
        let plan = &plan;
        let handles: Vec<_> = rest
            .map(|inst| {
                thread::Builder::new()
                    .name(format!("kbench-send-{}", inst.k))
                    .spawn_scoped(s, move || run_instance(inst, plan))
                    .expect("spawn sending thread")
            })
            .collect();
        let mut outs = vec![run_instance(first, plan)];
        outs.extend(handles.into_iter().map(|h| h.join().expect("sending thread panicked")));
        outs.into_iter().unzip()
    });

    let mut done = Vec::with_capacity(outs.len());
    let mut pre_err = None;
    for o in outs {
        match o {
            Ok(x) => done.push(x),
            Err(PreFail::Failed(e)) => {
                pre_err.get_or_insert(e);
            }
            Err(PreFail::Aborted) => {}
        }
    }
    if let Some(e) = pre_err {
        return Err(e);
    }
    let (Some(sampler), Some(first)) = (gate.take_sampler(), done.first()) else {
        return Err("measured window never started".into());
    };
    let t0 = first.t0;
    let end = done.iter().map(|o| o.end).max().unwrap_or(t0);
    let states: Vec<_> = insts.iter().map(|i| i.shared.state.lock().unwrap()).collect();
    let acked: u64 = states.iter().map(|s| s.acked).sum();
    let window = sampler.finish(end, acked);
    let duration = end.duration_since(t0);

    let mut hist = new_histogram();
    let mut buckets = Vec::new();
    let mut detail = Vec::with_capacity(states.len());
    let mut sent = 0;
    let mut outstanding = 0;
    let mut delivery_errors = 0;
    let mut first_error = None;
    for ((inst, st), o) in insts.iter().zip(&states).zip(&done) {
        hist.add(&st.hist).map_err(|e| format!("histogram merge failed: {e}"))?;
        instances::sum_buckets(&mut buckets, &st.buckets);
        let warmup = instances::share(a.warmup_messages, inst.k, kk);
        sent += o.sent;
        outstanding += (warmup + o.sent).saturating_sub(st.completed);
        delivery_errors += st.errors;
        if first_error.is_none() {
            first_error = st.first_error.clone();
        }
        detail.push(instances::detail(
            inst.k,
            st.acked,
            o.sent.saturating_sub(st.acked),
            o.end.duration_since(t0).as_secs_f64(),
            Some(&st.hist),
        ));
    }
    let truncated = done.iter().any(|o| o.truncated);
    fill_throughput(r, acked, acked * a.message_size as u64, duration.as_secs_f64());
    // Anything sent but not acked (delivery failures, or deliveries still
    // outstanding after a flush timeout) counts as an error.
    r.insert("errors".into(), json!(sent.saturating_sub(acked)));
    r.insert("truncated".into(), json!(truncated));
    if a.rate > 0 {
        let lag_max = done.iter().map(|o| o.lag_max).max().unwrap_or_default();
        r.insert("rate_lag_max_us".into(), json!(lag_max.as_micros() as u64));
    }
    r.insert("latency_us".into(), latency_summary(&hist));
    r.insert("instances_detail".into(), Value::Array(detail));
    r.insert(
        "timeseries".into(),
        resources::timeseries(&buckets, &window.samples, duration.as_nanos() as u64),
    );
    r.insert("resources".into(), window.resources);
    eprintln!(
        "produced {acked} msgs in {:.3} s with {kk} instance(s), {delivery_errors} errors, truncated={truncated}",
        duration.as_secs_f64(),
    );

    if let Some(e) = done.iter().find_map(|o| o.send_err.clone()) {
        return Err(e);
    }
    if done.iter().any(|o| !o.flushed) {
        return Err(format!("flush timed out with {outstanding} of {sent} deliveries outstanding"));
    }
    if let Some(e) = &first_error {
        eprintln!("warning: {delivery_errors} delivery errors, first: {e}");
    }
    r.insert("status".into(), json!("ok"));
    Ok(())
}
