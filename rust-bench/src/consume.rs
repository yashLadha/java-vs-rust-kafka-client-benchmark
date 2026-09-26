use crate::instances;
use crate::resources::{self, Sampler};
use crate::{ConsumeArgs, base_result, fill_throughput, latency_summary, ms_since, new_histogram, set_error};
use hdrhistogram::Histogram;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use rdkafka::error::KafkaError;
use rdkafka::message::Message;
use rdkafka::bindings;
use rdkafka::types::{RDKafkaErrorCode, RDKafkaMessage, RDKafkaQueue, RDKafkaRespErr};
use rdkafka::{ClientContext, Offset, TopicPartitionList};
use serde_json::{Map, Value, json};
use std::ptr;
use std::sync::{Barrier, OnceLock};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const POLL_TIMEOUT: Duration = Duration::from_millis(100);

struct Ctx;

impl ClientContext for Ctx {
    fn log(&self, level: RDKafkaLogLevel, fac: &str, msg: &str) {
        crate::log_to_stderr(level, fac, msg);
    }

    fn error(&self, error: KafkaError, reason: &str) {
        crate::error_to_stderr(error, reason);
    }
}

impl ConsumerContext for Ctx {}

fn config(a: &ConsumeArgs) -> Vec<(String, String)> {
    let mut cfg: Vec<(String, String)> = [
        ("bootstrap.servers", a.bootstrap.clone()),
        ("client.id", a.run_id.clone()),
        ("group.id", a.run_id.clone()),
        ("enable.auto.commit", "false".to_string()),
        ("enable.auto.offset.store", "false".to_string()),
        ("auto.offset.reset", "earliest".to_string()),
        ("fetch.min.bytes", a.fetch_min_bytes.to_string()),
        ("fetch.wait.max.ms", a.fetch_max_wait_ms.to_string()),
        ("max.partition.fetch.bytes", a.max_partition_fetch_bytes.to_string()),
        ("fetch.max.bytes", a.fetch_max_bytes.to_string()),
        ("check.crcs", a.check_crcs.to_string()),
    ]
    .into_iter()
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

pub fn run(a: &ConsumeArgs, params: Value, process_start: Instant) -> Map<String, Value> {
    let mut r = base_result("consume", &a.run_id, params);
    r.insert("instances".into(), json!(a.instances));
    r.insert("window_kind".into(), json!(if a.instances == 1 { "first_to_last" } else { "union" }));
    if a.measure_e2e {
        r.insert("latency_kind".into(), json!("e2e"));
    }
    if let Err(e) = run_inner(a, &mut r, process_start) {
        set_error(&mut r, e);
    }
    r
}

struct Instance {
    k: u64,
    consumer: BaseConsumer<Ctx>,
}

struct Plan<'a> {
    a: &'a ConsumeArgs,
    kk: u64,
    // None for K=1, where there is nothing to wait for.
    start_gate: Option<&'a Barrier>,
    // Set once by the first instance to cross its warmup boundary: the
    // process window start (min start_k) and the sampler anchored there.
    window_start: &'a OnceLock<(Instant, Sampler)>,
    deadline: Instant,
}

struct InstanceOut {
    k: u64,
    warmup: u64,
    received: u64,
    bytes: u64,
    errors: u64,
    first_error: Option<String>,
    hist: Histogram<u64>,
    buckets: Vec<u64>,
    released: Instant,
    first_msg: Option<Instant>,
    t_start: Option<Instant>,
    last_receipt: Option<Instant>,
    timed_out: bool,
    fatal: Option<String>,
}

impl InstanceOut {
    fn record_error(&mut self, e: KafkaError) {
        self.errors += 1;
        if self.first_error.is_none() {
            eprintln!("instance {}: consume error: {e}", self.k);
            self.first_error = Some(e.to_string());
        }
    }
}

// Taken by value so the client handle lives on the polling thread's stack,
// matching produce.rs (see the false-sharing note there).
fn run_instance(inst: Instance, plan: &Plan) -> (Instance, InstanceOut) {
    let out = drive_instance(&inst, plan);
    (inst, out)
}

fn drive_instance(inst: &Instance, plan: &Plan) -> InstanceOut {
    let a = plan.a;
    let k = inst.k;
    let consumer = &inst.consumer;
    let warmup = instances::share(a.warmup_messages, k, plan.kk);
    let total = warmup + instances::share(a.num_messages, k, plan.kk);
    if let Some(gate) = plan.start_gate {
        gate.wait();
    }
    let released = Instant::now();
    let mut out = InstanceOut {
        k,
        warmup,
        received: 0,
        bytes: 0,
        errors: 0,
        first_error: None,
        hist: new_histogram(),
        buckets: Vec::new(),
        released,
        first_msg: None,
        t_start: None,
        last_receipt: None,
        timed_out: false,
        fatal: None,
    };
    if let Err(e) = assign(consumer, &a.topic, a.partitions, k, plan.kk) {
        out.fatal = Some(format!("instance {k}: {e}"));
        return out;
    }
    if a.consume_api == "batch" {
        match BatchSource::new(consumer, a.batch_max) {
            Some(mut src) => consume_loop(&mut src, plan, &mut out, total),
            None => out.fatal = Some(format!("instance {k}: no consumer queue for the batch API")),
        }
    } else {
        consume_loop(&mut PollSource { consumer }, plan, &mut out, total);
    }
    out
}

trait Source {
    // Hands every record of one poll to `f`; false when nothing arrived before the poll timeout.
    fn next<F: FnMut(Result<&[u8], KafkaError>)>(&mut self, f: F) -> bool;
}

struct PollSource<'a> {
    consumer: &'a BaseConsumer<Ctx>,
}

impl Source for PollSource<'_> {
    #[inline(always)]
    fn next<F: FnMut(Result<&[u8], KafkaError>)>(&mut self, mut f: F) -> bool {
        match self.consumer.poll(POLL_TIMEOUT) {
            None => false,
            Some(Err(e)) => {
                f(Err(e));
                true
            }
            Some(Ok(m)) => {
                f(Ok(m.payload().unwrap_or(&[])));
                true
            }
        }
    }
}

// librdkafka's batch API returns up to `batch_max` records per call, the
// counterpart of Java's poll() returning up to max.poll.records. rust-rdkafka
// 0.39 does not wrap it, so it is called through the C bindings.
struct BatchSource {
    queue: *mut RDKafkaQueue,
    buf: Vec<*mut RDKafkaMessage>,
}

impl BatchSource {
    fn new(consumer: &BaseConsumer<Ctx>, batch_max: usize) -> Option<Self> {
        let queue = unsafe { bindings::rd_kafka_queue_get_consumer(consumer.client().native_ptr()) };
        if queue.is_null() {
            return None;
        }
        Some(BatchSource { queue, buf: vec![ptr::null_mut(); batch_max.max(1)] })
    }
}

// rd_kafka_consume_batch_queue blocks until the buffer is full or the timeout
// expires, so a single call with the poll timeout waits up to 100 ms even when
// records are queued. Java's poll() returns as soon as any record is
// available; this matches it: take what is queued without waiting, block for
// one record only when the queue is empty, then take what arrived with it.
fn fill<T>(buf: &mut [T], mut fetch: impl FnMut(i32, &mut [T]) -> usize) -> usize {
    let n = fetch(0, buf);
    if n > 0 {
        return n;
    }
    if fetch(POLL_TIMEOUT.as_millis() as i32, &mut buf[..1]) == 0 {
        return 0;
    }
    if buf.len() == 1 {
        return 1;
    }
    1 + fetch(0, &mut buf[1..])
}

impl Source for BatchSource {
    fn next<F: FnMut(Result<&[u8], KafkaError>)>(&mut self, mut f: F) -> bool {
        let queue = self.queue;
        let n = fill(&mut self.buf, |timeout_ms, buf| {
            let n = unsafe { bindings::rd_kafka_consume_batch_queue(queue, timeout_ms, buf.as_mut_ptr(), buf.len()) };
            n.max(0) as usize
        });
        if n == 0 {
            return false;
        }
        for &m in &self.buf[..n] {
            unsafe {
                let msg = &*m;
                if msg.err != RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR {
                    f(Err(KafkaError::MessageConsumption(RDKafkaErrorCode::from(msg.err))));
                } else if msg.payload.is_null() {
                    f(Ok(&[]));
                } else {
                    f(Ok(std::slice::from_raw_parts(msg.payload as *const u8, msg.len)));
                }
                bindings::rd_kafka_message_destroy(m);
            }
        }
        true
    }
}

impl Drop for BatchSource {
    fn drop(&mut self) {
        unsafe { bindings::rd_kafka_queue_destroy(self.queue) };
    }
}

fn consume_loop<S: Source>(src: &mut S, plan: &Plan, out: &mut InstanceOut, total: u64) {
    let a = plan.a;
    let deadline = plan.deadline;
    let warmup = out.warmup;
    let mut t_base: Option<Instant> = None;
    while out.received < total && !out.timed_out {
        let got = src.next(|r| {
            let payload = match r {
                Ok(p) => p,
                Err(e) => return out.record_error(e),
            };
            // A batch can overshoot the target; the surplus is not counted.
            if out.received >= total {
                return;
            }
            let now = Instant::now();
            if out.received == 0 {
                out.first_msg = Some(now);
            }
            if out.received >= warmup {
                let base = match t_base {
                    Some(t) => t,
                    None => {
                        let base = plan.window_start.get_or_init(|| (now, Sampler::start(now))).0;
                        out.t_start = Some(now);
                        t_base = Some(base);
                        base
                    }
                };
                out.last_receipt = Some(now);
                out.bytes += payload.len() as u64;
                let bucket = (now.duration_since(base).as_nanos() / 1_000_000_000) as usize;
                if bucket >= out.buckets.len() {
                    out.buckets.resize(bucket + 1, 0);
                }
                out.buckets[bucket] += 1;
                if a.measure_e2e && payload.len() >= 8 {
                    let wall = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_nanos() as i64)
                        .unwrap_or(0);
                    let ts = i64::from_be_bytes(payload[..8].try_into().expect("8 bytes"));
                    out.hist.saturating_record((wall - ts).max(0) as u64 / 1000);
                }
            }
            out.received += 1;
            if now >= deadline && out.received < total {
                out.timed_out = true;
            }
        });
        if !got && Instant::now() >= deadline {
            out.timed_out = true;
        }
    }
}

fn assign(consumer: &BaseConsumer<Ctx>, topic: &str, partitions: u32, k: u64, kk: u64) -> Result<(), String> {
    let mut tpl = TopicPartitionList::new();
    for p in (k as i32..partitions as i32).step_by(kk as usize) {
        tpl.add_partition_offset(topic, p, Offset::Beginning)
            .map_err(|e| format!("building assignment failed: {e}"))?;
    }
    consumer.assign(&tpl).map_err(|e| format!("assign failed: {e}"))
}

fn run_inner(a: &ConsumeArgs, r: &mut Map<String, Value>, process_start: Instant) -> Result<(), String> {
    if a.partitions == 0 {
        return Err("--partitions must be > 0".into());
    }
    if a.num_messages == 0 {
        return Err("--num-messages must be > 0".into());
    }
    instances::validate(a.instances, a.partitions)?;
    let kk = a.instances as u64;
    let cfg_pairs = config(a);
    r.insert(
        "effective_config".into(),
        Value::Object(cfg_pairs.iter().map(|(k, v)| (k.clone(), json!(v))).collect()),
    );
    let mut cfg = ClientConfig::new();
    for (k, v) in &cfg_pairs {
        cfg.set(k, v);
    }
    cfg.set_log_level(crate::log_level(&cfg_pairs));

    let mut insts = Vec::with_capacity(kk as usize);
    for k in 0..kk {
        let consumer: BaseConsumer<Ctx> =
            cfg.create_with_context(Ctx).map_err(|e| format!("consumer {k} creation failed: {e}"))?;
        insts.push(Instance { k, consumer });
    }
    r.insert("startup_ms".into(), json!(ms_since(process_start)));

    let start_gate = Barrier::new(kk as usize);
    let window_start = OnceLock::new();
    let plan = Plan {
        a,
        kk,
        start_gate: if kk > 1 { Some(&start_gate) } else { None },
        window_start: &window_start,
        deadline: Instant::now() + Duration::from_secs(a.timeout_s),
    };
    // Instance 0 polls on the calling thread so K=1 has the same threads as
    // before; instances 1..K each get one dedicated polling thread.
    let mut rest = insts.into_iter();
    let first = rest.next().expect("at least one instance");
    let (_insts, outs): (Vec<Instance>, Vec<InstanceOut>) = thread::scope(|s| {
        let plan = &plan;
        let handles: Vec<_> = rest
            .map(|inst| {
                thread::Builder::new()
                    .name(format!("kbench-poll-{}", inst.k))
                    .spawn_scoped(s, move || run_instance(inst, plan))
                    .expect("spawn polling thread")
            })
            .collect();
        let mut outs = vec![run_instance(first, plan)];
        outs.extend(handles.into_iter().map(|h| h.join().expect("polling thread panicked")));
        outs.into_iter().unzip()
    });

    if let Some(e) = outs.iter().find_map(|o| o.fatal.clone()) {
        return Err(e);
    }
    let received: u64 = outs.iter().map(|o| o.received).sum();
    let total = a.warmup_messages + a.num_messages;
    let measured: u64 = outs.iter().map(|o| o.received.saturating_sub(o.warmup)).sum();
    let errors: u64 = outs.iter().map(|o| o.errors).sum();
    let released = outs.iter().map(|o| o.released).min().expect("at least one instance");
    if let Some(first) = outs.iter().filter_map(|o| o.first_msg).min() {
        let ms = first.duration_since(released).as_secs_f64() * 1000.0;
        r.insert("first_message_ms".into(), json!(ms));
    }
    let mut hist = new_histogram();
    if let Some((t_start, sampler)) = window_start.into_inner() {
        let t_end = outs.iter().filter_map(|o| o.last_receipt).max().unwrap_or(t_start);
        let res = sampler.finish(t_end, measured);
        let duration = t_end.duration_since(t_start);
        let bytes = outs.iter().map(|o| o.bytes).sum();
        fill_throughput(r, measured, bytes, duration.as_secs_f64());
        let mut buckets = Vec::new();
        let mut detail = Vec::with_capacity(outs.len());
        for o in &outs {
            instances::sum_buckets(&mut buckets, &o.buckets);
            hist.add(&o.hist).map_err(|e| format!("histogram merge failed: {e}"))?;
            let d = match (o.t_start, o.last_receipt) {
                (Some(s), Some(e)) => e.duration_since(s).as_secs_f64(),
                _ => 0.0,
            };
            let lat = if a.measure_e2e { Some(&o.hist) } else { None };
            detail.push(instances::detail(o.k, o.received.saturating_sub(o.warmup), o.errors, d, lat));
        }
        r.insert(
            "timeseries".into(),
            resources::timeseries(&buckets, &res.samples, duration.as_nanos() as u64),
        );
        r.insert("resources".into(), res.resources);
        r.insert("instances_detail".into(), Value::Array(detail));
    }
    r.insert("errors".into(), json!(errors));
    if a.measure_e2e {
        r.insert("latency_us".into(), latency_summary(&hist));
    }
    eprintln!("consumed {measured} measured msgs ({received} total) with {kk} instance(s), {errors} errors");

    if outs.iter().any(|o| o.timed_out) {
        r.insert("status".into(), json!("timeout"));
        r.insert(
            "error".into(),
            json!(format!("timeout after {} s: received {received} of {total}", a.timeout_s)),
        );
        eprintln!("timeout: received {received} of {total}");
        return Ok(());
    }
    if let Some(e) = outs.iter().find_map(|o| o.first_error.as_ref()) {
        eprintln!("warning: {errors} consume errors, first: {e}");
    }
    r.insert("status".into(), json!("ok"));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{POLL_TIMEOUT, fill};

    // Simulates the batch API over a queue: arrivals[i] records become visible
    // only to the i-th blocking call.
    struct Queue {
        queued: usize,
        arrivals: Vec<usize>,
        calls: Vec<(i32, usize)>,
    }

    impl Queue {
        fn fetch(&mut self, timeout_ms: i32, buf: &mut [u32]) -> usize {
            self.calls.push((timeout_ms, buf.len()));
            if self.queued == 0 && timeout_ms > 0 && !self.arrivals.is_empty() {
                self.queued = self.arrivals.remove(0);
            }
            let n = self.queued.min(buf.len());
            self.queued -= n;
            n
        }
    }

    fn queue(queued: usize, arrivals: &[usize]) -> Queue {
        Queue { queued, arrivals: arrivals.to_vec(), calls: Vec::new() }
    }

    #[test]
    fn fill_returns_queued_records_without_blocking() {
        let mut q = queue(3, &[]);
        let mut buf = [0u32; 500];
        assert_eq!(fill(&mut buf, |t, b| q.fetch(t, b)), 3);
        assert_eq!(q.calls, vec![(0, 500)]);
    }

    #[test]
    fn fill_caps_at_buffer_size() {
        let mut q = queue(700, &[]);
        let mut buf = [0u32; 500];
        assert_eq!(fill(&mut buf, |t, b| q.fetch(t, b)), 500);
        assert_eq!(q.queued, 200);
        assert_eq!(q.calls, vec![(0, 500)]);
    }

    #[test]
    fn fill_blocks_for_one_record_then_drains() {
        let mut q = queue(0, &[4]);
        let mut buf = [0u32; 500];
        assert_eq!(fill(&mut buf, |t, b| q.fetch(t, b)), 4);
        let timeout = POLL_TIMEOUT.as_millis() as i32;
        assert_eq!(q.calls, vec![(0, 500), (timeout, 1), (0, 499)]);
    }

    #[test]
    fn fill_returns_zero_when_nothing_arrives() {
        let mut q = queue(0, &[]);
        let mut buf = [0u32; 500];
        assert_eq!(fill(&mut buf, |t, b| q.fetch(t, b)), 0);
        assert_eq!(q.calls.len(), 2);
    }

    #[test]
    fn fill_with_single_slot_buffer_skips_empty_drain() {
        let mut q = queue(0, &[2]);
        let mut buf = [0u32; 1];
        assert_eq!(fill(&mut buf, |t, b| q.fetch(t, b)), 1);
        assert_eq!(q.calls.len(), 2);
    }
}
