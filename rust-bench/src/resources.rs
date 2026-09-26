use serde_json::{Value, json};
use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const SAMPLE_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Clone, Copy)]
pub struct Sample {
    pub t_ns: u64,
    pub rss: u64,
    pub threads: u64,
}

pub fn clk_tck() -> i64 {
    // SAFETY: sysconf has no preconditions.
    unsafe { libc::sysconf(libc::_SC_CLK_TCK) as i64 }
}

pub fn read_status() -> (u64, u64) {
    let mut rss = 0;
    let mut threads = 0;
    if let Ok(s) = fs::read_to_string("/proc/self/status") {
        for line in s.lines() {
            if let Some(v) = line.strip_prefix("VmRSS:") {
                rss = v.trim().trim_end_matches("kB").trim().parse::<u64>().unwrap_or(0) * 1024;
            } else if let Some(v) = line.strip_prefix("Threads:") {
                threads = v.trim().parse().unwrap_or(0);
            }
        }
    }
    (rss, threads)
}

/// Returns (utime, stime) in clock ticks.
pub fn read_cpu_ticks() -> (u64, u64) {
    let Ok(s) = fs::read_to_string("/proc/self/stat") else {
        return (0, 0);
    };
    // The comm field may contain spaces and parentheses, so split after the last ')'.
    let Some(pos) = s.rfind(')') else {
        return (0, 0);
    };
    let fields: Vec<&str> = s[pos + 1..].split_whitespace().collect();
    // fields[0] is field 3 (state); utime and stime are fields 14 and 15.
    let utime = fields.get(11).and_then(|v| v.parse().ok()).unwrap_or(0);
    let stime = fields.get(12).and_then(|v| v.parse().ok()).unwrap_or(0);
    (utime, stime)
}

fn read_u64(path: &str) -> Option<u64> {
    fs::read_to_string(path).ok()?.trim().parse().ok()
}

/// Cumulative cgroup CPU usage in nanoseconds.
pub fn cgroup_cpu_ns() -> Option<u64> {
    if let Some(v) = read_u64("/sys/fs/cgroup/cpuacct/cpuacct.usage") {
        return Some(v);
    }
    if let Some(v) = read_u64("/sys/fs/cgroup/cpu,cpuacct/cpuacct.usage") {
        return Some(v);
    }
    let stat = fs::read_to_string("/sys/fs/cgroup/cpu.stat").ok()?;
    stat.lines()
        .find_map(|l| l.strip_prefix("usage_usec "))
        .and_then(|v| v.trim().parse::<u64>().ok())
        .map(|us| us * 1000)
}

pub fn cgroup_mem_peak() -> Option<u64> {
    read_u64("/sys/fs/cgroup/memory/memory.max_usage_in_bytes")
        .or_else(|| read_u64("/sys/fs/cgroup/memory.peak"))
}

pub struct Sampler {
    start: Instant,
    start_ticks: (u64, u64),
    start_cgroup_cpu: Option<u64>,
    first: Sample,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<Vec<Sample>>>,
}

pub struct Window {
    pub samples: Vec<Sample>,
    pub resources: Value,
}

impl Sampler {
    pub fn start(start: Instant) -> Self {
        let start_ticks = read_cpu_ticks();
        let start_cgroup_cpu = cgroup_cpu_ns();
        let (rss, threads) = read_status();
        let first = Sample { t_ns: 0, rss, threads };
        let stop = Arc::new(AtomicBool::new(false));
        let stop_t = Arc::clone(&stop);
        let handle = thread::Builder::new()
            .name("kbench-sampler".into())
            .spawn(move || {
                let mut samples = Vec::with_capacity(4096);
                let mut k: u32 = 1;
                loop {
                    let due = start + SAMPLE_INTERVAL * k;
                    let now = Instant::now();
                    if due > now {
                        thread::park_timeout(due - now);
                    }
                    if stop_t.load(Ordering::Acquire) {
                        break;
                    }
                    if Instant::now() < due {
                        continue;
                    }
                    let (rss, threads) = read_status();
                    samples.push(Sample { t_ns: start.elapsed().as_nanos() as u64, rss, threads });
                    k += 1;
                }
                samples
            })
            .expect("spawn sampler");
        Self { start, start_ticks, start_cgroup_cpu, first, stop, handle: Some(handle) }
    }

    pub fn finish(mut self, end: Instant, messages: u64) -> Window {
        let end_ticks = read_cpu_ticks();
        let end_cgroup_cpu = cgroup_cpu_ns();
        let mem_peak = cgroup_mem_peak();
        let (rss, threads) = read_status();
        self.stop.store(true, Ordering::Release);
        let handle = self.handle.take().expect("sampler handle");
        handle.thread().unpark();
        let mut samples = vec![self.first];
        samples.extend(handle.join().unwrap_or_default());
        samples.push(Sample { t_ns: end.duration_since(self.start).as_nanos() as u64, rss, threads });
        samples.sort_by_key(|s| s.t_ns);

        let tck = clk_tck().max(1) as f64;
        let user = end_ticks.0.saturating_sub(self.start_ticks.0) as f64 / tck;
        let sys = end_ticks.1.saturating_sub(self.start_ticks.1) as f64 / tck;
        let total = user + sys;
        let duration_s = end.duration_since(self.start).as_secs_f64();
        let rss_avg = samples.iter().map(|s| s.rss as u128).sum::<u128>() / samples.len() as u128;
        let rss_peak = samples.iter().map(|s| s.rss).max().unwrap_or(0);
        let threads_peak = samples.iter().map(|s| s.threads).max().unwrap_or(0);
        let cgroup_cpu_s = match (self.start_cgroup_cpu, end_cgroup_cpu) {
            (Some(a), Some(b)) => json!(b.saturating_sub(a) as f64 / 1e9),
            _ => Value::Null,
        };
        let resources = json!({
            "clk_tck": clk_tck(),
            "cpu_user_s": user,
            "cpu_sys_s": sys,
            "cpu_total_s": total,
            "cpu_cores_avg": if duration_s > 0.0 { total / duration_s } else { 0.0 },
            "cpu_s_per_million_msgs": if messages > 0 { total / messages as f64 * 1e6 } else { 0.0 },
            "rss_start_bytes": self.first.rss,
            "rss_avg_bytes": rss_avg as u64,
            "rss_peak_bytes": rss_peak,
            "threads_peak": threads_peak,
            "cgroup_cpu_s": cgroup_cpu_s,
            "cgroup_mem_peak_bytes": mem_peak,
        });
        Window { samples, resources }
    }
}

/// Builds the per-second series. `buckets[k]` holds the count for [k s, (k+1) s).
pub fn timeseries(buckets: &[u64], samples: &[Sample], duration_ns: u64) -> Value {
    let n_buckets = (duration_ns.div_ceil(1_000_000_000) as usize).max(buckets.len());
    let mut out = Vec::with_capacity(n_buckets);
    for k in 0..n_buckets {
        let bucket_end = ((k as u64 + 1) * 1_000_000_000).min(duration_ns);
        let rss = samples
            .iter()
            .take_while(|s| s.t_ns <= bucket_end)
            .last()
            .or(samples.first())
            .map(|s| s.rss)
            .unwrap_or(0);
        out.push(json!({"t": k + 1, "msgs": buckets.get(k).copied().unwrap_or(0), "rss_bytes": rss}));
    }
    Value::Array(out)
}
