use crate::resources::Sampler;
use hdrhistogram::Histogram;
use serde_json::{Value, json};
use std::sync::{Condvar, Mutex};
use std::time::Instant;

/// Share of `total` handled by instance `k` of `kk`: the first `total % kk`
/// instances take one extra.
pub fn share(total: u64, k: u64, kk: u64) -> u64 {
    total / kk + u64::from(k < total % kk)
}

pub fn validate(instances: u32, partitions: u32) -> Result<(), String> {
    if instances == 0 {
        return Err("--instances must be > 0".into());
    }
    if partitions < instances {
        return Err(format!("--partitions ({partitions}) must be >= --instances ({instances})"));
    }
    Ok(())
}

struct GateState {
    arrived: usize,
    t0: Option<Instant>,
    aborted: bool,
    sampler: Option<Sampler>,
}

/// One-shot barrier that yields a single `t0` for all instances. The last
/// instance to arrive takes `t0` and starts the resource sampler before
/// releasing the others, which for K=1 reproduces the old order (t0, sampler
/// start, first measured send on the same thread).
pub struct Gate {
    parties: usize,
    state: Mutex<GateState>,
    cv: Condvar,
}

impl Gate {
    pub fn new(parties: usize) -> Self {
        Self {
            parties,
            state: Mutex::new(GateState { arrived: 0, t0: None, aborted: false, sampler: None }),
            cv: Condvar::new(),
        }
    }

    /// Returns `None` if the gate was aborted or `deadline` passed first.
    pub fn arrive(&self, deadline: Option<Instant>) -> Option<Instant> {
        let mut st = self.state.lock().unwrap();
        if st.aborted {
            return None;
        }
        st.arrived += 1;
        if st.arrived == self.parties {
            let t0 = Instant::now();
            st.sampler = Some(Sampler::start(t0));
            st.t0 = Some(t0);
            self.cv.notify_all();
            return Some(t0);
        }
        loop {
            if let Some(t0) = st.t0 {
                return Some(t0);
            }
            if st.aborted {
                return None;
            }
            st = match deadline {
                None => self.cv.wait(st).unwrap(),
                Some(d) => {
                    let now = Instant::now();
                    if now >= d {
                        st.aborted = true;
                        self.cv.notify_all();
                        return None;
                    }
                    self.cv.wait_timeout(st, d - now).unwrap().0
                }
            };
        }
    }

    pub fn abort(&self) {
        let mut st = self.state.lock().unwrap();
        if st.t0.is_none() {
            st.aborted = true;
            self.cv.notify_all();
        }
    }

    pub fn take_sampler(&self) -> Option<Sampler> {
        self.state.lock().unwrap().sampler.take()
    }
}

pub fn sum_buckets(into: &mut Vec<u64>, from: &[u64]) {
    if from.len() > into.len() {
        into.resize(from.len(), 0);
    }
    for (a, b) in into.iter_mut().zip(from) {
        *a += b;
    }
}

pub fn detail(k: u64, messages: u64, errors: u64, duration_s: f64, hist: Option<&Histogram<u64>>) -> Value {
    let tput = if duration_s > 0.0 { messages as f64 / duration_s } else { 0.0 };
    let latency = match hist {
        None => Value::Null,
        Some(h) if h.is_empty() => json!({"p50": 0, "p99": 0, "max": 0}),
        Some(h) => json!({
            "p50": h.value_at_quantile(0.50),
            "p99": h.value_at_quantile(0.99),
            "max": h.max(),
        }),
    };
    json!({
        "k": k,
        "messages": messages,
        "errors": errors,
        "duration_s": duration_s,
        "throughput_msgs_per_s": tput,
        "latency_us": latency,
    })
}

#[cfg(test)]
mod tests {
    use super::share;

    #[test]
    fn share_splits_remainder_to_first_instances() {
        let parts: Vec<u64> = (0..4).map(|k| share(10, k, 4)).collect();
        assert_eq!(parts, vec![3, 3, 2, 2]);
        assert_eq!((0..16).map(|k| share(7, k, 16)).sum::<u64>(), 7);
        assert_eq!(share(5, 0, 1), 5);
    }
}
