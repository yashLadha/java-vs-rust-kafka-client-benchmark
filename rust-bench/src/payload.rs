use sha2::{Digest, Sha256};

const POOL_MAX_COUNT: usize = 16384;
const POOL_MAX_BYTES: usize = 67_108_864;

const WORDS: [&str; 32] = [
    "kafka", "stream", "broker", "topic", "partition", "offset", "consumer", "producer", "record",
    "batch", "leader", "follower", "replica", "commit", "segment", "index", "latency",
    "throughput", "cluster", "message", "payload", "header", "key", "value", "timestamp", "schema",
    "event", "log", "queue", "fetch", "poll", "ack",
];

pub struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    pub fn next(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum PayloadKind {
    Random,
    Text,
}

impl PayloadKind {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "random" => Some(Self::Random),
            "text" => Some(Self::Text),
            _ => None,
        }
    }
}

pub fn pool_count(message_size: usize) -> usize {
    POOL_MAX_COUNT.min(POOL_MAX_BYTES / message_size)
}

pub struct Pool {
    pub messages: Vec<Vec<u8>>,
    pub sha256: String,
}

pub fn build_pool(kind: PayloadKind, message_size: usize, seed: u64) -> Pool {
    let count = pool_count(message_size);
    let mut rng = SplitMix64::new(seed);
    let mut hasher = Sha256::new();
    let mut messages = Vec::with_capacity(count);
    for _ in 0..count {
        let msg = match kind {
            PayloadKind::Random => random_message(&mut rng, message_size),
            PayloadKind::Text => text_message(&mut rng, message_size),
        };
        hasher.update(&msg);
        messages.push(msg);
    }
    let digest = hasher.finalize();
    let mut sha256 = String::with_capacity(64);
    for b in digest.iter() {
        sha256.push_str(&format!("{b:02x}"));
    }
    Pool { messages, sha256 }
}

fn random_message(rng: &mut SplitMix64, size: usize) -> Vec<u8> {
    let mut msg = Vec::with_capacity(size);
    while msg.len() < size {
        let bytes = rng.next().to_le_bytes();
        let take = (size - msg.len()).min(8);
        msg.extend_from_slice(&bytes[..take]);
    }
    msg
}

fn text_message(rng: &mut SplitMix64, size: usize) -> Vec<u8> {
    let mut msg = Vec::with_capacity(size + 16);
    while msg.len() < size {
        msg.extend_from_slice(WORDS[(rng.next() % 32) as usize].as_bytes());
        msg.push(b' ');
    }
    msg.truncate(size);
    msg
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splitmix_reference_values() {
        let mut rng = SplitMix64::new(0);
        assert_eq!(rng.next(), 0xE220_A839_7B1D_CDAF);
        assert_eq!(rng.next(), 0x6E78_9E6A_A1B9_65F4);
    }

    #[test]
    fn pool_sizes() {
        assert_eq!(pool_count(100), 16384);
        assert_eq!(pool_count(10240), 6553);
        let pool = build_pool(PayloadKind::Text, 13, 42);
        assert!(pool.messages.iter().all(|m| m.len() == 13));
        let pool = build_pool(PayloadKind::Random, 13, 42);
        assert!(pool.messages.iter().all(|m| m.len() == 13));
    }
}
