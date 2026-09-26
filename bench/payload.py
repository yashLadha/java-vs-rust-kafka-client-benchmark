"""Independent Python reference of the contract payload corpus.

Used as a third opinion next to the Java and Rust `payload-hash` subcommands, so a
bug shared by both harnesses (unlikely, but possible when both were written from the
same text) still shows up.
"""

import hashlib
import json
import re
import struct

MASK = 0xFFFFFFFFFFFFFFFF
POOL_BYTES = 67108864
POOL_MAX = 16384
WORDS = [
    "kafka", "stream", "broker", "topic", "partition", "offset", "consumer", "producer",
    "record", "batch", "leader", "follower", "replica", "commit", "segment", "index",
    "latency", "throughput", "cluster", "message", "payload", "header", "key", "value",
    "timestamp", "schema", "event", "log", "queue", "fetch", "poll", "ack",
]
WORD_BYTES = [(w + " ").encode("ascii") for w in WORDS]


def pool_count(message_size):
    return min(POOL_MAX, POOL_BYTES // message_size)


def payload_sha256(payload, message_size, seed=42):
    state = seed & MASK
    h = hashlib.sha256()
    n_full, rem = divmod(message_size, 8)
    pack = struct.Struct("<Q").pack
    for _ in range(pool_count(message_size)):
        if payload == "random":
            outs = []
            for _ in range(n_full + (1 if rem else 0)):
                state = (state + 0x9E3779B97F4A7C15) & MASK
                z = state
                z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & MASK
                z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & MASK
                outs.append(z ^ (z >> 31))
            msg = b"".join(pack(o) for o in outs)[:message_size]
        elif payload == "text":
            parts = []
            length = 0
            while length < message_size:
                state = (state + 0x9E3779B97F4A7C15) & MASK
                z = state
                z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & MASK
                z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & MASK
                w = WORD_BYTES[(z ^ (z >> 31)) & 31]
                parts.append(w)
                length += len(w)
            msg = b"".join(parts)[:message_size]
        else:
            raise ValueError("unknown payload " + payload)
        h.update(msg)
    return h.hexdigest()


_HEX64 = re.compile(r"\b[0-9a-f]{64}\b")


def parse_hash_output(stdout):
    """Harness `payload-hash` output format is not fixed by the contract: accept a
    RESULT/JSON line with payload_sha256, or the first bare 64-char hex token."""
    for line in stdout.splitlines():
        s = line.strip()
        if s.startswith("RESULT "):
            s = s[len("RESULT "):]
        if s.startswith("{"):
            try:
                obj = json.loads(s)
            except ValueError:
                continue
            for k in ("payload_sha256", "sha256", "hash"):
                if isinstance(obj.get(k), str):
                    return obj[k].lower()
    m = _HEX64.search(stdout.lower())
    return m.group(0) if m else None


if __name__ == "__main__":
    import sys
    print(payload_sha256(sys.argv[1], int(sys.argv[2]), int(sys.argv[3]) if len(sys.argv) > 3 else 42))
