"""Fake RESULT generator for testing run.py and report.py without docker.

Invoked by run.py --fake in place of `docker run <image> ...`, with exactly the argv
that would follow the image name, so the orchestrator's command construction is
exercised too. Every object it emits carries "fake": true; report.py refuses to
present such data without a banner.

usage: fake.py <java|rust> <produce|consume|payload-hash> [harness flags]
       (rust-tuned runs the rust image, so it arrives here as rust with its extra flags)
       fake.py java-stock <producer|consumer> [stock-perf.sh flags]
       fake.py infra <hostinfo|broker-stats>
"""

import argparse
import hashlib
import json
import math
import os
import random
import re
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import payload as payload_mod  # noqa: E402

# msgs/s at 1024 B baseline, and per-size scaling, per client. Loosely plausible values.
BASE_RATE = {"java": 420_000.0, "rust": 470_000.0, "java-stock": 400_000.0}
SIZE_RATE = {100: 3.2, 1024: 1.0, 10240: 0.12}
CODEC_RATE = {"none": 1.0, "gzip": 0.28, "snappy": 1.15, "lz4": 1.25, "zstd": 0.9}
CODEC_RATE_RUST = {"gzip": 0.33, "zstd": 1.0}
CPU_PER_M = {"java": 2.4, "rust": 1.6, "java-stock": 2.6}
# Throughput multiplier by batch.size relative to 16 KiB, interpolated in log2(batch.size). Shaped
# after the diagnosis: librdkafka sends one partition per ProduceRequest, so Rust keeps gaining up
# to 1 MiB while Java peaks near 128 KiB.
BATCH_CURVE = {
    "java": {16384: 1.0, 32768: 1.5, 65536: 2.0, 131072: 2.45, 262144: 2.2, 524288: 1.8, 1048576: 1.5},
    "rust": {16384: 0.5, 32768: 0.75, 65536: 1.0, 131072: 1.3, 262144: 1.6, 524288: 1.9, 1048576: 2.1},
}
LIBRDKAFKA_DEFAULTS = {"batch.num.messages": "10000", "max.in.flight.requests.per.connection": "1000000",
                       "queue.buffering.max.kbytes": "1048576", "queue.buffering.max.messages": "100000",
                       "message.send.max.retries": "2147483647", "message.timeout.ms": "300000",
                       "request.timeout.ms": "30000"}
# Aggregate throughput of K instances is r1 * K / (1 + c * (K - 1)), then limited by the client's
# 16 vCPUs and by broker capacity, so the scaling curves have a knee and a bottleneck to find.
SCALE_CONTENTION = {"java": 0.22, "rust": 0.12}
CLIENT_VCPUS = 16
BROKER_VCPUS_BUSY = 13.4
BROKER_CAP_MSGS = {("produce", 100): 5.5e6, ("produce", 1024): 1.3e6, ("produce", 10240): 1.4e5,
                   ("consume", 100): 11e6, ("consume", 1024): 2.6e6, ("consume", 10240): 2.8e5}


def broker_cap(mode, size, compressed):
    return BROKER_CAP_MSGS[(mode, size)] * (1.6 if compressed else 1.0)


def scaled_rate(client, r1, k, rng, mode, size, compressed):
    r = r1 * k / (1 + SCALE_CONTENTION.get(client, 0.2) * (k - 1))
    cpu_per_m = CPU_PER_M[client] * (1 + 0.02 * (k - 1))
    r = min(r, (CLIENT_VCPUS - 0.3) * 1e6 / cpu_per_m * rng.uniform(0.97, 1.0))
    return min(r, broker_cap(mode, size, compressed) * rng.uniform(0.97, 1.0))


def broker_cpu_s(res):
    """Broker cgroup CPU charged for one fake run, consistent with broker_cap()."""
    mode = res.get("mode")
    p = res.get("params") or {}
    msgs, dur = res.get("messages") or 0, res.get("duration_s") or 0.0
    if mode not in ("produce", "consume") or not msgs or not dur:
        return 0.0
    if mode == "produce":
        size, compressed = p.get("message_size") or 1024, p.get("compression", "none") != "none"
    else:
        size, compressed = _size_from_topic(p.get("topic", "")), _compressed_topic(p.get("topic", ""))
    cores = BROKER_VCPUS_BUSY * min(1.0, (msgs / dur) / broker_cap(mode, size, compressed)) + 0.3
    warm = p.get("warmup_messages") or 0
    return cores * dur * (msgs + warm) / msgs


def batch_factor(client, batch_size):
    curve = BATCH_CURVE[client]
    pts = sorted(curve.items())
    x = math.log2(max(batch_size, 1))
    if x <= math.log2(pts[0][0]):
        return pts[0][1]
    for (b0, f0), (b1, f1) in zip(pts, pts[1:]):
        x0, x1 = math.log2(b0), math.log2(b1)
        if x <= x1:
            return f0 + (f1 - f0) * (x - x0) / (x1 - x0)
    return pts[-1][1]


def rust_consume_factor(size, extras, api):
    """The poll consumer at 100 B stalls on fetch.queue.backoff.ms=1000; a short backoff fixes
    most of it and the batch API adds a little more."""
    backoff = int(extras.get("fetch.queue.backoff.ms", 1000))
    f = 1.0
    if backoff >= 1000:
        f *= 0.35 if size == 100 else 0.9
    if api == "batch":
        f *= 1.35 if size == 100 else 1.1
    return f


def _extras(pairs):
    out = {}
    for kv in pairs:
        k, _, v = kv.partition("=")
        out[k] = v
    return out


def _params(client, a):
    # The Rust harness reports hyphenated param keys, the Java harness snake_case ones.
    sep = "-" if client == "rust" else "_"
    p = {k.replace("_", sep): v for k, v in vars(a).items()}
    p["extra"] = _extras(a.extra)
    return p


def _rng(run_id):
    return random.Random(int(hashlib.sha256(run_id.encode()).hexdigest()[:16], 16))


def _lat(rng, p50):
    p50 = max(1, int(p50))
    return {
        "count": 0, "min": max(1, int(p50 * 0.2)), "mean": p50 * 1.3, "stddev": p50 * 0.8,
        "p50": p50, "p90": int(p50 * 2.1), "p99": int(p50 * rng.uniform(4, 6)),
        "p99_9": int(p50 * rng.uniform(9, 14)), "p99_99": int(p50 * rng.uniform(15, 25)),
        "max": int(p50 * rng.uniform(30, 80)),
    }


def _resources(client, msgs, duration, rng, rss_mb, k=1):
    cpu = CPU_PER_M[client] * (1 + 0.02 * (k - 1)) * msgs / 1e6 * rng.uniform(0.95, 1.05)
    cpu = min(cpu, duration * (CLIENT_VCPUS - 0.1))
    rss = int(rss_mb * 1e6 * rng.uniform(0.97, 1.03))
    return {
        "clk_tck": 100, "cpu_user_s": cpu * 0.8, "cpu_sys_s": cpu * 0.2, "cpu_total_s": cpu,
        "cpu_cores_avg": cpu / duration if duration else 0.0,
        "cpu_s_per_million_msgs": cpu / msgs * 1e6 if msgs else 0.0,
        "rss_start_bytes": int(rss * 0.8), "rss_avg_bytes": int(rss * 0.95), "rss_peak_bytes": rss,
        "threads_peak": (20 + 4 * k) if client.startswith("java") else (5 + 4 * k),
        "cgroup_cpu_s": cpu * 1.02, "cgroup_mem_peak_bytes": int(rss * 1.1),
    }


def _timeseries(msgs, duration, rss, rng):
    out = []
    secs = max(1, int(math.ceil(duration)))
    per = msgs / duration if duration else msgs
    left = msgs
    for t in range(1, secs + 1):
        frac = min(1.0, duration - (t - 1))
        n = min(left, int(per * frac * rng.uniform(0.9, 1.1)))
        if t == secs:
            n = left
        left -= n
        out.append({"t": t, "msgs": n, "rss_bytes": rss})
    return out


def _instances(k, msgs, duration, lat, rng):
    out = []
    for i in range(k):
        m = msgs // k + (1 if i < msgs % k else 0)
        d = duration * (rng.uniform(0.93, 1.0) if i else 1.0)
        out.append({"k": i, "messages": m, "errors": 0, "duration_s": d,
                    "throughput_msgs_per_s": m / d if d else 0.0,
                    "latency_us": {x: (lat or {}).get(x) for x in ("p50", "p99", "max")} if lat else None})
    return out


def _jvm(rng, duration):
    young = int(duration * rng.uniform(1.5, 3.0))
    return {
        "vm": "OpenJDK 64-Bit Server VM 25.0.1+8-LTS (FAKE)",
        "flags": ["-Xms2g", "-Xmx2g", "-XX:+AlwaysPreTouch", "-XX:+UseG1GC", "-XX:+ExitOnOutOfMemoryError"],
        "gc": [{"name": "G1 Young Generation", "count": young, "time_ms": young * 3},
               {"name": "G1 Concurrent GC", "count": 0, "time_ms": 0},
               {"name": "G1 Old Generation", "count": 0, "time_ms": 0}],
        "gc_total_count": young, "gc_total_time_ms": young * 3,
        "jit_compile_time_ms": int(rng.uniform(200, 900)),
        "heap_used_peak_bytes": int(1.2e9), "heap_committed_bytes": 2147483648,
    }


def _base(client, mode, run_id, params, extras=None):
    lib = {"java": "kafka-clients", "rust": "rust-rdkafka", "java-stock": "kafka-perf-tools"}[client]
    ver = {"java": "4.3.1", "rust": "0.39.0", "java-stock": "4.3.1"}[client]
    runtime = "rustc 1.98.1 (FAKE)" if client == "rust" else "Temurin 25.0.1+8 (FAKE)"
    return {
        "schema_version": 1, "fake": True, "status": "ok", "error": None, "client": client,
        "client_lib": lib, "client_version": ver,
        "native_lib_version": "2.12.1 (FAKE)" if client == "rust" else None,
        "runtime": runtime, "mode": mode, "run_id": run_id, "params": params,
        "effective_config": dict({"client.id": run_id}, **(extras or {})),
    }


def produce(client, a):
    rng = _rng(a.run_id)
    rate = BASE_RATE[client] * SIZE_RATE.get(a.message_size, 1.0)
    codecs = dict(CODEC_RATE)
    if client == "rust":
        codecs.update(CODEC_RATE_RUST)
    rate *= codecs[a.compression]
    rate *= {"0": 1.25, "1": 1.0, "all": 0.8}[a.acks]
    if a.idempotence == "true":
        rate *= 0.93
    if a.linger_ms == 0:
        rate *= 0.55 if client == "java" else 0.7
    rate *= batch_factor(client, a.batch_size)
    if a.partitions == 1:
        rate *= 0.7
    rate *= rng.gauss(1.0, 0.025)
    if client == "rust" and "partitions-12" in a.run_id:
        rate *= rng.uniform(0.85, 1.1)
    if a.instances > 1:
        rate = scaled_rate(client, rate, a.instances, rng, "produce", a.message_size, a.compression != "none")
    msgs = a.num_messages
    truncated = False
    if a.rate:
        rate = min(rate, a.rate * rng.uniform(0.995, 1.0))
    duration = msgs / rate
    if duration > a.max_duration_s:
        truncated = True
        msgs = int(rate * a.max_duration_s)
        duration = msgs / rate
    params = _params(client, a)
    params["build"] = {"profile": "release", "opt_level": 3, "lto": "fat", "codegen_units": 1,
                       "panic": "abort", "target_cpu": "x86-64", "allocator": "glibc"} if client == "rust" else None
    r = _base(client, "produce", a.run_id, params, _extras(a.extra))
    if client == "rust" and a.config_profile == "native":
        r["library_defaults"] = dict(LIBRDKAFKA_DEFAULTS)
    rss_mb = {"java": 2300, "rust": 90 + a.buffer_memory / 1e6 * 0.3}[client]
    res = _resources(client, msgs, duration, rng, rss_mb * (1 + 0.05 * (a.instances - 1)), a.instances)
    p50 = 1000 * (a.linger_ms + 1.5) * (1.3 if a.acks == "all" else 1.0) * (0.9 if client == "rust" else 1.0)
    # Near broker saturation, queueing inflates send-to-ack latency.
    p50 *= 1 + 0.35 * (a.instances - 1) ** 0.8
    if a.rate:
        p50 = 1000 * (a.linger_ms * 0.6 + 0.9)
    lat = _lat(rng, p50)
    lat["count"] = msgs if a.acks != "0" else msgs
    r.update({
        "instances": a.instances, "instances_detail": _instances(a.instances, msgs, duration, lat, rng),
        "payload_sha256": _fake_hash(a.payload, a.message_size, a.seed),
        "pool_count": payload_mod.pool_count(a.message_size),
        "messages": msgs, "bytes": msgs * a.message_size, "errors": 0, "truncated": truncated,
        "duration_s": duration, "throughput_msgs_per_s": msgs / duration,
        "throughput_mb_per_s": msgs * a.message_size / duration / 1e6,
        "rate_lag_max_us": int(rng.uniform(50, 900)) if a.rate else None,
        "latency_kind": "send_to_ack", "latency_us": lat,
        "startup_ms": rng.uniform(250, 400) if client == "java" else rng.uniform(5, 15),
        "first_message_ms": None, "resources": res,
        "jvm": _jvm(rng, duration) if client == "java" else None,
        "timeseries": _timeseries(msgs, duration, res["rss_peak_bytes"], rng),
    })
    return r


def consume(client, a):
    rng = _rng(a.run_id)
    size = _size_from_topic(a.topic)
    rate = BASE_RATE[client] * 1.8 * SIZE_RATE.get(size, 1.0)
    if "lz4" in a.run_id:
        rate *= 1.1
    if "zstd" in a.run_id:
        rate *= 0.85
    if a.partitions == 1:
        rate *= 0.8 if client == "rust" else 0.75
    if client == "rust":
        rate *= rust_consume_factor(size, _extras(a.extra), a.consume_api)
    rate *= rng.gauss(1.0, 0.03)
    if a.instances > 1:
        rate = scaled_rate(client, rate, a.instances, rng, "consume", size, _compressed_topic(a.topic))
    msgs = a.num_messages
    if a.measure_e2e:
        m = _re_rate(a.run_id)
        rate = m * rng.uniform(0.995, 1.0) if m else rate
    duration = msgs / rate
    params = _params(client, a)
    r = _base(client, "consume", a.run_id, params, _extras(a.extra))
    res = _resources(client, msgs, duration, rng, (1900 if client == "java" else 140) * (1 + 0.1 * (a.instances - 1)),
                     a.instances)
    lat = None
    kind = None
    if a.measure_e2e:
        linger = 5 if "linger-5" in a.run_id else 0
        lat = _lat(rng, 1000 * (linger * 0.7 + 1.1) * (0.9 if client == "rust" else 1.0))
        lat["count"] = msgs
        kind = "e2e"
    r.update({
        "instances": a.instances, "instances_detail": _instances(a.instances, msgs, duration, lat, rng),
        "window_kind": "union" if a.instances > 1 else "first_to_last",
        "payload_sha256": None, "pool_count": None,
        "messages": msgs, "bytes": msgs * size, "errors": 0, "truncated": False,
        "duration_s": duration, "throughput_msgs_per_s": msgs / duration,
        "throughput_mb_per_s": msgs * size / duration / 1e6, "rate_lag_max_us": None,
        "latency_kind": kind, "latency_us": lat,
        "startup_ms": rng.uniform(200, 300) if client == "java" else rng.uniform(3, 10),
        "first_message_ms": rng.uniform(20, 120), "resources": res,
        "jvm": _jvm(rng, duration) if client == "java" else None,
        "timeseries": _timeseries(msgs, duration, res["rss_peak_bytes"], rng),
    })
    return r


def _re_rate(run_id):
    parts = run_id.split("-")
    if "rate" in parts:
        return float(parts[parts.index("rate") + 1])
    return None


def _size_from_topic(topic):
    m = re.search(r"(?:size|default|tuned)-(\d+)(?:-|$)", topic)
    return int(m.group(1)) if m and int(m.group(1)) in SIZE_RATE else 1024


def _compressed_topic(topic):
    return bool(re.search(r"lz4|zstd|tuned-\d", topic))


def _fake_hash(payload, size, seed):
    return hashlib.sha256(("FAKE-%s-%d-%d" % (payload, size, seed)).encode()).hexdigest()


def _harness_parser(mode):
    p = argparse.ArgumentParser(prog="fake " + mode)
    p.add_argument("--bootstrap", default="kbench-kafka:9092")
    p.add_argument("--topic", required=True)
    p.add_argument("--partitions", type=int, required=True)
    p.add_argument("--instances", type=int, default=1)
    p.add_argument("--config-profile", default="matched")
    p.add_argument("--num-messages", type=int, required=True)
    p.add_argument("--warmup-messages", type=int, default=0)
    p.add_argument("--run-id", required=True)
    p.add_argument("--extra", action="append", default=[])
    if mode == "produce":
        p.add_argument("--max-duration-s", type=float, default=120)
        p.add_argument("--message-size", type=int, required=True)
        p.add_argument("--payload", default="random", choices=["random", "text"])
        p.add_argument("--seed", type=int, default=42)
        p.add_argument("--acks", default="1", choices=["0", "1", "all"])
        p.add_argument("--compression", default="none", choices=list(CODEC_RATE))
        p.add_argument("--linger-ms", type=int, default=5)
        p.add_argument("--batch-size", type=int, default=16384)
        p.add_argument("--max-in-flight", type=int, default=5)
        p.add_argument("--idempotence", default="false", choices=["true", "false"])
        p.add_argument("--buffer-memory", type=int, default=268435456)
        p.add_argument("--rate", type=int, default=0)
        p.add_argument("--embed-timestamp", action="store_true")
    else:
        p.add_argument("--consume-api", default="poll", choices=["poll", "batch"])
        p.add_argument("--batch-max", type=int, default=500)
        p.add_argument("--fetch-min-bytes", type=int, default=1)
        p.add_argument("--fetch-max-wait-ms", type=int, default=500)
        p.add_argument("--max-partition-fetch-bytes", type=int, default=1048576)
        p.add_argument("--fetch-max-bytes", type=int, default=52428800)
        p.add_argument("--check-crcs", default="true", choices=["true", "false"])
        p.add_argument("--measure-e2e", action="store_true")
        p.add_argument("--timeout-s", type=float, default=180)
    return p


def stock(role, argv):
    p = argparse.ArgumentParser(prog="fake stock " + role)
    p.add_argument("--run-id", required=True)
    p.add_argument("--topic", required=True)
    if role == "producer":
        p.add_argument("--num-records", type=int, required=True)
        p.add_argument("--record-size", type=int, required=True)
        p.add_argument("--producer-props", required=True)
    else:
        p.add_argument("--messages", type=int, required=True)
    a = p.parse_args(argv)
    rng = _rng(a.run_id)
    if role == "producer":
        size, msgs = a.record_size, a.num_records
        rate = BASE_RATE["java-stock"] * SIZE_RATE[size] * rng.gauss(1.0, 0.03)
    else:
        size, msgs = 1024, a.messages
        rate = BASE_RATE["java-stock"] * 1.7 * rng.gauss(1.0, 0.03)
    duration = msgs / rate
    r = _base("java-stock", "produce" if role == "producer" else "consume", a.run_id, vars(a))
    r.update({
        "messages": msgs, "bytes": msgs * size, "errors": 0, "truncated": False, "duration_s": duration,
        "throughput_msgs_per_s": rate, "throughput_mb_per_s": rate * size / 1e6,
        "latency_kind": "send_to_ack" if role == "producer" else None,
        "latency_us": _lat(rng, 7000) if role == "producer" else None,
        "resources": None, "jvm": None, "timeseries": [],
    })
    return r


def hostinfo():
    """Same shape as infra/hostinfo.sh output for the parts report.py reads."""
    return {
        "fake": True, "schema_version": 1,
        "docker": {"context": "default", "server_version": "25.0.16 (FAKE)", "cgroup_version": "1", "ncpu": 32,
                   "mem_total_bytes": 66571993088},
        "os": {"operating_system": "Amazon Linux 2 (FAKE)", "kernel": "5.10.0-fake.amzn2.x86_64"},
        "cpu": {"model": "Intel(R) Xeon(R) Platinum 8375C CPU @ 2.90GHz (FAKE)", "sockets": "1",
                "cores_per_socket": "16", "threads_per_core": "2"},
        "memory": {"total_bytes": 66571993088},
        "ec2": {"instance_type": "m6i.8xlarge (FAKE)"},
        "broker": {
            "kafka_version": "4.3.1 (FAKE)",
            "container": {"cpuset_cpus": "0-6,16-22", "memory_bytes": 24 * 1024 ** 3,
                          "tmpfs": {"/var/lib/kafka/data": "size=16g"},
                          "env": ["KAFKA_HEAP_OPTS=-Xms4g -Xmx4g", "KAFKA_NUM_NETWORK_THREADS=8", "KAFKA_NUM_IO_THREADS=16"]},
            "config": {"num.network.threads": "8", "num.io.threads": "16", "log.dirs": "/var/lib/kafka/data"},
            "config_non_default": {"num.network.threads": {"value": "8", "source": "STATIC_BROKER_CONFIG"},
                                   "num.io.threads": {"value": "16", "source": "STATIC_BROKER_CONFIG"}},
        },
    }


def broker_stats():
    t = time.time()
    return {"cpu_ns": int(t * 1e9 * 0.8), "mem_bytes": int(5e9), "mem_peak_bytes": int(9e9),
            "data_dir_bytes": int(1e8), "cgroup_version": 1, "fake": True}


def main(argv):
    if len(argv) < 2:
        print(__doc__, file=sys.stderr)
        return 2
    client, mode, rest = argv[0], argv[1], argv[2:]
    print("fake %s %s starting" % (client, mode), file=sys.stderr)
    if client == "infra":
        obj = hostinfo() if mode == "hostinfo" else broker_stats()
        print(json.dumps(obj))
        return 0
    if client == "java-stock":
        r = stock(mode, rest)
    elif mode == "payload-hash":
        p = argparse.ArgumentParser()
        p.add_argument("--payload", required=True)
        p.add_argument("--message-size", type=int, required=True)
        p.add_argument("--seed", type=int, default=42)
        a = p.parse_args(rest)
        print(_fake_hash(a.payload, a.message_size, a.seed))
        return 0
    elif mode == "produce":
        r = produce(client, _harness_parser("produce").parse_args(rest))
    elif mode == "consume":
        r = consume(client, _harness_parser("consume").parse_args(rest))
    else:
        print("unknown mode " + mode, file=sys.stderr)
        return 2
    # Occasionally fail so the retry and failure-report paths get exercised.
    if os.environ.get("KBENCH_FAKE_FAIL") and r["run_id"].endswith(os.environ["KBENCH_FAKE_FAIL"]):
        r["status"] = "error"
        r["error"] = "injected fake failure"
    print("RESULT " + json.dumps(r, separators=(",", ":")))
    return 0 if r["status"] == "ok" else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
