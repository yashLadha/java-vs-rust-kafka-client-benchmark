"""Scenario matrix for the Java vs Rust Kafka client benchmark.

A scenario is a fully resolved, client independent configuration. The orchestrator
expands scenarios x reps x clients into runs.
"""

import json

STOCK_CLIENT = "java-stock"

# Client variants: a harness image plus extra harness args per mode. `kinds` lists the scenario
# kinds a variant runs; `inherits` names the variant whose per_client overrides it falls back to.
# rust-tuned shares the rust producer, so it skips produce-only scenarios and only differs where a
# consumer is measured (consume sweeps, consume scaling, the consumer side of e2e). rust-lowlat
# differs from rust only in the producer in-flight window, so it only runs e2e.
CLIENT_VARIANTS = {
    "java": {"harness": "java", "image": "kbench-java:latest", "kinds": ["produce", "consume", "e2e"],
             "args": {"produce": [], "consume": []}, "inherits": None,
             "description": "Java harness, contract configuration"},
    "rust": {"harness": "rust", "image": "kbench-rust:latest", "kinds": ["produce", "consume", "e2e"],
             "args": {"produce": [], "consume": []}, "inherits": None,
             "description": "Rust harness, contract configuration, BaseConsumer poll loop"},
    "rust-tuned": {"harness": "rust", "image": "kbench-rust:latest", "kinds": ["consume", "e2e"],
                   "args": {"produce": [],
                            "consume": ["--consume-api", "batch", "--batch-max", "500",
                                        "--extra", "fetch.queue.backoff.ms=10"]},
                   "inherits": "rust",
                   "description": "Rust consumer with rd_kafka_consume_batch_queue (up to 500 records per call, like Java "
                                  "max.poll.records) and fetch.queue.backoff.ms=10 instead of 1000; producer identical to rust"},
    "rust-lowlat": {"harness": "rust", "image": "kbench-rust:latest", "kinds": ["e2e"],
                    "args": {"produce": ["--extra", "max.in.flight.requests.per.connection=5"], "consume": []},
                    "inherits": "rust",
                    "description": "rust with the producer in-flight window forced to 5 (librdkafka default 1000000, "
                                   "dropped by the native profile); consumer identical to rust"},
}
HARNESS_CLIENTS = tuple(CLIENT_VARIANTS)
DEFAULT_CLIENTS = ("java", "rust", "rust-tuned")
# Keys a per_client override may set: producer config keys, plus `extra` (raw client properties)
# and `config_profile` (Rust producer profile, overriding --rust-config-profile).
PER_CLIENT_KEYS = set(("message_size", "payload", "acks", "compression", "linger_ms", "batch_size", "partitions",
                       "idempotence", "max_in_flight", "buffer_memory", "extra", "config_profile"))

BASE_COUNTS = {100: 20_000_000, 1024: 5_000_000, 10240: 500_000}
WARMUP_FRACTION = 0.10
WARMUP_MIN = 200_000
MAX_DURATION_S = 120
E2E_DURATION_S = 30
E2E_RATES = (1000, 10000, 50000)
# tmpfs is 16g. Topic data is written uncompressed-size at worst, so bound the raw
# payload bytes of a single topic well below it.
TOPIC_BYTES_LIMIT = 14_000_000_000
PAYLOAD_SEED = 42

SCALING_KS = (1, 2, 4, 8, 16)
SCALING_PARTITIONS = 16
# Scaling counts grow with K so each instance keeps the K=1 run length, until warmup+measured
# payload bytes would pass this budget (one such topic must fit the 16g tmpfs with headroom).
SCALING_BYTES_CAP = 12_000_000_000
# Totals are split across instances (N/K, +1 for the first N%K). Keeping N and W multiples of 16
# makes every consumer instance's share exactly match what its owned partitions hold, whatever K
# the prefill used, so no instance waits for a record that went to another instance's partition.
SCALING_COUNT_QUANTUM = 16
PREFILL_MAX_INSTANCES = 16

PRODUCER_BASELINE = {
    "message_size": 1024,
    "payload": "random",
    "acks": "1",
    "compression": "none",
    "linger_ms": 5,
    "batch_size": 16384,
    "partitions": 6,
    "idempotence": False,
    "max_in_flight": 5,
    "buffer_memory": 268435456,
    "instances": 1,
}

CONSUMER_DEFAULTS = {
    "fetch_min_bytes": 1,
    "fetch_max_wait_ms": 500,
    "max_partition_fetch_bytes": 1048576,
    "fetch_max_bytes": 52428800,
    "check_crcs": True,
    "timeout_s": 180,
    "instances": 1,
}

SCALING_PROFILES = {
    "default-100": {"message_size": 100},
    "default-1024": {"message_size": 1024},
    "tuned-1024": {"message_size": 1024, "payload": "text", "compression": "lz4", "linger_ms": 20,
                   "batch_size": 1048576},
}
SCALING_PROFILE_LABELS = {
    "default-100": "default, 100 B random",
    "default-1024": "default, 1024 B random",
    "tuned-1024": "tuned, 1024 B text lz4 linger 20 batch 1 MiB",
}

# Human labels used as the x axis of each sweep table/chart.
SWEEP_ORDER = ["message_size", "acks", "compression", "linger_ms", "batch_size", "partitions", "profile", "client_settings"]
BATCH_SIZES = (16384, 32768, 65536, 131072, 262144, 524288, 1048576)
JAVA_DEFAULTS = {"batch_size": 16384, "buffer_memory": 33554432}
LIBRDKAFKA_DEFAULTS = {"batch_size": 1000000, "extra": {"message.max.bytes": "1000000"}, "config_profile": "native"}
BEST_BATCH = {"java": 131072, "rust": 1048576}


def counts_for(message_size, scale):
    if message_size not in BASE_COUNTS:
        raise ValueError("no base message count for size %d" % message_size)
    measured = max(1, int(round(BASE_COUNTS[message_size] * scale)))
    return measured, warmup_for(measured)


def warmup_for(measured):
    return min(measured, max(WARMUP_MIN, int(round(measured * WARMUP_FRACTION))))


def scaling_counts_for(message_size, k, scale):
    """min(base * K, cap) measured, cap chosen so warmup+measured bytes stay within the budget."""
    q = SCALING_COUNT_QUANTUM
    measured = max(q, int(round(BASE_COUNTS[message_size] * scale)) * k)
    cap = SCALING_BYTES_CAP // message_size
    if measured + warmup_for(measured) > cap:
        measured = min(int(cap / (1 + WARMUP_FRACTION)), cap - WARMUP_MIN)
    measured = max(q, measured // q * q)
    warmup = max(q, warmup_for(measured) // q * q)
    return measured, warmup


def prefill_instances(partitions):
    return min(PREFILL_MAX_INSTANCES, partitions)


def is_scaling(sc):
    return bool(sc.get("scaling"))


def _key(kind, cfg, per_client=None):
    return json.dumps([kind, cfg, per_client or {}], sort_keys=True)


def variant(client):
    return CLIENT_VARIANTS[client]


def runs_kind(client, kind):
    return client in CLIENT_VARIANTS and kind in CLIENT_VARIANTS[client]["kinds"]


def client_overrides(sc, client):
    """The per_client entry for client, falling back along `inherits` (own entry wins, no merging)."""
    pc = sc.get("per_client") or {}
    c = client
    while c:
        if c in pc:
            return pc[c]
        c = CLIENT_VARIANTS.get(c, {}).get("inherits")
    return {}


def client_config(sc, client):
    """Scenario producer config with the client's per_client overrides applied; `extra` is merged."""
    cfg = dict(sc["config"])
    over = client_overrides(sc, client)
    for k, v in over.items():
        if k == "extra":
            cfg["extra"] = dict(cfg.get("extra") or {}, **v)
        else:
            cfg[k] = v
    return cfg


class _Builder:
    def __init__(self, scale):
        self.scale = scale
        self.scenarios = []
        self.by_key = {}

    def add(self, kind, name, cfg, sweep, value, extra=None, per_client=None):
        """Identical (kind, cfg, per_client) collapse into one scenario tagged with every sweep
        it belongs to, so the baseline is measured once and reused as the reference point
        of each one-factor sweep."""
        if per_client:
            if kind not in ("produce", "e2e"):
                raise ValueError("%s: per_client overrides only apply to produce and e2e scenarios" % name)
            for c, over in per_client.items():
                if c not in CLIENT_VARIANTS:
                    raise ValueError("%s: per_client names unknown client %s" % (name, c))
                bad = set(over) - PER_CLIENT_KEYS
                if bad:
                    raise ValueError("%s: per_client %s sets unsupported keys %s" % (name, c, sorted(bad)))
                if "config_profile" in over and CLIENT_VARIANTS[c]["harness"] != "rust":
                    raise ValueError("%s: config_profile is a Rust harness flag, not valid for %s" % (name, c))
        key = _key(kind, cfg, per_client)
        tag = {"sweep": sweep, "value": value}
        if key in self.by_key:
            sc = self.by_key[key]
            if tag not in sc["sweeps"]:
                sc["sweeps"].append(tag)
            return sc
        sc = {"name": name, "kind": kind, "config": dict(cfg), "sweeps": [tag]}
        if per_client:
            sc["per_client"] = json.loads(json.dumps(per_client))
        if extra:
            sc.update(extra)
        self.scenarios.append(sc)
        self.by_key[key] = sc
        return sc


def _producer_cfg(**over):
    cfg = dict(PRODUCER_BASELINE)
    cfg.update(over)
    return cfg


def build(scale=1.0, include_stock=True):
    b = _Builder(scale)

    for size in (100, 1024, 10240):
        b.add("produce", "produce-size-%d" % size, _producer_cfg(message_size=size), "message_size", size)
    for acks in ("0", "1", "all"):
        b.add("produce", "produce-acks-%s" % acks, _producer_cfg(acks=acks), "acks", acks)
    b.add("produce", "produce-acks-all-idempotent", _producer_cfg(acks="all", idempotence=True), "acks", "all+idempotence")
    for codec in ("none", "gzip", "snappy", "lz4", "zstd"):
        b.add("produce", "produce-compression-%s-text" % codec, _producer_cfg(payload="text", compression=codec), "compression", codec)
    for linger in (0, 5, 50):
        b.add("produce", "produce-linger-%d" % linger, _producer_cfg(linger_ms=linger), "linger_ms", linger)
    for batch in BATCH_SIZES:
        b.add("produce", "produce-batch-%d" % batch, _producer_cfg(batch_size=batch), "batch_size", batch)
    for parts in (1, 6, 12):
        b.add("produce", "produce-partitions-%d" % parts, _producer_cfg(partitions=parts), "partitions", parts)
    b.add("produce", "produce-max-throughput", _producer_cfg(
        message_size=100, payload="text", compression="lz4", linger_ms=50, batch_size=1048576,
        acks="1", partitions=6), "profile", "max-throughput")
    b.add("produce", "produce-defaults", _producer_cfg(), "client_settings", "library defaults",
          per_client={"java": dict(JAVA_DEFAULTS), "rust": json.loads(json.dumps(LIBRDKAFKA_DEFAULTS))})
    b.add("produce", "produce-best", _producer_cfg(), "client_settings", "best batch.size",
          per_client={c: {"batch_size": v} for c, v in BEST_BATCH.items()})

    # The baseline scenario is named after its role, not after whichever sweep saw it first.
    for sc in b.scenarios:
        if sc["kind"] == "produce" and sc["config"] == PRODUCER_BASELINE and not sc.get("per_client"):
            sc["name"] = "produce-baseline"

    consumer_specs = [
        ("consume-size-100", {"message_size": 100}, "message_size", 100),
        ("consume-size-1024", {}, "message_size", 1024),
        ("consume-size-10240", {"message_size": 10240}, "message_size", 10240),
        ("consume-lz4-text", {"payload": "text", "compression": "lz4"}, "compression", "lz4 (text)"),
        ("consume-zstd-text", {"payload": "text", "compression": "zstd"}, "compression", "zstd (text)"),
        ("consume-partitions-1", {"partitions": 1}, "partitions", 1),
        ("consume-partitions-6", {}, "partitions", 6),
    ]
    for name, over, sweep, value in consumer_specs:
        prefill = _producer_cfg(**over)
        cfg = {"prefill": prefill}
        cfg.update(CONSUMER_DEFAULTS)
        sc = b.add("consume", name, cfg, sweep, value)
        if sc["config"]["prefill"] == PRODUCER_BASELINE:
            sc["name"] = "consume-baseline"

    for linger in (0, 5):
        for rate in E2E_RATES:
            cfg = _producer_cfg(linger_ms=linger)
            cfg.update({"rate": rate, "duration_s": E2E_DURATION_S, "consumer": dict(CONSUMER_DEFAULTS)})
            b.add("e2e", "e2e-linger-%d-rate-%d" % (linger, rate), cfg, "e2e_linger_%d" % linger, rate)

    for profile, over in SCALING_PROFILES.items():
        for k in SCALING_KS:
            meta = {"profile": profile, "k": k}
            pcfg = _producer_cfg(partitions=SCALING_PARTITIONS, instances=k, **over)
            b.add("produce", "scale-produce-%s-k%d" % (profile, k), pcfg, "scaling_produce_" + profile, k,
                  extra={"scaling": dict(meta, mode="produce")})
            prefill = _producer_cfg(partitions=SCALING_PARTITIONS, **over)
            ccfg = {"prefill": prefill}
            ccfg.update(CONSUMER_DEFAULTS)
            ccfg["instances"] = k
            b.add("consume", "scale-consume-%s-k%d" % (profile, k), ccfg, "scaling_consume_" + profile, k,
                  extra={"scaling": dict(meta, mode="consume")})

    if include_stock:
        for size in (1024, 100, 10240):
            ref = "produce-baseline" if size == 1024 else "produce-size-%d" % size
            b.add("stock-produce", "stock-produce-size-%d" % size, _producer_cfg(message_size=size),
                  "stock_produce", size, extra={"reference": ref})
        cfg = {"prefill": dict(PRODUCER_BASELINE)}
        cfg.update(CONSUMER_DEFAULTS)
        b.add("stock-consume", "stock-consume-baseline", cfg, "stock_consume", 1024,
              extra={"reference": "consume-baseline"})

    for sc in b.scenarios:
        _resolve_counts(sc, scale)
    names = [sc["name"] for sc in b.scenarios]
    if len(set(names)) != len(names):
        raise ValueError("duplicate scenario names: %s" % names)
    return b.scenarios


def _resolve_counts(sc, scale):
    cfg = sc["config"]
    if sc["kind"] == "e2e":
        # Scale shortens the fixed-duration e2e runs as well, so --scale gives quick smoke passes.
        duration = max(1.0, E2E_DURATION_S * scale)
        measured = max(1, int(round(cfg["rate"] * duration)))
        cfg["duration_s"] = duration
        warmup = warmup_for(measured)
        size = cfg["message_size"]
    elif sc.get("scaling"):
        size = cfg["prefill"]["message_size"] if "prefill" in cfg else cfg["message_size"]
        measured, warmup = scaling_counts_for(size, sc["scaling"]["k"], scale)
    else:
        size = cfg["prefill"]["message_size"] if "prefill" in cfg else cfg["message_size"]
        measured, warmup = counts_for(size, scale)
    sc["num_messages"] = measured
    sc["warmup_messages"] = warmup
    sc["message_size"] = size
    sc["payload"] = cfg["prefill"]["payload"] if "prefill" in cfg else cfg["payload"]
    sc["partitions"] = cfg["prefill"]["partitions"] if "prefill" in cfg else cfg["partitions"]
    sc["instances"] = cfg.get("instances", 1)
    sc["topic_bytes"] = (measured + warmup) * size
    if sc["topic_bytes"] >= TOPIC_BYTES_LIMIT:
        raise ValueError("%s: %d warmup+measured bytes exceed the %d tmpfs budget; lower --scale"
                         % (sc["name"], sc["topic_bytes"], TOPIC_BYTES_LIMIT))


def payload_variants(scenarios):
    out = set()
    for sc in scenarios:
        out.add((sc["payload"], sc["message_size"]))
    return sorted(out, key=lambda t: (t[0], t[1]))


# Rough client-agnostic throughput guesses (msgs/s) used only for the ETA.
_GUESS_RATE = {100: 1_200_000, 1024: 350_000, 10240: 45_000}
_GUESS_MULT = {
    ("acks", "0"): 1.2, ("acks", "all"): 0.8,
    ("compression", "gzip"): 0.25, ("compression", "zstd"): 0.8,
    ("linger_ms", 0): 0.6, ("partitions", 1): 0.7,
}
# Per run: topic create/describe, offsets and delete/list each start an admin JVM inside the
# broker (about 2 s each), plus client container start/stop and two broker-stats reads.
RUN_OVERHEAD_S = 12.0
# K instances: sublinear aggregate gain, bounded by what the 7-core broker plausibly sustains.
_K_EXP = 0.6
_BROKER_CAP = {"produce": (1.6e9, 6.0e6), "consume": (3.0e9, 12.0e6)}
# Topic deletion frees tmpfs only after file.delete.delay.ms (60 s), so a big topic right after
# another big one waits for the previous segments to go.
TMPFS_WAIT_GUESS_S = 50.0
TMPFS_BUDGET_BYTES = 16 * 1024 ** 3 * 0.92


def _cap(mode, cfg, size, r):
    bytes_cap, msgs_cap = _BROKER_CAP[mode]
    if cfg.get("compression", "none") != "none":
        bytes_cap *= 2.5
    return min(r, bytes_cap / size, msgs_cap)


def estimate_produce_s(cfg, total_msgs, rate=0):
    if rate:
        return total_msgs / rate
    r = _GUESS_RATE[cfg["message_size"]]
    for field in ("acks", "compression", "linger_ms", "partitions"):
        r *= _GUESS_MULT.get((field, cfg[field]), 1.0)
    if cfg["compression"] in ("lz4", "snappy", "zstd") and cfg["message_size"] == 100:
        r *= 1.5
    k = cfg.get("instances", 1)
    if k > 1:
        r = _cap("produce", cfg, cfg["message_size"], r * k ** _K_EXP)
    return min(total_msgs / r, MAX_DURATION_S + 5)


def estimate_consume_s(size, total_msgs, instances=1, prefill_cfg=None):
    r = _GUESS_RATE[size] * 2.0
    if instances > 1:
        r = _cap("consume", prefill_cfg or {}, size, r * instances ** _K_EXP)
    return total_msgs / r


def estimate_unit_s(sc, cooldown_s):
    """Seconds for one client unit of a scenario (one measured run incl. overhead)."""
    total = sc["num_messages"] + sc["warmup_messages"]
    cfg = sc["config"]
    if sc["kind"] in ("produce", "stock-produce"):
        return RUN_OVERHEAD_S + cooldown_s + estimate_produce_s(cfg, total)
    if sc["kind"] in ("consume", "stock-consume"):
        return 4.0 + cooldown_s + estimate_consume_s(sc["message_size"], total, sc["instances"], cfg["prefill"])
    if sc["kind"] == "e2e":
        warm = sc["warmup_messages"] / _GUESS_RATE[sc["message_size"]]
        return RUN_OVERHEAD_S + cooldown_s + 8.0 + warm + cfg["duration_s"]
    raise ValueError(sc["kind"])


def prefill_config(sc):
    cfg = dict(sc["config"]["prefill"])
    cfg["instances"] = prefill_instances(cfg["partitions"])
    return cfg


def estimate_prefill_s(sc, cooldown_s):
    total = sc["num_messages"] + sc["warmup_messages"]
    return RUN_OVERHEAD_S + cooldown_s + 4.0 + estimate_produce_s(prefill_config(sc), total)


if __name__ == "__main__":
    for sc in build():
        print(sc["name"], sc["kind"], sc["num_messages"], sc["warmup_messages"],
              [(t["sweep"], t["value"]) for t in sc["sweeps"]])
