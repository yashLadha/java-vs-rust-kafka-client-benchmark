#!/usr/bin/env python3
"""Benchmark orchestrator: Java kafka-clients vs Rust rdkafka.

usage: python3 bench/run.py [--reps 3] [--scale 1.0] [--only REGEX] [--clients java,rust,rust-tuned]
                            [--seed 1] [--dry-run] [--skip-stock] [--skip-scaling] [--out DIR]
                            [--resume] [--fake] [--cooldown-s 5] [--no-report]
                            [--rust-config-profile matched|native]

Runs inside the kbench-runner container (see ./kbench.sh), which reaches the host daemon
through the mounted /var/run/docker.sock and writes to the kbench-results volume at
/results. Only resources named kbench* are ever touched.
"""

import argparse
import datetime as dt
import json
import os
import random
import re
import shlex
import signal
import subprocess
import sys
import threading
import time
import traceback

BENCH_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(BENCH_DIR)
INFRA_DIR = os.path.join(PROJECT_DIR, "infra")
sys.path.insert(0, BENCH_DIR)

import matrix  # noqa: E402
import payload as payload_mod  # noqa: E402

RESULTS_ROOT = os.environ.get("KBENCH_RESULTS_ROOT", "/results")
IMAGES = {c: v["image"] for c, v in matrix.CLIENT_VARIANTS.items()}
HARNESS_IMAGES = {v["harness"]: v["image"] for v in matrix.CLIENT_VARIANTS.values()}
NETWORK = "kbench-net"
CPUSET_CLIENT = "8-15,24-31"
CLIENT_MEMORY = "12g"
CPUSET_E2E_PRODUCER = "8-11,24-27"
CPUSET_E2E_CONSUMER = "12-15,28-31"
E2E_MEMORY = "8g"
PINNING = {
    "broker": "0-6,16-22 (infra/docker-compose.yml)", "runner": "7,23",
    "client": CPUSET_CLIENT, "client_memory": CLIENT_MEMORY,
    "e2e_producer": CPUSET_E2E_PRODUCER, "e2e_consumer": CPUSET_E2E_CONSUMER, "e2e_memory": E2E_MEMORY,
}
LAUNCHER_LABEL = "kbench.launcher=kbench-runner"
RUST_CONFIG_PROFILE = "matched"
PREFILL_CLIENT = "java"
PREFILL_MAX_DURATION_S = 900
TMPFS_BYTES = 16 * 1024 ** 3
# Kafka deletes segment files file.delete.delay.ms (60 s default) after topic deletion,
# so back-to-back big runs can briefly hold two topics on tmpfs. Wait for room.
TMPFS_WAIT_MAX_S = 240
E2E_READY_MAX_S = 30

# ----------------------------------------------------------------------------------------
# Process execution


class Exec:
    fake = False

    def run(self, argv, timeout=None):
        try:
            p = subprocess.run(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=timeout)
            return p.returncode, p.stdout, p.stderr
        except subprocess.TimeoutExpired as e:
            out = e.stdout.decode() if isinstance(e.stdout, bytes) else (e.stdout or "")
            err = e.stderr.decode() if isinstance(e.stderr, bytes) else (e.stderr or "")
            return None, out, err + "\n[orchestrator] timed out after %ss" % timeout

    def popen(self, argv):
        return subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    def sleep(self, s):
        time.sleep(s)


class FakeExec(Exec):
    """Maps every docker/infra argv onto bench/fake.py so the orchestration logic runs
    unchanged without a daemon."""

    fake = True

    def __init__(self):
        self.topics = {}
        self.fake_py = os.path.join(BENCH_DIR, "fake.py")
        self.broker_cpu_ns = 0

    def _translate(self, argv):
        py = [sys.executable, self.fake_py]
        while argv[0] == "env" or (argv[0] != "bash" and "=" in argv[0]):
            argv = argv[1:]
        if argv[0] == "bash":
            script = os.path.basename(argv[1])
            args = argv[2:]
            if script == "up.sh":
                return "READY\n"
            if script == "hostinfo.sh":
                return py + ["infra", "hostinfo"]
            if script == "broker-stats.sh":
                return json.dumps({"cpu_ns": self.broker_cpu_ns, "mem_bytes": int(5e9), "mem_peak_bytes": int(9e9),
                                   "data_dir_bytes": int(1e8), "cgroup_version": 1, "fake": True}) + "\n"
            if script == "topic.sh":
                if args[0] == "create":
                    self.topics[args[1]] = 0
                    return ""
                if args[0] == "delete":
                    self.topics.pop(args[1], None)
                    return ""
                if args[0] == "offsets":
                    return "%d\n" % self.topics.get(args[1], 0)
            if script == "stock-perf.sh":
                a = dict(zip(args[1::2], args[2::2]))
                if args[0] == "producer":
                    self.topics[a["--topic"]] = self.topics.get(a["--topic"], 0) + int(a["--num-records"])
                return py + ["java-stock"] + args
            raise ValueError("fake: unknown infra call %s" % argv)
        if argv[:2] == ["docker", "run"]:
            idx = next(i for i, x in enumerate(argv) if x in HARNESS_IMAGES.values())
            client = [h for h, img in HARNESS_IMAGES.items() if img == argv[idx]][0]
            args = argv[idx + 1:]
            if args[0] == "produce":
                a = _flag_map(args)
                self.topics[a["--topic"]] = self.topics.get(a["--topic"], 0) + int(a["--num-messages"]) + int(a.get("--warmup-messages", 0))
            return py + [client] + args
        if argv[:3] == ["docker", "image", "inspect"]:
            return json.dumps([{"Id": "sha256:" + "f" * 64, "RepoTags": [argv[-1]], "RepoDigests": [],
                                "Created": "2026-01-01T00:00:00Z", "Size": 1, "fake": True}]) + "\n"
        if argv[:2] == ["docker", "inspect"]:
            return "true\n"
        if argv[:2] == ["docker", "ps"]:
            return ""
        if argv[:2] in (["docker", "kill"], ["docker", "rm"]):
            return ""
        if argv[:2] == ["docker", "stats"]:
            return json.dumps({"Name": "fake-background", "CPUPerc": "0.10%", "MemUsage": "50MiB / 62GiB", "fake": True}) + "\n"
        if argv[:2] == ["docker", "version"]:
            return json.dumps({"Client": {"Version": "fake"}, "Server": {"Version": "fake"}, "fake": True}) + "\n"
        raise ValueError("fake: unknown call %s" % argv)

    def run(self, argv, timeout=None):
        t = self._translate(argv)
        if isinstance(t, str):
            return 0, t, ""
        rc, out, err = Exec.run(self, t, timeout)
        self._charge_broker(out)
        return rc, out, err

    def _charge_broker(self, out):
        import fake as fake_mod
        res = Orchestrator.parse_result(out)
        if res and res.get("duration_s"):
            self.broker_cpu_ns += int(fake_mod.broker_cpu_s(res) * 1e9)

    def popen(self, argv):
        t = self._translate(argv)
        return Exec.popen(self, t)

    def sleep(self, s):
        pass


def _flag_map(args):
    out = {}
    for i, a in enumerate(args):
        if a.startswith("--"):
            nxt = args[i + 1] if i + 1 < len(args) else None
            out[a] = nxt if nxt is not None and not nxt.startswith("--") else True
    return out


# ----------------------------------------------------------------------------------------
# Command construction


def infra(script, *args):
    return ["bash", os.path.join(INFRA_DIR, script)] + [str(a) for a in args]


def stock_cmd(*args):
    # Passed explicitly so the stock tool always gets the same cpuset and memory as the harnesses,
    # whatever infra/stock-perf.sh defaults to.
    return ["env", "KBENCH_CLIENT_CPUSET=" + CPUSET_CLIENT, "KBENCH_CLIENT_MEM=" + CLIENT_MEMORY] + infra("stock-perf.sh", *args)


def variant_args(client, mode):
    return list(matrix.variant(client)["args"].get(mode, []))


def docker_run(client, name, cpuset, memory, harness_args):
    harness_args = list(harness_args) + variant_args(client, harness_args[0])
    # The label lets ./kbench.sh stop kill leftovers of this orchestrator and nothing else.
    return ["docker", "run", "--rm", "--network", NETWORK, "--cpuset-cpus", cpuset,
            "--memory", memory, "--label", LAUNCHER_LABEL, "--name", name, IMAGES[client]] + [str(a) for a in harness_args]


def producer_cfg(sc, client):
    """Producer config for one client: per_client overrides applied, and for Rust harnesses the
    run-wide --rust-config-profile unless the scenario pins its own."""
    cfg = matrix.client_config(sc, client)
    if matrix.variant(client)["harness"] == "rust":
        cfg.setdefault("config_profile", RUST_CONFIG_PROFILE)
    return cfg


def produce_args(cfg, topic, run_id, num, warmup, max_duration_s=matrix.MAX_DURATION_S, rate=0, embed=False):
    args = ["produce", "--topic", topic, "--partitions", cfg["partitions"], "--instances", cfg.get("instances", 1),
            "--num-messages", num, "--warmup-messages", warmup, "--max-duration-s", max_duration_s,
            "--message-size", cfg["message_size"], "--payload", cfg["payload"], "--seed", matrix.PAYLOAD_SEED,
            "--acks", cfg["acks"], "--compression", cfg["compression"], "--linger-ms", cfg["linger_ms"],
            "--batch-size", cfg["batch_size"], "--max-in-flight", cfg["max_in_flight"],
            "--idempotence", "true" if cfg["idempotence"] else "false",
            "--buffer-memory", cfg["buffer_memory"], "--rate", rate, "--run-id", run_id]
    if embed:
        args.append("--embed-timestamp")
    for k, v in sorted((cfg.get("extra") or {}).items()):
        args += ["--extra", "%s=%s" % (k, v)]
    if cfg.get("config_profile"):
        args += ["--config-profile", cfg["config_profile"]]
    return args


def consume_args(ccfg, topic, partitions, run_id, num, warmup, e2e=False):
    args = ["consume", "--topic", topic, "--partitions", partitions, "--instances", ccfg.get("instances", 1),
            "--num-messages", num,
            "--warmup-messages", warmup, "--fetch-min-bytes", ccfg["fetch_min_bytes"],
            "--fetch-max-wait-ms", ccfg["fetch_max_wait_ms"],
            "--max-partition-fetch-bytes", ccfg["max_partition_fetch_bytes"],
            "--fetch-max-bytes", ccfg["fetch_max_bytes"],
            "--check-crcs", "true" if ccfg["check_crcs"] else "false",
            "--timeout-s", ccfg["timeout_s"], "--run-id", run_id]
    if e2e:
        args.append("--measure-e2e")
    return args


def stock_producer_props(cfg):
    # Same Java properties the custom harness sets, so the stock tool differs only in its
    # own payload generation, partitioner and measurement loop.
    props = {
        "acks": cfg["acks"], "linger.ms": cfg["linger_ms"], "batch.size": cfg["batch_size"],
        "compression.type": cfg["compression"],
        "max.in.flight.requests.per.connection": cfg["max_in_flight"],
        "enable.idempotence": "true" if cfg["idempotence"] else "false",
        "buffer.memory": cfg["buffer_memory"], "max.block.ms": 60000, "retries": 2147483647,
        "delivery.timeout.ms": 120000, "request.timeout.ms": 30000, "max.request.size": 10485760,
    }
    return " ".join("%s=%s" % kv for kv in props.items())


# ----------------------------------------------------------------------------------------
# Plan


def scenario_clients(sc, clients):
    return [c for c in clients if matrix.runs_kind(c, sc["kind"])]


def rotate(seq, n):
    if not seq:
        return []
    n %= len(seq)
    return list(seq[n:]) + list(seq[:n])


def make_plan(scenarios, reps, seed, clients):
    """Units in execution order. Scenario order is shuffled per rep from one seeded
    stream; the order of a scenario's clients rotates by one position per rep to spread
    slow drift over all of them."""
    rng = random.Random(seed)
    plan = []
    for rep in range(reps):
        scs = list(scenarios)
        rng.shuffle(scs)
        for sc in scs:
            kind = sc["kind"]
            order = rotate(scenario_clients(sc, clients), rep)
            if kind in ("produce", "e2e"):
                for c in order:
                    plan.append({"kind": kind, "scenario": sc["name"], "rep": rep, "clients": [c]})
            elif kind == "consume":
                if order:
                    plan.append({"kind": kind, "scenario": sc["name"], "rep": rep, "clients": list(order)})
            elif kind in ("stock-produce", "stock-consume"):
                plan.append({"kind": kind, "scenario": sc["name"], "rep": rep, "clients": [matrix.STOCK_CLIENT]})
    return plan


def base_id(scenario, rep, client):
    return "%s-r%d-%s" % (scenario, rep, client)


def attempt_id(base, attempt):
    return base if attempt == 1 else "%s-a%d" % (base, attempt)


def unit_runs(unit):
    """(base_run_id, role, client) of every measured record a unit produces."""
    out = []
    for c in unit["clients"]:
        b = base_id(unit["scenario"], unit["rep"], c)
        if unit["kind"] == "e2e":
            out += [(b, "producer", c), (b, "consumer", c)]
        elif unit["kind"] in ("consume", "stock-consume"):
            out.append((b, "consumer", c))
        else:
            out.append((b, "producer", c))
    return out


def unit_estimate(unit, sc, cooldown):
    est = matrix.estimate_unit_s(sc, cooldown) * len(unit["clients"])
    if unit["kind"] in ("consume", "stock-consume"):
        est += matrix.estimate_prefill_s(sc, cooldown) + 3.0
    return est


def plan_estimates(plan, sc_by, cooldown):
    """Per-unit seconds in plan order, including the tmpfs wait when a big topic follows another."""
    out = []
    prev_bytes = 0
    for u in plan:
        sc = sc_by[u["scenario"]]
        est = unit_estimate(u, sc, cooldown)
        need = sc["topic_bytes"] * 1.05
        if u["kind"] in ("consume", "stock-consume", "produce", "stock-produce") and prev_bytes + need > matrix.TMPFS_BUDGET_BYTES:
            est += matrix.TMPFS_WAIT_GUESS_S
        out.append(est)
        prev_bytes = sc["topic_bytes"] if u["kind"] != "e2e" else 0
    return out


def unit_family(unit, sc):
    if matrix.is_scaling(sc):
        return "scaling %s %s" % (sc["scaling"]["mode"], sc["scaling"]["profile"])
    return {"produce": "sweeps produce", "consume": "sweeps consume", "e2e": "e2e",
            "stock-produce": "stock reference", "stock-consume": "stock reference"}[unit["kind"]]


def runner_info(ex):
    """Where the orchestrator itself runs, to show it is off the measured cpusets."""
    info = {"hostname": os.uname().nodename, "in_container": os.path.exists("/.dockerenv")}
    for key, path in (("cpuset_cpus", "/sys/fs/cgroup/cpuset/cpuset.cpus"), ("cpuset_cpus", "/sys/fs/cgroup/cpuset.cpus.effective"),
                      ("memory_limit_bytes", "/sys/fs/cgroup/memory/memory.limit_in_bytes"),
                      ("memory_limit_bytes", "/sys/fs/cgroup/memory.max")):
        if key in info:
            continue
        try:
            with open(path) as f:
                info[key] = f.read().strip()
        except OSError:
            pass
    return info


def fmt_dur(s):
    s = int(max(0, s))
    return "%d:%02d:%02d" % (s // 3600, s % 3600 // 60, s % 60)


def unit_commands(unit, sc):
    """Commands for display in --dry-run."""
    cmds = []
    n, w = sc["num_messages"], sc["warmup_messages"]
    if unit["kind"] in ("consume", "stock-consume"):
        pid = base_id(unit["scenario"], unit["rep"], "prefill")
        topic = "kbench-" + pid
        cmds.append(docker_run(PREFILL_CLIENT, "kbench-client-" + pid, CPUSET_CLIENT, CLIENT_MEMORY,
                               produce_args(matrix.prefill_config(sc), topic, pid, n + w, 0, PREFILL_MAX_DURATION_S)))
    for c in unit["clients"]:
        rid = base_id(unit["scenario"], unit["rep"], c)
        cfg = producer_cfg(sc, c) if c in IMAGES else sc["config"]
        if unit["kind"] == "produce":
            cmds.append(docker_run(c, "kbench-client-" + rid, CPUSET_CLIENT, CLIENT_MEMORY,
                                   produce_args(cfg, "kbench-" + rid, rid, n, w)))
        elif unit["kind"] == "consume":
            cmds.append(docker_run(c, "kbench-client-" + rid, CPUSET_CLIENT, CLIENT_MEMORY,
                                   consume_args(sc["config"], topic, sc["partitions"], rid, n, w)))
        elif unit["kind"] == "e2e":
            t = "kbench-" + rid
            cmds.append(docker_run(c, "kbench-client-%s-c" % rid, CPUSET_E2E_CONSUMER, E2E_MEMORY,
                                   consume_args(cfg["consumer"], t, sc["partitions"], rid, n, w, e2e=True)))
            cmds.append(docker_run(c, "kbench-client-" + rid, CPUSET_E2E_PRODUCER, E2E_MEMORY,
                                   produce_args(cfg, t, rid, n, w, rate=cfg["rate"], embed=True)))
        elif unit["kind"] == "stock-produce":
            cmds.append(stock_cmd("producer", "--run-id", rid, "--topic", "kbench-" + rid,
                                  "--num-records", n + w, "--record-size", cfg["message_size"],
                                  "--producer-props", stock_producer_props(cfg)))
        elif unit["kind"] == "stock-consume":
            cmds.append(stock_cmd("consumer", "--run-id", rid, "--topic", topic, "--messages", n + w))
    return cmds


# ----------------------------------------------------------------------------------------
# Orchestrator


class Aborted(Exception):
    pass


class Orchestrator:
    def __init__(self, args, ex, scenarios, out):
        self.args = args
        self.ex = ex
        self.sc = {s["name"]: s for s in scenarios}
        self.out = out
        self.cooldown = args.cooldown_s
        self.raw_path = os.path.join(out, "raw.jsonl")
        self.logs = os.path.join(out, "logs")
        os.makedirs(self.logs, exist_ok=True)
        self.order_index = 0
        self.expected_hash = {}
        self.live_containers = set()
        self.live_topics = set()
        self.done = self._load_done()

    def _load_done(self):
        done = {}
        if not os.path.exists(self.raw_path):
            return done
        with open(self.raw_path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    r = json.loads(line)
                except ValueError:
                    continue
                key = (r.get("base_run_id"), r.get("role"))
                done.setdefault(key, []).append(r.get("status"))
                self.order_index = max(self.order_index, r.get("order_index", 0) + 1)
        return done

    def is_done(self, base, role):
        return "ok" in self.done.get((base, role), [])

    def attempts_so_far(self, base, role):
        return len(self.done.get((base, role), []))

    # -- small helpers

    def call(self, argv, timeout=180, what=None):
        rc, out, err = self.ex.run(argv, timeout)
        if rc != 0:
            raise RuntimeError("%s failed (rc=%s): %s" % (what or " ".join(argv), rc, (err or "").strip()[-2000:]))
        return out

    def call_json(self, argv, what):
        out = self.call(argv, what=what)
        for line in reversed(out.strip().splitlines()):
            line = line.strip()
            if line.startswith("{"):
                return json.loads(line)
        raise RuntimeError("%s printed no JSON: %r" % (what, out[-500:]))

    def broker_stats(self):
        try:
            return self.call_json(infra("broker-stats.sh"), "broker-stats")
        except Exception as e:  # noqa: BLE001
            return {"error": str(e)}

    def topic_create(self, topic, partitions):
        self.call(infra("topic.sh", "create", topic, partitions), what="topic create " + topic)
        self.live_topics.add(topic)

    def topic_delete(self, topic):
        try:
            self.call(infra("topic.sh", "delete", topic), what="topic delete " + topic)
        except Exception as e:  # noqa: BLE001
            print("  warning: %s" % e, file=sys.stderr)
        self.live_topics.discard(topic)

    def topic_offsets(self, topic):
        try:
            return int(self.call(infra("topic.sh", "offsets", topic), what="offsets").strip().splitlines()[-1])
        except Exception:  # noqa: BLE001
            return None

    def ensure_no_container(self, name):
        """A crashed earlier attempt may have left a same-named client container."""
        rc, out, _ = self.ex.run(["docker", "ps", "-a", "--filter", "name=^/%s$" % name, "--format", "{{.Names}}"], 60)
        if rc == 0 and name in out.split():
            self.ex.run(["docker", "rm", "-f", name], 60)

    def kill(self, name):
        self.ex.run(["docker", "kill", name], 60)

    def wait_for_tmpfs(self, needed_bytes):
        deadline = time.time() + TMPFS_WAIT_MAX_S
        while True:
            st = self.broker_stats()
            used = st.get("data_dir_bytes")
            if not isinstance(used, (int, float)) or used + needed_bytes * 1.05 < TMPFS_BYTES * 0.92:
                return used
            if time.time() > deadline:
                print("  warning: tmpfs still has %.1f GB used; proceeding" % (used / 1e9), file=sys.stderr)
                return used
            print("  waiting for broker to reclaim tmpfs (%.1f GB used)" % (used / 1e9), file=sys.stderr)
            time.sleep(10)

    def write(self, rec):
        rec["order_index"] = self.order_index
        self.order_index += 1
        with open(self.raw_path, "a") as f:
            f.write(json.dumps(rec, separators=(",", ":")) + "\n")
            f.flush()
            os.fsync(f.fileno())
        self.done.setdefault((rec["base_run_id"], rec["role"]), []).append(rec["status"])

    def save_log(self, run_id, suffix, argv, stderr, stdout):
        path = os.path.join(self.logs, "%s%s.log" % (run_id, suffix))
        with open(path, "w") as f:
            f.write("$ %s\n\n" % " ".join(shlex.quote(a) for a in argv))
            f.write(stderr or "")
            non_result = [l for l in (stdout or "").splitlines() if not l.startswith("RESULT ")]
            if non_result:
                f.write("\n--- unexpected stdout lines ---\n" + "\n".join(non_result) + "\n")
        return os.path.relpath(path, self.out)

    @staticmethod
    def parse_result(stdout):
        for line in reversed((stdout or "").splitlines()):
            if line.startswith("RESULT "):
                try:
                    return json.loads(line[len("RESULT "):])
                except ValueError:
                    return None
        return None

    # -- one container

    def exec_client(self, argv, name, timeout):
        self.ensure_no_container(name)
        self.live_containers.add(name)
        t0 = time.time()
        rc, out, err = self.ex.run(argv, timeout)
        t1 = time.time()
        if rc is None:
            self.kill(name)
        self.live_containers.discard(name)
        return rc, out, err, t0, t1

    def record(self, unit, client, role, base, run_id, attempt, argv, rc, out, err, t0, t1,
               before=None, after=None, topic=None, extra=None):
        res = self.parse_result(out)
        log = self.save_log(run_id, "-c" if role == "consumer" and unit["kind"] == "e2e" else "", argv, err, out)
        if res is None:
            status = "timeout" if rc is None else "no_result"
            error = "no RESULT line (rc=%s): %s" % (rc, (err or "").strip()[-400:])
        else:
            status = res.get("status") or "error"
            error = res.get("error")
            if status == "ok" and rc not in (0, None):
                status, error = "error", "RESULT ok but exit code %s" % rc
        rec = {
            "base_run_id": base, "run_id": run_id, "attempt": attempt, "scenario": unit["scenario"],
            "kind": unit["kind"], "rep": unit["rep"], "client": client, "role": role,
            "status": status, "error": error, "exit_code": rc, "topic": topic,
            "wall_start": dt.datetime.fromtimestamp(t0, dt.timezone.utc).isoformat(),
            "wall_end": dt.datetime.fromtimestamp(t1, dt.timezone.utc).isoformat(),
            "wall_s": t1 - t0, "docker_cmd": " ".join(shlex.quote(a) for a in argv), "log": log,
            "fake": bool(self.ex.fake or (res or {}).get("fake")),
            "scaling": self.sc.get(unit["scenario"], {}).get("scaling"),
            "result": res,
        }
        if before is not None and after is not None:
            rec["broker_before"], rec["broker_after"] = before, after
            if isinstance(before.get("cpu_ns"), (int, float)) and isinstance(after.get("cpu_ns"), (int, float)):
                rec["broker_cpu_s"] = (after["cpu_ns"] - before["cpu_ns"]) / 1e9
            rec["broker_mem_bytes"] = after.get("mem_bytes")
            rec["broker_mem_peak_bytes"] = after.get("mem_peak_bytes")
        if extra:
            rec.update(extra)
        return rec

    def validate(self, rec, sc, expect_msgs, expect_offsets=None, expect_instances=None):
        res = rec.get("result") or {}
        if rec["client"] in IMAGES:
            rec["instances"] = expect_instances or sc["instances"]
        if rec["status"] != "ok":
            return rec
        got_k = res.get("instances")
        if rec["client"] in IMAGES and got_k != rec["instances"]:
            rec["status"], rec["error"] = "instances_mismatch", "RESULT instances %s != requested %s" % (got_k, rec["instances"])
            return rec
        msgs = res.get("messages")
        if res.get("truncated"):
            rec["truncated"] = True
        elif isinstance(msgs, int) and msgs != expect_msgs:
            rec["status"], rec["error"] = "count_mismatch", "messages %s != expected %s" % (msgs, expect_msgs)
        h = res.get("payload_sha256")
        exp = self.expected_hash.get((sc["payload"], sc["message_size"]))
        if rec["role"] in ("producer", "prefill") and rec["client"] in IMAGES and h and exp and h != exp:
            rec["status"], rec["error"] = "hash_mismatch", "payload_sha256 %s != verified %s" % (h, exp)
        if expect_offsets is not None and rec.get("topic_end_offset") is not None:
            if rec["topic_end_offset"] != expect_offsets and rec["status"] == "ok":
                rec["status"] = "offset_mismatch"
                rec["error"] = "topic end offset %s != expected %s" % (rec["topic_end_offset"], expect_offsets)
        return rec

    # -- unit kinds

    def run_produce(self, unit, client, attempt):
        sc = self.sc[unit["scenario"]]
        cfg = producer_cfg(sc, client) if client in IMAGES else sc["config"]
        base = base_id(unit["scenario"], unit["rep"], client)
        rid = attempt_id(base, attempt)
        topic = "kbench-" + rid
        n, w = sc["num_messages"], sc["warmup_messages"]
        self.wait_for_tmpfs(sc["topic_bytes"])
        self.topic_create(topic, sc["partitions"])
        try:
            if unit["kind"] == "stock-produce":
                argv = stock_cmd("producer", "--run-id", rid, "--topic", topic,
                                 "--num-records", n + w, "--record-size", cfg["message_size"],
                                 "--producer-props", stock_producer_props(cfg))
                expect = n + w
            else:
                argv = docker_run(client, "kbench-client-" + rid, CPUSET_CLIENT, CLIENT_MEMORY,
                                  produce_args(cfg, topic, rid, n, w))
                expect = n
            before = self.broker_stats()
            rc, out, err, t0, t1 = self.exec_client(argv, "kbench-client-" + rid, matrix.MAX_DURATION_S + 420)
            after = self.broker_stats()
            off = self.topic_offsets(topic)
        finally:
            self.topic_delete(topic)
        rec = self.record(unit, client, "producer", base, rid, attempt, argv, rc, out, err, t0, t1,
                          before, after, topic, {"topic_end_offset": off, "client_config": cfg,
                                                 "variant_args": variant_args(client, "produce") if client in IMAGES else None})
        res = rec.get("result") or {}
        # acks=0 gives no delivery guarantee, so a short topic is a result, not a failure.
        exp_off = None
        if cfg["acks"] != "0" and rec["status"] == "ok":
            exp_off = (w + res["messages"]) if unit["kind"] == "produce" and res.get("truncated") else (w + n)
        self.validate(rec, sc, expect, exp_off)
        self.write(rec)
        self.ex.sleep(self.cooldown)
        return [rec]

    def run_e2e(self, unit, client, attempt):
        sc = self.sc[unit["scenario"]]
        cfg = producer_cfg(sc, client)
        base = base_id(unit["scenario"], unit["rep"], client)
        rid = attempt_id(base, attempt)
        topic = "kbench-" + rid
        n, w = sc["num_messages"], sc["warmup_messages"]
        cname, pname = "kbench-client-%s-c" % rid, "kbench-client-" + rid
        c_argv = docker_run(client, cname, CPUSET_E2E_CONSUMER, E2E_MEMORY,
                            consume_args(cfg["consumer"], topic, sc["partitions"], rid, n, w, e2e=True))
        p_argv = docker_run(client, pname, CPUSET_E2E_PRODUCER, E2E_MEMORY,
                            produce_args(cfg, topic, rid, n, w, rate=cfg["rate"], embed=True))
        self.wait_for_tmpfs(sc["topic_bytes"])
        self.topic_create(topic, sc["partitions"])
        try:
            self.ensure_no_container(cname)
            before = self.broker_stats()
            ct0 = time.time()
            self.live_containers.add(cname)
            proc = self.ex.popen(c_argv)
            c_out, c_err = [], []
            readers = [threading.Thread(target=lambda s=proc.stdout, b=c_out: b.extend(s), daemon=True),
                       threading.Thread(target=lambda s=proc.stderr, b=c_err: b.extend(s), daemon=True)]
            for r in readers:
                r.start()
            self.wait_consumer_ready(cname, proc, c_err)
            rc, out, err, t0, t1 = self.exec_client(p_argv, pname, matrix.MAX_DURATION_S + 420)
            try:
                c_rc = proc.wait(timeout=cfg["consumer"]["timeout_s"] + 60)
            except subprocess.TimeoutExpired:
                self.kill(cname)
                c_rc = None
                proc.wait(timeout=60)
            for r in readers:
                r.join(timeout=10)
            ct1 = time.time()
            self.live_containers.discard(cname)
            after = self.broker_stats()
        finally:
            self.topic_delete(topic)
        pcfg = {k: v for k, v in cfg.items() if k != "consumer"}
        prec = self.record(unit, client, "producer", base, rid, attempt, p_argv, rc, out, err, t0, t1,
                           before, after, topic, {"client_config": pcfg, "variant_args": variant_args(client, "produce")})
        crec = self.record(unit, client, "consumer", base, rid, attempt, c_argv, c_rc, "".join(c_out),
                           "".join(c_err), ct0, ct1, before, after, topic,
                           {"client_config": cfg["consumer"], "producer_config": pcfg,
                            "variant_args": variant_args(client, "consume")})
        self.validate(prec, sc, n)
        self.validate(crec, sc, n)
        # Broker stats span both containers; attribute them once, to the producer record.
        for k in ("broker_cpu_s", "broker_mem_bytes", "broker_mem_peak_bytes"):
            crec.pop(k, None)
        if prec["status"] != "ok" and crec["status"] == "ok":
            crec["status"], crec["error"] = "partner_failed", "producer side failed: %s" % prec["error"]
        if crec["status"] != "ok" and prec["status"] == "ok":
            prec["status"], prec["error"] = "partner_failed", "consumer side failed: %s" % crec["error"]
        self.write(prec)
        self.write(crec)
        self.ex.sleep(self.cooldown)
        return [prec, crec]

    def wait_consumer_ready(self, name, proc, err_lines):
        """The consumer reads from the beginning of the fresh topic, so starting late only
        delays warmup records; still wait for assignment so it is idle-polling when the
        measured producer phase begins."""
        t0 = time.time()
        running_since = None
        while time.time() - t0 < E2E_READY_MAX_S:
            if proc.poll() is not None:
                return
            if any(re.search(r"assign|ready", l, re.I) for l in err_lines):
                self.ex.sleep(1.0)
                return
            if running_since is None:
                rc, out, _ = self.ex.run(["docker", "inspect", "-f", "{{.State.Running}}", name], 30)
                if rc == 0 and out.strip() == "true":
                    running_since = time.time()
            elif time.time() - running_since > 5:
                return
            self.ex.sleep(0.5)

    def prefill(self, unit, sc):
        base = base_id(unit["scenario"], unit["rep"], "prefill")
        total = sc["num_messages"] + sc["warmup_messages"]
        for attempt in range(self.attempts_so_far(base, "prefill") + 1, self.attempts_so_far(base, "prefill") + 3):
            rid = attempt_id(base, attempt)
            topic = "kbench-" + rid
            self.wait_for_tmpfs(sc["topic_bytes"])
            try:
                self.topic_create(topic, sc["partitions"])
            except Exception as e:  # noqa: BLE001
                print("  prefill topic create failed: %s" % e, file=sys.stderr)
                continue
            argv = docker_run(PREFILL_CLIENT, "kbench-client-" + rid, CPUSET_CLIENT, CLIENT_MEMORY,
                              produce_args(matrix.prefill_config(sc), topic, rid, total, 0, PREFILL_MAX_DURATION_S))
            rc, out, err, t0, t1 = self.exec_client(argv, "kbench-client-" + rid, PREFILL_MAX_DURATION_S + 300)
            off = self.topic_offsets(topic)
            rec = self.record(unit, PREFILL_CLIENT, "prefill", base, rid, attempt, argv, rc, out, err, t0, t1,
                              topic=topic, extra={"topic_end_offset": off})
            self.validate(rec, sc, total, total, matrix.prefill_instances(sc["partitions"]))
            if rec["status"] == "ok" and off != total:
                rec["status"], rec["error"] = "offset_mismatch", "prefill offsets %s != %s" % (off, total)
            self.write(rec)
            print("  prefill %s: %s (%d msgs, offsets %s)" % (rid, rec["status"], total, off), file=sys.stderr)
            if rec["status"] == "ok":
                # Let the broker finish flushing/segment rolls before consumers start.
                self.ex.sleep(self.cooldown)
                return topic
            self.topic_delete(topic)
        return None

    def run_consume_group(self, unit, progress):
        sc = self.sc[unit["scenario"]]
        pending = [c for c in unit["clients"] if not self.is_done(base_id(unit["scenario"], unit["rep"], c), "consumer")]
        if not pending:
            return
        topic = self.prefill(unit, sc)
        n, w = sc["num_messages"], sc["warmup_messages"]
        try:
            for client in unit["clients"]:
                if client not in pending:
                    continue
                base = base_id(unit["scenario"], unit["rep"], client)
                if topic is None:
                    t = time.time()
                    rec = self.record(unit, client, "consumer", base, base, self.attempts_so_far(base, "consumer") + 1,
                                      [], None, "", "prefill failed", t, t)
                    rec["status"], rec["error"] = "skipped", "prefill failed twice"
                    self.write(rec)
                    progress(rec)
                    continue
                start_attempt = self.attempts_so_far(base, "consumer") + 1
                for attempt in (start_attempt, start_attempt + 1):
                    rid = attempt_id(base, attempt)
                    if unit["kind"] == "stock-consume":
                        argv = stock_cmd("consumer", "--run-id", rid, "--topic", topic, "--messages", n + w)
                        expect = n + w
                    else:
                        argv = docker_run(client, "kbench-client-" + rid, CPUSET_CLIENT, CLIENT_MEMORY,
                                          consume_args(sc["config"], topic, sc["partitions"], rid, n, w))
                        expect = n
                    before = self.broker_stats()
                    rc, out, err, t0, t1 = self.exec_client(argv, "kbench-client-" + rid, sc["config"]["timeout_s"] + 300)
                    after = self.broker_stats()
                    ccfg = {k: v for k, v in sc["config"].items() if k != "prefill"}
                    rec = self.record(unit, client, "consumer", base, rid, attempt, argv, rc, out, err, t0, t1,
                                      before, after, topic, {"prefill_topic": topic, "client_config": ccfg,
                                                             "variant_args": variant_args(client, "consume") if client in IMAGES else None})
                    self.validate(rec, sc, expect)
                    self.write(rec)
                    progress(rec)
                    self.ex.sleep(self.cooldown)
                    if rec["status"] == "ok":
                        break
        finally:
            if topic:
                self.topic_delete(topic)

    def unit_complete(self, unit):
        return all(self.is_done(b, r) for b, r, _ in unit_runs(unit))

    def run_unit(self, unit, progress):
        if unit["kind"] in ("consume", "stock-consume"):
            return self.run_consume_group(unit, progress)
        client = unit["clients"][0]
        base = base_id(unit["scenario"], unit["rep"], client)
        roles = ("producer", "consumer") if unit["kind"] == "e2e" else ("producer",)
        if all(self.is_done(base, r) for r in roles):
            return
        start = self.attempts_so_far(base, "producer") + 1
        for attempt in (start, start + 1):
            try:
                recs = (self.run_e2e if unit["kind"] == "e2e" else self.run_produce)(unit, client, attempt)
            except Aborted:
                raise
            except Exception as e:  # noqa: BLE001
                for name in list(self.live_containers):
                    self.kill(name)
                self.live_containers.clear()
                t = time.time()
                rec = self.record(unit, client, "producer", base, attempt_id(base, attempt), attempt, [], None, "",
                                  "orchestrator error: %s" % e, t, t)
                rec["status"], rec["error"] = "infra_error", str(e)
                self.write(rec)
                recs = [rec]
                self.ex.sleep(self.cooldown)
            for r in recs:
                progress(r)
            if all(r["status"] == "ok" for r in recs):
                return

    # -- setup

    def preflight(self, clients, scenarios):
        env = {"fake": self.ex.fake, "started": dt.datetime.now(dt.timezone.utc).isoformat(),
               "args": vars(self.args), "pinning": PINNING, "cpuset_client": CPUSET_CLIENT,
               "client_memory": CLIENT_MEMORY, "cpuset_e2e_producer": CPUSET_E2E_PRODUCER,
               "cpuset_e2e_consumer": CPUSET_E2E_CONSUMER, "e2e_memory": E2E_MEMORY, "network": NETWORK,
               "runner": runner_info(self.ex), "images": {}, "payload_hashes": [],
               "rust_config_profile": RUST_CONFIG_PROFILE,
               "client_variants": {c: matrix.CLIENT_VARIANTS[c] for c in clients if c in IMAGES}}
        out = self.call(infra("up.sh"), timeout=300, what="infra/up.sh")
        if "READY" not in out:
            raise Aborted("infra/up.sh did not print READY: %r" % out[-500:])
        rc, out, _ = self.ex.run(["docker", "ps", "--filter", "name=^/kbench-client-", "--format", "{{.Names}}"], 60)
        if rc == 0 and out.strip():
            raise Aborted("leftover client containers are running (%s); remove them first" % out.split())
        hostinfo = self.call_json(infra("hostinfo.sh"), "hostinfo")
        if self.ex.fake:
            hostinfo["fake"] = True
        with open(os.path.join(self.out, "hostinfo.json"), "w") as f:
            json.dump(hostinfo, f, indent=2)
        rc, out, _ = self.ex.run(["docker", "version", "--format", "{{json .}}"], 60)
        env["docker_version"] = _first_json(out) if rc == 0 else None

        # Variants sharing a harness share its image, so images and payload hashes are per harness.
        need = set(matrix.variant(c)["harness"] for c in clients if c in IMAGES)
        if any(s["kind"] in ("consume", "stock-consume") for s in scenarios):
            need.add(matrix.variant(PREFILL_CLIENT)["harness"])
        for c in sorted(need):
            rc, out, err = self.ex.run(["docker", "image", "inspect", HARNESS_IMAGES[c]], 60)
            if rc != 0:
                raise Aborted("image %s not found: %s" % (HARNESS_IMAGES[c], err.strip()))
            info = json.loads(out)[0]
            env["images"][c] = {"image": HARNESS_IMAGES[c], "id": info.get("Id"), "repo_digests": info.get("RepoDigests"),
                                "created": info.get("Created"), "size": info.get("Size"),
                                "labels": (info.get("Config") or {}).get("Labels"),
                                "variants": [v for v in clients if v in IMAGES and matrix.variant(v)["harness"] == c]}
            print("image %s: %s" % (HARNESS_IMAGES[c], info.get("Id")), file=sys.stderr)

        for pl, size in matrix.payload_variants(scenarios):
            entry = {"payload": pl, "message_size": size, "seed": matrix.PAYLOAD_SEED}
            for c in sorted(need):
                rc, out, err = self.ex.run(["docker", "run", "--rm", "--name", "kbench-hash-%s-%s-%d" % (c, pl, size),
                                            HARNESS_IMAGES[c], "payload-hash", "--payload", pl, "--message-size", str(size),
                                            "--seed", str(matrix.PAYLOAD_SEED)], 300)
                entry[c] = payload_mod.parse_hash_output(out) if rc == 0 else None
                if entry[c] is None:
                    raise Aborted("payload-hash failed for %s %s/%d: rc=%s %s" % (c, pl, size, rc, err.strip()[-500:]))
            vals = set(entry[c] for c in need)
            if len(vals) != 1:
                raise Aborted("payload hash MISMATCH for %s/%d: %s" % (pl, size, {c: entry[c] for c in need}))
            if not self.ex.fake:
                entry["python_reference"] = payload_mod.payload_sha256(pl, size, matrix.PAYLOAD_SEED)
                entry["python_reference_agrees"] = entry["python_reference"] in vals
                if not entry["python_reference_agrees"]:
                    print("WARNING: harnesses agree but differ from the Python reference for %s/%d" % (pl, size),
                          file=sys.stderr)
            self.expected_hash[(pl, size)] = vals.pop()
            env["payload_hashes"].append(entry)
            print("payload %s/%d: %s" % (pl, size, self.expected_hash[(pl, size)]), file=sys.stderr)
        env["background_load_start"] = self.docker_stats()
        return env

    def docker_stats(self):
        """Read-only snapshot of every container's load, to document background noise."""
        rc, out, _ = self.ex.run(["docker", "stats", "--no-stream", "--no-trunc", "--format", "{{json .}}"], 120)
        if rc != 0:
            return None
        rows = []
        for line in out.splitlines():
            try:
                rows.append(json.loads(line))
            except ValueError:
                pass
        return rows

    def cleanup(self):
        for name in list(self.live_containers):
            self.kill(name)
        for t in list(self.live_topics):
            self.topic_delete(t)


def _first_json(out):
    for line in (out or "").splitlines():
        try:
            return json.loads(line)
        except ValueError:
            continue
    return None


# ----------------------------------------------------------------------------------------
# Main


def parse_args(argv):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--reps", type=int, default=3)
    p.add_argument("--scale", type=float, default=1.0)
    p.add_argument("--only", default=None, help="regex on scenario name")
    p.add_argument("--clients", default=",".join(matrix.DEFAULT_CLIENTS),
                   help="comma-separated client variants: %s" % ", ".join(matrix.CLIENT_VARIANTS))
    p.add_argument("--seed", type=int, default=1, help="seed for the run order shuffle (payload seed is fixed at 42)")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--skip-stock", action="store_true")
    p.add_argument("--skip-scaling", action="store_true", help="drop the --instances K scaling scenarios")
    p.add_argument("--out", default=None,
                   help="results dir; relative names go under %s (default %s/<timestamp>)" % (RESULTS_ROOT, RESULTS_ROOT))
    p.add_argument("--resume", action="store_true", help="skip run_ids already completed ok in <out>/raw.jsonl")
    p.add_argument("--fake", action="store_true", help="no docker: use bench/fake.py (output marked fake)")
    p.add_argument("--cooldown-s", type=float, default=5.0)
    p.add_argument("--no-report", action="store_true", help="do not run bench/report.py at the end")
    p.add_argument("--rust-config-profile", choices=["matched", "native"], default="matched",
                   help="matched: librdkafka producer knobs forced to Java-like values; native: those knobs left at librdkafka defaults")
    a = p.parse_args(argv)
    a.clients = [c.strip() for c in a.clients.split(",") if c.strip()]
    for c in a.clients:
        if c not in IMAGES:
            p.error("unknown client %s (known: %s)" % (c, ", ".join(IMAGES)))
    if len(set(a.clients)) != len(a.clients):
        p.error("duplicate client in --clients")
    if a.resume and not a.out:
        p.error("--resume needs --out")
    return a


def select_scenarios(a):
    scenarios = matrix.build(scale=a.scale, include_stock=not a.skip_stock)
    if a.skip_scaling:
        scenarios = [s for s in scenarios if not matrix.is_scaling(s)]
    if a.only:
        rx = re.compile(a.only)
        scenarios = [s for s in scenarios if rx.search(s["name"])]
    return scenarios


def dry_run(a, scenarios, plan):
    sc_by = {s["name"]: s for s in scenarios}
    ests = plan_estimates(plan, sc_by, a.cooldown_s)
    idx = 0
    fam = {}
    by_client = {}
    msgs = byts = 0
    for unit, est in zip(plan, ests):
        sc = sc_by[unit["scenario"]]
        per_client = matrix.estimate_unit_s(sc, a.cooldown_s)
        for c in unit["clients"]:
            bc = by_client.setdefault(c, {"runs": 0, "s": 0.0})
            bc["runs"] += 1
            bc["s"] += per_client
        shared = by_client.setdefault("(prefill, tmpfs wait)", {"runs": 0, "s": 0.0})
        shared["s"] += max(0.0, est - per_client * len(unit["clients"]))
        if unit["kind"] in ("consume", "stock-consume"):
            shared["runs"] += 1
        cmds = unit_commands(unit, sc)
        for cmd in cmds:
            idx += 1
            print("%4d  rep %d  %-34s %-10s ~%5.0fs  %s" % (idx, unit["rep"], unit["scenario"],
                                                           ",".join(unit["clients"]), est,
                                                           " ".join(shlex.quote(x) for x in cmd)))
        f = fam.setdefault(unit_family(unit, sc), {"runs": 0, "prefills": 0, "s": 0.0, "bytes": 0})
        f["runs"] += len(unit["clients"])
        f["s"] += est
        per = sc["num_messages"] + sc["warmup_messages"]
        if unit["kind"] in ("consume", "stock-consume"):
            f["prefills"] += 1
            f["bytes"] += per * sc["message_size"]
        else:
            f["bytes"] += per * sc["message_size"] * len(unit["clients"])
        msgs += per * len(unit["clients"])
        byts += per * sc["message_size"] * len(unit["clients"])
    total = sum(ests)
    measured = sum(len(unit_runs(u)) for u in plan)
    runs = sum(len(u["clients"]) for u in plan)
    prefills = sum(1 for u in plan if u["kind"] in ("consume", "stock-consume"))
    by_kind = {}
    for s in scenarios:
        k = s["kind"] + (" (scaling)" if matrix.is_scaling(s) else "")
        by_kind[k] = by_kind.get(k, 0) + 1
    print()
    print("scenarios: %d (%s)" % (len(scenarios), ", ".join("%s %d" % kv for kv in sorted(by_kind.items()))))
    print("reps: %d, clients: %s, rust config profile: %s, seed: %d, scale: %g" % (
        a.reps, ",".join(a.clients), a.rust_config_profile, a.seed, a.scale))
    for c in a.clients:
        v = matrix.variant(c)
        print("  variant %-11s image %s, kinds %s, produce args %s, consume args %s" % (
            c, v["image"], "/".join(v["kinds"]), " ".join(v["args"]["produce"]) or "-", " ".join(v["args"]["consume"]) or "-"))
    print("client runs: %d (measured result records: %d, e2e runs count producer+consumer), prefill runs: %d, "
          "containers: %d" % (runs, measured, prefills, idx))
    print("client-side messages: %s, payload bytes: %.1f GB (prefills excluded)" % ("{:,}".format(msgs), byts / 1e9))
    print()
    print("%-34s %6s %9s %14s %10s %7s" % ("breakdown", "runs", "prefills", "prefill+run GB", "ETA", "share"))
    for name, f in sorted(fam.items(), key=lambda kv: -kv[1]["s"]):
        print("%-34s %6d %9d %14.1f %10s %6.1f%%" % (name, f["runs"], f["prefills"], f["bytes"] / 1e9, fmt_dur(f["s"]),
                                                   100.0 * f["s"] / total if total else 0.0))
    print("%-34s %6d %9d %14s %10s" % ("TOTAL", runs, prefills, "", fmt_dur(total)))
    print()
    print("%-34s %6s %10s %7s" % ("by client", "runs", "ETA", "share"))
    for name, bc in sorted(by_client.items(), key=lambda kv: -kv[1]["s"]):
        print("%-34s %6d %10s %6.1f%%" % (name, bc["runs"], fmt_dur(bc["s"]), 100.0 * bc["s"] / total if total else 0.0))
    print()
    print("estimated duration: %s (heuristic throughput guesses + %.0fs docker/topic overhead per run + %.0fs cooldown"
          " + %.0fs tmpfs reclaim wait when a big topic follows another)"
          % (fmt_dur(total), matrix.RUN_OVERHEAD_S, a.cooldown_s, matrix.TMPFS_WAIT_GUESS_S))
    if total > 5 * 3600:
        print("NOTE: the plan exceeds 5 hours; use --only, --skip-scaling, --skip-stock, --reps or --scale to shorten it.")


def main(argv):
    a = parse_args(argv)
    global RUST_CONFIG_PROFILE
    RUST_CONFIG_PROFILE = a.rust_config_profile
    try:
        scenarios = select_scenarios(a)
    except ValueError as e:
        print("invalid matrix: %s" % e, file=sys.stderr)
        return 2
    if not scenarios:
        print("no scenarios selected", file=sys.stderr)
        return 2
    for sc in scenarios:
        if sc.get("per_client"):
            sc["resolved_per_client"] = {c: producer_cfg(sc, c) for c in scenario_clients(sc, a.clients)}
    plan = make_plan(scenarios, a.reps, a.seed, a.clients)
    if a.dry_run:
        dry_run(a, scenarios, plan)
        return 0

    stamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    out = a.out or (("FAKE-" if a.fake else "") + stamp)
    if not os.path.isabs(out):
        if not os.path.isdir(RESULTS_ROOT):
            print("%s does not exist: run inside the kbench-runner container (./kbench.sh run ...) or pass an "
                  "absolute --out" % RESULTS_ROOT, file=sys.stderr)
            return 2
        out = os.path.join(RESULTS_ROOT, out)
    if os.path.exists(os.path.join(out, "raw.jsonl")) and not a.resume:
        print("%s already has raw.jsonl; use --resume or a new --out" % out, file=sys.stderr)
        return 2
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "matrix.json"), "w") as f:
        json.dump({"fake": a.fake, "scenarios": scenarios, "plan": plan, "reps": a.reps, "seed": a.seed,
                   "scale": a.scale, "clients": a.clients, "payload_seed": matrix.PAYLOAD_SEED,
                   "client_variants": {c: matrix.CLIENT_VARIANTS[c] for c in a.clients},
                   "rust_config_profile": a.rust_config_profile,
                   "warmup_rule": {"fraction": matrix.WARMUP_FRACTION, "min": matrix.WARMUP_MIN},
                   "scaling_rule": {"ks": matrix.SCALING_KS, "partitions": matrix.SCALING_PARTITIONS,
                                    "bytes_cap": matrix.SCALING_BYTES_CAP, "quantum": matrix.SCALING_COUNT_QUANTUM,
                                    "profiles": matrix.SCALING_PROFILES},
                   "prefill": {"client": PREFILL_CLIENT, "max_instances": matrix.PREFILL_MAX_INSTANCES},
                   "pinning": PINNING,
                   "max_duration_s": matrix.MAX_DURATION_S, "cooldown_s": a.cooldown_s}, f, indent=2)

    ex = FakeExec() if a.fake else Exec()
    orch = Orchestrator(a, ex, scenarios, out)
    # docker stop sends SIGTERM; as PID 1 python would ignore it, so turn it into the same
    # cleanup path as Ctrl-C (kill live kbench-client-* containers, delete topics).
    signal.signal(signal.SIGTERM, _sigterm)
    try:
        env = orch.preflight(a.clients, scenarios)
    except (Aborted, RuntimeError) as e:
        print("ABORT: %s" % e, file=sys.stderr)
        return 3
    env_path = os.path.join(out, "env.json")
    if a.resume and os.path.exists(env_path):
        with open(env_path) as f:
            prev = json.load(f)
        prev.setdefault("resumes", []).append(env)
        env = prev
    with open(env_path, "w") as f:
        json.dump(env, f, indent=2)

    sc_by = {s["name"]: s for s in scenarios}
    todo = [u for u in plan if not orch.unit_complete(u)]
    ests = plan_estimates(todo, sc_by, a.cooldown_s)
    total_runs = sum(len(u["clients"]) for u in todo)
    state = {"n": 0, "t0": time.time(), "est_done": 0.0, "i": 0, "unit_done": 0}
    print("%d client runs to do (%d units skipped as complete), estimated %s; writing %s"
          % (total_runs, len(plan) - len(todo), fmt_dur(sum(ests)), out), file=sys.stderr)

    def progress(rec):
        if rec["role"] == "consumer" and rec["kind"] == "e2e":
            return
        state["n"] += 1
        state["unit_done"] += 1
        i = state["i"]
        frac = min(1.0, state["unit_done"] / float(len(todo[i]["clients"])))
        elapsed = time.time() - state["t0"]
        done_est = state["est_done"] + ests[i] * frac
        ratio = elapsed / done_est if done_est > 0 else 1.0
        eta = (sum(ests[i + 1:]) + ests[i] * (1 - frac)) * ratio
        res = rec.get("result") or {}
        thr = res.get("throughput_msgs_per_s")
        mb = res.get("throughput_mb_per_s")
        perf = ""
        if isinstance(thr, (int, float)) and isinstance(mb, (int, float)):
            perf = "%12s msg/s %8.1f MB/s" % ("{:,.0f}".format(thr), mb)
        print("[%d/%d] rep %d %-34s %-10s %-14s %s%s wall %.1fs | elapsed %s ETA %s" % (
            state["n"], total_runs, rec["rep"], rec["scenario"], rec["client"], rec["status"], perf,
            " TRUNCATED" if rec.get("truncated") else "", rec["wall_s"], fmt_dur(elapsed), fmt_dur(eta)),
            flush=True)
        if rec["status"] != "ok":
            print("    error: %s (log %s)" % (rec.get("error"), rec.get("log")), flush=True)

    try:
        for i, unit in enumerate(todo):
            state["i"] = i
            state["unit_done"] = 0
            try:
                orch.run_unit(unit, progress)
            except KeyboardInterrupt:
                raise
            except Exception:  # noqa: BLE001
                # Any bug or infra hiccup costs one unit, not the whole multi-hour run.
                print("unit %s rep %d crashed:\n%s" % (unit["scenario"], unit["rep"], traceback.format_exc()),
                      file=sys.stderr, flush=True)
                orch.cleanup()
            state["est_done"] += ests[i]
    except KeyboardInterrupt:
        print("\ninterrupted; cleaning up kbench containers/topics (resume with --resume --out %s)" % out, file=sys.stderr)
        orch.cleanup()
        return 130
    env["background_load_end"] = orch.docker_stats()
    env["finished"] = dt.datetime.now(dt.timezone.utc).isoformat()
    with open(env_path, "w") as f:
        json.dump(env, f, indent=2)
    print("done: %s" % out, flush=True)
    if a.no_report:
        return 0
    rc, rout, rerr = Exec().run([sys.executable, os.path.join(BENCH_DIR, "report.py"), out], timeout=1800)
    sys.stderr.write(rerr or "")
    print((rout or "").strip() or "report.py printed nothing", flush=True)
    if rc != 0:
        print("report.py failed (rc=%s); rerun with ./kbench.sh report %s" % (rc, os.path.basename(out)), flush=True)
        return 4
    return 0


def _sigterm(signum, frame):
    raise KeyboardInterrupt()


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
