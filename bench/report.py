#!/usr/bin/env python3
"""Report generator: python3 bench/report.py <out>

Reads <out>/raw.jsonl, hostinfo.json, matrix.json (and env.json if present) and writes
<out>/summary.csv, <out>/report.md and <out>/charts/*.png.
"""

import csv
import json
import math
import os
import statistics
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

BENCH_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BENCH_DIR)
import matrix as matrix_mod  # noqa: E402

CLIENT_ORDER = tuple(matrix_mod.CLIENT_VARIANTS)
STOCK = "java-stock"
COLORS = {"java": "#d9822b", "rust": "#3b6ea5", "rust-tuned": "#2a9d8f", "rust-lowlat": "#8e6bb0", "java-stock": "#9a9a9a"}
LABELS = {"java": "Java", "rust": "Rust", "rust-tuned": "rust-tuned", "rust-lowlat": "rust-lowlat", "java-stock": "java-stock"}
PROP_NAMES = {"batch_size": "batch.size", "buffer_memory": "buffer.memory", "linger_ms": "linger.ms",
              "max_in_flight": "max.in.flight", "message_size": "message size", "idempotence": "enable.idempotence",
              "compression": "compression.type"}
CV_FLAG_PCT = 5.0
SHORT_WINDOW_S = 2.0
# A step to the next K that adds less than this is where a client "stops scaling".
SCALING_GAIN_MIN = 1.10
# Fraction of a cpuset's vCPUs above which that side is called CPU-saturated.
SATURATED = 0.85
VALUE_ORDER = {"acks": ["0", "1", "all", "all+idempotence"],
               "compression": ["none", "gzip", "snappy", "lz4", "zstd", "lz4 (text)", "zstd (text)"],
               "client_settings": ["library defaults", "best batch.size"]}
FLAT_SKIP = {"broker.config", "containers_stats"}
SWEEP_TITLES = {
    "message_size": "Message size (bytes)", "acks": "acks", "compression": "Compression (text payload)",
    "linger_ms": "linger.ms", "batch_size": "batch.size (bytes)", "partitions": "Partitions",
    "profile": "Max throughput profile", "client_settings": "Per-client settings (each client at its own defaults or best batch.size)",
}

# ----------------------------------------------------------------------------------------
# Loading and statistics


def load(out):
    raw = []
    with open(os.path.join(out, "raw.jsonl")) as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    raw.append(json.loads(line))
                except ValueError:
                    print("warning: skipping unparsable raw.jsonl line", file=sys.stderr)

    def opt(name):
        p = os.path.join(out, name)
        if os.path.exists(p):
            with open(p) as f:
                return json.load(f)
        return {}

    return raw, opt("hostinfo.json"), opt("matrix.json"), opt("env.json")


def final_records(raw):
    """Latest ok attempt per (base_run_id, role), else the latest attempt."""
    best = {}
    for r in raw:
        key = (r.get("base_run_id"), r.get("role"))
        cur = best.get(key)
        if cur is None or r.get("status") == "ok" or cur.get("status") != "ok":
            best[key] = r
    return list(best.values())


def num(x):
    return x if isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(x) else None


def med(vals):
    v = [x for x in vals if num(x) is not None]
    return statistics.median(v) if v else None


def spread(vals):
    v = [x for x in vals if num(x) is not None]
    if not v:
        return None
    mean = statistics.fmean(v)
    sd = statistics.stdev(v) if len(v) > 1 else 0.0
    return {"n": len(v), "median": statistics.median(v), "min": min(v), "max": max(v), "mean": mean,
            "stddev": sd, "cv_pct": (sd / mean * 100.0) if mean else None, "values": v}


def label(c):
    return LABELS.get(c, c)


def param(res, name):
    """Harness params: the Java harness uses snake_case keys, the Rust harness hyphenated ones."""
    p = (res or {}).get("params") or {}
    for k in (name, name.replace("_", "-")):
        if k in p:
            return p[k]
    return None


def fmt_overrides(over):
    parts = []
    for k, v in sorted(over.items(), key=lambda kv: (kv[0] != "batch_size", kv[0])):
        if k == "extra":
            parts += ["`%s=%s`" % kv for kv in sorted(v.items())]
        elif k == "config_profile":
            parts.append("Rust config profile `%s`" % v)
        else:
            parts.append("`%s=%s`" % (PROP_NAMES.get(k, k), v))
    return ", ".join(parts) or "none"


def g(d, *path):
    for p in path:
        if not isinstance(d, dict):
            return None
        d = d.get(p)
    return d


def jvm_gc(res):
    j = res.get("jvm") or {}
    count = j.get("gc_total_count")
    tm = j.get("gc_total_time_ms")
    if count is None and isinstance(j.get("gc"), list):
        count = sum(x.get("count") or 0 for x in j["gc"])
        tm = sum(x.get("time_ms") or 0 for x in j["gc"])
    return count, tm, j.get("jit_compile_time_ms")


def aggregate(recs):
    """recs: ok records of one (scenario, client, role)."""
    rs = [r.get("result") or {} for r in recs]
    a = {
        "n": len(recs),
        "msgs": spread([r.get("throughput_msgs_per_s") for r in rs]),
        "mb": spread([r.get("throughput_mb_per_s") for r in rs]),
        "lat_kind": next((r.get("latency_kind") for r in rs if r.get("latency_kind")), None),
    }
    for p in ("p50", "p90", "p99", "p99_9", "p99_99", "max", "mean"):
        a["lat_" + p] = med([g(r, "latency_us", p) for r in rs])
    for k in ("cpu_total_s", "cpu_cores_avg", "cpu_s_per_million_msgs", "rss_peak_bytes", "rss_avg_bytes",
              "threads_peak", "cgroup_cpu_s", "cgroup_mem_peak_bytes"):
        a[k] = med([g(r, "resources", k) for r in rs])
    a["broker_cpu_s"] = med([r.get("broker_cpu_s") for r in recs])
    a["broker_cores"] = med([broker_cores(r) for r in recs])
    a["instances"] = med([r.get("instances") or g(r, "result", "instances") for r in recs])
    a["broker_mem_peak_bytes"] = med([r.get("broker_mem_peak_bytes") for r in recs])
    gcs = [jvm_gc(r) for r in rs if r.get("jvm")]
    a["gc_count"] = med([x[0] for x in gcs]) if gcs else None
    a["gc_time_ms"] = med([x[1] for x in gcs]) if gcs else None
    a["jit_ms"] = med([x[2] for x in gcs]) if gcs else None
    a["gc_pct"] = med([(x[1] or 0) / (r.get("duration_s") * 1000.0) * 100 for x, r in
                       zip(gcs, [r for r in rs if r.get("jvm")]) if num(r.get("duration_s"))]) if gcs else None
    a["duration_s"] = med([r.get("duration_s") for r in rs])
    a["startup_ms"] = med([r.get("startup_ms") for r in rs])
    a["first_message_ms"] = med([r.get("first_message_ms") for r in rs])
    a["rate_lag_max_us"] = med([r.get("rate_lag_max_us") for r in rs])
    a["truncated"] = sum(1 for r in rs if r.get("truncated"))
    thr = [(num(r.get("throughput_msgs_per_s")), rec) for r, rec in zip(rs, recs) if num(r.get("throughput_msgs_per_s"))]
    if thr:
        thr.sort(key=lambda t: t[0])
        a["median_rec"] = thr[(len(thr) - 1) // 2][1]
    return a


def broker_cores(rec):
    """Average broker vCPUs while the client was active. broker_cpu_s spans the whole client
    container (startup, warmup, measured window), so divide by the measured window stretched to
    include the warmup share, bounded by the container wall time."""
    bcpu = num(rec.get("broker_cpu_s"))
    res = rec.get("result") or {}
    dur, msgs = num(res.get("duration_s")), num(res.get("messages"))
    if bcpu is None or not dur or not msgs:
        return None
    warm = num(param(res, "warmup_messages")) or 0
    active = dur * (msgs + warm) / msgs
    wall = num(rec.get("wall_s"))
    if wall and wall >= dur:
        active = min(active, wall)
    return bcpu / active if active > 0 else None


def cpuset_count(spec):
    if not spec:
        return None
    n = 0
    try:
        for part in str(spec).split(","):
            part = part.strip()
            if "-" in part:
                lo, hi = part.split("-")
                n += int(hi) - int(lo) + 1
            elif part:
                n += 1
    except ValueError:
        return None
    return n or None


def is_scaling(sc):
    return bool((sc or {}).get("scaling"))


def compare(ja, ra):
    """ra/ja ratio of median throughput and whether the min/max ranges across reps
    are disjoint (a deliberately conservative noise test for small rep counts)."""
    if not ja or not ra or not ja.get("msgs") or not ra.get("msgs"):
        return None
    j, r = ja["msgs"], ra["msgs"]
    ratio = r["median"] / j["median"] if j["median"] else None
    if j["n"] < 2 or r["n"] < 2:
        sig = None
    else:
        sig = r["min"] > j["max"] or j["min"] > r["max"]
    return {"ratio": ratio, "significant": sig}


# ----------------------------------------------------------------------------------------
# Formatting


def f_int(x):
    return "-" if num(x) is None else "{:,.0f}".format(x)


def f_1(x):
    return "-" if num(x) is None else "{:,.1f}".format(x)


def f_2(x):
    return "-" if num(x) is None else "{:,.2f}".format(x)


def f_ms(us):
    return "-" if num(us) is None else ("{:,.2f}".format(us / 1000.0) if us < 100000 else "{:,.0f}".format(us / 1000.0))


def f_mb(b):
    return "-" if num(b) is None else "{:,.0f}".format(b / 1e6)


def f_ratio(c):
    if not c or c["ratio"] is None:
        return "-"
    return "%.2fx" % c["ratio"]


def f_sig(c):
    if not c:
        return "-"
    if c["significant"] is None:
        return "n/a (<2 reps)"
    return "yes" if c["significant"] else "no (within noise)"


def f_thr(a, key="msgs", fmt=f_int):
    s = a.get(key) if a else None
    if not s:
        return "-"
    return "%s [%s - %s]" % (fmt(s["median"]), fmt(s["min"]), fmt(s["max"]))


def md_table(headers, rows):
    out = ["| " + " | ".join(headers) + " |", "|" + "|".join("---" for _ in headers) + "|"]
    for r in rows:
        out.append("| " + " | ".join(str(c).replace("|", "\\|") for c in r) + " |")
    return "\n".join(out)


def value_key(sweep, v):
    order = VALUE_ORDER.get(sweep)
    if order and str(v) in order:
        return (0, order.index(str(v)), "")
    if isinstance(v, (int, float)):
        return (0, v, "")
    return (1, 0, str(v))


# ----------------------------------------------------------------------------------------
# Report


class Report:
    def __init__(self, out):
        self.out = out
        self.raw, self.hostinfo, self.matrix, self.env = load(out)
        self.scenarios = {s["name"]: s for s in self.matrix.get("scenarios", [])}
        if not self.scenarios:
            self.scenarios = {s["name"]: s for s in matrix_mod.build()}
        self.final = final_records(self.raw)
        self.fake = (bool(self.matrix.get("fake")) or bool(self.env.get("fake")) or bool(self.hostinfo.get("fake"))
                     or any(r.get("fake") or g(r, "result", "fake") for r in self.raw))
        self.agg = {}
        groups = {}
        for r in self.final:
            if r.get("role") == "prefill" or r.get("status") != "ok":
                continue
            groups.setdefault((r["scenario"], r["client"], r["role"]), []).append(r)
        for k, recs in groups.items():
            self.agg[k] = aggregate(recs)
        seen = {k[1] for k in self.agg}
        self.clients = [c for c in (self.matrix.get("clients") or []) if c in CLIENT_ORDER]
        self.clients += [c for c in CLIENT_ORDER if c in seen and c not in self.clients]
        if not self.clients:
            self.clients = list(matrix_mod.DEFAULT_CLIENTS)
        self.variants = (self.matrix.get("client_variants") or self.env.get("client_variants")
                         or {c: matrix_mod.CLIENT_VARIANTS[c] for c in self.clients if c in matrix_mod.CLIENT_VARIANTS})
        pin = self.env.get("pinning") or {}
        self.client_vcpus = cpuset_count(pin.get("client") or self.env.get("cpuset_client")) or 16
        self.broker_cpuset = g(self.hostinfo, "broker", "container", "cpuset_cpus")
        self.broker_vcpus = cpuset_count(self.broker_cpuset) or 14
        self.charts_dir = os.path.join(out, "charts")
        os.makedirs(self.charts_dir, exist_ok=True)
        self.charts = {}

    def with_data(self, names, role=None):
        """Harness clients (run order) with at least one ok result in the given scenarios."""
        return [c for c in self.clients if any(self.a(n, c, role) for n in names)]

    def core(self, *kinds):
        """Non-scaling scenarios of the given kinds, in matrix order."""
        return [sc for sc in self.scenarios.values() if sc["kind"] in kinds and not is_scaling(sc)]

    def a(self, scenario, client, role=None):
        sc = self.scenarios.get(scenario, {})
        if role is None:
            role = "consumer" if sc.get("kind") in ("consume", "stock-consume") else "producer"
        return self.agg.get((scenario, client, role))

    def cmp(self, scenario, role=None, client="rust", ref="java"):
        return compare(self.a(scenario, ref, role), self.a(scenario, client, role))

    def cfg_for(self, sc, client):
        resolved = g(sc, "resolved_per_client", client)
        return resolved if resolved else matrix_mod.client_config(sc, client)

    def value_cell(self, value, sc, cols):
        if not sc.get("per_client"):
            return value
        return "%s (%s)" % (value, ", ".join("%s batch.size %s" % (label(c), f_int(self.cfg_for(sc, c).get("batch_size")))
                                            for c in cols))

    def overrides_note(self, scs, cols):
        """One line per scenario with per_client overrides, listing each shown client's overrides."""
        L = []
        for sc in scs:
            if not sc.get("per_client"):
                continue
            parts = []
            for c in cols:
                over = matrix_mod.client_overrides(sc, c)
                parts.append("%s %s" % (label(c), fmt_overrides(over)))
            L.append("- `%s`: %s." % (sc["name"], "; ".join(parts)))
        if not L:
            return []
        return ["Per-client overrides (everything else as in the baseline):", ""] + L + [""]

    def ratio_cols(self, cols, a_of, extra_ref=None):
        """Header and cell values for client/Java ratio and noise columns, plus optional client/ref pairs."""
        hdr, cells = [], []
        if "java" in cols:
            for c in cols:
                if c == "java":
                    continue
                cm = compare(a_of("java"), a_of(c))
                hdr += ["%s/Java" % label(c), "beyond noise (%s)" % label(c)]
                cells += [f_ratio(cm), f_sig(cm)]
        for c, ref in extra_ref or []:
            if c in cols and ref in cols:
                cm = compare(a_of(ref), a_of(c))
                hdr += ["%s/%s" % (label(c), label(ref))]
                cells += [f_ratio(cm)]
        return hdr, cells

    def sweep_rows(self, kind, sweep):
        rows = []
        for sc in self.scenarios.values():
            if sc["kind"] != kind:
                continue
            for t in sc["sweeps"]:
                if t["sweep"] == sweep:
                    rows.append((t["value"], sc))
        rows.sort(key=lambda t: value_key(sweep, t[0]))
        return rows

    # -- csv

    def write_csv(self):
        cols = ["scenario", "kind", "sweeps", "scaling_profile", "instances", "client", "role", "client_overrides",
                "batch_size", "n_ok", "n_truncated",
                "msgs_per_s_median", "msgs_per_s_min", "msgs_per_s_max", "msgs_per_s_mean", "msgs_per_s_stddev",
                "msgs_per_s_cv_pct", "mb_per_s_median", "mb_per_s_min", "mb_per_s_max", "mb_per_s_mean",
                "mb_per_s_stddev", "mb_per_s_cv_pct", "latency_kind", "lat_p50_us", "lat_p99_us", "lat_p99_9_us",
                "lat_max_us", "cpu_total_s", "cpu_cores_avg", "cpu_s_per_million_msgs", "rss_peak_bytes",
                "threads_peak", "broker_cpu_s", "broker_cores_avg", "gc_count", "gc_time_ms", "gc_pct_of_window", "jit_compile_ms",
                "startup_ms", "rate_lag_max_us", "ratio_vs_java_msgs", "beyond_noise_vs_java", "ratio_vs_rust_msgs", "fake"]
        path = os.path.join(self.out, "summary.csv")
        with open(path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(cols)
            for (scn, client, role), a in sorted(self.agg.items()):
                sc = self.scenarios.get(scn, {})
                harness = client in matrix_mod.CLIENT_VARIANTS
                c = self.cmp(scn, role, client) if harness and client != "java" else None
                cr = self.cmp(scn, role, client, "rust") if harness and client not in ("java", "rust") else None
                over = matrix_mod.client_overrides(sc, client) if harness and sc.get("per_client") else None
                batch = None
                if role == "producer" and sc.get("config"):
                    batch = (self.cfg_for(sc, client) if harness else sc["config"]).get("batch_size")
                m, mb = a["msgs"] or {}, a["mb"] or {}
                w.writerow([scn, sc.get("kind"), ";".join("%s=%s" % (t["sweep"], t["value"]) for t in sc.get("sweeps", [])),
                            g(sc, "scaling", "profile"), a["instances"] if a["instances"] is not None else sc.get("instances", 1),
                            client, role, json.dumps(over, sort_keys=True) if over else None, batch, a["n"], a["truncated"],
                            m.get("median"), m.get("min"), m.get("max"), m.get("mean"), m.get("stddev"), m.get("cv_pct"),
                            mb.get("median"), mb.get("min"), mb.get("max"), mb.get("mean"), mb.get("stddev"), mb.get("cv_pct"),
                            a["lat_kind"], a["lat_p50"], a["lat_p99"], a["lat_p99_9"], a["lat_max"],
                            a["cpu_total_s"], a["cpu_cores_avg"], a["cpu_s_per_million_msgs"], a["rss_peak_bytes"],
                            a["threads_peak"], a["broker_cpu_s"], a["broker_cores"], a["gc_count"], a["gc_time_ms"], a["gc_pct"], a["jit_ms"],
                            a["startup_ms"], a["rate_lag_max_us"], c["ratio"] if c else None,
                            (c["significant"] if c else None), cr["ratio"] if cr else None, self.fake])
        return path

    # -- charts

    def _finish(self, fig, name, title):
        if self.fake:
            fig.text(0.5, 0.5, "FAKE DATA", fontsize=60, color="red", alpha=0.25, ha="center", va="center", rotation=25)
        fig.tight_layout()
        path = os.path.join(self.charts_dir, name + ".png")
        fig.savefig(path, dpi=110)
        plt.close(fig)
        self.charts[name] = (title, "charts/%s.png" % name)

    def _bars(self, ax, labels, series, key, ylabel, log=False):
        n = len(series)
        width = 0.8 / max(1, n)
        for i, (client, aggs) in enumerate(series):
            meds, lo, hi, xs = [], [], [], []
            for j, a in enumerate(aggs):
                s = (a or {}).get(key) if a else None
                if not s:
                    continue
                xs.append(j - 0.4 + width * (i + 0.5))
                meds.append(s["median"])
                lo.append(s["median"] - s["min"])
                hi.append(s["max"] - s["median"])
            if xs:
                ax.bar(xs, meds, width, yerr=[lo, hi], capsize=3, label=label(client), color=COLORS.get(client))
        ax.set_xticks(range(len(labels)))
        ax.set_xticklabels([str(x) for x in labels], rotation=0 if len(labels) < 6 else 30, ha="center")
        ax.set_ylabel(ylabel)
        if log:
            ax.set_yscale("log")
        ax.grid(axis="y", alpha=0.3)
        ax.legend(fontsize=8)

    def chart_sweep(self, kind, sweep, name, title, stock=None):
        rows = self.sweep_rows(kind, sweep)
        if not rows:
            return
        labels = [v for v, _ in rows]
        series = [(c, [self.a(sc["name"], c) for _, sc in rows]) for c in self.clients]
        if stock:
            by_val = {t["value"]: sc for sc in self.scenarios.values() if sc["kind"] == stock
                      for t in sc["sweeps"]}
            aggs = [self.a(by_val[v]["name"], STOCK) if v in by_val else None for v in labels]
            series.append((STOCK, aggs))
        series = [(c, aggs) for c, aggs in series if any(aggs)]
        if not series:
            return
        fig, axes = plt.subplots(1, 2, figsize=(12, 4.2))
        self._bars(axes[0], labels, series, "msgs", "msgs/s (median, min/max bars)")
        self._bars(axes[1], labels, series, "mb", "MB/s (10^6 B payload)")
        fig.suptitle(title)
        self._finish(fig, name, title)

    def chart_consumer(self):
        scs = self.core("consume")
        if not scs:
            return
        labels = [sc["name"].replace("consume-", "") for sc in scs]
        series = [(c, [self.a(sc["name"], c) for sc in scs]) for c in self.clients]
        series = [(c, aggs) for c, aggs in series if any(aggs)]
        if not series:
            return
        fig, axes = plt.subplots(1, 2, figsize=(13, 4.5))
        self._bars(axes[0], labels, series, "msgs", "msgs/s")
        self._bars(axes[1], labels, series, "mb", "MB/s")
        for ax in axes:
            plt.setp(ax.get_xticklabels(), rotation=25, ha="right")
        title = "Consumer throughput"
        fig.suptitle(title)
        self._finish(fig, "consume_throughput", title)

    def chart_producer_latency(self):
        pcts = [("p50", "p50"), ("p99", "p99"), ("p99_9", "p99.9"), ("max", "max")]
        names = [sc["name"] for sc in self.core("produce")]
        if not self.scenarios.get("produce-baseline"):
            return
        cols = self.with_data(names)
        if not cols:
            return
        fig, axes = plt.subplots(1, 2, figsize=(13, 4.5), gridspec_kw={"width_ratios": [1, 2.2]})
        width = 0.8 / len(cols)
        for i, c in enumerate(cols):
            a = self.a("produce-baseline", c)
            if not a:
                continue
            vals = [(a["lat_" + k] or 0) / 1000.0 for k, _ in pcts]
            axes[0].bar([j - 0.4 + width * (i + 0.5) for j in range(len(pcts))], vals, width, label=label(c), color=COLORS[c])
        axes[0].set_xticks(range(len(pcts)))
        axes[0].set_xticklabels([l for _, l in pcts])
        axes[0].set_yscale("log")
        axes[0].set_ylabel("send-to-ack latency, ms (median of reps)")
        axes[0].set_title("baseline")
        axes[0].legend(fontsize=8)
        axes[0].grid(axis="y", alpha=0.3)
        for i, c in enumerate(cols):
            vals = [((self.a(n, c) or {}).get("lat_p99") or 0) / 1000.0 for n in names]
            axes[1].bar([j - 0.4 + width * (i + 0.5) for j in range(len(names))], vals, width, label=label(c), color=COLORS[c])
        axes[1].set_xticks(range(len(names)))
        axes[1].set_xticklabels([n.replace("produce-", "") for n in names], rotation=40, ha="right", fontsize=8)
        axes[1].set_yscale("log")
        axes[1].set_ylabel("p99 ms")
        axes[1].set_title("p99 across producer scenarios (acks=0 is time to local completion)")
        axes[1].grid(axis="y", alpha=0.3)
        title = "Producer latency percentiles"
        fig.suptitle(title)
        self._finish(fig, "producer_latency", title)

    def e2e_scenarios(self):
        out = {}
        for sc in self.scenarios.values():
            if sc["kind"] == "e2e":
                out.setdefault(sc["config"]["linger_ms"], []).append(sc)
        for v in out.values():
            v.sort(key=lambda s: s["config"]["rate"])
        return out

    def chart_e2e(self):
        groups = self.e2e_scenarios()
        if not groups:
            return
        fig, axes = plt.subplots(1, len(groups), figsize=(6.5 * len(groups), 4.5), squeeze=False)
        styles = {"p50": "-", "p99": "--", "p99_9": ":"}
        drew = False
        for ax, (linger, scs) in zip(axes[0], sorted(groups.items())):
            rates = [s["config"]["rate"] for s in scs]
            for c in self.clients:
                for p, ls in styles.items():
                    vals = [((self.a(s["name"], c, "consumer") or {}).get("lat_" + p)) for s in scs]
                    pts = [(r, v / 1000.0) for r, v in zip(rates, vals) if num(v)]
                    if pts:
                        drew = True
                        ax.plot([x for x, _ in pts], [y for _, y in pts], ls, marker="o", color=COLORS[c],
                                label="%s %s" % (label(c), p.replace("_", ".")))
            ax.set_xscale("log")
            ax.set_yscale("log")
            ax.set_xlabel("target rate, msgs/s")
            ax.set_ylabel("end-to-end latency, ms")
            ax.set_title("linger.ms=%s" % linger)
            ax.grid(alpha=0.3, which="both")
            ax.legend(fontsize=7)
        if not drew:
            plt.close(fig)
            return
        title = "End-to-end latency vs rate"
        fig.suptitle(title)
        self._finish(fig, "e2e_latency", title)

    def chart_resource(self, key, name, title, ylabel, scale=1.0):
        scs = self.core("produce", "consume")
        names = [sc["name"] for sc in scs if any(self.a(sc["name"], c) for c in self.clients)]
        cols = self.with_data(names)
        if not names or not cols:
            return
        fig, ax = plt.subplots(figsize=(8, 0.32 * len(names) * max(1, len(cols) / 2.0) + 1.5))
        h = 0.8 / len(cols)
        for i, c in enumerate(cols):
            vals = [(((self.a(n, c) or {}).get(key)) or 0) * scale for n in names]
            ax.barh([j - 0.4 + h * (i + 0.5) for j in range(len(names))], vals, h, label=label(c), color=COLORS[c])
        ax.set_yticks(range(len(names)))
        ax.set_yticklabels(names, fontsize=8)
        ax.invert_yaxis()
        ax.set_xlabel(ylabel)
        ax.grid(axis="x", alpha=0.3)
        ax.legend(fontsize=8)
        ax.set_title(title)
        self._finish(fig, name, title)

    def chart_timeseries(self):
        fig, ax = plt.subplots(figsize=(10, 4))
        any_data = False
        for c in self.clients:
            a = self.a("produce-baseline", c)
            rec = (a or {}).get("median_rec")
            ts = g(rec, "result", "timeseries") or []
            if not ts:
                continue
            any_data = True
            ax.plot([p["t"] for p in ts], [p["msgs"] for p in ts], marker=".", color=COLORS[c],
                    label="%s (rep %s)" % (label(c), rec.get("rep")))
        if not any_data:
            plt.close(fig)
            return
        ax.set_xlabel("second of measured window")
        ax.set_ylabel("acked msgs in bucket")
        ax.grid(alpha=0.3)
        ax.legend()
        title = "Baseline producer per-second throughput (median rep)"
        ax.set_title(title)
        self._finish(fig, "baseline_timeseries", title)

    # -- scaling (--instances K)

    def scaling_profiles(self):
        seen = []
        for p in list(matrix_mod.SCALING_PROFILES) + [g(sc, "scaling", "profile") for sc in self.scenarios.values()]:
            if p and p not in seen and any(g(sc, "scaling", "profile") == p for sc in self.scenarios.values()):
                seen.append(p)
        return seen

    def scaling_series(self, mode, profile):
        scs = [sc for sc in self.scenarios.values()
               if g(sc, "scaling", "mode") == mode and g(sc, "scaling", "profile") == profile]
        return sorted(((sc["scaling"]["k"], sc) for sc in scs), key=lambda t: t[0])

    def scaling_panels(self):
        return [(mode, prof) for mode in ("produce", "consume") for prof in self.scaling_profiles()
                if self.scaling_series(mode, prof)]

    @staticmethod
    def profile_label(profile):
        return matrix_mod.SCALING_PROFILE_LABELS.get(profile, profile)

    def _scaling_grid(self, panels, plot, name, title, ylabel, logy=False, hlines=None):
        if not panels:
            return
        modes = [m for m in ("produce", "consume") if any(p[0] == m for p in panels)]
        profs = self.scaling_profiles()
        fig, axes = plt.subplots(len(modes), len(profs), figsize=(5.2 * len(profs), 3.9 * len(modes)), squeeze=False)
        drew = False
        for i, mode in enumerate(modes):
            for j, prof in enumerate(profs):
                ax = axes[i][j]
                series = self.scaling_series(mode, prof)
                if not series:
                    ax.set_visible(False)
                    continue
                drew = plot(ax, mode, series) or drew
                ks = [k for k, _ in series]
                ax.set_xscale("log", base=2)
                ax.set_xticks(ks)
                ax.set_xticklabels([str(k) for k in ks])
                ax.set_xlabel("client instances K")
                ax.set_ylabel(ylabel)
                if logy:
                    ax.set_yscale("log")
                lines = (hlines or {}).get(mode, [])
                if lines:
                    ax.set_ylim(0, max(ax.get_ylim()[1], max(y for y, _ in lines) * 1.12))
                for y, label in lines:
                    ax.axhline(y, color="#555555", lw=0.8, ls=":")
                    ax.text(ks[-1], y, label + " ", fontsize=7, ha="right", va="bottom", color="#555555")
                ax.set_title("%s: %s" % (mode, self.profile_label(prof)), fontsize=9)
                ax.grid(alpha=0.3, which="both")
                ax.legend(fontsize=7)
        if not drew:
            plt.close(fig)
            return
        fig.suptitle(title)
        self._finish(fig, name, title)

    def chart_scaling(self):
        panels = self.scaling_panels()

        def thr(ax, mode, series):
            any_pts = False
            for c in self.clients:
                pts = [(k, (self.a(sc["name"], c) or {}).get("msgs")) for k, sc in series]
                pts = [(k, st) for k, st in pts if st]
                if not pts:
                    continue
                any_pts = True
                ax.errorbar([k for k, _ in pts], [st["median"] for _, st in pts],
                            yerr=[[st["median"] - st["min"] for _, st in pts], [st["max"] - st["median"] for _, st in pts]],
                            marker="o", capsize=3, color=COLORS[c], label=label(c))
            ax.set_ylim(bottom=0)
            return any_pts

        def cores(ax, mode, series):
            any_pts = False
            for c in self.clients:
                for key, ls, lab in (("cpu_cores_avg", "-", "%s client"), ("broker_cores", "--", "broker (%s run)")):
                    pts = [(k, (self.a(sc["name"], c) or {}).get(key)) for k, sc in series]
                    pts = [(k, v) for k, v in pts if num(v) is not None]
                    if pts:
                        any_pts = True
                        ax.plot([k for k, _ in pts], [v for _, v in pts], ls, marker="o", color=COLORS[c], label=lab % label(c))
            ax.set_ylim(bottom=0)
            return any_pts

        def per_m(ax, mode, series):
            any_pts = False
            for c in self.clients:
                pts = [(k, (self.a(sc["name"], c) or {}).get("cpu_s_per_million_msgs")) for k, sc in series]
                pts = [(k, v) for k, v in pts if num(v) is not None]
                if pts:
                    any_pts = True
                    ax.plot([k for k, _ in pts], [v for _, v in pts], marker="o", color=COLORS[c], label=label(c))
            ax.set_ylim(bottom=0)
            return any_pts

        def p99(ax, mode, series):
            any_pts = False
            for c in self.clients:
                pts = [(k, (self.a(sc["name"], c) or {}).get("lat_p99")) for k, sc in series]
                pts = [(k, v / 1000.0) for k, v in pts if num(v)]
                if pts:
                    any_pts = True
                    ax.plot([k for k, _ in pts], [v for _, v in pts], marker="o", color=COLORS[c], label="%s p99" % label(c))
            return any_pts

        caps = [(self.client_vcpus, "client cpuset %d vCPUs" % self.client_vcpus),
                (self.broker_vcpus, "broker cpuset %d vCPUs" % self.broker_vcpus)]
        self._scaling_grid(panels, thr, "scaling_throughput", "Throughput vs client instances K (median, min/max bars)", "msgs/s")
        self._scaling_grid(panels, cores, "scaling_cpu_cores", "CPU used vs K: client process (solid) and broker (dashed)",
                           "vCPUs (cores) busy", hlines={"produce": caps, "consume": caps})
        self._scaling_grid(panels, per_m, "scaling_cpu_per_million", "Client CPU seconds per million messages vs K", "CPU s / 1M msgs")
        self._scaling_grid([p for p in panels if p[0] == "produce"], p99, "scaling_latency_p99",
                           "Producer send-to-ack p99 vs K", "p99 ms", logy=True)

    def peak(self, mode, profile, client):
        best = None
        for k, sc in self.scaling_series(mode, profile):
            a = self.a(sc["name"], client)
            m = g(a, "msgs", "median")
            if num(m) and (best is None or m > best[2]):
                best = (k, sc, m, a)
        return best

    def bottleneck(self, a):
        cc, bc = (a or {}).get("cpu_cores_avg"), (a or {}).get("broker_cores")
        if num(cc) is None:
            return "client CPU not reported"
        cl = "client %.1f of %d vCPUs" % (cc, self.client_vcpus)
        if num(bc) is None:
            return "%s, broker CPU not measured" % cl
        br = "broker %.1f of %d vCPUs" % (bc, self.broker_vcpus)
        client_sat = cc >= SATURATED * self.client_vcpus
        broker_sat = bc >= SATURATED * self.broker_vcpus
        if client_sat and broker_sat:
            return "both sides near their cpuset limits (%s, %s)" % (cl, br)
        if client_sat:
            return "the client (CPU-bound: %s; %s)" % (cl, br)
        if broker_sat:
            return "the broker (CPU-bound: %s; %s)" % (br, cl)
        if bc >= 0.6 * self.broker_vcpus and cc < 0.6 * self.client_vcpus:
            return "likely the broker (%s while %s)" % (br, cl)
        return "neither side is CPU-saturated (%s, %s): look at contention, fetch/batch sizing or partition limits" % (cl, br)

    def scaling_stop(self, mode, profile, client):
        """(k where scaling stops or None, gain of the step after it, K=1 median, series of (k, median))."""
        pts = [(k, g(self.a(sc["name"], client), "msgs", "median")) for k, sc in self.scaling_series(mode, profile)]
        pts = [(k, m) for k, m in pts if num(m)]
        if len(pts) < 2:
            return None, None, pts
        for (k0, m0), (_, m1) in zip(pts, pts[1:]):
            if m1 < SCALING_GAIN_MIN * m0:
                return k0, m1 / m0, pts
        return None, pts[-1][1] / pts[-2][1], pts

    def sec_scaling(self):
        panels = self.scaling_panels()
        if not panels:
            return ""
        L = ["### Scaling with K client instances (`--instances K`)", "",
             "Each K runs K independent client objects with one sending or polling thread each inside one container on the client cpuset (%d vCPUs); instance k owns partitions p %% K == k of a %d-partition topic. Throughput is total messages over the common window: producers from barrier release (after every instance's flushed warmup) to the last instance done; consumers have no barrier (an idle consumer would keep prefetching), so the window runs from the first instance crossing its warmup boundary to the last instance's final record. Broker cores are the broker cgroup CPU delta divided by the active window (see Methodology)." % (
                 self.client_vcpus, matrix_mod.SCALING_PARTITIONS), ""]
        for mode, prof in panels:
            series = self.scaling_series(mode, prof)
            cols = self.with_data([sc["name"] for _, sc in series])
            if not cols:
                continue
            rows = []
            hdr = None
            for k, sc in series:
                aa = {c: self.a(sc["name"], c) for c in cols}
                rh, rc = self.ratio_cols(cols, lambda c: aa.get(c), [("rust-tuned", "rust")])
                row = [k, "{:,}".format(sc["num_messages"])] + [f_thr(aa[c]) for c in cols] + rc
                row += [f_1(g(aa[c], "mb", "median")) for c in cols]
                row += [f_1((aa[c] or {}).get("cpu_cores_avg")) for c in cols]
                row += [f_1((aa[c] or {}).get("broker_cores")) for c in cols]
                row += [f_2((aa[c] or {}).get("cpu_s_per_million_msgs")) for c in cols]
                if mode == "produce":
                    row += [f_ms((aa[c] or {}).get("lat_p99")) for c in cols]
                rows.append(row)
                hdr = (["K", "measured msgs"] + ["%s msg/s [min - max]" % label(c) for c in cols] + rh
                       + ["%s MB/s" % label(c) for c in cols] + ["%s client cores" % label(c) for c in cols]
                       + ["broker cores (%s)" % label(c) for c in cols] + ["%s CPU s/M" % label(c) for c in cols]
                       + (["%s p99 ms" % label(c) for c in cols] if mode == "produce" else []))
            L += ["#### %s: %s" % (mode, self.profile_label(prof)), "", md_table(hdr, rows), ""]
        L += ["#### Peak throughput on this host", "",
              "Best K per client (highest median msgs/s) for each profile and mode. A client without data for a mode (rust-tuned only runs consumers) shows `-`.", ""]
        cols = self.with_data([sc["name"] for m, p in panels for _, sc in self.scaling_series(m, p)])
        rows = []
        rh = []
        for mode, prof in panels:
            pk = {c: self.peak(mode, prof, c) for c in cols}
            row = [mode, self.profile_label(prof)]
            for c in cols:
                p = pk[c]
                row += ["K=%d" % p[0] if p else "-", f_thr(p[3]) if p else "-", f_1(g(p[3], "mb", "median")) if p else "-"]
            rh, rc = self.ratio_cols(cols, lambda c: pk[c][3] if pk.get(c) else None, [("rust-tuned", "rust")])
            rows.append(row + rc)
        hdr = ["mode", "profile"]
        for c in cols:
            hdr += ["%s best K" % label(c), "%s peak msg/s [min - max]" % label(c), "%s MB/s" % label(c)]
        L += [md_table(hdr + [h.replace("/Java", "/Java at peaks") for h in rh], rows), ""]
        for name in ("scaling_throughput", "scaling_cpu_cores", "scaling_cpu_per_million", "scaling_latency_p99"):
            L.append(self.chart_md(name))
        return "\n".join(L)

    def findings_scaling(self):
        L = []
        for mode, prof in self.scaling_panels():
            parts = []
            for c in self.clients:
                pk = self.peak(mode, prof, c)
                if not pk:
                    continue
                stop, gain, pts = self.scaling_stop(mode, prof, c)
                base = dict(pts).get(1)
                speedup = " (%.1fx of K=1)" % (pk[2] / base) if base else ""
                if stop is None:
                    where = "still scaling at K=%d (last step %+.1f%%)" % (pts[-1][0], (gain - 1) * 100) if gain else "too few K values"
                    at = pk[3]
                else:
                    where = "stops scaling at K=%d (the next step adds %+.1f%%)" % (stop, (gain - 1) * 100)
                    at = self.a(dict(self.scaling_series(mode, prof))[stop]["name"], c)
                parts.append("%s peaks at K=%d with %s msg/s%s, %s; bottleneck there: %s" % (
                    label(c), pk[0], f_int(pk[2]), speedup, where, self.bottleneck(at)))
            if parts:
                L.append("- Scaling, %s %s: %s." % (mode, self.profile_label(prof), "; ".join(parts)))
        return L

    def sec_variants(self):
        L = ["### Client variants", "",
             "Each variant is a harness image plus extra harness arguments per mode, defined in `bench/matrix.py` (`CLIENT_VARIANTS`) and recorded in `matrix.json` and `env.json`. A variant runs only the scenario kinds listed; rust-tuned has the same producer as rust, so it skips produce-only scenarios (producer sweeps, producer scaling, stock) and in e2e runs its producer is configured exactly like rust, which makes its e2e latency a measure of the tuned consumer. rust-lowlat runs only e2e; it is rust with the producer in-flight window forced to 5, which makes its e2e latency a measure of that one knob.", ""]
        rows = []
        for c in self.clients:
            v = self.variants.get(c) or matrix_mod.CLIENT_VARIANTS.get(c) or {}
            args = v.get("args") or {}
            rows.append(["`%s`" % c, "`%s`" % v.get("image", "?"), ", ".join(v.get("kinds") or []),
                         "`%s`" % " ".join(args.get("produce") or []) if args.get("produce") else "none",
                         "`%s`" % " ".join(args.get("consume") or []) if args.get("consume") else "none",
                         v.get("description", "")])
        L += [md_table(["variant", "image", "scenario kinds", "extra produce args", "extra consume args", "description"], rows), ""]
        pcs = [sc for sc in self.scenarios.values() if sc.get("per_client")]
        if pcs:
            L += ["### Per-client overrides", "",
                  "Some scenarios override producer settings for one client only (`per_client` in `matrix.json`; the resolved per-client config of every run is stored as `client_config` in `raw.jsonl`). A variant without its own entry inherits the entry of the variant it derives from (rust-tuned from rust). The matrix never merges scenarios that differ only in these overrides.", ""]
            cols = [c for c in self.clients if c in ("java", "rust") or any(c in (sc.get("per_client") or {}) for sc in pcs)]
            L += self.overrides_note(pcs, [c for c in cols if any(matrix_mod.runs_kind(c, sc["kind"]) for sc in pcs)])
        return "\n".join(L)

    def make_charts(self):
        self.chart_sweep("produce", "message_size", "produce_by_size", "Producer throughput by message size", stock="stock-produce")
        self.chart_sweep("produce", "acks", "produce_by_acks", "Producer throughput by acks")
        self.chart_sweep("produce", "compression", "produce_by_compression", "Producer throughput by compression (text payload)")
        self.chart_sweep("produce", "linger_ms", "produce_by_linger", "Producer throughput by linger.ms")
        self.chart_sweep("produce", "batch_size", "produce_by_batch", "Producer throughput by batch.size")
        self.chart_sweep("produce", "partitions", "produce_by_partitions", "Producer throughput by partitions")
        self.chart_consumer()
        self.chart_producer_latency()
        self.chart_e2e()
        self.chart_resource("cpu_s_per_million_msgs", "cpu_per_million", "Client CPU seconds per million messages",
                            "CPU s / 1M msgs (user+sys, median of reps)")
        self.chart_resource("rss_peak_bytes", "rss_peak", "Client peak RSS", "MB (10^6 B)", scale=1e-6)
        self.chart_timeseries()
        self.chart_scaling()

    def chart_md(self, name):
        if name not in self.charts:
            return ""
        title, rel = self.charts[name]
        return "![%s](%s)\n" % (title, rel)

    # -- sections

    def sec_banner(self):
        if not self.fake:
            return ""
        return ("> **WARNING: FAKE DATA.** This report was generated from synthetic results produced by `bench/fake.py` "
                "(`\"fake\": true`). None of the numbers in this report were measured. Do not cite them.\n\n")

    def client_info(self):
        info = {}
        for r in self.final:
            res = r.get("result") or {}
            c = r.get("client")
            if r.get("status") == "ok" and c not in info and res:
                info[c] = res
        return info

    def sec_environment(self):
        L = ["## Environment", ""]
        rows = []

        def flat(prefix, d):
            for k, v in d.items():
                key = "%s.%s" % (prefix, k) if prefix else k
                if key in FLAT_SKIP:
                    continue
                if isinstance(v, dict):
                    flat(key, v)
                else:
                    rows.append((key, v if not isinstance(v, list) else ", ".join(str(x) for x in v)))
        L += self.pinning_lines()
        flat("", self.hostinfo or {})
        if rows:
            L += ["Host and broker (from `infra/hostinfo.sh`; the full broker config and per-container stats are in `hostinfo.json`):",
                  "", md_table(["key", "value"], rows), ""]
        else:
            L += ["`hostinfo.json` missing or empty.", ""]
        dv = self.env.get("docker_version") or {}
        if dv:
            L.append("- Docker: client %s, server %s" % (g(dv, "Client", "Version"), g(dv, "Server", "Version")))
        for c, img in (self.env.get("images") or {}).items():
            L.append("- Image `%s`: id `%s`, digests %s, created %s" % (img.get("image"), img.get("id"),
                                                                       img.get("repo_digests") or "[] (local build)", img.get("created")))
        info = self.client_info()
        j, r = info.get("java", {}), info.get("rust", {})
        if j:
            L.append("- Java client: %s %s on %s" % (j.get("client_lib"), j.get("client_version"), j.get("runtime")))
            jv = j.get("jvm") or {}
            L.append("- JVM: %s, flags `%s`" % (jv.get("vm"), " ".join(jv.get("flags") or [])))
        if r:
            L.append("- Rust client: %s %s, librdkafka %s, %s" % (r.get("client_lib"), r.get("client_version"),
                                                                 r.get("native_lib_version"), r.get("runtime")))
            b = g(r, "params", "build")
            L.append("- Rust build: %s" % (json.dumps(b) if b else "not reported by harness (contract: release, opt-level=3, lto=fat, codegen-units=1, panic=abort, generic x86-64, glibc malloc)"))
        if info.get(STOCK):
            s = info[STOCK]
            L.append("- Stock tools: %s %s" % (s.get("client_lib"), s.get("client_version")))
        for label, key in (("start", "background_load_start"), ("end", "background_load_end")):
            rows = self.env.get(key)
            if rows:
                items = ["%s cpu %s mem %s" % (x.get("Name"), x.get("CPUPerc"), x.get("MemUsage")) for x in rows]
                L.append("- Background container load at %s of run (`docker stats --no-stream`): %s" % (label, "; ".join(items)))
        hashes = self.env.get("payload_hashes") or []
        if hashes:
            L.append("- Payload corpus hashes verified identical between harnesses before any run: " + ", ".join(
                "%s/%d `%s...`%s" % (h["payload"], h["message_size"], (h.get("java") or h.get("rust") or "")[:12],
                                    "" if h.get("python_reference_agrees", True) else " (DIFFERS from Python reference)")
                for h in hashes))
        L.append("")
        return "\n".join(L)

    def broker_setting(self, key):
        v = g(self.hostinfo, "broker", "config", key)
        if v is None:
            v = g(self.hostinfo, "broker", "config_non_default", key, "value")
        if v is None:
            env_key = "KAFKA_" + key.upper().replace(".", "_") + "="
            for e in g(self.hostinfo, "broker", "container", "env") or []:
                if isinstance(e, str) and e.startswith(env_key):
                    v = e[len(env_key):]
        return v

    def pinning_lines(self):
        pin = self.env.get("pinning") or {}
        bc = g(self.hostinfo, "broker", "container") or {}
        mem = bc.get("memory_bytes")
        heap = next((e.split("=", 1)[1] for e in bc.get("env") or [] if isinstance(e, str) and e.startswith("KAFKA_HEAP_OPTS=")), None)
        runner = self.env.get("runner") or {}
        L = ["### Pinning and broker threads", ""]
        L.append("- Broker `kbench-kafka`: cpuset `%s` (%d vCPUs, from `docker inspect`), memory limit %s, heap `%s`, `num.network.threads=%s`, `num.io.threads=%s`." % (
            self.broker_cpuset or "not recorded", self.broker_vcpus,
            "%.0f GiB" % (mem / 1024 ** 3) if num(mem) else "not recorded", heap or "not recorded",
            self.broker_setting("num.network.threads") or "not recorded", self.broker_setting("num.io.threads") or "not recorded"))
        L.append("- Client under test (produce, consume, prefill, stock tools): cpuset `%s` (%d vCPUs), memory `%s`." % (
            pin.get("client") or self.env.get("cpuset_client", "?"), self.client_vcpus, pin.get("client_memory") or self.env.get("client_memory", "?")))
        L.append("- E2E: producer cpuset `%s`, consumer cpuset `%s`, memory `%s` each." % (
            pin.get("e2e_producer", "?"), pin.get("e2e_consumer", "?"), pin.get("e2e_memory", "?")))
        L.append("- Orchestrator `kbench-runner`: cpuset `%s` (%s), memory limit %s; it only issues docker calls and waits, off the measured cpusets. Physical core 7 is shared with the host, the docker daemon and the unrelated containers." % (
            runner.get("cpuset_cpus") or pin.get("runner", "?"),
            "read from its own cgroup" if runner.get("cpuset_cpus") else "configured, not read back",
            "%.1f GiB" % (int(runner["memory_limit_bytes"]) / 1024 ** 3) if str(runner.get("memory_limit_bytes", "")).isdigit() else "not recorded"))
        L.append("- Network `%s` (bridge)." % self.env.get("network", "kbench-net"))
        L.append("")
        return L

    def sec_methodology(self):
        m = self.matrix
        reps = m.get("reps", "?")
        counts = ", ".join("%d B: %s measured" % (s, "{:,}".format(int(round(c * m.get("scale", 1.0)))))
                           for s, c in sorted(matrix_mod.BASE_COUNTS.items()))
        order = self.clients
        rot = "; ".join("rep %d: %s" % (i, ", ".join(order[i % len(order):] + order[:i % len(order)])) for i in range(min(len(order), 3)))
        L = ["## Methodology", ""]
        L += [
            "- Each scenario runs %s reps. Within each rep the scenario order is shuffled with seed %s; within a scenario the order of its clients rotates by one position per rep (%s, and so on; variants that do not run a scenario are left out), so slow drift (thermal, page cache, background load) spreads over all clients." % (reps, m.get("seed", "?"), rot),
            "- Message counts per measured run (scale %s): %s. Warmup is %d%% of the measured count, minimum %s messages, capped at the measured count, identical for all clients. Every run is capped at %s s of measured sending (`truncated` otherwise)." % (
                m.get("scale", 1.0), counts, int(matrix_mod.WARMUP_FRACTION * 100), "{:,}".format(matrix_mod.WARMUP_MIN), m.get("max_duration_s", 120)),
            "- Every producer run uses a fresh topic `kbench-<run_id>` created with the scenario's partition count, deleted afterwards, followed by a %s s cooldown. Broker CPU is the broker cgroup CPU delta around the client container." % m.get("cooldown_s", 5),
            "- Consumer scenarios: the topic is prefilled once per rep by the Java harness producer with `--instances min(16, partitions)` for speed (warmup+measured messages, baseline producer settings except the noted payload/compression/partitions), the end offset is verified, then every consumer variant reads the same topic from the beginning with manual assignment (no group, no commits). The first W messages are warmup; the window runs from receipt of message W to message W+N-1.",
            "- Every K=1 scenario passes `--instances 1` explicitly; it behaves exactly as the single-instance harness.",
            "- Scaling scenarios: K in %s on a %d-partition topic, acks=1, for produce and consume and every client variant that runs the mode. Profiles: %s. Measured messages are min(base count x K, cap), with the cap keeping warmup+measured payload bytes at or below %.0f GB so one topic fits the 16 GiB tmpfs; counts are rounded down to multiples of %d so every instance's share matches its partitions exactly. Consumers read a topic prefilled with the same producer settings (size, payload, compression, linger, batch). K=1 consumers use the old first-to-last receipt window; K>1 consumers use the union of per-instance windows (earliest warmup-boundary receipt to the last instance's final record, no barrier, assign after a start gate), because an instance idling at a barrier keeps prefetching and librdkafka prefetches far more than the Java classic consumer." % (
                ", ".join(str(k) for k in (g(m, "scaling_rule", "ks") or matrix_mod.SCALING_KS)), matrix_mod.SCALING_PARTITIONS,
                "; ".join("%s = %s" % (p, matrix_mod.SCALING_PROFILE_LABELS.get(p, p)) for p in matrix_mod.SCALING_PROFILES),
                (g(m, "scaling_rule", "bytes_cap") or matrix_mod.SCALING_BYTES_CAP) / 1e9, matrix_mod.SCALING_COUNT_QUANTUM),
            "- Bottleneck attribution: client cores are the harness process CPU over the measured window (`cpu_cores_avg`). Broker cores are the broker cgroup CPU delta around the client container divided by the active window, taken as the measured window stretched by (warmup+measured)/measured and bounded by the container wall time. A side is called CPU-saturated at %.0f%% of its cpuset; a step to the next K that adds less than %.0f%% is where scaling stops." % (
                SATURATED * 100, (SCALING_GAIN_MIN - 1) * 100),
            "- E2E scenarios: consumer container (cpuset `12-15,28-31`) started first on a fresh topic with `--measure-e2e`, then the producer of the same variant (cpuset `8-11,24-27`) with `--embed-timestamp --rate R`, paced by schedule. Latency is consumer wall clock minus the embedded producer wall clock; both containers share the host clock, so no cross-host skew applies.",
            "- Partitioning is explicit in both harnesses: record i goes to partition `i % partitions`, so the Java sticky partitioner and librdkafka's consistent_random partitioner play no role. Keys are null, no headers.",
            "- Payloads come from a SplitMix64-generated pool (seed 42, up to 16384 distinct messages, at most 64 MiB) that is byte-identical in both harnesses; the SHA-256 of the pool was checked for every (payload, size) before any run and the run aborts on mismatch. Compression sweeps use the `text` (word list) payload so `none` is the uncompressed reference.",
            "- Producer throughput window: before the first measured send until the final flush returns. Producer latency: before the first send attempt to the delivery callback (microsecond HdrHistogram, 3 significant digits). Durations use monotonic clocks (`System.nanoTime`, `Instant`).",
            "- MB means 10^6 bytes of payload only (no keys, headers or protocol overhead). CPU is process user+sys from `/proc/self/stat` over the measured window; RSS and threads are sampled every 100 ms.",
            "- Summary statistics: medians across reps; min/max are across reps. A difference between a client and Java is called beyond noise only when the min/max ranges of the two do not overlap, which is conservative with 3 reps.",
            "- Stock tool reference: `kafka-producer-perf-test` / `kafka-consumer-perf-test` via `infra/stock-perf.sh`, with the same Java producer properties. The stock tools have no warmup phase, so they process warmup+measured messages in one window, use their own payload and the default partitioner; treat them as a sanity reference for the custom Java harness, not as another contestant.",
            "",
            self.sec_variants(),
            "",
            "### Configuration mapping",
            "",
            "Producer configuration mapping (both harnesses set every property explicitly):",
            "",
            md_table(["Concept", "Java property", "librdkafka property"], [
                ("acks", "`acks`", "`acks`"), ("linger", "`linger.ms`", "`linger.ms`"),
                ("batch bytes", "`batch.size`", "`batch.size`"),
                ("batch message cap", "n/a", "`batch.num.messages=1000000` (byte limit governs, like Java)"),
                ("compression", "`compression.type`", "`compression.type`"),
                ("in-flight", "`max.in.flight.requests.per.connection`", "`max.in.flight.requests.per.connection`"),
                ("idempotence", "`enable.idempotence`", "`enable.idempotence`"),
                ("buffer", "`buffer.memory`, `max.block.ms=60000`", "`queue.buffering.max.kbytes=buffer/1024`, `queue.buffering.max.messages=2147483647`"),
                ("retries", "`retries=2147483647`, `delivery.timeout.ms=120000`, `request.timeout.ms=30000`", "`message.send.max.retries=2147483647`, `message.timeout.ms=120000`, `request.timeout.ms=30000`"),
                ("request size", "`max.request.size=10485760`", "`message.max.bytes=10485760`"),
                ("client id", "`client.id=<run_id>`", "`client.id=<run_id>`"),
            ]),
            "",
            self.rust_profile_note(),
            "",
            "Consumer configuration mapping:",
            "",
            md_table(["Concept", "Java property", "librdkafka property"], [
                ("group id (unused)", "`group.id=<run_id>`", "`group.id=<run_id>`"),
                ("commits", "`enable.auto.commit=false`", "`enable.auto.commit=false`, `enable.auto.offset.store=false`"),
                ("reset", "`auto.offset.reset=earliest`", "`auto.offset.reset=earliest`"),
                ("fetch min / wait", "`fetch.min.bytes`, `fetch.max.wait.ms`", "`fetch.min.bytes`, `fetch.wait.max.ms`"),
                ("fetch sizes", "`max.partition.fetch.bytes`, `fetch.max.bytes`", "`max.partition.fetch.bytes`, `fetch.max.bytes`"),
                ("crc", "`check.crcs`", "`check.crcs`"),
            ]),
            "",
            "API shape: Java `KafkaProducer.send` with a callback and `KafkaConsumer.poll(100 ms)` (up to `max.poll.records=500` per call); Rust `BaseProducer`/`ThreadedProducer` with a `ProducerContext` delivery callback (timestamp passed via `DeliveryOpaque`, no per-message allocation) and a `BaseConsumer` poll loop, one record per call; rust-tuned replaces the poll loop with `rd_kafka_consume_batch_queue` (see Client variants). One sending thread and one polling thread per instance in all variants.",
            "",
        ]
        return "\n".join(L)

    def producer_table(self, rows):
        names = [sc["name"] for _, sc in rows]
        cols = self.with_data(names) or [c for c in self.clients if matrix_mod.runs_kind(c, "produce")]
        out = []
        rh = []
        for value, sc in rows:
            n = sc["name"]
            aa = {c: self.a(n, c) for c in cols}
            rh, rc = self.ratio_cols(cols, lambda c: aa.get(c))
            out.append([self.value_cell(value, sc, cols), "`%s`" % n] + [f_thr(aa[c]) for c in cols] + rc
                       + [f_1(g(aa[c], "mb", "median")) for c in cols]
                       + [f_ms((aa[c] or {}).get("lat_p50")) for c in cols]
                       + [f_ms((aa[c] or {}).get("lat_p99")) for c in cols]
                       + [f_2((aa[c] or {}).get("cpu_s_per_million_msgs")) for c in cols])
        hdr = (["value", "scenario"] + ["%s msg/s median [min - max]" % label(c) for c in cols] + rh
               + ["%s MB/s" % label(c) for c in cols] + ["%s p50 ms" % label(c) for c in cols]
               + ["%s p99 ms" % label(c) for c in cols] + ["%s CPU s/M" % label(c) for c in cols])
        text = md_table(hdr, out)
        note = self.overrides_note([sc for _, sc in rows], cols)
        return text + ("\n\n" + "\n".join(note) if note else "")

    def sec_results(self):
        L = ["## Results", "", "Throughput cells show the median across reps with the [min - max] range. Latency and CPU are medians across reps. Columns appear only for client variants with data in that table.", ""]
        L += ["### Producer sweeps (one factor at a time from the baseline)", "",
              "Baseline: 1024 B random payload, acks=1, no compression, linger.ms=5, batch.size=16384, 6 partitions, idempotence off, max.in.flight=5.", ""]
        chart_for = {"message_size": "produce_by_size", "acks": "produce_by_acks", "compression": "produce_by_compression",
                     "linger_ms": "produce_by_linger", "batch_size": "produce_by_batch", "partitions": "produce_by_partitions"}
        for sweep in matrix_mod.SWEEP_ORDER:
            rows = self.sweep_rows("produce", sweep)
            if not rows:
                continue
            L += ["#### %s" % SWEEP_TITLES.get(sweep, sweep), "", self.producer_table(rows), ""]
            if sweep == "acks":
                L += ["With acks=0 the delivery callback fires once the request is written, so its latency is not comparable to acked latency.", ""]
            if sweep == "profile":
                L += ["Max throughput profile: 100 B text, lz4, linger.ms=50, batch.size=1048576, acks=1, 6 partitions.", ""]
            if sweep == "batch_size":
                L += ["librdkafka builds one ProduceRequest per partition and the broker serves one connection's requests in order, so Rust throughput depends on how much each request carries; Java packs several partitions' batches into one request.", ""]
            if sweep == "client_settings":
                L += ["`produce-defaults` runs each client at its own library defaults for batching and buffering (Java `batch.size=16384`, `buffer.memory=33554432`; librdkafka `batch.size=1000000`, `message.max.bytes=1000000` with the `native` profile, so `batch.num.messages`, `queue.buffering.max.*`, in-flight, retries and timeouts are librdkafka defaults too). `produce-best` runs each client at the batch.size where it peaked in the diagnosis (Java 131072, Rust 1048576) with the run's Rust config profile. The Rust/Java ratio here compares different configurations by design.", ""]
            if sweep in chart_for:
                L.append(self.chart_md(chart_for[sweep]))
        L.append(self.chart_md("producer_latency"))
        L.append(self.chart_md("baseline_timeseries"))

        cons = self.core("consume")
        cols = self.with_data([sc["name"] for sc in cons])
        if cons and cols:
            L += ["### Consumer", "", "Every consumer variant reads the identical prefilled topic of each rep.", ""]
            rows = []
            rh = []
            for sc in cons:
                n = sc["name"]
                aa = {c: self.a(n, c) for c in cols}
                pf = sc["config"]["prefill"]
                rh, rc = self.ratio_cols(cols, lambda c: aa.get(c), [("rust-tuned", "rust")])
                rows.append(["`%s`" % n, "%d B %s %s, %d partitions" % (pf["message_size"], pf["payload"], pf["compression"], pf["partitions"])]
                            + [f_thr(aa[c]) for c in cols] + rc + [f_1(g(aa[c], "mb", "median")) for c in cols]
                            + [f_2((aa[c] or {}).get("cpu_s_per_million_msgs")) for c in cols]
                            + [f_1((aa[c] or {}).get("first_message_ms")) for c in cols])
            L += [md_table(["scenario", "data"] + ["%s msg/s [min - max]" % label(c) for c in cols] + rh
                           + ["%s MB/s" % label(c) for c in cols] + ["%s CPU s/M" % label(c) for c in cols]
                           + ["%s first msg ms" % label(c) for c in cols], rows), ""]
            L.append(self.chart_md("consume_throughput"))

        e2e = self.e2e_scenarios()
        cols = self.with_data([sc["name"] for scs in e2e.values() for sc in scs], "consumer")
        if e2e and cols:
            L += ["### End-to-end latency", "", "1024 B, acks=1, 6 partitions, fixed target rate; latency measured by the consumer of the same variant (rust-tuned: rust producer, tuned consumer; rust-lowlat: rust producer with max.in.flight=5, rust consumer).", ""]
            rows = []
            for linger, scs in sorted(e2e.items()):
                for sc in scs:
                    n = sc["name"]
                    row = [linger, "{:,}".format(sc["config"]["rate"])]
                    for c in cols:
                        a = self.a(n, c, "consumer") or {}
                        p = self.a(n, c, "producer") or {}
                        row += ["%s / %s / %s / %s" % (f_ms(a.get("lat_p50")), f_ms(a.get("lat_p99")), f_ms(a.get("lat_p99_9")), f_ms(a.get("lat_max"))),
                                f_int(g(p, "msgs", "median")), f_int(p.get("rate_lag_max_us"))]
                    for c in cols:
                        if c != "java" and "java" in cols:
                            j = (self.a(n, "java", "consumer") or {}).get("lat_p99")
                            x = (self.a(n, c, "consumer") or {}).get("lat_p99")
                            row.append("%.2fx" % (x / j) if num(j) and num(x) and j else "-")
                    rows.append(row)
            hdr = ["linger.ms", "target msg/s"]
            for c in cols:
                hdr += ["%s p50/p99/p99.9/max ms" % label(c), "%s achieved msg/s" % label(c), "%s max rate lag us" % label(c)]
            hdr += ["%s/Java p99" % label(c) for c in cols if c != "java" and "java" in cols]
            L += [md_table(hdr, rows), ""]
            L.append(self.chart_md("e2e_latency"))

        stock = [sc for sc in self.scenarios.values() if sc["kind"] in ("stock-produce", "stock-consume")]
        if stock:
            L += ["### Stock tool reference vs custom Java harness", "",
                  "Same Java client and properties; differences come from the tools' own payload, partitioner, measurement loop and the absent warmup.", ""]
            rows = []
            for sc in stock:
                ref = sc.get("reference")
                sa = self.a(sc["name"], STOCK)
                ja = self.a(ref, "java") if ref else None
                ratio = (g(sa, "msgs", "median") / g(ja, "msgs", "median")) if g(sa, "msgs", "median") and g(ja, "msgs", "median") else None
                rows.append(["`%s`" % sc["name"], "`%s`" % ref, f_thr(sa), f_thr(ja), "-" if ratio is None else "%.2fx" % ratio,
                             f_ms((sa or {}).get("lat_p99")), f_ms((ja or {}).get("lat_p99"))])
            L += [md_table(["stock scenario", "harness scenario", "stock msg/s [min - max]", "Java harness msg/s [min - max]",
                            "stock/harness", "stock p99 ms", "harness p99 ms"], rows), ""]

        L.append(self.sec_scaling())

        L += ["### Resources (K=1 scenarios)", ""]
        for kind, title in (("produce", "Producer"), ("consume", "Consumer")):
            scs = self.core(kind)
            cols = self.with_data([sc["name"] for sc in scs])
            if not cols:
                continue
            rows = []
            for sc in scs:
                aa = {c: self.a(sc["name"], c) or {} for c in cols}
                if not any(aa.values()):
                    continue
                ja = aa.get("java") or {}
                rows.append(["`%s`" % sc["name"]] + [f_2(aa[c].get("cpu_s_per_million_msgs")) for c in cols]
                            + [f_2(aa[c].get("cpu_cores_avg")) for c in cols] + [f_mb(aa[c].get("rss_peak_bytes")) for c in cols]
                            + [f_int(aa[c].get("threads_peak")) for c in cols] + [f_1(aa[c].get("broker_cpu_s")) for c in cols]
                            + ([f_int(ja.get("gc_count")), f_int(ja.get("gc_time_ms")), f_1(ja.get("gc_pct")), f_int(ja.get("jit_ms"))]
                               if "java" in cols else []))
            hdr = (["scenario"] + ["%s CPU s/M" % label(c) for c in cols] + ["%s cores" % label(c) for c in cols]
                   + ["%s RSS MB" % label(c) for c in cols] + ["%s threads" % label(c) for c in cols]
                   + ["broker CPU s (%s)" % label(c) for c in cols]
                   + (["Java GCs", "Java GC ms", "Java GC % of window", "Java JIT ms"] if "java" in cols else []))
            L += ["%s:" % title, "", md_table(hdr, rows), ""]
        L += ["Java RSS includes the pre-touched 2 GiB heap (`-Xms2g -XX:+AlwaysPreTouch`), so it reflects the configured heap rather than live data.", ""]
        L.append(self.chart_md("cpu_per_million"))
        L.append(self.chart_md("rss_peak"))
        return "\n".join(L)

    def findings_client_settings(self):
        L = []
        pts = []
        for v, sc in self.sweep_rows("produce", "batch_size"):
            j, r = g(self.a(sc["name"], "java"), "msgs", "median"), g(self.a(sc["name"], "rust"), "msgs", "median")
            if num(j) and num(r) and j:
                pts.append((v, j, r))
        if pts:
            cross = next((p for p in pts if p[2] >= p[1]), None)
            jb, rb = max(pts, key=lambda p: p[1]), max(pts, key=lambda p: p[2])
            first = "at batch.size=%s Rust/Java is %.2fx" % (f_int(pts[0][0]), pts[0][2] / pts[0][1])
            if cross:
                head = "Rust's median first reaches Java's at batch.size=%s (Rust/Java %.2fx there; %s)" % (
                    f_int(cross[0]), cross[2] / cross[1], first)
            else:
                head = "Rust's median stays below Java's at every batch.size up to %s (%s)" % (f_int(pts[-1][0]), first)
            L.append("- batch.size crossover: %s. Java peaks at batch.size=%s with %s msg/s, Rust at batch.size=%s with %s msg/s." % (
                head, f_int(jb[0]), f_int(jb[1]), f_int(rb[0]), f_int(rb[2])))
        for name, what in (("produce-defaults", "each client at its own library defaults"),
                           ("produce-best", "each client at its best batch.size")):
            sc = self.scenarios.get(name)
            c = self.cmp(name) if sc else None
            if not c or c["ratio"] is None:
                continue
            ja, ra = self.a(name, "java"), self.a(name, "rust")
            L.append("- `%s` (%s; Java batch.size %s, Rust batch.size %s): Rust/Java %.2fx (Java %s msg/s, Rust %s msg/s), beyond noise: %s." % (
                name, what, f_int(self.cfg_for(sc, "java").get("batch_size")), f_int(self.cfg_for(sc, "rust").get("batch_size")),
                c["ratio"], f_int(g(ja, "msgs", "median")), f_int(g(ra, "msgs", "median")), f_sig(c)))
        return L

    def findings_tuned(self):
        gains = []
        for sc in self.scenarios.values():
            if sc["kind"] != "consume":
                continue
            c = self.cmp(sc["name"], "consumer", "rust-tuned", "rust")
            if c and c["ratio"]:
                vj = self.cmp(sc["name"], "consumer", "rust-tuned")
                gains.append((sc["name"], c, vj))
        if not gains:
            return []
        gm = math.exp(statistics.fmean(math.log(x[1]["ratio"]) for x in gains))
        items = ", ".join("`%s` %.2fx%s%s" % (n, c["ratio"], "" if c["significant"] else " (within noise)" if c["significant"] is False else "",
                                              " (%.2fx of Java)" % vj["ratio"] if vj and vj["ratio"] else "")
                          for n, c, vj in gains)
        return ["- rust-tuned vs rust consumer throughput (batch API, `fetch.queue.backoff.ms=10`): geometric mean gain %.2fx over %d consume scenarios. Per scenario: %s." % (
            gm, len(gains), items)]

    def sec_findings(self):
        L = ["## Findings", "", "Generated from the data above; re-read the tables before quoting.", ""]
        for kind, lab, role in (("produce", "Producer", "producer"), ("consume", "Consumer", "consumer")):
            scs = [sc for sc in self.core(kind) if not sc.get("per_client")]
            for client in self.with_data([sc["name"] for sc in scs], role):
                if client == "java":
                    continue
                comps = []
                for sc in scs:
                    c = self.cmp(sc["name"], role, client)
                    if c and c["ratio"]:
                        comps.append((sc["name"], c))
                if not comps:
                    continue
                cl = label(client)
                faster = [x for x in comps if x[1]["ratio"] > 1]
                sig_r = [x for x in comps if x[1]["ratio"] > 1 and x[1]["significant"]]
                sig_j = [x for x in comps if x[1]["ratio"] < 1 and x[1]["significant"]]
                noise = [x for x in comps if x[1]["significant"] is False]
                gm = math.exp(statistics.fmean(math.log(x[1]["ratio"]) for x in comps))
                L.append("- %s throughput (K=1 scenarios, same config for both), %s vs Java: %s has the higher median in %d of %d scenarios (geometric mean %s/Java %.2fx). Beyond run-to-run noise: %s faster in %d, Java faster in %d, within noise in %d." % (
                    lab, cl, cl, len(faster), len(comps), cl, gm, cl, len(sig_r), len(sig_j), len(noise)))
                srt = sorted(comps, key=lambda x: x[1]["ratio"])
                if srt[-1][1]["ratio"] > 1:
                    L.append("  - Largest %s lead: " % cl + ", ".join("`%s` %.2fx%s" % (n, c["ratio"], "" if c["significant"] else " (within noise)")
                                                                   for n, c in reversed(srt[-3:]) if c["ratio"] > 1))
                if srt[0][1]["ratio"] < 1:
                    L.append("  - Largest Java lead: " + ", ".join("`%s` %.2fx%s" % (n, c["ratio"], "" if c["significant"] else " (within noise)")
                                                             for n, c in srt[:3] if c["ratio"] < 1))
                if noise:
                    L.append("  - Within noise: " + ", ".join("`%s`" % n for n, _ in noise))

        L += self.findings_client_settings()
        L += self.findings_tuned()
        L += self.findings_scaling()

        e2e = self.e2e_scenarios()
        parts = []
        for linger, scs in sorted(e2e.items()):
            for sc in scs:
                vals = [(c, (self.a(sc["name"], c, "consumer") or {}).get("lat_p99")) for c in self.clients]
                vals = [(c, v) for c, v in vals if num(v)]
                j = dict(vals).get("java")
                if len(vals) < 2:
                    continue
                parts.append("linger %s @ %s/s: %s" % (linger, "{:,}".format(sc["config"]["rate"]), ", ".join(
                    "%s %s ms%s" % (label(c), f_ms(v), " (%.2fx)" % (v / j) if c != "java" and j else "") for c, v in vals)))
        if parts:
            L.append("- E2E p99 latency (ratio vs Java in parentheses): " + "; ".join(parts) + ".")

        cpu_r, rss_j, rss_r = [], [], []
        for sc in self.core("produce", "consume"):
            if sc.get("per_client"):
                continue
            ja, ra = self.a(sc["name"], "java") or {}, self.a(sc["name"], "rust") or {}
            if num(ja.get("cpu_s_per_million_msgs")) and num(ra.get("cpu_s_per_million_msgs")) and ja["cpu_s_per_million_msgs"]:
                cpu_r.append((sc["name"], ra["cpu_s_per_million_msgs"] / ja["cpu_s_per_million_msgs"]))
            if num(ja.get("rss_peak_bytes")):
                rss_j.append(ja["rss_peak_bytes"])
            if num(ra.get("rss_peak_bytes")):
                rss_r.append(ra["rss_peak_bytes"])
        if cpu_r:
            vals = [x[1] for x in cpu_r]
            lo, hi = min(cpu_r, key=lambda x: x[1]), max(cpu_r, key=lambda x: x[1])
            L.append("- CPU efficiency: Rust uses a median %.2fx the CPU seconds per million messages of Java (range %.2fx in `%s` to %.2fx in `%s`)." % (
                statistics.median(vals), lo[1], lo[0], hi[1], hi[0]))
        if rss_j and rss_r:
            L.append("- Memory footprint: median peak RSS Java %s MB (range %s - %s), Rust %s MB (range %s - %s)." % (
                f_mb(statistics.median(rss_j)), f_mb(min(rss_j)), f_mb(max(rss_j)),
                f_mb(statistics.median(rss_r)), f_mb(min(rss_r)), f_mb(max(rss_r))))
        gcp = [(sc["name"], (self.a(sc["name"], "java") or {}).get("gc_pct")) for sc in self.core("produce", "consume")]
        gcp = [x for x in gcp if num(x[1])]
        if gcp:
            top = max(gcp, key=lambda x: x[1])
            L.append("- GC impact: Java GC pause time is a median %.2f%% of the measured window (max %.2f%% in `%s`)." % (
                statistics.median([x[1] for x in gcp]), top[1], top[0]))
        st = [(self.a("produce-baseline", c) or {}).get("startup_ms") for c in ("java", "rust")]
        if all(num(x) for x in st):
            L.append("- Startup (process start to client ready, baseline): Java %s ms, Rust %s ms. Not part of any throughput window." % (f_1(st[0]), f_1(st[1])))
        ratios = []
        for sc in self.scenarios.values():
            if sc["kind"] in ("stock-produce", "stock-consume") and sc.get("reference"):
                s = g(self.a(sc["name"], STOCK), "msgs", "median")
                j = g(self.a(sc["reference"], "java"), "msgs", "median")
                if s and j:
                    ratios.append((sc["name"], s / j))
        if ratios:
            L.append("- Stock tools vs custom Java harness: " + ", ".join("`%s` %.2fx" % r for r in ratios) + ". Large gaps would point at a harness problem rather than a client difference.")
        L.append("")
        return "\n".join(L)

    def sec_stability(self):
        L = ["## Stability", ""]
        flagged = []
        for (scn, client, role), a in sorted(self.agg.items()):
            cv = g(a, "msgs", "cv_pct")
            if num(cv) and cv > CV_FLAG_PCT:
                flagged.append(["`%s`" % scn, client, role, "%.1f%%" % cv, ", ".join(f_int(v) for v in a["msgs"]["values"])])
        if flagged:
            L += ["Scenario/client pairs with throughput CV above %.0f%% across reps (treat their comparisons with extra caution):" % CV_FLAG_PCT, "",
                  md_table(["scenario", "client", "role", "CV", "per-rep msgs/s"], flagged), ""]
        else:
            L += ["No scenario/client pair exceeded %.0f%% throughput CV across reps." % CV_FLAG_PCT, ""]
        short = []
        for (scn, client, role), a in sorted(self.agg.items()):
            d = a.get("duration_s")
            if num(d) and d < SHORT_WINDOW_S:
                short.append(["`%s`" % scn, client, role, "%.2f s" % d, f_int(g(a, "msgs", "median"))])
        if short:
            L += ["Scenario/client pairs whose median measured window is under %.0f s. The 16 GiB tmpfs caps topic size, so the fastest runs are brief; startup transients and client read-ahead (librdkafka queues up to `queued.max.messages.kbytes`, 64 MiB by default, per consumer instance) are a larger share of such windows, so treat these throughputs as upper-bound estimates:" % SHORT_WINDOW_S, "",
                  md_table(["scenario", "client", "role", "median window", "median msgs/s"], short), ""]
        return "\n".join(L)

    def sec_failures(self):
        L = ["## Failed, retried and truncated runs", ""]
        final_ok = {(r.get("base_run_id"), r.get("role")) for r in self.final if r.get("status") == "ok"}
        rows = []
        for r in self.raw:
            res = r.get("result") or {}
            if r.get("status") != "ok":
                recovered = (r.get("base_run_id"), r.get("role")) in final_ok
                rows.append(["`%s`" % r.get("run_id"), r.get("role"), r.get("status"),
                             "recovered on retry" if recovered else "no valid result", str(r.get("error") or "")[:160], r.get("log") or "-"])
            elif res.get("truncated") or r.get("truncated"):
                rows.append(["`%s`" % r.get("run_id"), r.get("role"), "truncated",
                             "kept (%s of %s msgs)" % (f_int(res.get("messages")), f_int(param(res, "num_messages"))),
                             "hit --max-duration-s", r.get("log") or "-"])
        if rows:
            L += [md_table(["run_id", "role", "status", "outcome", "error", "log"], rows), ""]
        else:
            L += ["None.", ""]
        expected = {}
        for u in self.matrix.get("plan", []):
            for c in u["clients"]:
                roles = ["producer", "consumer"] if u["kind"] == "e2e" else (["consumer"] if u["kind"] in ("consume", "stock-consume") else ["producer"])
                for role in roles:
                    expected[("%s-r%d-%s" % (u["scenario"], u["rep"], c), role)] = u
        seen = {(r.get("base_run_id"), r.get("role")) for r in self.raw}
        missing = [k for k in expected if k not in seen]
        if missing:
            L += ["%d planned runs have no record at all (run interrupted or still in progress): %s" % (
                len(missing), ", ".join("`%s`" % k[0] for k in missing[:30]) + (" ..." if len(missing) > 30 else "")), ""]
        return "\n".join(L)

    def rust_profile_note(self):
        rust_prod = [r for r in self.raw if matrix_mod.CLIENT_VARIANTS.get(r.get("client"), {}).get("harness") == "rust"
                     and g(r, "result", "mode") == "produce" and r.get("status") == "ok"]
        profile = self.matrix.get("rust_config_profile") or self.env.get("rust_config_profile")
        if not profile:
            plain = [r for r in rust_prod if not self.scenarios.get(r.get("scenario"), {}).get("per_client")]
            profile = param((plain[0].get("result") if plain else {}), "config_profile") or "matched"
        native = next((r["result"] for r in rust_prod if param(r["result"], "config_profile") == "native"
                       and r["result"].get("library_defaults")), None)
        pinned = sorted(sc["name"] for sc in self.scenarios.values()
                        if any("config_profile" in (o or {}) for o in (sc.get("per_client") or {}).values()))
        pin_note = (" Scenarios that pin their own profile through per-client overrides: %s." % ", ".join("`%s`" % n for n in pinned)) if pinned else ""
        vals = ", ".join("`%s=%s`" % (k, v) for k, v in sorted(((native or {}).get("library_defaults") or {}).items()))
        native_text = ("With the `native` profile, `batch.num.messages`, `max.in.flight.requests.per.connection`, `queue.buffering.max.*`, "
                       "`message.send.max.retries`, `message.timeout.ms` and `request.timeout.ms` are NOT set, so librdkafka uses its own "
                       "defaults, as reported by librdkafka itself: %s." % (vals or "not recorded"))
        if profile != "native":
            return ("Rust producer config profile for this run: `matched` (the table above applies as written).%s%s" % (
                pin_note, (" " + native_text) if pinned else ""))
        return ("Rust producer config profile for this run: `native`. The swept settings (acks, linger, batch size, compression, idempotence) and "
                "`message.max.bytes` are set as in the table. %s Java is unchanged.%s" % (native_text, pin_note))

    def sec_caveats(self):
        return "\n".join([
            "## Caveats", "",
            "- Single broker, replication factor 1, log dirs on tmpfs: acks=all means one replica, and there is no disk or replication cost. Results describe client overhead against a fast local broker, not production cluster behaviour.",
            "- Clients and broker share one host and communicate over a docker bridge: there is no real network latency or bandwidth limit, which magnifies client-side CPU differences relative to a networked deployment.",
            "- The host also runs unrelated containers and non-container processes (security and monitoring agents such as falcon-sensor, chronicled and the CloudWatch agent, plus interactive user sessions). They are not pinned, so the kernel may schedule them on the broker or client cores; that time is not attributed to any measured cgroup. Container load is recorded above; non-container load is neither recorded nor controlled.",
            "- Scaling runs share one broker with 7 physical cores; the peak numbers describe this host, not the clients' limits on a cluster. Broker cores are an average over the active window and can understate short bursts.",
            "- Client API differences that cannot be equalized: Java batches per partition in the RecordAccumulator and sends from its own I/O thread; librdkafka queues messages centrally and uses per-broker threads, so `linger.ms`, `batch.size` and `max.in.flight` do not map to identical internal behaviour. librdkafka copies payloads on produce; Java serializes into the batch buffer. Java's `buffer.memory` blocks the caller, while the Rust harness polls and retries on `QueueFull`.",
            "- Java numbers include JIT and GC behaviour after a warmup of at least 200k messages; JIT compilation can continue inside the measured window (reported as JIT ms). Rust has no warmup effects beyond librdkafka connection setup.",
            "- acks=0 latency is time to local completion, not to broker receipt, and acks=0 topics may legitimately be short.",
            "- Consumer runs read a topic written moments before, so all data is in broker page cache (tmpfs); fetch cost is purely broker CPU and memory copy.",
            "- Measured windows are bounded by topic size (tmpfs): the highest-throughput scaling runs last only about a second. Short windows are listed in the Stability section.",
            "- Min/max range overlap is a coarse significance test; with 3 reps, small real differences can be reported as within noise.",
            "",
        ])

    def write_md(self):
        m = self.matrix
        L = []
        L.append("# Kafka client benchmark: Java kafka-clients vs Rust rust-rdkafka%s" % (" (FAKE DATA)" if self.fake else ""))
        L.append("")
        L.append(self.sec_banner())
        n_ok = sum(1 for r in self.final if r.get("status") == "ok" and r.get("role") != "prefill")
        L.append("Output directory `%s`: %d scenarios, %s reps, %d valid measured results (%d raw records). Started %s, finished %s." % (
            os.path.basename(os.path.abspath(self.out)), len(self.scenarios), m.get("reps", "?"), n_ok, len(self.raw),
            self.env.get("started", "?"), self.env.get("finished", "not recorded")))
        L.append("")
        L.append(self.sec_findings())
        L.append(self.sec_environment())
        L.append(self.sec_methodology())
        L.append(self.sec_results())
        L.append(self.sec_stability())
        L.append(self.sec_failures())
        L.append(self.sec_caveats())
        if self.fake:
            L.append(self.sec_banner())
        text = "\n".join(L)
        text = "\n".join(l.rstrip() for l in text.splitlines())
        while "\n\n\n" in text:
            text = text.replace("\n\n\n", "\n\n")
        path = os.path.join(self.out, "report.md")
        with open(path, "w") as f:
            f.write(text.strip() + "\n")
        return path


def main(argv):
    if len(argv) != 1:
        print(__doc__, file=sys.stderr)
        return 2
    rep = Report(argv[0])
    rep.make_charts()
    csv_path = rep.write_csv()
    md_path = rep.write_md()
    print("wrote %s, %s, %d charts in %s%s" % (md_path, csv_path, len(rep.charts), rep.charts_dir,
                                              "  [FAKE DATA]" if rep.fake else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
