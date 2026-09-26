#!/usr/bin/env python3
"""Charts for the narrative write-up: first run, diagnosis steps, rerun.

usage: python3 bench/story_plots.py --results /results --out /results/story-plots

Reads <results>/full-1, <results>/diag-1 and, when present, <results>/full-2 and <results>/diag-java.
Charts that need full-2 or diag-java are skipped until they exist.
"""
import argparse
import csv
import json
import math
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import FancyBboxPatch  # noqa: E402

PREVIEW = os.environ.get("PREVIEW") == "1"
BG = "#faf9f6"
FG = "#1c1a17"
MUTED = "#6b665f"
GRID = "#e4e0d8"
COLORS = {"java": "#e76f00", "rust": "#1f3a4d", "rust-tuned": "#3f9aa8", "rust-lowlat": "#8aa3b3", "java-stock": "#b8a58a"}
THREAD_NAMES = {"rdk:broker1": "librdkafka broker thread", "kbench-rust": "application thread (send / poll)",
                "producer pollin": "delivery-callback poll thread", "kbench-sampler": "resource sampler"}
LABELS = {"java": "Java kafka-clients 4.3.1", "rust": "Rust rdkafka 0.39 (librdkafka 2.12.1)",
          "rust-tuned": "Rust, tuned consumer", "rust-lowlat": "Rust, max.in.flight 5",
          "java-stock": "Kafka perf-test tools"}

plt.rcParams.update({
    "figure.facecolor": BG, "axes.facecolor": BG, "savefig.facecolor": BG,
    "axes.edgecolor": GRID, "axes.labelcolor": FG, "text.color": FG,
    "xtick.color": MUTED, "ytick.color": MUTED, "axes.grid": True, "grid.color": GRID,
    "grid.linewidth": 0.8, "axes.spines.top": False, "axes.spines.right": False,
    "font.size": 11, "axes.titlesize": 13, "axes.titleweight": "bold", "axes.titlelocation": "left",
    "legend.frameon": False, "svg.fonttype": "path", "axes.axisbelow": True,
})


def fmt_rate(v):
    if v >= 999.5e3:
        return "%.2fM" % (v / 1e6)
    if v >= 1e3:
        return "%.0fk" % (v / 1e3)
    return "%.0f" % v


def rate_axis(ax, axis="y"):
    f = matplotlib.ticker.FuncFormatter(lambda v, _: fmt_rate(v))
    (ax.yaxis if axis == "y" else ax.xaxis).set_major_formatter(f)


def save(fig, out, name):
    path = os.path.join(out, name)
    fig.savefig(path, format="svg", bbox_inches="tight", pad_inches=0.25)
    if PREVIEW:
        fig.savefig(path[:-4] + ".preview.png", format="png", dpi=80, bbox_inches="tight", pad_inches=0.25)
    plt.close(fig)
    print("wrote", path)


def num(v):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(f) else f


def load_summary(run_dir):
    path = os.path.join(run_dir, "summary.csv")
    if not os.path.exists(path):
        return None
    rows = {}
    with open(path) as fh:
        for r in csv.DictReader(fh):
            rows[(r["scenario"], r["client"], r["role"])] = r
    return rows


def med(rows, scenario, client, role, col="msgs_per_s_median"):
    r = rows.get((scenario, client, role)) if rows else None
    return num(r.get(col)) if r else None


def diag_result(diag, run_id):
    path = os.path.join(diag, run_id + ".out")
    if not os.path.exists(path):
        return None
    with open(path) as fh:
        for line in fh:
            if line.startswith("RESULT "):
                return json.loads(line[7:])
    return None


def diag_rate(diag, run_id):
    r = diag_result(diag, run_id)
    return r["throughput_msgs_per_s"] if r and r.get("status") == "ok" else None


def thread_cpu(diag, a, b):
    def load(name):
        with open(os.path.join(diag, name)) as fh:
            lines = fh.read().splitlines()
        up = None
        if lines and "|" not in lines[0]:
            up = float(lines[0].split()[0])
            lines = lines[1:]
        d = {}
        for i, line in enumerate(lines):
            n, v = line.split("|")
            u, s = map(int, v.split())
            d[(i, n)] = (u, s)
        return up, d
    (u1, t1), (u2, t2) = load(a), load(b)
    dt = (u2 - u1) if (u1 is not None and u2 is not None) else 3.0
    res = []
    for k, (u, s) in t2.items():
        if k in t1:
            du, ds = (u - t1[k][0]) / 100 / dt, (s - t1[k][1]) / 100 / dt
            if du + ds > 0.02:
                res.append((k[1], du, ds))
    return res


def bars(ax, labels, values, colors, fmt=fmt_rate, horizontal=False):
    pos = range(len(labels))
    if horizontal:
        b = ax.barh(pos, values, color=colors, height=0.62)
        ax.set_yticks(list(pos), labels)
        ax.invert_yaxis()
        for rect, v in zip(b, values):
            ax.text(rect.get_width(), rect.get_y() + rect.get_height() / 2, "  " + fmt(v), va="center", fontsize=10, color=FG)
    else:
        b = ax.bar(pos, values, color=colors, width=0.62)
        ax.set_xticks(list(pos), labels)
        for rect, v in zip(b, values):
            ax.text(rect.get_x() + rect.get_width() / 2, rect.get_height(), fmt(v), ha="center", va="bottom", fontsize=10, color=FG)
    return b


def plot_testbench(out):
    fig, ax = plt.subplots(figsize=(10, 5.2))
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 56)
    ax.axis("off")

    def box(x, y, w, h, title, lines, color, fill):
        ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.4,rounding_size=1.6", fc=fill, ec=color, lw=1.6))
        ax.text(x + 1.5, y + h - 2.6, title, fontsize=12, fontweight="bold", color=color, va="top")
        for i, line in enumerate(lines):
            ax.text(x + 1.5, y + h - 7.2 - i * 3.7, line, fontsize=9.5, color=FG, va="top")

    ax.add_patch(FancyBboxPatch((1, 1), 98, 53, boxstyle="round,pad=0.4,rounding_size=2", fc="#f3f1ec", ec=GRID, lw=1.2))
    ax.text(3, 51.5, "EC2 c6i.8xlarge: Xeon Platinum 8375C, 16 cores / 32 vCPUs, 62 GiB, Docker 25", fontsize=11, color=MUTED, va="top")
    box(4, 8, 30, 38, "Kafka 4.3.1 broker", ["single node, KRaft", "cores 0-6 (14 vCPUs)", "log dirs on 16 GiB tmpfs",
                                              "8 network + 16 I/O threads", "no disk, no replication"], "#5b4a8a", "#efecf6")
    box(38, 29.8, 26, 16.2, "Runner container", ["core 7 (2 vCPUs)", "orchestrates every run", "via docker.sock"], MUTED, "#f7f5f0")
    box(38, 5, 26, 17, "Results volume", ["raw.jsonl per run", "summary.csv, report.md", "charts, logs"], MUTED, "#f7f5f0")
    box(68, 8, 29, 38, "Client under test", ["cores 8-15 (16 vCPUs), 12 GiB", "one container per run:", "Java harness (JDK 25)",
                                               "or Rust harness (rdkafka)", "K = 1..16 client instances", "identical payload bytes"], COLORS["java"], "#fdf1e6")
    ax.annotate("", xy=(34.8, 27.3), xytext=(67.2, 27.3), arrowprops=dict(arrowstyle="<->", color=FG, lw=1.4))
    ax.text(51, 26.2, "docker bridge (kbench-net)", ha="center", fontsize=9, color=MUTED, va="top")
    ax.set_title("Test bench: one host, pinned cores, nothing shared between broker and client", pad=6)
    save(fig, out, "01-testbench.svg")


def plot_full1_ratios(out, f1):
    rows = []
    for (scn, client, role), r in f1.items():
        if client != "rust" or role not in ("producer", "consumer"):
            continue
        if not (scn.startswith("produce-") or scn.startswith("consume-")):
            continue
        j, ru = med(f1, scn, "java", role), num(r.get("msgs_per_s_median"))
        if j and ru:
            rows.append((scn, ru / j, r.get("beyond_noise", "")))
    rows.sort(key=lambda x: x[1])
    fig, ax = plt.subplots(figsize=(9.5, 0.34 * len(rows) + 1.6))
    colors = ["#bdb7ab" if "no" in noise else ("#2e7d5b" if ratio > 1 else "#c0392b") for _, ratio, noise in rows]
    pos = range(len(rows))
    ax.barh(list(pos), [math.log2(r) for _, r, _ in rows], color=colors, height=0.66)
    ax.set_yticks(list(pos), [s for s, _, _ in rows], fontsize=9)
    ax.invert_yaxis()
    ticks = [1 / 16, 1 / 8, 1 / 4, 1 / 2, 1, 2]
    ax.set_xticks([math.log2(t) for t in ticks], ["1/16x", "1/8x", "1/4x", "1/2x", "1x", "2x"])
    ax.axvline(0, color=FG, lw=1)
    for i, (_, ratio, _) in enumerate(rows):
        x = math.log2(ratio)
        ax.text(x + (0.08 if x >= 0 else -0.08), i, "%.2fx" % ratio, va="center", ha="left" if x >= 0 else "right", fontsize=8.5)
    ax.set_xlim(math.log2(1 / 32), math.log2(3))
    ax.set_xlabel("Rust / Java throughput (median of 3 reps, log scale; grey = within run-to-run noise)")
    ax.set_title("First run: Rust behind Java almost everywhere with Java-equivalent settings")
    save(fig, out, "02-first-run-ratios.svg")


def plot_producer_hypotheses(out, f1, diag):
    labels = ["Java-like settings\n(max.in.flight=5)", "librdkafka defaults\n(max.in.flight=1M)", "+ Nagle off",
              "+ backpressure\nthreshold 10", "both"]
    vals = [med(f1, "produce-baseline", "rust", "producer"), diag_rate(diag, "r-native"), diag_rate(diag, "r-nagle-off"),
            diag_rate(diag, "r-bp10"), diag_rate(diag, "r-nagle-bp10")]
    java = diag_rate(diag, "j-base")
    fig, ax = plt.subplots(figsize=(9.5, 4.4))
    bars(ax, labels, vals, [COLORS["rust"]] * len(vals))
    ax.axhline(java, color=COLORS["java"], lw=2, ls="--")
    ax.set_ylim(0, java * 1.15)
    ax.text(-0.3, java * 1.02, "Java: %s msg/s" % fmt_rate(java), color=COLORS["java"], ha="left", va="bottom", fontsize=10)
    rate_axis(ax)
    ax.set_ylabel("Rust producer msg/s")
    ax.set_xlabel("1 KB messages, batch.size 16 KB, 6 partitions, acks=1")
    ax.set_title("Producer: the usual knobs do nothing, Rust stays at about 240k msg/s")
    save(fig, out, "03-producer-knobs.svg")


def plot_threads(out, diag, a, b, name, title):
    th = thread_cpu(diag, a, b)
    th.sort(key=lambda x: -(x[1] + x[2]))
    fig, ax = plt.subplots(figsize=(8, 0.5 * len(th) + 1.6))
    pos = list(range(len(th)))
    ax.barh(pos, [t[1] for t in th], color=COLORS["rust"], height=0.6, label="user")
    ax.barh(pos, [t[2] for t in th], left=[t[1] for t in th], color="#8aa3b3", height=0.6, label="system")
    ax.set_yticks(pos, [THREAD_NAMES.get(t[0], t[0]) for t in th])
    ax.invert_yaxis()
    ax.set_xlim(0, 1.1)
    ax.axvline(1.0, color=MUTED, ls=":", lw=1)
    ax.text(1.0, -0.55, " 1 core", color=MUTED, fontsize=9, va="bottom")
    for i, t in enumerate(th):
        ax.text(t[1] + t[2] + 0.02, i, "%.2f" % (t[1] + t[2]), va="center", fontsize=10)
    ax.set_xlabel("CPU cores used per thread")
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.25), ncol=2)
    ax.set_title(title)
    save(fig, out, name)


def batch_sweep_data(f2, diag):
    sizes = [16384, 32768, 65536, 131072, 262144, 524288, 1048576]
    data = {}
    for client in ("java", "rust"):
        pts = []
        for b in sizes:
            scn = "produce-baseline" if b == 16384 else "produce-batch-%d" % b
            m = med(f2, scn, client, "producer") if f2 else None
            lo = med(f2, scn, client, "producer", "msgs_per_s_min") if f2 else None
            hi = med(f2, scn, client, "producer", "msgs_per_s_max") if f2 else None
            if m is None:
                m = diag_rate(diag, ("j-b%d" if client == "java" else "r-b%d") % b)
                lo = hi = None
            pts.append((b, m, lo, hi))
        data[client] = pts
    return data, "full-2, median of 3 reps with min/max" if f2 and med(f2, "produce-batch-65536", "rust", "producer") else "diagnosis runs"


def plot_batch_sweep(out, f2, diag):
    data, src = batch_sweep_data(f2, diag)
    fig, ax = plt.subplots(figsize=(9.5, 4.8))
    for client, pts in data.items():
        xs = [math.log2(b) for b, m, _, _ in pts if m]
        ys = [m for _, m, _, _ in pts if m]
        ax.plot(xs, ys, marker="o", lw=2.4, color=COLORS[client], label=LABELS[client])
        for b, m, lo, hi in pts:
            if m and lo and hi:
                ax.plot([math.log2(b)] * 2, [lo, hi], color=COLORS[client], lw=1)
    ticks = [16384, 32768, 65536, 131072, 262144, 524288, 1048576]
    names = ["16 KB", "32 KB", "64 KB", "128 KB", "256 KB", "512 KB", "1 MB"]
    ax.set_ylim(0, max(m for pts in data.values() for _, m, _, hi in pts if m) * 1.18)
    for client, pts in data.items():
        b, m = max(((b, m) for b, m, _, _ in pts if m), key=lambda p: p[1])
        ax.plot(math.log2(b), m, marker="o", ms=15, mfc="none", mec=COLORS[client], mew=1.6)
        ax.annotate("%s peak: %s" % (LABELS[client].split()[0], names[ticks.index(b)]), (math.log2(b), m), xytext=(0, 14),
                    textcoords="offset points", ha="right" if b == ticks[-1] else "center", color=COLORS[client], fontsize=9.5, fontweight="bold")
    ax.set_xticks([math.log2(t) for t in ticks], names)
    rate_axis(ax)
    ax.set_xlabel("batch.size (1 KB messages, 6 partitions, acks=1)")
    ax.set_ylabel("producer msg/s")
    ax.legend(loc="lower right")
    ax.set_title("Producer: each client peaks at a different batch.size\n%s" % src)
    save(fig, out, "05-batch-size-sweep.svg")


def plot_consumer_steps(out, diag):
    steps = [("Rust poll()\ndefaults", "c100-poll", COLORS["rust"]),
             ("Rust poll()\nbackoff 10 ms", "c100-poll-fqb10", "#2d6178"),
             ("Rust batch API\nbackoff 10 ms", "c100-batch-fqb10", COLORS["rust-tuned"]),
             ("Java poll()", "c100-java2", COLORS["java"])]
    vals = [diag_rate(diag, s[1]) for s in steps]
    fig, ax = plt.subplots(figsize=(9.5, 4.6))
    bars(ax, [s[0] for s in steps], vals, [s[2] for s in steps])
    rate_axis(ax)
    ax.set_ylabel("consumer msg/s")
    ax.set_xlabel("100 B messages, 6 partitions; backoff = fetch.queue.backoff.ms")
    ax.set_title("Consumer: a 1-second fetch backoff, then per-message costs")
    save(fig, out, "07-consumer-100b-steps.svg")


def plot_consumer_sizes(out, f2):
    scns = [("consume-size-100", "100 B"), ("consume-baseline", "1 KB"), ("consume-size-10240", "10 KB")]
    clients = [c for c in ("java", "rust", "rust-tuned") if any(med(f2, s, c, "consumer") for s, _ in scns)]
    fig, axes = plt.subplots(1, len(scns), figsize=(11, 4.2))
    for ax, (scn, label) in zip(axes, scns):
        vals = [med(f2, scn, c, "consumer") or 0 for c in clients]
        bars(ax, [c for c in clients], vals, [COLORS[c] for c in clients])
        rate_axis(ax)
        ax.set_title(label, fontsize=12)
    axes[0].set_ylabel("consumer msg/s (median of 3 reps)")
    fig.suptitle("Consumer after the fixes, by message size", x=0.01, ha="left", fontweight="bold", fontsize=13)
    fig.tight_layout()
    save(fig, out, "08-consumer-by-size.svg")


def plot_before_after(out, f1, f2):
    pairs = [
        ("Producer, 1 KB, Java-equivalent 16 KB batches", ("produce-baseline", "rust", "producer"), ("produce-baseline", "rust", "producer")),
        ("Producer, each client at its own defaults", ("produce-baseline", "rust", "producer"), ("produce-defaults", "rust", "producer")),
        ("Producer, each client at its best batch.size", ("produce-baseline", "rust", "producer"), ("produce-best", "rust", "producer")),
        ("Consumer, 100 B", ("consume-size-100", "rust", "consumer"), ("consume-size-100", "rust-tuned", "consumer")),
        ("Consumer, 1 KB", ("consume-baseline", "rust", "consumer"), ("consume-baseline", "rust-tuned", "consumer")),
        ("Consumer, 1 KB lz4", ("consume-lz4-text", "rust", "consumer"), ("consume-lz4-text", "rust-tuned", "consumer")),
    ]
    labels, before, after = [], [], []
    for label, b, a in pairs:
        bj, aj = med(f1, b[0], "java", b[2]), med(f2, a[0], "java", a[2])
        bv, av = med(f1, *b), med(f2, *a)
        if bj and aj and bv and av:
            labels.append(label)
            before.append(bv / bj)
            after.append(av / aj)
    fig, ax = plt.subplots(figsize=(10, 0.62 * len(labels) + 1.8))
    pos = list(range(len(labels)))
    h = 0.36
    ax.barh([p - h / 2 for p in pos], before, height=h, color=GRID, label="first run (Rust with Java-equivalent settings)")
    ax.barh([p + h / 2 for p in pos], after, height=h, color=COLORS["rust-tuned"], label="after the fixes")
    ax.set_yticks(pos, labels)
    ax.invert_yaxis()
    ax.axvline(1, color=FG, lw=1)
    for p, bv, av in zip(pos, before, after):
        ax.text(bv, p - h / 2, " %.2fx" % bv, va="center", fontsize=9)
        ax.text(av, p + h / 2, " %.2fx" % av, va="center", fontsize=9, fontweight="bold")
    ax.set_xlabel("Rust / Java throughput (median of 3 reps; 1x = parity)")
    ax.legend(loc="upper center", bbox_to_anchor=(0.4, -0.16), ncol=2)
    ax.set_title("Where the gaps went")
    save(fig, out, "09-before-after.svg")


def plot_scaling(out, f2):
    profiles = [("produce", "default-100", "Produce, 100 B"), ("produce", "default-1024", "Produce, 1 KB"),
                ("produce", "tuned-1024", "Produce, 1 KB lz4, 1 MB batches"), ("consume", "default-100", "Consume, 100 B"),
                ("consume", "default-1024", "Consume, 1 KB"), ("consume", "tuned-1024", "Consume, 1 KB lz4")]
    ks = [1, 2, 4, 8, 16]
    fig, axes = plt.subplots(2, 3, figsize=(13, 7.4), sharex=True)
    for ax, (mode, prof, title) in zip(axes.flat, profiles):
        role = "producer" if mode == "produce" else "consumer"
        for c in ("java", "rust", "rust-tuned"):
            pts = [(k, med(f2, "scale-%s-%s-k%d" % (mode, prof, k), c, role)) for k in ks]
            pts = [(k, v) for k, v in pts if v]
            if pts:
                ax.plot([math.log2(k) for k, _ in pts], [v for _, v in pts], marker="o", lw=2.2, color=COLORS[c], label=c)
        ax.set_title(title, fontsize=11.5)
        rate_axis(ax)
        ax.set_xticks([math.log2(k) for k in ks], [str(k) for k in ks])
    for ax in axes[1]:
        ax.set_xlabel("client instances K (one connection each)")
    fig.legend(*axes[1][0].get_legend_handles_labels(), loc="upper right", ncol=3, bbox_to_anchor=(1.0, 1.0))
    fig.suptitle("Scaling to the whole host (16 client vCPUs)", x=0.01, ha="left", fontweight="bold", fontsize=13)
    fig.tight_layout()
    save(fig, out, "10-scaling.svg")


def plot_e2e(out, f2):
    rates = [1000, 10000, 50000]
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.2), sharey=True)
    for ax, linger in zip(axes, (0, 5)):
        for c in ("java", "rust", "rust-tuned"):
            pts = [(r, med(f2, "e2e-linger-%d-rate-%d" % (linger, r), c, "consumer", "lat_p99_us")) for r in rates]
            pts = [(r, v / 1000.0) for r, v in pts if v]
            if pts:
                ax.plot([math.log10(r) for r, _ in pts], [v for _, v in pts], marker="o", lw=2.2, color=COLORS[c], label=c)
        ax.set_xticks([math.log10(r) for r in rates], ["1k/s", "10k/s", "50k/s"])
        ax.set_title("linger.ms=%d" % linger, fontsize=12)
        ax.set_xlabel("target rate (1 KB messages)")
    axes[0].set_yscale("log")
    axes[0].yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: "%g" % v))
    axes[0].set_ylabel("end-to-end p99 latency (ms, log scale)")
    fig.legend(*axes[0].get_legend_handles_labels(), loc="upper right", ncol=3, bbox_to_anchor=(1.0, 1.0))
    fig.suptitle("End-to-end latency, producer to consumer", x=0.01, ha="left", fontweight="bold", fontsize=13)
    fig.tight_layout()
    save(fig, out, "11-e2e-latency.svg")


def plot_e2e_rerun(out, e2):
    rates = [1000, 10000, 50000]
    styles = {"java": ("o", "-", 0.0), "rust": ("o", "-", -0.03), "rust-tuned": ("s", ":", 0.0), "rust-lowlat": ("D", "--", 0.03)}
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.4), sharey=True)
    for ax, linger in zip(axes, (0, 5)):
        for c, (marker, ls, dx) in styles.items():
            pts = [(r, med(e2, "e2e-linger-%d-rate-%d" % (linger, r), c, "consumer", "lat_p99_us")) for r in rates]
            pts = [(r, v / 1000.0) for r, v in pts if v]
            if pts:
                ax.plot([math.log10(r) + dx for r, _ in pts], [v for _, v in pts], marker=marker, ls=ls, lw=2.2, ms=6,
                        color=COLORS[c], label=LABELS[c])
        ax.set_xticks([math.log10(r) for r in rates], ["1k/s", "10k/s", "50k/s"])
        ax.set_title("linger.ms=%d" % linger, fontsize=12)
        ax.set_xlabel("target rate (1 KB messages)")
    axes[0].set_yscale("log")
    axes[0].yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: "%g" % v))
    axes[0].set_ylabel("end-to-end p99 latency (ms, log scale)")
    fig.legend(*axes[0].get_legend_handles_labels(), loc="lower center", ncol=4, bbox_to_anchor=(0.5, -0.01), fontsize=9.5)
    fig.suptitle("End-to-end latency after the fixes (e2e-2, median of 3 reps)", x=0.01, ha="left", fontweight="bold", fontsize=13)
    fig.tight_layout(rect=(0, 0.08, 1, 1))
    save(fig, out, "19-e2e-rerun.svg")


def plot_resources(out, f2):
    scns = ["produce-baseline", "produce-defaults", "produce-best", "consume-baseline", "consume-size-100"]
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.4))
    width = 0.26
    clients = ("java", "rust", "rust-tuned")
    for ax, col, ylabel, scale in ((axes[0], "rss_peak_bytes", "peak RSS (MB)", 1e6),
                                   (axes[1], "cpu_s_per_million_msgs", "CPU seconds per million messages", 1)):
        for i, c in enumerate(clients):
            vals = []
            for s in scns:
                role = "producer" if s.startswith("produce") else "consumer"
                v = med(f2, s, c, role, col)
                vals.append(v / scale if v else 0)
            if any(vals):
                ax.bar([p + (i - 1) * width for p in range(len(scns))], vals, width=width, color=COLORS[c], label=c)
        ax.set_xticks(range(len(scns)), [s.replace("produce-", "P ").replace("consume-", "C ") for s in scns], fontsize=9)
        ax.set_ylabel(ylabel)
    axes[0].set_yscale("log")
    axes[0].yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: "%g" % v))
    axes[0].set_ylabel("peak RSS (MB, log scale)")
    axes[0].set_ylim(bottom=5)
    fig.legend(*axes[1].get_legend_handles_labels(), loc="upper right", ncol=3, bbox_to_anchor=(1.0, 1.0))
    fig.suptitle("Memory and CPU cost (Java RSS includes a pre-touched 2 GiB heap)", x=0.01, ha="left", fontweight="bold", fontsize=13)
    fig.tight_layout()
    save(fig, out, "12-resources.svg")


def plot_producer_settings(out, f2):
    scns = [("produce-baseline", "same settings\n(batch.size 16 KB)"),
            ("produce-defaults", "each at its library defaults\n(Java 16 KB, Rust 1 MB)"),
            ("produce-best", "each at its best batch.size\n(Java 128 KB, Rust 1 MB)")]
    fig, ax = plt.subplots(figsize=(9.5, 4.6))
    width = 0.36
    for i, c in enumerate(("java", "rust")):
        vals = [med(f2, s, c, "producer") or 0 for s, _ in scns]
        pos = [p + (i - 0.5) * width for p in range(len(scns))]
        b = ax.bar(pos, vals, width=width, color=COLORS[c], label=LABELS[c])
        for rect, v in zip(b, vals):
            ax.text(rect.get_x() + rect.get_width() / 2, v, fmt_rate(v), ha="center", va="bottom", fontsize=10)
    ax.set_xticks(range(len(scns)), [l for _, l in scns])
    ax.set_ylim(0, 1.25e6)
    rate_axis(ax)
    ax.set_ylabel("producer msg/s (median of 3 reps)")
    ax.set_xlabel("1 KB messages, 6 partitions, acks=1, linger.ms 5")
    ax.legend(loc="upper left")
    ax.set_title("Producer rerun: the ranking depends on which batch.size you compare")
    save(fig, out, "13-producer-settings.svg")


def plot_inflight_latency(out, diag):
    variants = [("librdkafka defaults\n(max.in.flight 1,000,000)", "e2e50k-native", COLORS["rust"]),
                ("defaults +\nmax.in.flight 5", "e2e50k-native-mif5", COLORS["rust-lowlat"]),
                ("Java-equivalent settings\n(max.in.flight 5)", "e2e50k-matched", "#9a948a")]
    res = [diag_result(diag, v[1]) for v in variants]
    if not all(res):
        print("e2e50k diagnosis runs missing: skipped 14-inflight-latency.svg")
        return
    fig, ax = plt.subplots(figsize=(9.5, 4.6))
    width = 0.36
    for i, (pct, alpha) in enumerate((("p50", 0.55), ("p99", 1.0))):
        vals = [r["latency_us"][pct] / 1000.0 for r in res]
        pos = [p + (i - 0.5) * width for p in range(len(variants))]
        b = ax.bar(pos, vals, width=width, color=[v[2] for v in variants], alpha=alpha)
        for rect, v in zip(b, vals):
            ax.text(rect.get_x() + rect.get_width() / 2, v, "%s\n%.1f ms" % (pct, v), ha="center", va="bottom", fontsize=9.5)
    ax.set_xticks(range(len(variants)), [v[0] for v in variants])
    ax.set_ylim(0, max(r["latency_us"]["p99"] for r in res) / 1000.0 * 1.25)
    ax.set_ylabel("send-to-ack latency (ms)")
    ax.set_xlabel("Rust producer, 1 KB messages at 50k msg/s, linger.ms 0, batch.size 16 KB, 6 partitions")
    ax.set_title("linger.ms 0: an unlimited in-flight window costs about 20x at p99")
    save(fig, out, "14-inflight-latency.svg")


def diag_median(diag, run_ids):
    vals = sorted(v for v in (diag_rate(diag, i) for i in run_ids) if v)
    if not vals:
        return None
    n = len(vals)
    return vals[n // 2] if n % 2 else (vals[n // 2 - 1] + vals[n // 2]) / 2


def plot_java_batch_sweep(out, jdiag, diag):
    sizes = [16384, 131072, 262144, 524288, 1048576]
    default = [["pb-16384-b"], ["pb-128k-b"], ["pb-262144-b"], ["pb-524288-b"], ["pb-1m-b"]]
    sb = [["pb-16384-sb-1"], ["pb-128k-sb-1"], ["pb-262144-sb-1"], ["pb-524288-sb-1"],
          ["pb-1m-sb-1", "pb-1m-sb-1-r2", "pb-1m-sb-1-thr"]]
    series = [("Java, defaults (send.buffer.bytes 128 KB)", [diag_median(jdiag, ids) for ids in default], COLORS["java"], "-"),
              ("Java, send.buffer.bytes=-1 (OS autotuning)", [diag_median(jdiag, ids) for ids in sb], COLORS["java"], "--"),
              ("Rust, librdkafka defaults", [diag_rate(diag, "r-b%d" % b) for b in sizes], COLORS["rust"], "-")]
    if not all(all(v) for _, v, _, _ in series):
        print("java batch sweep runs missing: skipped 15-java-batch-sweep.svg")
        return
    fig, ax = plt.subplots(figsize=(9.5, 4.8))
    xs = [math.log2(b) for b in sizes]
    for label, vals, color, ls in series:
        ax.plot(xs, vals, marker="o", lw=2.4, color=color, ls=ls, label=label)
    mrs = diag_median(jdiag, ["pb-1m-mrs1m", "pb-1m-mrs1m-r2"])
    both = diag_rate(jdiag, "pb-1m-sb-1-mrs1m")
    if mrs and both:
        x = math.log2(1048576)
        ax.plot([x], [mrs], marker="s", ms=8, color=COLORS["java"], ls="none", mfc=BG, mew=2,
                label="Java 1 MB, one batch per request (max.request.size)")
        ax.plot([x], [both], marker="D", ms=8, color=COLORS["java"], ls="none", label="Java 1 MB, both fixes")
    ax.set_xticks(xs, ["16 KB", "128 KB", "256 KB", "512 KB", "1 MB"])
    ax.set_ylim(0, 1.3e6)
    rate_axis(ax)
    ax.set_xlabel("batch.size (1 KB messages, 6 partitions, acks=1, linger.ms 5)")
    ax.set_ylabel("producer msg/s")
    ax.legend(loc="lower right", fontsize=9.5)
    ax.set_title("Java producer: large requests are copied again on every partial socket write")
    save(fig, out, "15-java-batch-sweep.svg")


def plot_java_consumer(out, jdiag):
    cases = [("1 KB, 6 partitions", ["c1k-base", "c1k-base-r2", "c1k-thr"], ["c1k-rb-1", "c1k-rb-1-r2", "c1k-rb-1-thr"],
              "receive.buffer.bytes=-1", "c1k-rust"),
             ("10 KB, 6 partitions", ["c10k-base"], ["c10k-rb-1"], "receive.buffer.bytes=-1", "c10k-rust"),
             ("1 KB, 1 partition", ["c1p-base"], ["c1p-mpfb8m-rb-1"], "receive.buffer.bytes=-1\n+ 8 MB fetch", "c1p-rust")]
    fig, axes = plt.subplots(1, len(cases), figsize=(12, 4.4))
    for ax, (title, base, fixed, fixed_label, rust) in zip(axes, cases):
        vals = [diag_median(jdiag, base), diag_median(jdiag, fixed), diag_rate(jdiag, rust)]
        if not all(vals):
            print("java consumer runs missing: skipped 16-java-consumer.svg")
            plt.close(fig)
            return
        bars(ax, ["Java\ndefaults", "Java\n" + fixed_label, "Rust"], vals, [COLORS["java"], "#f2a65a", COLORS["rust"]])
        ax.tick_params(axis="x", labelsize=9)
        ax.set_ylim(0, max(vals) * 1.15)
        rate_axis(ax)
        ax.set_title(title, fontsize=12)
    axes[0].set_ylabel("consumer msg/s")
    fig.suptitle("Java consumer: one polling thread, and a 64 KB socket buffer", x=0.01, ha="left", fontweight="bold", fontsize=13)
    fig.tight_layout()
    save(fig, out, "16-java-consumer.svg")


def plot_java_k16(out, jdiag, f2):
    runs = [("K=8, defaults", "k8-base"), ("K=16, defaults", "k16-base"), ("K=16, 8 GB heap", "k16-heap8g"),
            ("K=16, buffer.memory 64 MB", "k16-bm64m"), ("K=16, buffer.memory 32 MB", "k16-bm32m")]
    res = [diag_result(jdiag, r) for _, r in runs]
    if not all(res):
        print("java scaling runs missing: skipped 17-java-k16.svg")
        return
    fig, ax = plt.subplots(figsize=(9.5, 3.9))
    vals = [r["throughput_msgs_per_s"] for r in res]
    colors = [COLORS["java"] if i < 2 else "#f2a65a" for i in range(len(runs))]
    pos = list(range(len(runs)))
    ax.barh(pos, vals, color=colors, height=0.62)
    ax.set_yticks(pos, [l for l, _ in runs])
    ax.invert_yaxis()
    for i, r in enumerate(res):
        ax.text(2e5, i, "p99 %.0f ms, GC %.1f s" % (r["latency_us"]["p99"] / 1000.0, r["jvm"]["gc_total_time_ms"] / 1000.0),
                va="center", fontsize=9.5, color="white" if i < 2 else FG)
        ax.text(vals[i] + 2e5, i, fmt_rate(vals[i]), va="center", fontsize=10, fontweight="bold",
                bbox=dict(fc=BG, ec="none", pad=1.5), zorder=3)
    rust = med(f2, "scale-produce-default-100-k16", "rust", "producer") if f2 else None
    if rust:
        ax.axvline(rust, color=COLORS["rust"], ls="--", lw=1.6)
        ax.text(rust, -0.62, " Rust K=16: %s" % fmt_rate(rust), color=COLORS["rust"], fontsize=9.5, va="center")
    ax.set_ylim(len(runs) - 0.5, -0.95)
    ax.set_xlim(0, 1.5e7)
    ax.set_xticks([0, 5e6, 1e7, 1.5e7])
    rate_axis(ax, "x")
    ax.set_xlabel("producer msg/s (100 B messages, 16 partitions, 16 KB batches, one JVM with a 2 GB heap)")
    ax.set_title("Java producer at K=16: the backlog fills the heap; capping it restores throughput")
    save(fig, out, "17-java-k16.svg")


def gzip_ratio(diag, run_id):
    r = diag_result(diag, run_id)
    path = os.path.join(diag, run_id + ".logdirs.json")
    if not r or not os.path.exists(path):
        return None
    with open(path) as fh:
        d = json.load(fh)
    stored = sum(p["size"] for b in d["brokers"] for ld in b["logDirs"] for p in ld["partitions"])
    p = {k.replace("-", "_"): v for k, v in r["params"].items()}
    return (p["num_messages"] + p["warmup_messages"]) * p["message_size"] / stored


def plot_gzip(out, jdiag):
    pts = [("Java, batch.size 16 KB\n(about 43 KB input per batch)", "gz-java-b16k", COLORS["java"], "o", (-10, -22)),
           ("Rust, batch.size 16 KB", "gz-rust", COLORS["rust"], "o", (8, 6)),
           ("Rust, batch.size 88 KB", "gz-rust-b88k", COLORS["rust"], "s", (8, -4)),
           ("Java, batch.size 3,000 B\n(about 8 KB input)", "gz-java-b3k", COLORS["java"], "s", (8, -24)),
           ("Java, level 1", "gz-java-l1", COLORS["java"], "D", (10, -4)),
           ("Rust, level 1", "gz-rust-l1", COLORS["rust"], "D", (10, -4))]
    data = [(label, diag_rate(jdiag, rid), gzip_ratio(jdiag, rid), c, m, off) for label, rid, c, m, off in pts]
    if not all(d[1] and d[2] for d in data):
        print("gzip runs missing: skipped 18-gzip.svg")
        return
    fig, ax = plt.subplots(figsize=(9.5, 4.8))
    for label, rate, ratio, c, m, off in data:
        ax.plot([ratio], [rate], marker=m, ms=9, color=c, ls="none")
        ax.annotate("%s: %s, %.2fx" % (label, fmt_rate(rate), ratio), (ratio, rate), textcoords="offset points", xytext=off,
                    ha="left" if off[0] > 0 else "right", fontsize=9)
    ax.set_yscale("log")
    ax.set_yticks([2.5e4, 5e4, 1e5, 2e5])
    ax.yaxis.set_minor_locator(matplotlib.ticker.NullLocator())
    rate_axis(ax)
    ax.set_xlim(3.6, 6.3)
    ax.set_ylim(2.0e4, 2.5e5)
    ax.set_xlabel("compression ratio on the broker (1 KB text messages, higher is smaller)")
    ax.set_ylabel("producer msg/s (log scale)")
    ax.set_title("gzip: more input per batch compresses better and runs slower, in both clients")
    save(fig, out, "18-gzip.svg")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--results", default="/results")
    p.add_argument("--out", default="/results/story-plots")
    a = p.parse_args()
    os.makedirs(a.out, exist_ok=True)
    f1 = load_summary(os.path.join(a.results, "full-1"))
    f2 = load_summary(os.path.join(a.results, "full-2"))
    diag = os.path.join(a.results, "diag-1")
    plot_testbench(a.out)
    plot_full1_ratios(a.out, f1)
    plot_producer_hypotheses(a.out, f1, diag)
    plot_threads(a.out, diag, "threads-s1.txt", "threads-s2.txt", "04-producer-threads.svg",
                 "Producer: no thread is saturated, the client is waiting")
    plot_batch_sweep(a.out, f2, diag)
    plot_threads(a.out, diag, "threads-b1.txt", "threads-b2.txt", "06-consumer-threads.svg",
                 "Consumer (batch API, backoff fixed): the librdkafka broker thread saturates")
    plot_consumer_steps(a.out, diag)
    plot_inflight_latency(a.out, diag)
    if f2 and any(k[1] == "rust-tuned" for k in f2):
        plot_consumer_sizes(a.out, f2)
        plot_before_after(a.out, f1, f2)
        plot_scaling(a.out, f2)
        plot_e2e(a.out, f2)
        plot_resources(a.out, f2)
        plot_producer_settings(a.out, f2)
    else:
        print("full-2 not complete: skipped charts 08-13")
    e2 = load_summary(os.path.join(a.results, "e2e-2"))
    if e2 and any(k[1] == "rust-lowlat" for k in e2):
        plot_e2e_rerun(a.out, e2)
    else:
        print("e2e-2 missing: skipped 19-e2e-rerun.svg")
    jdiag = os.path.join(a.results, "diag-java")
    if os.path.isdir(jdiag):
        plot_java_batch_sweep(a.out, jdiag, diag)
        plot_java_consumer(a.out, jdiag)
        plot_java_k16(a.out, jdiag, f2)
        plot_gzip(a.out, jdiag)
    else:
        print("diag-java missing: skipped charts 15-18")


if __name__ == "__main__":
    main()
