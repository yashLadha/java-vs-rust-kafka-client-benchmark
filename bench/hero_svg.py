#!/usr/bin/env python3
"""Hand-built SVG hero for the write-up: producer msg/s vs batch.size.

usage: python3 bench/hero_svg.py [--results /results] > hero.svg

Uses full-2 medians for the batch.size sweep when available, else the diagnosis runs.
"""
import argparse
import csv
import os
import random

SIZES = [16384, 32768, 65536, 131072, 262144, 524288, 1048576]
SIZE_TEXT = ["16 KB", "32 KB", "64 KB", "128 KB", "256 KB", "512 KB", "1 MB"]
DIAG = {"java": [552, 769, 956, 1042, 1000, 890, 622], "rust": [233, 390, 577, 768, 903, 991, 995]}
JAVA, RUST, TUNED, FG, INK, MUTED, NEUTRAL, BG = "#e76f00", "#1f3a4d", "#3f9aa8", "#1c1a17", "#4a463f", "#6b665f", "#e4e0d8", "#faf9f6"


def sweep(results):
    path = os.path.join(results, "full-2", "summary.csv")
    if not os.path.exists(path):
        return DIAG
    med = {}
    with open(path) as fh:
        for r in csv.DictReader(fh):
            if r["role"] == "producer" and r.get("msgs_per_s_median"):
                med[(r["scenario"], r["client"])] = float(r["msgs_per_s_median"]) / 1000
    out = {}
    for client in ("java", "rust"):
        vals = [med.get(("produce-baseline" if b == 16384 else "produce-batch-%d" % b, client)) for b in SIZES]
        if any(v is None for v in vals):
            return DIAG
        out[client] = vals
    return out


def render(data):
    x0, x1, y0, y1, vmax = 118, 1030, 500, 190, 1250
    jv, rv = data["java"], data["rust"]

    def X(i):
        return x0 + i * (x1 - x0) / 6

    def Y(v):
        return y0 - (v / vmax) * (y0 - y1)

    def path(vals):
        return " ".join(("M" if i == 0 else "L") + "%.1f %.1f" % (X(i), Y(v)) for i, v in enumerate(vals))

    def curve(vals, color):
        d = path(vals)
        return ('    <path d="%s" fill="none" stroke="%s" stroke-width="14" stroke-linejoin="round" stroke-linecap="round"/>\n'
                '    <path d="%s" fill="none" stroke="%s" stroke-width="6" stroke-linejoin="round" stroke-linecap="round"/>\n'
                % (d, BG, d, color)
                + "\n".join('    <circle cx="%.1f" cy="%.1f" r="6.5" fill="%s" stroke="%s" stroke-width="3"/>' % (X(i), Y(v), color, BG)
                            for i, v in enumerate(vals)))

    def peak(vals, color, name, anchor, nudge):
        i = max(range(len(vals)), key=vals.__getitem__)
        x, y = X(i), Y(vals[i])
        return ('    <circle cx="%.1f" cy="%.1f" r="13" fill="none" stroke="%s" stroke-width="2.5"/>\n'
                '    <text x="%.1f" y="%.1f" text-anchor="%s" class="sans halo" font-size="21" font-weight="700" fill="%s">%s peak: %s</text>\n'
                '    <text x="%.1f" y="%.1f" text-anchor="%s" class="sans halo" font-size="18" font-weight="600" fill="%s">%.2fM msg/s</text>'
                % (x, y, color, x + nudge, y - 46, anchor, color, name, SIZE_TEXT[i], x + nudge, y - 23, anchor, INK, vals[i] / 1000))

    grid = []
    for v, label in ((250, "250k"), (500, "500k"), (750, "750k"), (1000, "1M")):
        grid.append('    <line x1="%d" x2="%d" y1="%.1f" y2="%.1f" stroke="%s" stroke-width="1.5"/>' % (x0 - 8, x1 + 12, Y(v), Y(v), NEUTRAL))
        grid.append('    <text x="%d" y="%.1f" text-anchor="end" class="sans axis">%s</text>' % (x0 - 16, Y(v) + 6, label))
    grid.append('    <line x1="%d" x2="%d" y1="%d" y2="%d" stroke="#c9c3b8" stroke-width="2"/>' % (x0 - 8, x1 + 12, y0, y0))
    xt = "\n".join('    <text x="%.1f" y="%d" text-anchor="middle" class="sans axis">%s</text>' % (X(i), y0 + 30, t) for i, t in enumerate(SIZE_TEXT))

    rng = random.Random(7)
    log = []
    for row, y in enumerate((548, 562)):
        x = rng.choice([0, 12, 26])
        while x < 1200:
            w = rng.choice([22, 34, 48, 64, 90])
            c = rng.choice([JAVA, RUST, TUNED, NEUTRAL, NEUTRAL])
            op = 0.9 if c == NEUTRAL else 0.35
            log.append('    <rect x="%d" y="%d" width="%d" height="10" rx="3" fill="%s" fill-opacity="%.2f"/>' % (x, y, w, c, op))
            x += w + 6
    return f'''<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="630" viewBox="0 0 1200 630" role="img" aria-labelledby="title desc">
  <title id="title">Java vs Rust Kafka clients: chasing down a 16x gap, one default at a time, on both clients</title>
  <desc id="desc">Producer throughput of the Java Kafka client and rust-rdkafka as batch.size grows from 16 KB to 1 MB. Java peaks at 128 KB with {jv[3] / 1000:.2f}M msg/s and Rust at 1 MB with {rv[6] / 1000:.2f}M msg/s: each client peaks at a different batch.size.</desc>
  <defs>
    <style>
      .sans {{ font-family: "Commissioner Variable", "Commissioner", system-ui, -apple-system, "Segoe UI", Helvetica, Arial, sans-serif; }}
      .mono {{ font-family: "Source Code Pro Variable", "Source Code Pro", ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }}
      .axis {{ font-size: 18px; font-weight: 500; fill: {INK}; }}
      .halo {{ paint-order: stroke; stroke: {BG}; stroke-width: 7px; stroke-linejoin: round; }}
    </style>
  </defs>

  <rect width="1200" height="630" fill="{BG}"/>

  <g class="sans">
    <text x="56" y="80" font-size="58" font-weight="800" fill="{FG}"><tspan fill="{JAVA}">Java</tspan> vs <tspan fill="{RUST}">Rust</tspan> Kafka clients</text>
    <text x="58" y="122" font-size="25" fill="{MUTED}">Chasing down a 16x gap, one default at a time, on both clients</text>
    <text x="{x0 - 8}" y="172" font-size="22" font-weight="700" fill="{FG}">Each client peaks at a different batch.size</text>
  </g>

  <g>
{chr(10).join(grid)}
{xt}
  </g>

  <g>
{curve(rv, RUST)}
{curve(jv, JAVA)}
{peak(jv, JAVA, "Java", "middle", 0)}
{peak(rv, RUST, "Rust", "end", 14)}
    <text x="{X(6) + 22:.1f}" y="{Y(rv[6]) + 8:.1f}" class="sans halo" font-size="23" font-weight="700" fill="{RUST}">Rust</text>
    <text x="{X(6) + 22:.1f}" y="{Y(jv[6]) + 8:.1f}" class="sans halo" font-size="23" font-weight="700" fill="{JAVA}">Java</text>
  </g>

  <g>
{chr(10).join(log)}
  </g>

  <text x="56" y="608" class="mono" font-size="16" fill="{INK}">kafka-clients 4.3.1 vs rdkafka 0.39 / librdkafka 2.12.1</text>
  <text x="1144" y="608" text-anchor="end" class="sans" font-size="17" fill="{INK}">producer msg/s, 1 KB messages, 6 partitions</text>
</svg>
'''


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--results", default="/results")
    a = p.parse_args()
    print(render(sweep(a.results)), end="")


if __name__ == "__main__":
    main()
