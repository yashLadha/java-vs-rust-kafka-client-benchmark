#!/usr/bin/env bash
set -euo pipefail

image="apache/kafka:4.3.1"
bootstrap="kbench-kafka:9092"
client_cpuset="${KBENCH_CLIENT_CPUSET:-8-15,24-31}"
client_mem="${KBENCH_CLIENT_MEM:-12g}"
client_heap="-Xms2g -Xmx2g"

usage() {
  cat >&2 <<USAGE
usage:
  $0 producer --run-id ID --topic T --num-records N --record-size B [--producer-props "k=v k=v ..."] [--warmup-records W]
  $0 consumer --run-id ID --topic T --messages N [--consumer-props "k=v ..."] [--timeout-ms MS]
env: KBENCH_CLIENT_CPUSET (default 8-15,24-31), KBENCH_CLIENT_MEM (default 12g)
USAGE
  exit 2
}

mode="${1:-}"
[[ "$mode" == "producer" || "$mode" == "consumer" ]] || usage
shift

run_id=""
topic=""
num_records=""
record_size=""
producer_props=""
warmup_records=""
messages=""
consumer_props=""
timeout_ms=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --run-id) run_id="$2"; shift 2 ;;
    --topic) topic="$2"; shift 2 ;;
    --num-records) num_records="$2"; shift 2 ;;
    --record-size) record_size="$2"; shift 2 ;;
    --producer-props) producer_props="$2"; shift 2 ;;
    --warmup-records) warmup_records="$2"; shift 2 ;;
    --messages) messages="$2"; shift 2 ;;
    --consumer-props) consumer_props="$2"; shift 2 ;;
    --timeout-ms) timeout_ms="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; usage ;;
  esac
done

[[ -n "$run_id" && -n "$topic" ]] || usage

read -r -a pprops <<<"$producer_props"
read -r -a cprops <<<"$consumer_props"

tool_args=()
if [[ "$mode" == "producer" ]]; then
  [[ -n "$num_records" && -n "$record_size" ]] || usage
  tool_args=(/opt/kafka/bin/kafka-producer-perf-test.sh
    --bootstrap-server "$bootstrap" --topic "$topic"
    --num-records "$num_records" --record-size "$record_size"
    --throughput -1 --print-metrics)
  if [[ ${#pprops[@]} -gt 0 ]]; then
    tool_args+=(--command-property "${pprops[@]}")
  fi
  if [[ -n "$warmup_records" ]]; then
    tool_args+=(--warmup-records "$warmup_records")
  fi
else
  [[ -n "$messages" ]] || usage
  tool_args=(/opt/kafka/bin/kafka-consumer-perf-test.sh
    --bootstrap-server "$bootstrap" --topic "$topic"
    --num-records "$messages" --print-metrics)
  # Unlike the producer tool, the consumer tool takes one k=v per --command-property.
  for p in ${cprops[@]+"${cprops[@]}"}; do
    tool_args+=(--command-property "$p")
  done
  if [[ -n "$timeout_ms" ]]; then
    tool_args+=(--timeout "$timeout_ms")
  fi
fi

out="$(mktemp "${TMPDIR:-/tmp}/kbench-stock-perf.XXXXXX")"
trap 'rm -f "$out"' EXIT

echo "running: ${tool_args[*]}" >&2
set +e
docker run --rm --name "kbench-client-$run_id" --network kbench-net \
  --cpuset-cpus "$client_cpuset" --memory "$client_mem" \
  -e KAFKA_HEAP_OPTS="$client_heap" \
  "$image" "${tool_args[@]}" >"$out"
rc=$?
set -e
cat "$out" >&2

python3 - "$mode" "$run_id" "$topic" "$num_records" "$record_size" "$producer_props" \
  "$warmup_records" "$messages" "$consumer_props" "$timeout_ms" "$rc" "$out" "${tool_args[*]}" \
  "$client_cpuset" "$client_mem" "$client_heap" <<'PY'
import json
import re
import sys

(mode, run_id, topic, num_records, record_size, producer_props, warmup_records,
 messages, consumer_props, timeout_ms, rc, out_path, command, cpuset, memory, heap) = sys.argv[1:]
rc = int(rc)
lines = open(out_path, encoding="utf-8", errors="replace").read().splitlines()

MIB = 1024 * 1024


def props(s):
    d = {}
    for kv in s.split():
        k, _, v = kv.partition("=")
        d[k] = v
    return d


def num(s):
    f = float(s)
    return int(f) if f.is_integer() and "." not in s else f


def parse_metrics(lines):
    metrics = {}
    in_metrics = False
    for line in lines:
        if line.startswith("Metric Name"):
            in_metrics = True
            continue
        if in_metrics and " : " in line:
            name, _, value = line.rpartition(" : ")
            value = value.strip()
            try:
                metrics[name.strip()] = float(value)
            except ValueError:
                metrics[name.strip()] = value
    return metrics


params = {"topic": topic, "bootstrap": "kbench-kafka:9092", "image": "apache/kafka:4.3.1",
          "cpuset": cpuset, "memory": memory, "heap": heap, "command": command}
result = {
    "schema_version": 1,
    "client": "java-stock",
    "client_lib": "kafka-clients (kafka-%s-perf-test)" % mode,
    "client_version": "4.3.1",
    "mode": "produce" if mode == "producer" else "consume",
    "run_id": run_id,
    "params": params,
    "messages": None,
    "bytes": None,
    "throughput_msgs_per_s": None,
    "throughput_mb_per_s": None,
    "latency_us": None,
    "raw": None,
    "raw_summary_line": None,
    "metrics": parse_metrics(lines),
    "exit_code": rc,
    "status": "error",
    "error": None,
}

if mode == "producer":
    params.update({"num_records": int(num_records), "record_size": int(record_size),
                   "producer_props": props(producer_props), "throughput": -1,
                   "warmup_records": int(warmup_records) if warmup_records else 0})
    pat = re.compile(
        r"^(?P<n>\d+) (?P<steady>steady state )?records sent, (?P<rps>[\d.]+) records/sec "
        r"\((?P<mib>[\d.]+) MB/sec\), (?P<avg>[\d.]+) ms avg latency, (?P<max>[\d.]+) ms max latency, "
        r"(?P<p50>\d+) ms 50th, (?P<p95>\d+) ms 95th, (?P<p99>\d+) ms 99th, (?P<p999>\d+) ms 99.9th\.$")
    matches = [(l, m) for l in lines for m in [pat.match(l.strip())] if m]
    # With --warmup-records the tool prints a whole-run line and a steady-state line; prefer the latter.
    steady = [x for x in matches if x[1].group("steady")]
    chosen = (steady or matches or [None])[-1]
    if chosen:
        line, m = chosen
        n = int(m.group("n"))
        rps = float(m.group("rps"))
        size = int(record_size)
        result["raw_summary_line"] = line.strip()
        result["messages"] = n
        result["bytes"] = n * size
        result["throughput_msgs_per_s"] = rps
        result["throughput_mb_per_s"] = rps * size / 1e6
        result["raw"] = {"records_per_s": rps, "mib_per_s": float(m.group("mib")),
                         "avg_ms": float(m.group("avg")), "max_ms": float(m.group("max")),
                         "p50_ms": int(m.group("p50")), "p95_ms": int(m.group("p95")),
                         "p99_ms": int(m.group("p99")), "p99_9_ms": int(m.group("p999")),
                         "steady_state": bool(m.group("steady"))}
        result["latency_kind"] = "send_to_ack"
        # The tool reports percentiles as whole milliseconds, so these have 1000 us resolution.
        result["latency_us"] = {"avg": float(m.group("avg")) * 1000, "max": float(m.group("max")) * 1000,
                                "p50": int(m.group("p50")) * 1000, "p95": int(m.group("p95")) * 1000,
                                "p99": int(m.group("p99")) * 1000, "p99_9": int(m.group("p999")) * 1000,
                                "resolution_us": 1000}
        expected = int(num_records)
        if rc == 0 and n == (expected - int(warmup_records or 0) if steady else expected):
            result["status"] = "ok"
        else:
            result["error"] = "exit code %d, %d records reported" % (rc, n)
    else:
        result["error"] = "no summary line found (exit code %d)" % rc
else:
    params.update({"messages": int(messages), "consumer_props": props(consumer_props),
                   "timeout_ms": int(timeout_ms) if timeout_ms else 10000})
    header_idx = next((i for i, l in enumerate(lines) if l.startswith("start.time,")), None)
    if header_idx is not None and header_idx + 1 < len(lines):
        header = [h.strip() for h in lines[header_idx].split(",")]
        values = [v.strip() for v in lines[header_idx + 1].split(",")]
        row = dict(zip(header, values))
        n = int(row["data.consumed.in.nMsg"])
        mib = float(row["data.consumed.in.MB"])
        fetch_ms = float(row["fetch.time.ms"])
        result["raw_summary_line"] = lines[header_idx + 1].strip()
        result["raw"] = {"header": lines[header_idx].strip(),
                         **{k: (v if k.endswith(".time") else num(v)) for k, v in row.items()}}
        result["messages"] = n
        # data.consumed.in.MB is rounded to 4 decimals of MiB, so bytes can be off by up to ~50 B.
        result["bytes"] = round(mib * MIB)
        # fetch.* excludes the group join (rebalance.time.ms), which matches the harness window
        # (first to last receipt) better than the total elapsed time.
        if fetch_ms > 0:
            result["throughput_msgs_per_s"] = n / (fetch_ms / 1000.0)
            result["throughput_mb_per_s"] = mib * MIB / 1e6 / (fetch_ms / 1000.0)
        result["duration_s"] = fetch_ms / 1000.0
        result["latency_kind"] = None
        if rc == 0 and n >= int(messages):
            result["status"] = "ok"
        else:
            result["error"] = "exit code %d, %d records consumed of %s" % (rc, n, messages)
    else:
        result["error"] = "no summary line found (exit code %d)" % rc

print("RESULT " + json.dumps(result, separators=(",", ":")))
sys.exit(0 if result["status"] == "ok" else 1)
PY
