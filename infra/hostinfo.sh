#!/usr/bin/env bash
set -euo pipefail

probe_image="debian:bookworm-slim"
curl_image="curlimages/curl:latest"
broker_image="apache/kafka:4.3.1"

tmp="$(mktemp -d "${TMPDIR:-/tmp}/kbench-hostinfo.XXXXXX")"
trap 'rm -rf "$tmp"' EXIT

# Each capture is best effort: a missing piece becomes null in the JSON instead of failing the report.
capture() {
  local name="$1"
  shift
  if ! "$@" >"$tmp/$name" 2>"$tmp/$name.err"; then
    echo "hostinfo: '$name' failed: $(head -c 300 "$tmp/$name.err")" >&2
    : >"$tmp/$name"
  fi
}

capture docker_version docker version --format '{{json .}}'
capture docker_info docker info --format '{{json .}}'
capture docker_context docker context show

# /proc and /sys are host-wide inside a plain container (no lxcfs on the host), so this reads host facts.
capture host docker run --rm --name "kbench-hostinfo-probe-$$" "$probe_image" sh -c '
sec() { echo "@@@ $1"; }
sec lscpu_json; lscpu -J
sec lscpu_caches; lscpu -C -J
sec lscpu_numa; lscpu -p=CPU,CORE,SOCKET,NODE | grep -v "^#"
sec cpuinfo_flags; grep -m1 "^flags" /proc/cpuinfo
sec cpuinfo_mhz; grep "^cpu MHz" /proc/cpuinfo
sec meminfo; cat /proc/meminfo
sec governor; cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null
sec scaling_driver; cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_driver 2>/dev/null
sec thp_enabled; cat /sys/kernel/mm/transparent_hugepage/enabled 2>/dev/null
sec thp_defrag; cat /sys/kernel/mm/transparent_hugepage/defrag 2>/dev/null
sec clocksource; cat /sys/devices/system/clocksource/clocksource0/current_clocksource 2>/dev/null
sec smt_active; cat /sys/devices/system/cpu/smt/active 2>/dev/null
sec uname; uname -a
sec loadavg; cat /proc/loadavg
sec uptime; cat /proc/uptime
sec vulnerabilities; grep -H . /sys/devices/system/cpu/vulnerabilities/* 2>/dev/null
'

capture imds docker run --rm --name "kbench-hostinfo-imds-$$" --network host --entrypoint sh "$curl_image" -c '
t=$(curl -sf -m 2 -X PUT http://169.254.169.254/latest/api/token -H "X-aws-ec2-metadata-token-ttl-seconds: 60") || exit 0
for k in instance-type placement/availability-zone placement/availability-zone-id placement/region ami-id instance-life-cycle; do
  v=$(curl -sf -m 2 -H "X-aws-ec2-metadata-token: $t" "http://169.254.169.254/latest/meta-data/$k") || v=""
  echo "$k=$v"
done
'

capture broker_inspect docker inspect kbench-kafka
capture broker_image_inspect docker image inspect "$broker_image"
capture stats docker stats --no-stream --format '{{json .}}'
capture broker_java_version docker exec kbench-kafka sh -c 'java -version 2>&1'
capture broker_cmdline docker exec kbench-kafka sh -c '
for p in /proc/[0-9]*; do
  if tr "\0" "\n" < "$p/cmdline" 2>/dev/null | grep -qx kafka.Kafka; then
    echo "pid=${p#/proc/}"; tr "\0" "\n" < "$p/cmdline"; exit 0
  fi
done
exit 1'
capture broker_id docker exec kbench-kafka sh -c 'sed -n "s/^node.id=//p" /opt/kafka/config/server.properties'
broker_id="$(tr -d '[:space:]' <"$tmp/broker_id")"
capture broker_config docker exec -e KAFKA_HEAP_OPTS="-Xmx256m" kbench-kafka /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server kbench-kafka:9092 --entity-type brokers --entity-name "${broker_id:-1}" --describe --all
capture broker_version docker exec -e KAFKA_HEAP_OPTS="-Xmx256m" kbench-kafka /opt/kafka/bin/kafka-topics.sh --version

# When this script runs inside a container (the kbench-runner), find our own container id so the
# daemon's view of our limits can be recorded next to what the cgroup files say. cgroup v1 paths
# carry the id; with a private cgroup v2 namespace only the docker-managed mounts in mountinfo do.
self_id="$( { grep -oE '[0-9a-f]{64}' /proc/self/cgroup 2>/dev/null \
  || grep -oE '/containers/[0-9a-f]{64}/' /proc/self/mountinfo 2>/dev/null | grep -oE '[0-9a-f]{64}'; } \
  | head -n 1 || true)"
if [[ -n "$self_id" ]]; then
  capture self_inspect docker inspect "$self_id"
fi

python3 - "$tmp" "$self_id" "$BASH_VERSION" <<'PY'
import json
import os
import re
import sys
import time

d, self_id, bash_version = sys.argv[1:4]


def raw(name):
    p = os.path.join(d, name)
    if not os.path.exists(p):
        return ""
    return open(p, encoding="utf-8", errors="replace").read()


def js(name):
    s = raw(name).strip()
    if not s:
        return None
    try:
        return json.loads(s)
    except ValueError:
        return None


def sections(text):
    out, cur = {}, None
    for line in text.splitlines():
        if line.startswith("@@@ "):
            cur = line[4:].strip()
            out[cur] = []
        elif cur is not None:
            out[cur].append(line)
    return {k: "\n".join(v).strip() for k, v in out.items()}


def or_none(s):
    return s if s else None


def kb_to_bytes(v):
    parts = v.split()
    return int(parts[0]) * 1024 if len(parts) > 1 and parts[1] == "kB" else int(parts[0])


host = sections(raw("host"))

lscpu = {}
try:
    for e in json.loads(host.get("lscpu_json", ""))["lscpu"]:
        lscpu[e["field"].rstrip(":")] = e["data"]
        for c in e.get("children", []) or []:
            lscpu[c["field"].rstrip(":")] = c["data"]
except (ValueError, KeyError):
    lscpu = None

caches = None
try:
    caches = json.loads(host.get("lscpu_caches", ""))["caches"]
except (ValueError, KeyError):
    pass

topology = None
if host.get("lscpu_numa"):
    rows = [r.split(",") for r in host["lscpu_numa"].splitlines() if r]
    siblings = {}
    for cpu, core, sock, node in rows:
        siblings.setdefault((sock, core), []).append(int(cpu))
    nodes = {}
    for cpu, core, sock, node in rows:
        nodes.setdefault(node, []).append(int(cpu))
    topology = {
        "numa_nodes": {n: sorted(c) for n, c in nodes.items()},
        "core_siblings": sorted(sorted(v) for v in siblings.values()),
    }

flags = host.get("cpuinfo_flags", "").split(":", 1)[-1].split()
interesting = ["avx", "avx2", "avx512f", "avx512bw", "avx512cd", "avx512dq", "avx512vl", "avx512_vnni",
               "avx512ifma", "avx512vbmi", "avx512_vbmi2", "avx512_bitalg", "avx512_vpopcntdq", "sse4_2",
               "aes", "vaes", "pclmulqdq", "vpclmulqdq", "sha_ni", "bmi1", "bmi2", "adx", "popcnt", "erms",
               "fsrm", "rdrand", "rdseed", "constant_tsc", "nonstop_tsc", "tsc_known_freq", "hypervisor",
               "ht"]
flag_subset = {f: (f in flags) for f in interesting}

mhz = [float(l.split(":")[1]) for l in host.get("cpuinfo_mhz", "").splitlines() if ":" in l]

meminfo = {}
for line in host.get("meminfo", "").splitlines():
    k, _, v = line.partition(":")
    try:
        meminfo[k.strip()] = kb_to_bytes(v.strip())
    except (ValueError, IndexError):
        pass

imds = None
if raw("imds").strip():
    imds = {}
    for line in raw("imds").splitlines():
        k, _, v = line.partition("=")
        imds[k] = v or None

dv = js("docker_version") or {}
di = js("docker_info") or {}

bi = js("broker_inspect")
bi = bi[0] if isinstance(bi, list) and bi else None
broker_container = None
if bi:
    hc = bi["HostConfig"]
    broker_container = {
        "id": bi["Id"],
        "image_id": bi["Image"],
        "image_ref": bi["Config"]["Image"],
        "started_at": bi["State"]["StartedAt"],
        "health": (bi["State"].get("Health") or {}).get("Status"),
        "cpuset_cpus": hc.get("CpusetCpus"),
        "cpuset_mems": hc.get("CpusetMems"),
        "nano_cpus": hc.get("NanoCpus"),
        "cpu_shares": hc.get("CpuShares"),
        "memory_bytes": hc.get("Memory"),
        "memory_swap_bytes": hc.get("MemorySwap"),
        "memory_reservation_bytes": hc.get("MemoryReservation"),
        "oom_kill_disable": hc.get("OomKillDisable"),
        "tmpfs": hc.get("Tmpfs"),
        "network_mode": hc.get("NetworkMode"),
        "env": [e for e in bi["Config"].get("Env", []) if e.startswith(("KAFKA_", "CLUSTER_ID"))],
    }

bii = js("broker_image_inspect")
bii = bii[0] if isinstance(bii, list) and bii else None
broker_image = None
if bii:
    broker_image = {"id": bii["Id"], "repo_digests": bii.get("RepoDigests"), "repo_tags": bii.get("RepoTags"),
                    "created": bii.get("Created")}

stats = []
for line in raw("stats").splitlines():
    try:
        stats.append(json.loads(line))
    except ValueError:
        pass

cmd = raw("broker_cmdline").splitlines()
jvm_process = None
if cmd and cmd[0].startswith("pid="):
    argv = cmd[1:]
    main_idx = argv.index("kafka.Kafka") if "kafka.Kafka" in argv else len(argv)
    jvm_args, skip = [], False
    for a in argv[1:main_idx]:
        if skip:
            skip = False
            continue
        if a in ("-cp", "-classpath"):
            skip = True
            continue
        jvm_args.append(a)
    jvm_process = {"pid": int(cmd[0][4:]), "executable": argv[0] if argv else None,
                   "jvm_args": jvm_args, "main_class": argv[main_idx] if main_idx < len(argv) else None,
                   "program_args": argv[main_idx + 1:]}

broker_config = None
broker_config_non_default = None
line_re = re.compile(r"^\s+(?P<k>[^=\s]+)=(?P<v>.*?) sensitive=(?P<s>true|false) synonyms=\{(?P<syn>.*)\}$")
cfg_text = raw("broker_config")
if cfg_text.strip():
    broker_config, broker_config_non_default = {}, {}
    for line in cfg_text.splitlines():
        m = line_re.match(line)
        if not m:
            continue
        v = m.group("v")
        v = None if v == "null" else v
        broker_config[m.group("k")] = v
        syn = m.group("syn")
        first = syn.split(",")[0].split(":")[0] if syn else ""
        if first and first != "DEFAULT_CONFIG":
            broker_config_non_default[m.group("k")] = {"value": v, "source": first}

def read_file(path):
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            return f.read().strip()
    except OSError:
        return None


def first_file(*paths):
    for p in paths:
        v = read_file(p)
        if v is not None:
            return {"path": p, "value": v}
    return None


def cpu_list(cpus):
    return ",".join(str(c) for c in sorted(cpus))


proc_cgroup = read_file("/proc/self/cgroup")
cgroup_lines = proc_cgroup.splitlines() if proc_cgroup else []
in_container_signals = [s for s, hit in [
    ("dockerenv", os.path.exists("/.dockerenv")),
    ("cgroup_path", any(re.search(r"/(docker|containerd|kubepods|libpod)", l) for l in cgroup_lines)),
    ("self_id", bool(self_id)),
] if hit]

si = js("self_inspect")
si = si[0] if isinstance(si, list) and si else None
affinity = cpu_list(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else None
runner = {
    "in_container": bool(in_container_signals),
    "detected_by": in_container_signals,
    "container_id": self_id or None,
    "hostname": os.uname().nodename,
    "python_version": sys.version.split()[0],
    "bash_version": bash_version,
    "proc_self_cgroup": cgroup_lines or None,
    # v1 path first (the benchmark host is cgroup v1); v2 exposes the effective set at the root.
    "cgroup_cpuset": first_file("/sys/fs/cgroup/cpuset/cpuset.cpus", "/sys/fs/cgroup/cpuset.cpus.effective"),
    "cgroup_memory_limit": first_file("/sys/fs/cgroup/memory/memory.limit_in_bytes", "/sys/fs/cgroup/memory.max"),
    "sched_affinity": affinity,
    "docker_inspect": {
        "name": si["Name"].lstrip("/"),
        "image_ref": si["Config"]["Image"],
        "cpuset_cpus": si["HostConfig"].get("CpusetCpus"),
        "memory_bytes": si["HostConfig"].get("Memory"),
        "network_mode": si["HostConfig"].get("NetworkMode"),
    } if si else None,
}

report = {
    "schema_version": 1,
    "collected_at_unix": time.time(),
    "docker": {
        "context": or_none(raw("docker_context").strip()),
        "server_version": (dv.get("Server") or {}).get("Version"),
        "server_api_version": (dv.get("Server") or {}).get("ApiVersion"),
        "client_version": (dv.get("Client") or {}).get("Version"),
        "storage_driver": di.get("Driver"),
        "cgroup_driver": di.get("CgroupDriver"),
        "cgroup_version": di.get("CgroupVersion"),
        "runtime": di.get("DefaultRuntime"),
        "ncpu": di.get("NCPU"),
        "mem_total_bytes": di.get("MemTotal"),
    },
    "os": {
        "operating_system": di.get("OperatingSystem"),
        "os_type": di.get("OSType"),
        "architecture": di.get("Architecture"),
        "kernel": di.get("KernelVersion"),
        "uname": or_none(host.get("uname")),
        "loadavg": or_none(host.get("loadavg")),
        "uptime_s": float(host["uptime"].split()[0]) if host.get("uptime") else None,
    },
    "cpu": {
        "model": (lscpu or {}).get("Model name"),
        "lscpu": lscpu,
        "sockets": (lscpu or {}).get("Socket(s)"),
        "cores_per_socket": (lscpu or {}).get("Core(s) per socket"),
        "threads_per_core": (lscpu or {}).get("Thread(s) per core"),
        "numa_node_count": (lscpu or {}).get("NUMA node(s)"),
        "caches": caches,
        "topology": topology,
        "flags_subset": flag_subset if flags else None,
        "flags_count": len(flags) if flags else None,
        "mhz_min": min(mhz) if mhz else None,
        "mhz_max": max(mhz) if mhz else None,
        "scaling_governor": or_none(host.get("governor")),
        "scaling_driver": or_none(host.get("scaling_driver")),
        "smt_active": or_none(host.get("smt_active")),
        "clocksource": or_none(host.get("clocksource")),
        "vulnerabilities": {l.split(":", 1)[0].rsplit("/", 1)[-1]: l.split(":", 1)[1]
                            for l in host.get("vulnerabilities", "").splitlines() if ":" in l} or None,
    },
    "memory": {
        "total_bytes": meminfo.get("MemTotal"),
        "available_bytes": meminfo.get("MemAvailable"),
        "free_bytes": meminfo.get("MemFree"),
        "cached_bytes": meminfo.get("Cached"),
        "swap_total_bytes": meminfo.get("SwapTotal"),
        "hugepages_total": meminfo.get("HugePages_Total"),
        "thp_enabled": or_none(host.get("thp_enabled")),
        "thp_defrag": or_none(host.get("thp_defrag")),
    },
    "ec2": {
        "instance_type": (imds or {}).get("instance-type"),
        "availability_zone": (imds or {}).get("placement/availability-zone"),
        "availability_zone_id": (imds or {}).get("placement/availability-zone-id"),
        "region": (imds or {}).get("placement/region"),
        "ami_id": (imds or {}).get("ami-id"),
        "instance_life_cycle": (imds or {}).get("instance-life-cycle"),
    } if imds else None,
    "broker": {
        "image": broker_image,
        "container": broker_container,
        "kafka_version": or_none(raw("broker_version").strip()),
        "java_version": or_none(raw("broker_java_version").strip()),
        "jvm_process": jvm_process,
        "config": broker_config,
        "config_non_default": broker_config_non_default,
    },
    "runner": runner,
    "containers_stats": stats,
}
print(json.dumps(report, separators=(",", ":")))
PY
