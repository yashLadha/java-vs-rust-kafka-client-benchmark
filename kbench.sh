#!/usr/bin/env bash
# Local launcher. Uses only docker commands against the active context (remote daemon);
# everything else runs inside the kbench-runner container on that host.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
runner_image="kbench-runner:latest"
runner_name="kbench-runner"
volume="kbench-results"
network="kbench-net"
runner_cpuset="7,23"
runner_mem="2g"
sock="/var/run/docker.sock:/var/run/docker.sock"

usage() {
  cat >&2 <<USAGE
usage: ./kbench.sh <command> [args]

  build [java|rust|runner ...]   build images (default: all three)
  run [run.py args...]           start the benchmark detached in container $runner_name
                                 (e.g. ./kbench.sh run --reps 3; ./kbench.sh run --dry-run)
  logs                           follow the runner output
  status                         runner state, live client containers, last log lines
  stop                           stop the runner and kill leftover kbench-client-* containers
  report <dir>                   (re)generate report.md, summary.csv, charts for /results/<dir>
  fetch <dir> [local_dest]       copy /results/<dir> to local_dest (default ./results/<dir>)
  ls                             list result dirs in volume $volume
USAGE
  exit 2
}

runner_state() {
  local s
  s="$(docker inspect -f '{{.State.Status}}' "$runner_name" 2>/dev/null)" || s=missing
  echo "${s:-missing}"
}

ensure_volume() {
  docker volume inspect "$volume" >/dev/null 2>&1 || docker volume create "$volume" >/dev/null
}

ensure_image() {
  docker image inspect "$runner_image" >/dev/null 2>&1 || { echo "$runner_image missing: run ./kbench.sh build runner" >&2; exit 1; }
}

check_dir() {
  [[ -n "${1:-}" && "$1" != */* && "$1" != .* ]] || { echo "expected a result dir name as shown by ./kbench.sh ls, got '${1:-}'" >&2; exit 2; }
}

cmd_build() {
  local targets=("$@")
  [[ ${#targets[@]} -gt 0 ]] || targets=(java rust runner)
  for t in "${targets[@]}"; do
    case "$t" in
      java) docker build -t kbench-java:latest "$here/java-bench" ;;
      rust) docker build -t kbench-rust:latest "$here/rust-bench" ;;
      runner) docker build -t "$runner_image" -f "$here/runner/Dockerfile" "$here" ;;
      *) echo "unknown build target: $t" >&2; exit 2 ;;
    esac
  done
}

cmd_run() {
  ensure_image
  ensure_volume
  local state
  state="$(runner_state)"
  if [[ "$state" == "running" || "$state" == "restarting" || "$state" == "paused" ]]; then
    echo "$runner_name is already $state; use ./kbench.sh logs, or ./kbench.sh stop first" >&2
    exit 1
  fi
  if [[ "$state" != "missing" ]]; then
    docker rm "$runner_name" >/dev/null
  fi
  # The runner joins kbench-net, which compose creates with the broker. If it does not exist
  # yet, bring the broker up from a throwaway runner container first (same cpuset).
  if ! docker network inspect "$network" >/dev/null 2>&1; then
    echo "$network missing: starting the broker via infra/up.sh inside a runner container" >&2
    docker run --rm --name kbench-runner-up --cpuset-cpus "$runner_cpuset" --memory "$runner_mem" \
      -v "$sock" "$runner_image" bash infra/up.sh
  fi
  docker run -d --name "$runner_name" --cpuset-cpus "$runner_cpuset" --memory "$runner_mem" \
    --network "$network" -v "$sock" -v "$volume:/results" \
    "$runner_image" python3 bench/run.py "$@" >/dev/null
  echo "started $runner_name: python3 bench/run.py $*"
  echo "follow:  ./kbench.sh logs      (or docker logs -f $runner_name)"
  echo "status:  ./kbench.sh status"
  echo "results: ./kbench.sh ls, then ./kbench.sh fetch <dir>"
}

cmd_logs() {
  docker logs -f "$runner_name"
}

cmd_status() {
  echo "runner:"
  docker ps -a --filter "name=^/${runner_name}\$" --format '  {{.Names}}  {{.Status}}  {{.Image}}'
  [[ "$(runner_state)" != "missing" ]] || echo "  (no $runner_name container)"
  echo "client containers:"
  docker ps --filter "name=^/kbench-client-" --format '  {{.Names}}  {{.Status}}'
  if [[ "$(runner_state)" != "missing" ]]; then
    echo "last log lines:"
    docker logs --tail 5 "$runner_name" 2>&1
  fi
}

cmd_stop() {
  if [[ "$(runner_state)" == "running" ]]; then
    # SIGTERM makes run.py kill its live client containers and delete its topics.
    docker stop -t 90 "$runner_name" >/dev/null
    echo "stopped $runner_name"
  else
    echo "$runner_name is not running"
  fi
  # run.py labels every client container it starts, so containers of other kbench users
  # (smoke tests, infra checks) are never touched.
  local left filter=(--filter "label=kbench.launcher=kbench-runner" --filter "name=^/kbench-client-")
  left="$(docker ps -q "${filter[@]}")"
  if [[ -n "$left" ]]; then
    echo "killing leftover client containers:"
    docker ps "${filter[@]}" --format '  {{.Names}}'
    # shellcheck disable=SC2086
    docker kill $left >/dev/null || true
  fi
}

cmd_report() {
  check_dir "${1:-}"
  ensure_image
  docker run --rm --name "kbench-report-$$" --cpuset-cpus "$runner_cpuset" --memory "$runner_mem" \
    -v "$volume:/results" "$runner_image" python3 bench/report.py "/results/$1"
}

cmd_fetch() {
  check_dir "${1:-}"
  ensure_image
  local dir="$1" dest="${2:-$here/results/$1}"
  fetch_helper="kbench-fetch-$$"
  if [[ -e "$dest" ]]; then
    echo "$dest already exists; pass another local_dest or remove it" >&2
    exit 1
  fi
  mkdir -p "$(dirname "$dest")"
  docker run -d --rm --name "$fetch_helper" --cpuset-cpus "$runner_cpuset" --memory 256m \
    -v "$volume:/results:ro" "$runner_image" sleep 900 >/dev/null
  trap 'docker stop -t 0 "$fetch_helper" >/dev/null 2>&1 || true' EXIT
  docker exec "$fetch_helper" test -d "/results/$dir" || { echo "no /results/$dir in $volume" >&2; exit 1; }
  docker cp "$fetch_helper:/results/$dir" "$dest"
  echo "copied /results/$dir to $dest"
}

cmd_ls() {
  ensure_image
  ensure_volume
  docker run --rm --name "kbench-ls-$$" --cpuset-cpus "$runner_cpuset" --memory 256m \
    -v "$volume:/results:ro" "$runner_image" sh -c '
cd /results
printf "%-32s %8s %8s %6s %s\n" DIR RECORDS SIZE REPORT MODIFIED
for d in */; do
  [ -d "$d" ] || continue
  d=${d%/}
  n=$( [ -f "$d/raw.jsonl" ] && wc -l < "$d/raw.jsonl" || echo 0)
  r=$( [ -f "$d/report.md" ] && echo yes || echo no)
  printf "%-32s %8s %8s %6s %s\n" "$d" "$n" "$(du -sh "$d" | cut -f1)" "$r" "$(date -r "$d" "+%Y-%m-%d %H:%M")"
done'
}

cmd="${1:-}"
[[ -n "$cmd" ]] || usage
shift
case "$cmd" in
  build) cmd_build "$@" ;;
  run) cmd_run "$@" ;;
  logs) cmd_logs ;;
  status) cmd_status ;;
  stop) cmd_stop ;;
  report) cmd_report "$@" ;;
  fetch) cmd_fetch "$@" ;;
  ls) cmd_ls ;;
  *) usage ;;
esac
