#!/usr/bin/env bash
set -euo pipefail

bootstrap="kbench-kafka:9092"
timeout_s=60

usage() {
  echo "usage: $0 create <name> <partitions> | delete <name> | list | offsets <name>" >&2
  exit 2
}

# The broker container sets KAFKA_HEAP_OPTS=-Xms4g for the broker itself; admin tools
# started via exec would inherit it, so give them a small heap instead.
kt() {
  local tool="$1"
  shift
  docker exec -e KAFKA_HEAP_OPTS="-Xmx256m" kbench-kafka "/opt/kafka/bin/$tool" --bootstrap-server "$bootstrap" "$@"
}

list_topics() {
  kt kafka-topics.sh --list
}

topic_exists() {
  local topics
  topics="$(list_topics)"
  grep -Fxq -- "$1" <<<"$topics"
}

cmd="${1:-}"
[[ -n "$cmd" ]] || usage
shift

case "$cmd" in
  create)
    [[ $# -eq 2 ]] || usage
    name="$1"
    partitions="$2"
    kt kafka-topics.sh --create --topic "$name" --partitions "$partitions" --replication-factor 1 >&2
    deadline=$(( $(date +%s) + timeout_s ))
    while true; do
      desc="$(kt kafka-topics.sh --describe --topic "$name" 2>/dev/null || true)"
      total="$(grep -c 'Partition: ' <<<"$desc" || true)"
      led="$(grep 'Partition: ' <<<"$desc" | grep -Ec 'Leader: [0-9]+' || true)"
      if [[ "$total" -eq "$partitions" && "$led" -eq "$partitions" ]]; then
        break
      fi
      if (( $(date +%s) >= deadline )); then
        echo "topic $name: only $led/$partitions partitions have a leader after ${timeout_s}s" >&2
        exit 1
      fi
      sleep 0.5
    done
    echo "created $name partitions=$partitions" >&2
    ;;
  delete)
    [[ $# -eq 1 ]] || usage
    name="$1"
    kt kafka-topics.sh --delete --topic "$name" >&2
    deadline=$(( $(date +%s) + timeout_s ))
    while topic_exists "$name"; do
      if (( $(date +%s) >= deadline )); then
        echo "topic $name still listed after ${timeout_s}s" >&2
        exit 1
      fi
      sleep 0.5
    done
    echo "deleted $name" >&2
    ;;
  list)
    [[ $# -eq 0 ]] || usage
    list_topics
    ;;
  offsets)
    [[ $# -eq 1 ]] || usage
    name="$1"
    # --topic is a regex, so filter the topic:partition:offset lines on the exact name.
    kt kafka-get-offsets.sh --topic "$name" --time latest \
      | awk -F: -v t="$name" '
          { off = $NF; part = $(NF-1); topic = substr($0, 1, length($0) - length(part) - length(off) - 2) }
          topic == t { sum += off; n++ }
          END { if (n == 0) { print "no partitions found for topic " t > "/dev/stderr"; exit 1 } print sum }'
    ;;
  *)
    usage
    ;;
esac
