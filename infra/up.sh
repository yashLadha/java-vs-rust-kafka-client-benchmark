#!/usr/bin/env bash
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
timeout_s=120

docker compose -p kbench -f "$here/docker-compose.yml" up -d >&2

deadline=$(( $(date +%s) + timeout_s ))
while true; do
  status="$(docker inspect -f '{{.State.Health.Status}}' kbench-kafka 2>/dev/null || echo missing)"
  if [[ "$status" == "healthy" ]]; then
    break
  fi
  if (( $(date +%s) >= deadline )); then
    echo "kbench-kafka not healthy after ${timeout_s}s (status: $status)" >&2
    docker logs --tail 50 kbench-kafka >&2 || true
    exit 1
  fi
  sleep 1
done
echo READY
