#!/usr/bin/env bash
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

docker compose -p kbench -f "$here/docker-compose.yml" down --timeout 30 >&2
