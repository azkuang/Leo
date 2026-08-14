#!/usr/bin/env bash
#
# Stop everything scripts/dev-up.sh started.

set -uo pipefail
cd "$(dirname "$0")/.."

RUN_DIR="${ORCH_RUN_DIR:-.run}"

for f in "$RUN_DIR"/*.pid; do
  [[ -e "$f" ]] || continue
  pid=$(cat "$f")
  if kill "$pid" 2>/dev/null; then
    echo "stopped $(basename "$f" .pid) ($pid)"
  fi
  rm -f "$f"
done
