#!/usr/bin/env bash
#
# Bring up a local control plane and a simulated fleet.
#
# Uses whatever Postgres is already running rather than a container, so this
# works on a machine without Docker. Point ORCH_DSN somewhere else to override.
#
#   scripts/dev-up.sh      start
#   scripts/dev-down.sh    stop

set -euo pipefail

cd "$(dirname "$0")/.."

DSN="${ORCH_DSN:-postgres:///orch?host=/var/run/postgresql}"
LISTEN="${ORCH_LISTEN:-:9443}"
RUN_DIR="${ORCH_RUN_DIR:-.run}"
NODES="${ORCH_NODES:-cad-01 cad-02 render-01}"

mkdir -p "$RUN_DIR"

if [[ ! -x bin/orchd ]]; then
  echo "binaries missing; run: make build"
  exit 1
fi

echo "starting control plane on $LISTEN"
ORCH_DSN="$DSN" bin/orchd --listen "$LISTEN" --log-level info \
  > "$RUN_DIR/orchd.log" 2>&1 &
echo $! > "$RUN_DIR/orchd.pid"

# The agents connect immediately, so give the listener a moment rather than
# making every agent burn its first reconnect backoff.
sleep 2

for n in $NODES; do
  echo "starting simulated node $n"
  bin/orchd-agent \
    --server "http://localhost${LISTEN}" \
    --simulated --profile "deploy/profiles/$n.yaml" \
    --cache-dir "$RUN_DIR/cache-$n" \
    --sim-task-min 5s --sim-task-max 12s \
    --log-level info > "$RUN_DIR/agent-$n.log" 2>&1 &
  echo $! > "$RUN_DIR/agent-$n.pid"
done

sleep 2
echo
echo "up. UI: http://localhost${LISTEN}"
echo "logs: $RUN_DIR/*.log"
bin/orchctl fleet
