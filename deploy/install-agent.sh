#!/usr/bin/env bash
#
# Install the node agent as a systemd unit.
#
#   sudo ORCH_SERVER=orch.local:9443 ORCH_JOIN_TOKEN=<token> ./install-agent.sh
#
# Mint the token first with: orchctl join-token --pool cad
#
# Everything else the control plane needs about this machine, the agent
# discovers for itself. Nothing about the hardware is typed here.

set -euo pipefail

SERVER="${ORCH_SERVER:-}"
TOKEN="${ORCH_JOIN_TOKEN:-}"
LABELS="${ORCH_LABELS:-}"
BINARY="${ORCH_BINARY:-./orchd-agent}"

if [[ $EUID -ne 0 ]]; then
  echo "run as root" >&2
  exit 1
fi

if [[ -z "$SERVER" ]]; then
  echo "set ORCH_SERVER, e.g. orch.local:9443" >&2
  exit 1
fi

if [[ ! -x "$BINARY" ]]; then
  echo "agent binary not found at $BINARY (set ORCH_BINARY)" >&2
  exit 1
fi

install -m 0755 "$BINARY" /usr/local/bin/orchd-agent
install -d -m 0755 /etc/orch /var/lib/orch/cache

# The join token is a credential, so it lands in a root-only file rather than in
# the unit or on a command line where `ps` would show it.
umask 077
cat > /etc/orch/agent.env <<EOF
ORCH_SERVER=$SERVER
ORCH_JOIN_TOKEN=$TOKEN
ORCH_LABELS=$LABELS
EOF
chmod 0600 /etc/orch/agent.env

install -m 0644 "$(dirname "$0")/orchd-agent.service" \
  /etc/systemd/system/orchd-agent.service

systemctl daemon-reload
systemctl enable --now orchd-agent

echo
echo "agent installed and started."
echo "  logs:   journalctl -u orchd-agent -f"
echo "  status: systemctl status orchd-agent"
echo
echo "The join token is single-use and has now been spent. On reconnect the"
echo "agent identifies itself with the node ID the control plane assigned."
