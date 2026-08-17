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
#
# GPU attachment (see README.md's "Real hardware" section): the default
# --gpu-mode is cdi, which needs a one-time node prerequisite this script
# does not attempt for you:
#
#   nvidia-ctk cdi generate --output=/etc/cdi/nvidia.yaml
#
# Set ORCH_GPU_MODE=runtime-binary below to fall back to invoking
# nvidia-container-runtime directly instead, on a node where that has not
# been done.

set -euo pipefail

SERVER="${ORCH_SERVER:-}"
TOKEN="${ORCH_JOIN_TOKEN:-}"
LABELS="${ORCH_LABELS:-}"
BINARY="${ORCH_BINARY:-./orchd-agent}"

# GPU / containerd. Defaults match orchd-agent's own flag defaults; set
# these to override a specific node without editing the binary invocation.
CONTAINERD_SOCKET="${ORCH_CONTAINERD_SOCKET:-/run/containerd/containerd.sock}"
CONTAINERD_NAMESPACE="${ORCH_CONTAINERD_NAMESPACE:-orch}"
GPU_MODE="${ORCH_GPU_MODE:-cdi}"
NVIDIA_CONTAINER_RUNTIME_BINARY="${ORCH_NVIDIA_CONTAINER_RUNTIME_BINARY:-/usr/bin/nvidia-container-runtime}"
DEV_SHM_MB="${ORCH_DEV_SHM_MB:-1024}"

# Object store (data plane). Left blank disables uploads entirely -- the
# node behaves exactly as it did before the data plane existed. When the
# control plane mints per-lease STS credentials these are only ever the
# fallback; when it does not, they are what every upload uses.
S3_ENDPOINT="${ORCH_S3_ENDPOINT:-}"
S3_BUCKET="${ORCH_S3_BUCKET:-}"
S3_REGION="${ORCH_S3_REGION:-}"
S3_ACCESS_KEY="${ORCH_S3_ACCESS_KEY:-}"
S3_SECRET_KEY="${ORCH_S3_SECRET_KEY:-}"
S3_USE_SSL="${ORCH_S3_USE_SSL:-false}"
S3_PATH_STYLE="${ORCH_S3_PATH_STYLE:-true}"

WORK_DIR="${ORCH_WORK_DIR:-/var/lib/orch/work}"

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
install -d -m 0755 /etc/orch /var/lib/orch/cache "$WORK_DIR"

# The join token and S3 secret are credentials, so they land in a root-only
# file rather than the unit or a command line where `ps` would show them.
umask 077
cat > /etc/orch/agent.env <<EOF
ORCH_SERVER=$SERVER
ORCH_JOIN_TOKEN=$TOKEN
ORCH_LABELS=$LABELS
ORCH_CACHE_DIR=/var/lib/orch/cache
ORCH_WORK_DIR=$WORK_DIR

ORCH_CONTAINERD_SOCKET=$CONTAINERD_SOCKET
ORCH_CONTAINERD_NAMESPACE=$CONTAINERD_NAMESPACE
ORCH_GPU_MODE=$GPU_MODE
ORCH_NVIDIA_CONTAINER_RUNTIME_BINARY=$NVIDIA_CONTAINER_RUNTIME_BINARY
ORCH_DEV_SHM_MB=$DEV_SHM_MB

ORCH_S3_ENDPOINT=$S3_ENDPOINT
ORCH_S3_BUCKET=$S3_BUCKET
ORCH_S3_REGION=$S3_REGION
ORCH_S3_ACCESS_KEY=$S3_ACCESS_KEY
ORCH_S3_SECRET_KEY=$S3_SECRET_KEY
ORCH_S3_USE_SSL=$S3_USE_SSL
ORCH_S3_PATH_STYLE=$S3_PATH_STYLE
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
