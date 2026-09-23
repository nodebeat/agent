#!/usr/bin/env bash
# One-command NodeBeat agent install (systemd hosts).
#
#   sudo packaging/install.sh --bin ./bin/nodebeat-agent \
#     --target 127.0.0.1 \
#     --remote-write-url https://ingest.example:8428/api/v1/write \
#     [--instance NAME] [--el-rpc-port 8545 ...]
#
# Port overrides (devnet ephemeral host ports; see --help for the full list).
# Unset = standard ports (metrics 0 = per detected client default).
# Every NB_*_PORT is always written to agent.env so the systemd unit never
# expands an empty flag value.
#
# Installs: binary -> /usr/local/bin, unit -> /etc/systemd/system,
# env -> /etc/nodebeat/agent.env (0640), user `nodebeat`.
# Alloy + ethereum-metrics-exporter must already be on PATH for that user
# (same directory as the agent binary, or /usr/local/bin). Re-runnable.
set -euo pipefail

BIN=""; TARGET=""; RW_URL=""; INSTANCE=""
EL_RPC_PORT=8545; BEACON_PORT=5052; EL_METRICS_PORT=0; CL_METRICS_PORT=0
EL_P2P_PORT=30303; CL_P2P_PORT=9000
COSMOS_RPC_PORT=26657; COSMOS_REST_PORT=1317; COSMOS_METRICS_PORT=0; COSMOS_P2P_PORT=26656
while [ $# -gt 0 ]; do
  case "$1" in
    --bin) BIN="$2"; shift 2 ;;
    --target) TARGET="$2"; shift 2 ;;
    --remote-write-url) RW_URL="$2"; shift 2 ;;
    --instance) INSTANCE="$2"; shift 2 ;;
    --el-rpc-port) EL_RPC_PORT="$2"; shift 2 ;;
    --beacon-port) BEACON_PORT="$2"; shift 2 ;;
    --el-metrics-port) EL_METRICS_PORT="$2"; shift 2 ;;
    --cl-metrics-port) CL_METRICS_PORT="$2"; shift 2 ;;
    --el-p2p-port) EL_P2P_PORT="$2"; shift 2 ;;
    --cl-p2p-port) CL_P2P_PORT="$2"; shift 2 ;;
    --cosmos-rpc-port) COSMOS_RPC_PORT="$2"; shift 2 ;;
    --cosmos-rest-port) COSMOS_REST_PORT="$2"; shift 2 ;;
    --cosmos-metrics-port) COSMOS_METRICS_PORT="$2"; shift 2 ;;
    --cosmos-p2p-port) COSMOS_P2P_PORT="$2"; shift 2 ;;
    -h|--help) sed -n '2,/^$/p' "$0"; exit 0 ;;
    *) echo "unknown flag: $1 (see --help)" >&2; exit 2 ;;
  esac
done

[ "$(id -u)" = 0 ] || { echo "run as root" >&2; exit 1; }
[ -n "$BIN" ] && [ -f "$BIN" ] || { echo "--bin <agent binary> is required" >&2; exit 2; }
[ -n "$TARGET" ] || { echo "--target is required" >&2; exit 2; }
[ -n "$RW_URL" ] || { echo "--remote-write-url is required" >&2; exit 2; }
command -v systemctl >/dev/null || { echo "systemd not found" >&2; exit 1; }

# The agent resolves these via PATH at startup; fail here with a clear
# message instead of a crash-looping unit later.
command -v alloy >/dev/null || { echo "alloy not on PATH (same dir as the agent binary, or /usr/local/bin)" >&2; exit 1; }
command -v ethereum-metrics-exporter >/dev/null || command -v cosmos-validator-watcher >/dev/null || {
  echo "no chain exporter on PATH (need ethereum-metrics-exporter and/or cosmos-validator-watcher)" >&2; exit 1; }

HERE="$(cd "$(dirname "$0")" && pwd)"

if ! id nodebeat >/dev/null 2>&1; then
  useradd --system --no-create-home --shell /usr/sbin/nologin nodebeat
  echo "created user nodebeat"
fi

install -m 0755 "$BIN" /usr/local/bin/nodebeat-agent
install -m 0644 "$HERE/systemd/nodebeat-agent.service" /etc/systemd/system/nodebeat-agent.service
install -d -m 0750 -o root -g nodebeat /etc/nodebeat
umask 027
cat > /etc/nodebeat/agent.env <<EOF
NB_TARGET=$TARGET
NB_REMOTE_WRITE_URL=$RW_URL
NB_INSTANCE=$INSTANCE
NB_EL_RPC_PORT=$EL_RPC_PORT
NB_BEACON_PORT=$BEACON_PORT
NB_EL_METRICS_PORT=$EL_METRICS_PORT
NB_CL_METRICS_PORT=$CL_METRICS_PORT
NB_EL_P2P_PORT=$EL_P2P_PORT
NB_CL_P2P_PORT=$CL_P2P_PORT
NB_COSMOS_RPC_PORT=$COSMOS_RPC_PORT
NB_COSMOS_REST_PORT=$COSMOS_REST_PORT
NB_COSMOS_METRICS_PORT=$COSMOS_METRICS_PORT
NB_COSMOS_P2P_PORT=$COSMOS_P2P_PORT
EOF
chown root:nodebeat /etc/nodebeat/agent.env
chmod 0640 /etc/nodebeat/agent.env

systemctl daemon-reload
# restart (not start): re-running install on new flags must recycle the
# already-running unit; on a fresh install restart simply starts it.
systemctl enable nodebeat-agent
systemctl restart nodebeat-agent
sleep 3
systemctl --no-pager --lines=5 status nodebeat-agent || true
echo
echo "installed. Data manifest: /var/lib/nodebeat/manifest.json"
echo "(also served at http://127.0.0.1:19090/manifest on the host)"
