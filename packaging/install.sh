#!/usr/bin/env bash
# One-command NodeBeat agent install (systemd hosts).
#
# End users run ONLY this script (plus the portal's Add Node first for SaaS).
# One invocation does everything, in order:
#   1. pre-flight via the sibling nodebeat-onboard (aborts on FAIL unless
#      --skip-checks; warns and continues when the binary is absent),
#   2. install binary + unit + env,
#   3. activate via nodebeat-agent enroll when --control-plane is given
#      (needs the node ingest token; flips the portal node to 'active' and
#      switches the unit to --enrolled server-pipeline mode).
#
#   sudo packaging/install.sh --bin ./bin/nodebeat-agent \
#     --target 127.0.0.1 \
#     --remote-write-url https://ingest.example:8428/api/v1/write \
#     [--instance NAME] [--ingest-token TOKEN | NODEBEAT_INGEST_TOKEN=... sudo -E ...] \
#     [--control-plane https://app-dev.nodebeat.stream] [--chain ethereum] \
#     [--validators 12345,0xa1b2...] [--skip-checks] [--el-rpc-port 8545 ...]
#
# --validators (Ethereum): validator indices or 0x pubkeys whose duties to
# alert on (missed attestations/proposals, effectiveness, slashing). Public
# chain identifiers only, written to agent.env as NB_VALIDATORS. Omit to
# monitor the node without duty alerts.
#
# Token via flag is convenient but leaks into history/ps; prefer
# NODEBEAT_INGEST_TOKEN=... sudo -E packaging/install.sh ...
#
# Port overrides (devnet ephemeral host ports; same names everywhere).
# Unset = standard ports (metrics 0 = per detected client default).
# Every NB_*_PORT is always written to agent.env so the systemd unit never
# expands an empty flag value.
#
# Installs: binary -> /usr/local/bin, unit -> /etc/systemd/system,
# env -> /etc/nodebeat/agent.env (0640), user `nodebeat`.
# Alloy must already be on PATH for that user (same directory as the agent
# binary, or /usr/local/bin); Cosmos also needs cosmos-validator-watcher.
# Ethereum needs nothing else: the agent polls the node itself. Re-runnable.
set -euo pipefail

BIN=""; TARGET=""; RW_URL=""; INSTANCE=""; INGEST_TOKEN=""
# Prefer env (avoids the secret in shell history / process list at install):
#   NODEBEAT_INGEST_TOKEN=... sudo -E packaging/install.sh ...
# NODEBEAT_TOKEN is accepted as a legacy fallback (old portal/docs snippet).
[ -n "${NODEBEAT_INGEST_TOKEN:-}" ] && INGEST_TOKEN="$NODEBEAT_INGEST_TOKEN"
[ -z "$INGEST_TOKEN" ] && [ -n "${NODEBEAT_TOKEN:-}" ] && INGEST_TOKEN="$NODEBEAT_TOKEN"
CONTROL_PLANE=""; CHAIN=""; SKIP_CHECKS=0; VALIDATORS=""
EL_RPC_PORT=8545; BEACON_PORT=5052; EL_METRICS_PORT=0; CL_METRICS_PORT=0
EL_P2P_PORT=30303; CL_P2P_PORT=9000
COSMOS_RPC_PORT=26657; COSMOS_REST_PORT=1317; COSMOS_METRICS_PORT=0; COSMOS_P2P_PORT=26656
while [ $# -gt 0 ]; do
  case "$1" in
    --bin) BIN="$2"; shift 2 ;;
    --target) TARGET="$2"; shift 2 ;;
    --remote-write-url) RW_URL="$2"; shift 2 ;;
    --instance) INSTANCE="$2"; shift 2 ;;
    --ingest-token) INGEST_TOKEN="$2"; INGEST_TOKEN_FROM_FLAG=1; shift 2 ;;
    --control-plane) CONTROL_PLANE="$2"; shift 2 ;;
    --chain) CHAIN="$2"; shift 2 ;;
    --validators) VALIDATORS="$2"; shift 2 ;;
    --skip-checks) SKIP_CHECKS=1; shift ;;
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

# Commas only, and only index/pubkey characters: the value goes into an
# EnvironmentFile. The agent validates each entry at start.
VALIDATORS="$(printf '%s' "$VALIDATORS" | tr -s ' \t\n' ',' | sed 's/^,//; s/,$//')"
case "$VALIDATORS" in *[!0-9a-fA-Fx,]*) echo "--validators: comma-separated indices or 0x pubkeys only" >&2; exit 2 ;; esac

PORT_FLAGS="--el-rpc-port $EL_RPC_PORT --beacon-port $BEACON_PORT --el-metrics-port $EL_METRICS_PORT --cl-metrics-port $CL_METRICS_PORT --el-p2p-port $EL_P2P_PORT --cl-p2p-port $CL_P2P_PORT --cosmos-rpc-port $COSMOS_RPC_PORT --cosmos-rest-port $COSMOS_REST_PORT --cosmos-metrics-port $COSMOS_METRICS_PORT --cosmos-p2p-port $COSMOS_P2P_PORT"

[ "$(id -u)" = 0 ] || { echo "run as root" >&2; exit 1; }
if [ "${INGEST_TOKEN_FROM_FLAG:-0}" = 1 ]; then
  echo "warning: --ingest-token exposes the secret in the process list and shell history; prefer NODEBEAT_INGEST_TOKEN env with sudo -E" >&2
fi
[ -n "$BIN" ] && [ -f "$BIN" ] || { echo "--bin <agent binary> is required" >&2; exit 2; }
[ -n "$TARGET" ] || { echo "--target is required" >&2; exit 2; }
[ -n "$RW_URL" ] || { echo "--remote-write-url is required" >&2; exit 2; }
command -v systemctl >/dev/null || { echo "systemd not found" >&2; exit 1; }

# The agent resolves these via PATH at startup; fail here with a clear
# message instead of a crash-looping unit later.
command -v alloy >/dev/null || { echo "alloy not on PATH (same dir as the agent binary, or /usr/local/bin)" >&2; exit 1; }
if ! command -v cosmos-validator-watcher >/dev/null; then
  if [ "$CHAIN" = cosmos ]; then
    echo "cosmos-validator-watcher not on PATH (required for --chain cosmos)" >&2; exit 1
  fi
  [ -n "$CHAIN" ] || echo "note: cosmos-validator-watcher not on PATH (only needed for Cosmos nodes)" >&2
fi

# 1. Pre-flight via the sibling onboard binary (ships in the same tarball).
# Child command is echoed first so failures are attributable, not hidden.
case "$BIN" in
  */*) BINDIR="$(cd "$(dirname "$BIN")" && pwd)" ;;
  *) BINDIR="$(dirname "$(command -v "$BIN")")" ;;
esac
if [ "$SKIP_CHECKS" = 1 ]; then
  echo "pre-flight skipped (--skip-checks)"
elif [ -x "$BINDIR/nodebeat-onboard" ]; then
  # --chain picks the P2P port set (onboard default ethereum); empty = default.
  ONBOARD_CHAIN_FLAGS=""
  [ -n "$CHAIN" ] && ONBOARD_CHAIN_FLAGS="--chain $CHAIN"
  # shellcheck disable=SC2086
  echo "+ $BINDIR/nodebeat-onboard --target $TARGET $ONBOARD_CHAIN_FLAGS $PORT_FLAGS"
  # shellcheck disable=SC2086
  "$BINDIR/nodebeat-onboard" --target "$TARGET" $ONBOARD_CHAIN_FLAGS $PORT_FLAGS --non-interactive
  echo "pre-flight passed"
else
  echo "warning: no nodebeat-onboard next to $BIN; skipping pre-flight" >&2
fi

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
NB_INGEST_TOKEN=$INGEST_TOKEN
NB_CHAIN=$CHAIN
NB_VALIDATORS=$VALIDATORS
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

DROPIN=/etc/systemd/system/nodebeat-agent.service.d/enrolled.conf
if [ -n "$CONTROL_PLANE" ]; then
  # 3. Activate: upload detection, flip the portal node to 'active', save
  # enrollment.json. The unit then runs server-pipeline (--enrolled) mode.
  [ -n "$INGEST_TOKEN" ] || { echo "--control-plane needs the node ingest token (--ingest-token or NODEBEAT_INGEST_TOKEN env; shown once at Add Node)" >&2; exit 2; }
  install -d -m 0750 -o nodebeat -g nodebeat /var/lib/nodebeat
  # --chain enumerates detection to one family on mixed hosts (empty = auto).
  ENROLL_CHAIN_FLAGS=""
  [ -n "$CHAIN" ] && ENROLL_CHAIN_FLAGS="--chain $CHAIN"
  echo "+ nodebeat-agent enroll --control-plane $CONTROL_PLANE --target $TARGET --state-dir /var/lib/nodebeat (token redacted)"
  # shellcheck disable=SC2086
  # Token via exported env, not `env VAR=...` (that puts it in argv / ps).
  NODEBEAT_INGEST_TOKEN="$INGEST_TOKEN" sudo --preserve-env=NODEBEAT_INGEST_TOKEN -u nodebeat \
    /usr/local/bin/nodebeat-agent enroll --control-plane "$CONTROL_PLANE" \
    --target "$TARGET" --state-dir /var/lib/nodebeat $ENROLL_CHAIN_FLAGS $PORT_FLAGS
  install -d -m 0755 -o root -g root "$(dirname "$DROPIN")"
  cat > "$DROPIN" <<EOF
# Written by packaging/install.sh (enrolled mode). Removed on standalone installs.
[Service]
ExecStart=
ExecStart=/usr/local/bin/nodebeat-agent run --enrolled --state-dir /var/lib/nodebeat
EOF
  echo "enrolled mode (server pipeline)"
else
  rm -f "$DROPIN"
  rmdir --ignore-fail-on-non-empty "$(dirname "$DROPIN")" 2>/dev/null || true
fi

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
