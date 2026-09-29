#!/usr/bin/env bash
# One-command NodeBeat agent install (systemd hosts).
#
# End users run ONLY this script (plus the portal's Add Node first for SaaS).
# One invocation does everything, in order:
#   1. pre-flight via the sibling nodebeat-onboard (aborts on FAIL unless
#      --skip-checks; warns and continues when the binary is absent),
#   2. fetches the Alloy pinned in packaging/deps.lock for this CPU
#      (archive and binary sha256-checked; skipped when already installed),
#      takes the Cosmos watcher shipped next to --bin, and copies both to
#      /usr/local/lib/nodebeat/bin, the only place the unit may execute
#      children from,
#   3. installs the agent binary, unit and env file,
#   4. with --control-plane: activates the node (nodebeat-agent enroll) and
#      runs the unit in --enrolled mode.
#
# SaaS (portal Add Node shows the token once):
#   NODEBEAT_INGEST_TOKEN=... sudo -E packaging/install.sh --bin ./nodebeat-agent \
#     --target 127.0.0.1 --control-plane https://app.nodebeat.stream
# Standalone (any Prometheus remote-write endpoint, token optional):
#   sudo packaging/install.sh --bin ./nodebeat-agent --target 127.0.0.1 \
#     --remote-write-url https://ingest.example/api/v1/write [--instance NAME]
#
# Other flags: [--chain ethereum|cosmos] [--validators 12345,0xa1b2...]
#   [--alloy-bin PATH] [--watcher-bin PATH] [--skip-checks] [--dry-run]
#   [--el-rpc-port 8545 ...] (all detect port flags; devnet ephemeral ports)
#
# --dry-run prints every change (the pre-flight still runs; it is read-only)
# and changes nothing (no download either).
#
# --alloy-bin PATH: use this Alloy (>= 1.19.0) instead of downloading, e.g.
# on hosts without outbound access to github.com or with a distro package.
# --watcher-bin PATH: a cosmos-validator-watcher other than the shipped one.
#
# --validators: validators whose duties to alert on. Ethereum: indices or 0x
# pubkeys. Cosmos: consensus addresses, hex or bech32 ...valcons1.... Public
# chain identifiers only, written to agent.env as NB_VALIDATORS.
#
# The ingest token is stored once, in /var/lib/nodebeat/ingest-token (0600,
# user nodebeat); agent.env holds no secret. Pass it via
# NODEBEAT_INGEST_TOKEN with sudo -E; --ingest-token works but leaks into
# shell history and the process list.
#
# Installs: agent -> /usr/local/bin, children -> /usr/local/lib/nodebeat/bin,
# unit -> /etc/systemd/system, env -> /etc/nodebeat/agent.env (0640),
# state -> /var/lib/nodebeat, user `nodebeat`. Re-runnable.
set -euo pipefail

ALLOY_MIN=1.19.0 # oldest Alloy (--alloy-bin) this agent's generated config is tested with
STATE=/var/lib/nodebeat
LIB=/usr/local/lib/nodebeat/bin

BIN=""; TARGET=""; RW_URL=""; INSTANCE=""; CONTROL_PLANE=""; CHAIN=""; VALIDATORS=""
INGEST_TOKEN="${NODEBEAT_INGEST_TOKEN:-}"; INGEST_TOKEN_FROM_FLAG=0
ALLOY_BIN=""; WATCHER_BIN=""; SKIP_CHECKS=0; DRY_RUN=0
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
    --alloy-bin) ALLOY_BIN="$2"; shift 2 ;;
    --watcher-bin) WATCHER_BIN="$2"; shift 2 ;;
    --skip-checks) SKIP_CHECKS=1; shift ;;
    --dry-run) DRY_RUN=1; shift ;;
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
    -h|--help) sed -n '2,/^set -euo/p' "$0" | sed '$d; s/^# \{0,1\}//'; exit 0 ;;
    *) echo "unknown flag: $1 (see --help)" >&2; exit 2 ;;
  esac
done

die() { echo "install: $1" >&2; exit "${2:-2}"; }
# run prints a change and makes it unless --dry-run.
run() { echo "+ $*"; [ "$DRY_RUN" = 1 ] || "$@"; }
# write_file PATH MODE OWNER:GROUP CONTENT [redact]
write_file() {
  if [ "${5:-}" = redact ]; then echo "+ write $1 ($2 $3, secret redacted)"; else
    echo "+ write $1 ($2 $3):"; printf '%s\n' "$4" | sed 's/^/    /'; fi
  [ "$DRY_RUN" = 1 ] && return
  local tmp; tmp="$(mktemp "$(dirname "$1")/.nodebeat.XXXXXX")"
  printf '%s\n' "$4" > "$tmp"; chmod "$2" "$tmp"; chown "$3" "$tmp"; mv -f "$tmp" "$1"
}
# One token per value: everything below lands in an EnvironmentFile or argv.
single_word() { case "$2" in *[[:space:]]*) die "$1 must not contain whitespace" ;; esac; }

# --- 1. Validate ---------------------------------------------------------
[ "$DRY_RUN" = 1 ] || [ "$(id -u)" = 0 ] || die "run as root (or with --dry-run)" 1
[ -n "$BIN" ] && [ -f "$BIN" ] || die "--bin <agent binary> is required"
[ -n "$TARGET" ] || die "--target is required"
case "$CHAIN" in ""|ethereum|cosmos) ;; *) die "--chain must be ethereum or cosmos" ;; esac
if [ -n "$CONTROL_PLANE" ]; then
  ENROLLED=true
  [ -z "$RW_URL" ] && [ -z "$INSTANCE" ] ||
    die "--remote-write-url and --instance come from the portal with --control-plane; drop them"
  # Re-enrolling may reuse the stored token.
  [ -n "$INGEST_TOKEN" ] || [ -f "$STATE/ingest-token" ] ||
    die "--control-plane needs the node ingest token (NODEBEAT_INGEST_TOKEN env with sudo -E; shown once at Add Node)"
else
  ENROLLED=false
  [ -n "$RW_URL" ] || die "--remote-write-url is required (or --control-plane for SaaS)"
fi
single_word --target "$TARGET"; single_word --remote-write-url "$RW_URL"; single_word --instance "$INSTANCE"
single_word --control-plane "$CONTROL_PLANE"; single_word "the ingest token" "$INGEST_TOKEN"
for p in EL_RPC_PORT BEACON_PORT EL_METRICS_PORT CL_METRICS_PORT EL_P2P_PORT CL_P2P_PORT \
         COSMOS_RPC_PORT COSMOS_REST_PORT COSMOS_METRICS_PORT COSMOS_P2P_PORT; do
  case "${!p}" in ''|*[!0-9]*) die "--$(echo "$p" | tr '[:upper:]_' '[:lower:]-') must be a number" ;; esac
done
# Commas only, and only letters/digits (indices, hex, bech32). The agent
# validates each entry for its chain.
VALIDATORS="$(printf '%s' "$VALIDATORS" | tr -s ' \t\n' ',' | sed 's/^,//; s/,$//')"
case "$VALIDATORS" in *[!0-9a-zA-Z,]*) die "--validators: comma-separated indices, 0x pubkeys or consensus addresses only" ;; esac
[ "$INGEST_TOKEN_FROM_FLAG" = 0 ] ||
  echo "warning: --ingest-token exposes the secret in the process list and shell history; prefer NODEBEAT_INGEST_TOKEN env with sudo -E" >&2
command -v systemctl >/dev/null || die "systemd not found" 1

# Platform: picks the pinned Alloy build and catches an archive for the
# wrong CPU before anything is installed.
[ "$(uname -s)" = Linux ] || die "Linux only (found $(uname -s))" 1
case "$(uname -m)" in
  x86_64|amd64) ARCH=amd64 ;;
  aarch64|arm64) ARCH=arm64 ;;
  *) die "unsupported CPU architecture $(uname -m) (amd64 and arm64 only)" 1 ;;
esac
"$BIN" version >/dev/null 2>&1 || die "$BIN does not run on this host: use the linux_$ARCH release archive" 1
echo "platform linux/$ARCH"

HERE="$(cd "$(dirname "$0")" && pwd)"
case "$BIN" in
  */*) BINDIR="$(cd "$(dirname "$BIN")" && pwd)" ;;
  *) BINDIR="$(dirname "$(command -v "$BIN")")" ;;
esac
sha256_of() { sha256sum "$1" | cut -d' ' -f1; }

# Alloy (the generated config and the unit's exec allowlist assume a known
# Alloy): --alloy-bin wins; otherwise the version deps.lock pins for this
# CPU, downloaded in step 3 unless $LIB already holds that exact binary.
ALLOY_FETCH=0
if [ -n "$ALLOY_BIN" ]; then
  [ -x "$ALLOY_BIN" ] || die "--alloy-bin $ALLOY_BIN is not executable" 1
  ALLOY_VERSION="$("$ALLOY_BIN" --version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1 || true)"
  [ -n "$ALLOY_VERSION" ] || die "cannot read the version of $ALLOY_BIN" 1
  [ "$(printf '%s\n%s\n' "$ALLOY_MIN" "$ALLOY_VERSION" | sort -V | head -1)" = "$ALLOY_MIN" ] ||
    die "alloy $ALLOY_VERSION is older than the supported minimum $ALLOY_MIN" 1
  echo "alloy $ALLOY_VERSION ($ALLOY_BIN)"
else
  [ -f "$HERE/deps.lock" ] || die "$HERE/deps.lock missing (run install.sh from the release archive, or pass --alloy-bin)" 1
  read -r _ ALLOY_PIN _ ALLOY_ZIP_SHA ALLOY_BIN_SHA ALLOY_MEMBER ALLOY_URL \
    < <(awk -v a="$ARCH" '$1 == "alloy" && $3 == a' "$HERE/deps.lock") || true
  [ -n "${ALLOY_URL:-}" ] || die "deps.lock pins no alloy for $ARCH (pass --alloy-bin)" 1
  if [ -x "$LIB/alloy" ] && [ "$(sha256_of "$LIB/alloy")" = "$ALLOY_BIN_SHA" ]; then
    ALLOY_BIN="$LIB/alloy"
    echo "alloy $ALLOY_PIN (already installed, sha256 verified)"
  else
    ALLOY_FETCH=1
    command -v curl >/dev/null || die "curl is needed to download alloy (or pass --alloy-bin)" 1
    command -v unzip >/dev/null || command -v python3 >/dev/null ||
      die "unzip or python3 is needed to unpack alloy (or pass --alloy-bin)" 1
    echo "alloy $ALLOY_PIN (to download: $ALLOY_URL)"
  fi
fi

# Cosmos watcher: --watcher-bin, else the one shipped next to the agent in
# the release archive, else the copy a previous install made.
if [ -n "$WATCHER_BIN" ]; then
  [ -x "$WATCHER_BIN" ] || die "--watcher-bin $WATCHER_BIN is not executable" 1
elif [ -x "$BINDIR/cosmos-validator-watcher" ]; then
  WATCHER_BIN="$BINDIR/cosmos-validator-watcher"
elif [ -x "$LIB/cosmos-validator-watcher" ]; then
  WATCHER_BIN="$LIB/cosmos-validator-watcher"
fi
if [ -z "$WATCHER_BIN" ]; then
  [ "$CHAIN" != cosmos ] || die "cosmos-validator-watcher not found next to $BIN (required for --chain cosmos; pass --watcher-bin)" 1
  [ -n "$CHAIN" ] || echo "note: cosmos-validator-watcher not found (only needed for Cosmos nodes)" >&2
fi

PORT_FLAGS=(--el-rpc-port "$EL_RPC_PORT" --beacon-port "$BEACON_PORT" --el-metrics-port "$EL_METRICS_PORT"
  --cl-metrics-port "$CL_METRICS_PORT" --el-p2p-port "$EL_P2P_PORT" --cl-p2p-port "$CL_P2P_PORT"
  --cosmos-rpc-port "$COSMOS_RPC_PORT" --cosmos-rest-port "$COSMOS_REST_PORT"
  --cosmos-metrics-port "$COSMOS_METRICS_PORT" --cosmos-p2p-port "$COSMOS_P2P_PORT")
CHAIN_FLAGS=(); [ -z "$CHAIN" ] || CHAIN_FLAGS=(--chain "$CHAIN")

# --- 2. Pre-flight (read-only; runs in --dry-run too) ---------------------
if [ "$SKIP_CHECKS" = 1 ]; then
  echo "pre-flight skipped (--skip-checks)"
elif [ -x "$BINDIR/nodebeat-onboard" ]; then
  echo "pre-flight: $BINDIR/nodebeat-onboard --target $TARGET ${CHAIN_FLAGS[*]} ${PORT_FLAGS[*]}"
  "$BINDIR/nodebeat-onboard" --target "$TARGET" "${CHAIN_FLAGS[@]}" "${PORT_FLAGS[@]}" --non-interactive
  echo "pre-flight passed"
else
  echo "warning: no nodebeat-onboard next to $BIN; skipping pre-flight" >&2
fi

# --- 3. Fetch Alloy ---------------------------------------------------------
# Unpacked on disk, not in /tmp (often a small tmpfs): the binary is ~550 MB.
if [ "$ALLOY_FETCH" = 1 ] && [ "$DRY_RUN" = 1 ]; then
  echo "+ download $ALLOY_URL (archive sha256 $ALLOY_ZIP_SHA, binary sha256 $ALLOY_BIN_SHA)"
  ALLOY_BIN="<downloaded alloy>"
elif [ "$ALLOY_FETCH" = 1 ]; then
  FETCH_DIR="$(mktemp -d /var/tmp/nodebeat-alloy.XXXXXX)"
  trap 'rm -rf "$FETCH_DIR"' EXIT
  echo "downloading alloy $ALLOY_PIN for $ARCH (~150 MB)"
  for i in 1 2 3 4 5; do
    # -C -: a dropped connection resumes instead of starting over.
    curl -fL --proto '=https' --tlsv1.2 --connect-timeout 15 -sS -C - \
      -o "$FETCH_DIR/alloy.zip" "$ALLOY_URL" && break
    [ "$i" -lt 5 ] || die "downloading $ALLOY_URL failed (needs outbound HTTPS to github.com; offline hosts: --alloy-bin)" 1
    echo "download interrupted, retrying ($i/5)" >&2; sleep 2
  done
  GOT="$(sha256_of "$FETCH_DIR/alloy.zip")"
  [ "$GOT" = "$ALLOY_ZIP_SHA" ] || die "alloy archive sha256 $GOT does not match deps.lock ($ALLOY_ZIP_SHA): refusing to install" 1
  if command -v unzip >/dev/null; then
    unzip -p "$FETCH_DIR/alloy.zip" "$ALLOY_MEMBER" > "$FETCH_DIR/alloy"
  else
    python3 -c 'import shutil, sys, zipfile
with zipfile.ZipFile(sys.argv[1]) as z, z.open(sys.argv[2]) as src, open(sys.argv[3], "wb") as dst:
    shutil.copyfileobj(src, dst)' "$FETCH_DIR/alloy.zip" "$ALLOY_MEMBER" "$FETCH_DIR/alloy"
  fi
  rm -f "$FETCH_DIR/alloy.zip"
  GOT="$(sha256_of "$FETCH_DIR/alloy")"
  [ "$GOT" = "$ALLOY_BIN_SHA" ] || die "alloy binary sha256 $GOT does not match deps.lock ($ALLOY_BIN_SHA): refusing to install" 1
  chmod 0755 "$FETCH_DIR/alloy"
  ALLOY_BIN="$FETCH_DIR/alloy"
  echo "alloy $ALLOY_PIN downloaded, sha256 verified"
fi

# --- 4. Install -----------------------------------------------------------
id nodebeat >/dev/null 2>&1 || run useradd --system --no-create-home --shell /usr/sbin/nologin nodebeat
run install -m 0755 "$BIN" /usr/local/bin/nodebeat-agent
run install -d -m 0755 "$LIB"
[ "$ALLOY_BIN" -ef "$LIB/alloy" ] || run install -m 0755 "$ALLOY_BIN" "$LIB/alloy"
[ -z "$WATCHER_BIN" ] || [ "$WATCHER_BIN" -ef "$LIB/cosmos-validator-watcher" ] ||
  run install -m 0755 "$WATCHER_BIN" "$LIB/cosmos-validator-watcher"
run install -m 0644 "$HERE/systemd/nodebeat-agent.service" /etc/systemd/system/nodebeat-agent.service
# Earlier versions switched modes with a drop-in; the unit now reads NB_ENROLLED.
[ ! -e /etc/systemd/system/nodebeat-agent.service.d ] || run rm -rf /etc/systemd/system/nodebeat-agent.service.d
run install -d -m 0750 -o root -g nodebeat /etc/nodebeat
run install -d -m 0750 -o nodebeat -g nodebeat "$STATE"
write_file /etc/nodebeat/agent.env 0640 root:nodebeat "# Written by packaging/install.sh. No secrets: the token is in $STATE/ingest-token.
NB_ENROLLED=$ENROLLED
NB_TARGET=$TARGET
NB_REMOTE_WRITE_URL=$RW_URL
NB_INSTANCE=$INSTANCE
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
NB_COSMOS_P2P_PORT=$COSMOS_P2P_PORT"

# --- 5. Token + activation ------------------------------------------------
if [ "$ENROLLED" = true ]; then
  # enroll saves the token to $STATE/ingest-token and the node's parameters
  # (incl. the pinned remote-write URL) to $STATE/enrollment.json. The token
  # travels by exported env, never argv.
  # Without a new token, enroll reuses the stored one.
  echo "+ nodebeat-agent enroll --control-plane $CONTROL_PLANE --target $TARGET --state-dir $STATE ${CHAIN_FLAGS[*]} ${PORT_FLAGS[*]} (token via env)"
  if [ "$DRY_RUN" = 0 ]; then
    NODEBEAT_INGEST_TOKEN="$INGEST_TOKEN" sudo --preserve-env=NODEBEAT_INGEST_TOKEN -u nodebeat \
      /usr/local/bin/nodebeat-agent enroll --control-plane "$CONTROL_PLANE" \
      --target "$TARGET" --state-dir "$STATE" "${CHAIN_FLAGS[@]}" "${PORT_FLAGS[@]}"
  fi
elif [ -n "$INGEST_TOKEN" ]; then
  write_file "$STATE/ingest-token" 0600 nodebeat:nodebeat "$INGEST_TOKEN" redact
else
  run rm -f "$STATE/ingest-token" "$STATE/enrollment.json"
fi

run systemctl daemon-reload
run systemctl enable nodebeat-agent
# restart (not start): re-running install must recycle a running unit.
run systemctl restart nodebeat-agent
if [ "$DRY_RUN" = 1 ]; then
  echo "dry run: nothing changed"
  exit 0
fi
sleep 3
systemctl --no-pager --lines=5 status nodebeat-agent || true
echo
echo "installed ($([ "$ENROLLED" = true ] && echo enrolled || echo standalone)). Data manifest: $STATE/manifest.json"
echo "(also served at http://127.0.0.1:19090/manifest on the host)"
