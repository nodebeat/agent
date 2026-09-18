#!/usr/bin/env bash
# One-command NodeBeat agent install (systemd hosts).
#
#   sudo packaging/install.sh --bin ./bin/nodebeat-agent \
#     --target 127.0.0.1 \
#     --remote-write-url https://ingest.example:8428/api/v1/write \
#     [--instance NAME]
#
# Installs: binary -> /usr/local/bin, unit -> /etc/systemd/system,
# env -> /etc/nodebeat/agent.env (0640), user `nodebeat`.
# Alloy + ethereum-metrics-exporter must already be on PATH for that user
# (same directory as the agent binary, or /usr/local/bin). Re-runnable.
set -euo pipefail

BIN=""; TARGET=""; RW_URL=""; INSTANCE=""
while [ $# -gt 0 ]; do
  case "$1" in
    --bin) BIN="$2"; shift 2 ;;
    --target) TARGET="$2"; shift 2 ;;
    --remote-write-url) RW_URL="$2"; shift 2 ;;
    --instance) INSTANCE="$2"; shift 2 ;;
    -h|--help) sed -n '2,/^$/p' "$0"; exit 0 ;;
    *) echo "unknown flag: $1 (see --help)" >&2; exit 2 ;;
  esac
done

[ "$(id -u)" = 0 ] || { echo "run as root" >&2; exit 1; }
[ -n "$BIN" ] && [ -f "$BIN" ] || { echo "--bin <agent binary> is required" >&2; exit 2; }
[ -n "$TARGET" ] || { echo "--target is required" >&2; exit 2; }
[ -n "$RW_URL" ] || { echo "--remote-write-url is required" >&2; exit 2; }
command -v systemctl >/dev/null || { echo "systemd not found" >&2; exit 1; }

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
EOF
chown root:nodebeat /etc/nodebeat/agent.env
chmod 0640 /etc/nodebeat/agent.env

systemctl daemon-reload
systemctl enable --now nodebeat-agent
sleep 3
systemctl --no-pager --lines=5 status nodebeat-agent || true
echo
echo "installed. Data manifest: /var/lib/nodebeat/manifest.json"
echo "(also served at http://127.0.0.1:19090/manifest on the host)"
