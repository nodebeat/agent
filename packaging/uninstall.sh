#!/usr/bin/env bash
# One-command clean uninstall. Removes the service, binary, unit, env file,
# state dir and the nodebeat user: nothing left.
#
#   sudo packaging/uninstall.sh
set -euo pipefail

[ "$(id -u)" = 0 ] || { echo "run as root" >&2; exit 1; }

systemctl disable --now nodebeat-agent 2>/dev/null || true
rm -f /etc/systemd/system/nodebeat-agent.service
systemctl daemon-reload
rm -f /usr/local/bin/nodebeat-agent
rm -rf /etc/nodebeat /var/lib/nodebeat
if id nodebeat >/dev/null 2>&1; then
  userdel nodebeat
  echo "removed user nodebeat"
fi
echo "uninstalled: service, binary, unit, env, state dir and user removed"
