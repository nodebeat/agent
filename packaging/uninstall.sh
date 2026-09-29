#!/usr/bin/env bash
# One-command clean uninstall. Removes the service, binaries, unit (and any
# drop-in left by older installs), env file, state dir (token included) and
# the nodebeat user: nothing left.
#
#   sudo packaging/uninstall.sh
set -euo pipefail

[ "$(id -u)" = 0 ] || { echo "run as root" >&2; exit 1; }

systemctl disable --now nodebeat-agent 2>/dev/null || true
rm -rf /etc/systemd/system/nodebeat-agent.service /etc/systemd/system/nodebeat-agent.service.d
systemctl daemon-reload
rm -rf /usr/local/bin/nodebeat-agent /usr/local/lib/nodebeat
rm -rf /etc/nodebeat /var/lib/nodebeat
if id nodebeat >/dev/null 2>&1; then
  userdel nodebeat
  echo "removed user nodebeat"
fi
echo "uninstalled: service, binaries, unit, env, state dir and user removed"
