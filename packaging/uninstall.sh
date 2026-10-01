#!/usr/bin/env bash
# One-command clean uninstall. Says what to remove, so a host running
# several agents never loses all of them by accident:
#
#   sudo packaging/uninstall.sh --node testnet-cos-02
#     one agent (by agent or node name, as install.sh prints them): its
#     service, unit, env file and state dir with its token. The binaries and
#     the nodebeat user stay while another agent remains.
#   sudo packaging/uninstall.sh --all
#     every agent, the units (and any drop-in left by older installs), the
#     binaries and the nodebeat user: nothing left.
#
# Without either it lists the installed agents and changes nothing.
set -euo pipefail

NODE=""; ALL=0
while [ $# -gt 0 ]; do
  case "$1" in
    --node) NODE="$2"; shift 2 ;;
    --all) ALL=1; shift ;;
    -h|--help) sed -n '2,/^set -euo/p' "$0" | sed '$d; s/^# \{0,1\}//'; exit 0 ;;
    *) echo "unknown flag: $1 (see --help)" >&2; exit 2 ;;
  esac
done

[ -z "$NODE" ] || [ "$ALL" = 0 ] || { echo "--node and --all are exclusive" >&2; exit 2; }
[ "$(id -u)" = 0 ] || { echo "run as root" >&2; exit 1; }

# Same layout as install.sh: "default" is the plain nodebeat-agent unit.
inst_env() { if [ "$1" = default ]; then echo /etc/nodebeat/agent.env; else echo "/etc/nodebeat/agent-$1.env"; fi; }
inst_state() { if [ "$1" = default ]; then echo /var/lib/nodebeat; else echo "/var/lib/nodebeat-$1"; fi; }
inst_unit() { if [ "$1" = default ]; then echo nodebeat-agent; else echo "nodebeat-agent@$1"; fi; }
list_instances() {
  [ ! -f /etc/nodebeat/agent.env ] || echo default
  local f
  for f in /etc/nodebeat/agent-*.env; do
    [ -f "$f" ] || continue
    f="${f#/etc/nodebeat/agent-}"; echo "${f%.env}"
  done
}
node_name() {
  sed -n 's/^  "node_name": "\(.*\)",\{0,1\}$/\1/p' "$(inst_state "$1")/enrollment.json" 2>/dev/null || true
}
describe_instances() {
  local i
  for i in $(list_instances); do echo "  $(inst_unit "$i")  node $(node_name "$i")" >&2; done
}
remove_instance() {
  systemctl disable --now "$(inst_unit "$1")" 2>/dev/null || true
  [ "$1" != default ] ||
    rm -rf /etc/systemd/system/nodebeat-agent.service /etc/systemd/system/nodebeat-agent.service.d
  rm -rf "$(inst_env "$1")" "$(inst_state "$1")"
  echo "removed $(inst_unit "$1")"
}

if [ -n "$NODE" ]; then
  PICK=""
  for i in $(list_instances); do
    if [ "$i" = "$NODE" ] || [ "$(node_name "$i")" = "$NODE" ]; then PICK="$i"; break; fi
  done
  if [ -z "$PICK" ]; then
    echo "no agent for --node $NODE on this host; installed:" >&2
    describe_instances; exit 2
  fi
  remove_instance "$PICK"
  if [ -n "$(list_instances)" ]; then
    systemctl daemon-reload
    echo "uninstalled $(inst_unit "$PICK"); other agents, the binaries and the nodebeat user stay"
    exit 0
  fi
elif [ "$ALL" = 0 ]; then
  if [ -n "$(list_instances)" ]; then
    echo "pass --node NAME to remove one agent, or --all to remove every agent and the binaries; installed:" >&2
    describe_instances
  else
    echo "no agent installed; --all still removes leftover binaries, units and the nodebeat user" >&2
  fi
  exit 2
fi

for i in $(list_instances); do remove_instance "$i"; done
# The default unit even without its env file (half-finished installs).
systemctl disable --now nodebeat-agent 2>/dev/null || true
rm -rf /etc/systemd/system/nodebeat-agent.service /etc/systemd/system/nodebeat-agent.service.d \
  /etc/systemd/system/nodebeat-agent@.service
systemctl daemon-reload
rm -rf /usr/local/bin/nodebeat-agent /usr/local/lib/nodebeat
rm -rf /etc/nodebeat /var/lib/nodebeat
if id nodebeat >/dev/null 2>&1; then
  userdel nodebeat
  echo "removed user nodebeat"
fi
echo "uninstalled: services, binaries, units, env files, state dirs and user removed"
