#!/usr/bin/env bash
# Sample egress allowlist for a NodeBeat-monitored host (ufw).
# The agent only needs HTTPS to the ingest endpoint, plus DNS and NTP.
# Fill in INGEST_HOST, review, then run as root. NOT applied by install.sh.
set -euo pipefail

INGEST_HOST="${INGEST_HOST:?set INGEST_HOST to the ingest hostname}"
[ "$(id -u)" = 0 ] || { echo "run as root" >&2; exit 1; }

ufw --force reset
ufw default deny incoming
ufw default deny outgoing
ufw allow out 53/udp comment 'DNS (agent + clients)'
ufw allow out 123/udp comment 'NTP (attestation timing)'
ufw allow out to "$INGEST_HOST" port 443 proto tcp comment 'NodeBeat ingest'
# Chain clients need their P2P ports too; add per deployment, e.g.:
# ufw allow out 30303 comment 'EL P2P'
# ufw allow out 9000 comment 'CL P2P'
ufw --force enable
ufw status verbose
