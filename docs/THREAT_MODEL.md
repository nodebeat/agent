# NodeBeat Agent — Threat Model

The agent runs on customer validator/RPC hosts, next to keys. This document bounds what it can do and what a compromise costs.

## In scope (what the agent does)

* Probes well-known local ports on an explicit `--target` to detect EL/CL (Ethereum) or CometBFT (Cosmos) clients and their native Prometheus endpoints.
* Renders an Alloy config (hot 5s / standard 15s / node 15s scrape jobs) and supervises two child processes: Alloy and the chain exporter.
* Remote-writes metrics over TLS to the configured endpoint (SaaS ingest with per-node `nb_ingest_...` bearer token, or any standalone remote-write URL).
* Serves `/metrics` + `/manifest` on localhost (`127.0.0.1:19090` by default).
* Stores state only in `--state-dir`: `config.alloy` (0600, may hold the ingest token when server-rendered), `enrollment.json` (0600), `manifest.json` (0644, no secrets), Alloy WAL.

## Out of scope (what the agent never does)

* Never reads validator keys, never holds custody in any form.
* Never SSHes anywhere, never restarts validators or chain clients, never executes arbitrary commands on the host (it only supervises its own bundled children).
* The agent binary itself opens no inbound ports beyond localhost diagnostics (`/metrics` + `/manifest` on 127.0.0.1:19090, Alloy UI on 127.0.0.1:12345). EXCEPTION: the supervised chain exporter has no bind-address flag and listens on `0.0.0.0:9090` (default) — this is an upstream limitation, not agent code. Restrict it with the host firewall (`packaging/firewall/`); it serves metrics only, no control interface. No listener on the chain P2P or RPC interfaces.
* Never runs as root in supported installs (systemd unit uses `NoNewPrivileges`, `ProtectSystem=strict`, `ProtectHome=true`, `PrivateTmp`).

## Worst-case compromise

An attacker who fully controls the agent process gets:

1. The metrics stream (host + chain + validator-duty metrics) — a confidentiality leak, not a signing risk.
2. The per-node ingest token (`nb_ingest_...`) — scoped to pushing that node's metrics and polling its own Alloy config. It cannot read other nodes, query history, or change alerting. Rotate via the portal; old token stops working.

They do **not** get: validator keys, withdrawal keys, SSH access, restart/failover capability, or control-plane admin.

## Operator controls

* Review this source (small, Apache-2.0) before installing; verify cosign signatures, checksums, SBOM, and SLSA provenance on releases.
* Restrict egress (see `packaging/firewall/`): allowlist only the ingest endpoint.
* Treat `--state-dir` as secret (0600 files); `manifest.json` is safe to share, `config.alloy`/`enrollment.json` are not.
* Uninstall cleanly: `sudo packaging/uninstall.sh`.
