# NodeBeat Agent — Threat Model

The agent runs on customer validator/RPC hosts, next to keys. This document bounds what it can do and what a compromise costs.

## In scope (what the agent does)

* Probes well-known local ports on an explicit `--target` to detect EL/CL (Ethereum) or CometBFT (Cosmos) clients and their native Prometheus endpoints.
* Renders an Alloy config (hot 5s with the Ethereum poller target at 1s / standard 15s / node 15s scrape jobs) and supervises Alloy, plus `cosmos-validator-watcher` on Cosmos nodes.
* Ethereum: polls the node in-process with read-only calls only — `GET /eth/v1/node/syncing`, `GET /eth/v1/node/peer_count`, the `chain_reorg` event stream, and a JSON-RPC batch of `eth_syncing`, `eth_getBlockByNumber("latest", false)`, `net_peerCount`, `eth_gasPrice`. With `--validators` it also reads those validators' duties once per epoch: validator state (`GET /eth/v1/beacon/states/head/validators`), proposer duties, block headers of their proposal slots, and two POSTs that only carry validator indices in the body — the validator-liveness query and the attestation-rewards query; neither changes node state. It never calls signing, key-manager or admin endpoints and never follows redirects. The exact list is in the manifest (`chain_api_calls`).
* Remote-writes metrics over TLS to the configured endpoint (SaaS ingest with per-node `nb_ingest_...` bearer token, or any standalone remote-write URL).
* Serves `/metrics`, `/manifest` and (Ethereum) `/chain/metrics` on localhost (`127.0.0.1:19090` by default).
* Stores state only in `--state-dir`: `config.alloy` (0600, may hold the ingest token when server-rendered), `enrollment.json` (0600), `manifest.json` (0644, no secrets), Alloy WAL.

## Out of scope (what the agent never does)

* Never reads validator keys, never holds custody in any form.
* Never SSHes anywhere, never restarts validators or chain clients, never executes arbitrary commands on the host (it only supervises its own bundled children).
* Opens no inbound ports beyond loopback, with no exceptions: agent diagnostics (`/metrics`, `/manifest`, `/chain/metrics` on 127.0.0.1:19090) and the Alloy UI (127.0.0.1:12345). Host metrics come from node_exporter embedded in Alloy (no socket of its own); the Cosmos collector `cosmos-validator-watcher` is pinned to 127.0.0.1 via `--http-addr`. Native client endpoints (Geth :6060, Lighthouse :5054, CometBFT :26660, …) are the chain client's own listeners; the agent only scrapes them. No listener on the chain P2P or RPC interfaces.
* Never runs as root in supported installs (systemd unit uses `NoNewPrivileges`, `ProtectSystem=strict`, `ProtectHome=true`, `PrivateTmp`).

## Worst-case compromise

An attacker who fully controls the agent process gets:

1. The metrics stream (host + chain + validator-duty metrics) — a confidentiality leak, not a signing risk.
2. The per-node ingest token (`nb_ingest_...`) — scoped to pushing that node's metrics and polling its own Alloy config. It cannot read other nodes, query history, or change alerting. Rotate via the portal; old token stops working.

They do **not** get: validator keys, withdrawal keys, SSH access, restart/failover capability, or control-plane admin.

## Operator controls

* Review this source (small, Apache-2.0) before installing; verify cosign signatures, checksums, SBOM, and SLSA provenance on releases.
* Check the listeners yourself (`ss -tlnp`): everything the agent runs is on 127.0.0.1 (`docs/USER_GUIDE.md`, "Listening ports").
* Optionally restrict egress (samples in `packaging/firewall/`, full-host lockdowns that reset ufw and deny all in/out: add SSH and P2P rules first).
* Treat `--state-dir` as secret (0600 files); `manifest.json` is safe to share, `config.alloy`/`enrollment.json` are not.
* Uninstall cleanly: `sudo packaging/uninstall.sh`.
