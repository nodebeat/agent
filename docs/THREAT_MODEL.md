# NodeBeat Agent — Threat Model

The agent runs on customer validator/RPC hosts, next to keys. This document bounds what it can do and what a compromise costs.

## In scope (what the agent does)

* Probes well-known local ports on an explicit `--target` to detect EL/CL (Ethereum) or CometBFT (Cosmos) clients and their native Prometheus endpoints.
* Renders its Alloy config locally, in every mode (hot 5s with the Ethereum poller target at 1s / standard 15s / node 15s scrape jobs) and supervises Alloy, plus `cosmos-validator-watcher` on Cosmos nodes.
* Ethereum: polls the node in-process with read-only calls only — `GET /eth/v1/node/syncing`, `GET /eth/v1/node/peer_count`, the `chain_reorg` event stream, and a JSON-RPC batch of `eth_syncing`, `eth_getBlockByNumber("latest", false)`, `net_peerCount`, `eth_gasPrice`. With `--validators` it also reads those validators' duties once per epoch: validator state (`GET /eth/v1/beacon/states/head/validators`), proposer duties, block headers of their proposal slots, and two POSTs that only carry validator indices in the body — the validator-liveness query and the attestation-rewards query; neither changes node state. It never calls signing, key-manager or admin endpoints and never follows redirects. The exact list is in the manifest (`chain_api_calls`).
* Remote-writes metrics over TLS to the configured endpoint (SaaS ingest with per-node `nb_ingest_...` bearer token, or any standalone remote-write URL).
* Sends the ingest token only as a `Bearer` header to the control plane given at `enroll` (activation and heartbeats; redirects are not followed) and to the remote-write endpoint (Alloy reads it via `credentials_file`). It is the agent's only credential, stored once in `--state-dir/ingest-token` (0600); child processes do not inherit it through the environment. Detection probes also never follow redirects.
* Reports its detection to the control plane: chain, endpoint URLs and hostname at activation, and the detected client names with their self-reported version strings (`web3_clientVersion`, Beacon `/eth/v1/node/version`) at activation and on every heartbeat. The same fields are in the manifest (`el_client`, `cl_client`, `el_version`, `cl_version`).
* Takes only parameters from the control plane — node name, tenant id, remote-write URL — never pipeline config. The remote-write URL is pinned at `enroll` (and must be https when the control plane is); later heartbeats can rename the node but cannot redirect metrics or the token.
* Serves `/metrics`, `/manifest` and (Ethereum) `/chain/metrics` on localhost (`127.0.0.1:19090` by default).
* Stores state only in `--state-dir`: `ingest-token` (0600, the only secret), `config.alloy` (0600, references the token file), `enrollment.json` (0600, no secrets), `manifest.json` (0644, no secrets), Alloy WAL.

## Out of scope (what the agent never does)

* Never reads validator keys, never holds custody in any form.
* Never SSHes anywhere, never restarts validators or chain clients, never executes arbitrary commands on the host (it only supervises its own bundled children).
* Opens no inbound ports beyond loopback, with no exceptions: agent diagnostics (`/metrics`, `/manifest`, `/chain/metrics` on 127.0.0.1:19090) and the Alloy UI (127.0.0.1:12345). Host metrics come from node_exporter embedded in Alloy (no socket of its own); the Cosmos collector `cosmos-validator-watcher` is pinned to 127.0.0.1 via `--http-addr`. Native client endpoints (Geth :6060, Lighthouse :5054, CometBFT :26660, …) are the chain client's own listeners; the agent only scrapes them. No listener on the chain P2P or RPC interfaces.
* Never runs as root in supported installs. The systemd unit drops every capability, uses `NoNewPrivileges`, `ProtectSystem=strict`, `ProtectHome=true`, `PrivateTmp`, `PrivateDevices`, a `@system-service` syscall filter, and `NoExecPaths=/` with `ExecPaths=` limited to the agent, the Alloy/watcher copies in `/usr/local/lib/nodebeat/bin` and shared libraries (`systemd-analyze security`: 1.8 OK).

## Worst-case compromise

An attacker who fully controls the agent process gets:

1. The metrics stream (host + chain + validator-duty metrics) — a confidentiality leak, not a signing risk.
2. The per-node ingest token (`nb_ingest_...`) — scoped to pushing that node's metrics and polling its own Alloy config. It cannot read other nodes, query history, or change alerting. Rotate via the portal; old token stops working.

They do **not** get: validator keys, withdrawal keys, SSH access, restart/failover capability, or control-plane admin.

An attacker who controls the NodeBeat control plane (or can forge its responses) gets less: it can rename the node's series. It cannot change the agent's pipeline, what it scrapes, or where metrics and the token go, since the agent renders its config itself and pinned the remote-write URL at `enroll`.

## Operator controls

* Review this source (small, Apache-2.0) before installing; verify cosign signatures, checksums, SBOM, and SLSA provenance on releases.
* Check the listeners yourself (`ss -tlnp`): everything the agent runs is on 127.0.0.1 (`docs/USER_GUIDE.md`, "Listening ports").
* Optionally restrict egress (samples in `packaging/firewall/`, full-host lockdowns that reset ufw and deny all in/out: add SSH and P2P rules first).
* Treat `--state-dir/ingest-token` as secret; `manifest.json` and `config.alloy` hold no secret.
* Uninstall cleanly: `sudo packaging/uninstall.sh`.
