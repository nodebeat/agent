# NodeBeat Agent

Open-source, read-only monitoring agent for crypto validators and RPC nodes (Ethereum first, Cosmos supported). This is the only NodeBeat component that runs on your infrastructure.

**What it does:** auto-detects the chain client on a target host, renders a Grafana Alloy pipeline, and supervises Alloy (plus `cosmos-validator-watcher` for Cosmos) as child processes. For Ethereum it polls the standard Beacon API and JSON-RPC itself with read-only calls (`internal/ethpoll`), so there is no third-party exporter to install; with `--validators` (indices or pubkeys) it also tracks those validators' duties per epoch — attestations, proposals, attestation effectiveness, slashing — with the same read-only API, served on `127.0.0.1:19090/chain/duties`. Alloy owns scrape + `remote_write` (WAL/retry/TLS) to the NodeBeat SaaS backend or any Prometheus remote-write endpoint.

**What it never does:** no keys, no SSH, no restarts, no writes outside its state dir, no inbound ports beyond localhost diagnostics — no exceptions.

## Quick start

```bash
# Detect what is running on a node (explicit target, never assumes localhost)
go run ./cmd/nodebeat-agent detect --target 10.0.1.5 --remote-write-url http://localhost:8428/api/v1/write

# Run against a node (standalone, no SaaS account needed)
go run ./cmd/nodebeat-agent run --target 10.0.1.5 \
  --remote-write-url http://localhost:8428/api/v1/write \
  --state-dir .nodebeat

# SaaS mode (portal Add Node first — ingest token shown once; token via env, never a flag).
# enroll stores the token in .nodebeat/ingest-token and pins the remote-write URL;
# the control plane never sends pipeline config: the agent renders it locally.
export NODEBEAT_INGEST_TOKEN=<token from the portal>
nodebeat-agent enroll --control-plane https://app.nodebeat.stream \
  --target 10.0.1.5 --state-dir .nodebeat
unset NODEBEAT_INGEST_TOKEN
nodebeat-agent run --enrolled --target 10.0.1.5 --state-dir .nodebeat
```

Binaries: `nodebeat-agent` (supervisor) and `nodebeat-onboard` (pre-flight checks: disk, NTP, P2P ports, static IP, client detection).

## Trust model

* **Read-only:** the agent only supervises its own children (Alloy, Cosmos watcher), makes only read calls to the node, and writes inside `--state-dir` (`ingest-token` 0600 — the only secret, `config.alloy` 0600, `manifest.json` 0644). The control plane supplies parameters only (node name, tenant, remote-write URL pinned at enroll), never config. It never touches validator keys or restarts clients.
* **Egress-only:** opens no inbound ports. Diagnostics (`/metrics`, `/manifest`, Ethereum `/chain/metrics`) bind to localhost by default.
* **Manifest:** every run writes exactly what is collected, which API calls the agent makes to the node (`chain_api_calls`), and where data goes. Inspect it at `http://127.0.0.1:19090/manifest` or `.nodebeat/manifest.json`.
* **Releases:** signed with cosign (keyless Sigstore), checksums + SBOM + SLSA provenance via GoReleaser. See `.goreleaser.yaml`.
* **Third-party binaries:** each release archive ships our portable build of `cosmos-validator-watcher` (MIT, `scripts/release/build-watcher.sh`). Alloy is not shipped: `packaging/install.sh` downloads the version pinned in `packaging/deps.lock` and checks the SHA-256 of both the archive and the binary, so the signed release fixes exactly which Alloy runs.
* **Worst case of compromise:** metrics leak only. See `docs/THREAT_MODEL.md`.
* **Uninstall:** `sudo packaging/uninstall.sh` removes binaries, systemd unit, env, state dir and user — nothing left.

One agent run monitors one chain. An Ethereum EL+CL pair counts as one chain (one poller reads both). For two different chains on one host (e.g. Ethereum + Cosmos), run twice with disjoint `--state-dir`, `--exporter-port`, `--metrics-addr`, and `--alloy-ui-addr`, plus `--chain ethereum` / `--chain cosmos` per instance (empty = auto-detect all families, which misattributes mixed hosts — Ethereum wins and Cosmos duties are missed).

## Layout

```
cmd/nodebeat-agent/   # detect | enroll | run (--enrolled adds the heartbeat)
cmd/nodebeat-onboard/ # pre-flight checklist
internal/agent/       # supervisor: detect → render → run Alloy (+ Cosmos watcher)
internal/ethpoll/     # read-only Ethereum poller (Beacon API + JSON-RPC) and validator duty tracker
internal/detect/      # chain/client probing (EL/CL, CometBFT)
internal/alloycfg/    # Alloy pipeline renderer (hot 5s, poller 1s / standard 15s / node 15s)
internal/enroll/      # SaaS enrollment client + ingest-token file (0600)
internal/supervise/   # child-process supervisor
internal/version/     # build-time version stamp
deploy/dev/vmalert/rules/        # alert rules (hot + standard + infra)
deploy/dev/grafana/dashboards/   # fleet dashboard
packaging/            # systemd unit, install/uninstall, firewall allowlists
docs/THREAT_MODEL.md  # what the agent can and cannot do
```

## Build / test

```bash
make build          # bin/nodebeat-agent
make build-onboard  # bin/nodebeat-onboard
make test
make vet
```

Requires Go 1.26+, plus `alloy` (and `cosmos-validator-watcher` for Cosmos) on `PATH` when running `nodebeat-agent run` from source; `packaging/install.sh` provides both on installed hosts. `scripts/release/build-watcher.sh` (Docker) builds the watcher; `make release-snapshot` runs it. The agent reads client-agnostic chain state through the standard APIs and leaves client internals to each client's native metrics.

## License

Apache-2.0. See `LICENSE`.
