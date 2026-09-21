# NodeBeat Agent

Open-source, read-only monitoring agent for crypto validators and RPC nodes (Ethereum first, Cosmos supported). This is the only NodeBeat component that runs on your infrastructure.

**What it does:** auto-detects the chain client on a target host, renders a Grafana Alloy pipeline, and supervises Alloy + a bundled chain exporter as child processes. Alloy owns scrape + `remote_write` (WAL/retry/TLS) to the NodeBeat SaaS backend or any Prometheus remote-write endpoint.

**What it never does:** no keys, no SSH, no restarts, no writes outside its state dir, no inbound ports beyond localhost diagnostics.

## Quick start

```bash
# Detect what is running on a node (explicit target, never assumes localhost)
go run ./cmd/nodebeat-agent detect --target 10.0.1.5 --remote-write-url http://localhost:8428/api/v1/write

# Run against a node (standalone, no SaaS account needed)
go run ./cmd/nodebeat-agent run --target 10.0.1.5 \
  --remote-write-url http://localhost:8428/api/v1/write \
  --state-dir .nodebeat

# SaaS mode (after creating a node in the portal; JWT via env, never a flag)
export NODEBEAT_TOKEN=$CLERK_JWT
nodebeat-agent enroll --control-plane https://api.nodebeat.stream \
  --name my-node --target 10.0.1.5
nodebeat-agent run --enrolled --state-dir .nodebeat
```

Binaries: `nodebeat-agent` (supervisor) and `nodebeat-onboard` (pre-flight checks: disk, NTP, P2P ports, static IP, client detection).

## Trust model

* **Read-only:** the agent only supervises its own children (Alloy + exporter) and writes inside `--state-dir` (`config.alloy` 0600, `manifest.json` 0644). It never touches validator keys or restarts clients.
* **Egress-only:** opens no inbound ports. Diagnostics (`/metrics`, `/manifest`) bind to localhost by default.
* **Manifest:** every run writes exactly what is collected and where it goes. Inspect it at `http://127.0.0.1:19090/manifest` or `.nodebeat/manifest.json`.
* **Releases:** signed with cosign (keyless Sigstore), checksums + SBOM + SLSA provenance via GoReleaser. See `.goreleaser.yaml`.
* **Worst case of compromise:** metrics leak only. See `docs/THREAT_MODEL.md`.
* **Uninstall:** `sudo packaging/uninstall.sh` removes binary, systemd unit, and firewall rule — nothing left.

One agent run monitors one chain. An Ethereum EL+CL pair counts as one chain (single `ethereum-metrics-exporter` with `--execution-url` + `--consensus-url`). For two different chains on one host (e.g. Ethereum + Cosmos), run twice with disjoint `--state-dir`, `--exporter-port`, `--metrics-addr`, and `--alloy-ui-addr`.

## Layout

```
cmd/nodebeat-agent/   # detect | enroll | run (incl. --enrolled poll loop)
cmd/nodebeat-onboard/ # pre-flight checklist
internal/agent/       # supervisor: detect → render → run Alloy + exporter
internal/detect/      # chain/client probing (EL/CL, CometBFT)
internal/alloycfg/    # Alloy pipeline renderer (hot 5s / standard 15s / node 15s)
internal/enroll/      # SaaS enrollment client (ingest token, 0600)
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

Requires Go 1.26+, plus `alloy` and `ethereum-metrics-exporter` (or `cosmos-validator-watcher` for Cosmos) on `PATH` at runtime — the agent supervises them, it does not re-implement collection.

## License

Apache-2.0. See `LICENSE`.
