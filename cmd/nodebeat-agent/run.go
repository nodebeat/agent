package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nodebeat/agent/internal/agent"
	"github.com/nodebeat/agent/internal/enroll"
)

// runRun starts the supervised agent. SIGINT/SIGTERM stop it gracefully;
// SIGHUP re-detects the target and reloads the Alloy pipeline.
func runRun(args []string) int {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	target := fs.String("target", "", "node hostname or IP to monitor (required; never defaults to localhost)")
	remoteWriteURL := fs.String("remote-write-url", "", "ingest endpoint, e.g. https://ingest:8428/api/v1/write (required)")
	instance := fs.String("instance", "", "instance label for all series (default: --target)")
	exporterBin := fs.String("exporter-bin", "", "chain exporter binary (resolved via PATH; default auto-selects per chain: ethereum-metrics-exporter or cosmos-validator-watcher)")
	alloyBin := fs.String("alloy-bin", "alloy", "Alloy binary (resolved via PATH)")
	exporterPort := fs.Int("exporter-port", 9090, "port for the bundled chain exporter on the agent host")
	exporterURL := fs.String("exporter-url", "", "chain exporter /metrics URL (default http://127.0.0.1:<port>/metrics)")
	stateDir := fs.String("state-dir", ".nodebeat", "agent state dir; the only path the agent writes to")
	metricsAddr := fs.String("metrics-addr", "127.0.0.1:19090", "localhost diagnostics address for /metrics and /manifest")
	alloyUIAddr := fs.String("alloy-ui-addr", "127.0.0.1:12345", "localhost address for the Alloy UI")
	disableReporting := fs.Bool("disable-reporting", true, "pass --disable-reporting to Alloy (no usage telemetry)")
	enrolled := fs.Bool("enrolled", false, "run from enrollment.json in --state-dir (created by `enroll`); fetches the server pipeline and polls it")
	pollInterval := fs.Duration("poll-interval", 60*time.Second, "config poll interval in enrolled mode (also the server heartbeat)")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	logger := log.New(os.Stderr, "", log.LstdFlags)
	if *enrolled {
		return runEnrolled(logger, enrolledOptions{
			stateDir: *stateDir, exporterBin: *exporterBin, alloyBin: *alloyBin,
			exporterPort: *exporterPort, exporterURL: *exporterURL,
			metricsAddr: *metricsAddr, alloyUIAddr: *alloyUIAddr,
			disableReporting: *disableReporting, pollInterval: *pollInterval,
		})
	}
	if *target == "" || *remoteWriteURL == "" {
		fmt.Fprintln(os.Stderr, "run: --target and --remote-write-url are required (or use --enrolled)")
		fs.Usage()
		return 2
	}
	r := agent.New(agent.Config{
		Target:           *target,
		RemoteWriteURL:   *remoteWriteURL,
		Instance:         *instance,
		ExporterBin:      *exporterBin,
		AlloyBin:         *alloyBin,
		ExporterPort:     *exporterPort,
		ExporterURL:      *exporterURL,
		StateDir:         *stateDir,
		MetricsAddr:      *metricsAddr,
		AlloyUIAddr:      *alloyUIAddr,
		DisableReporting: *disableReporting,
	}, logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	defer signal.Stop(hup)

	return runWithSignals(ctx, hup, r, logger)
}

// runWithSignals serves SIGHUP reloads until ctx ends, then runs supervision.
func runWithSignals(ctx context.Context, hup <-chan os.Signal, r *agent.Runner, logger *log.Logger) int {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-hup:
				if err := r.Reload(ctx); err != nil {
					logger.Printf("agent: reload failed: %v", err)
				}
			}
		}
	}()

	if err := r.Run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "run:", err)
		return 1
	}
	return 0
}

type enrolledOptions struct {
	stateDir         string
	exporterBin      string
	alloyBin         string
	exporterPort     int
	exporterURL      string
	metricsAddr      string
	alloyUIAddr      string
	disableReporting bool
	pollInterval     time.Duration
}

// runEnrolled supervises from the server-rendered pipeline stored by
// `enroll`. It fetches the config once, prepares, then polls the control
// plane (each poll doubles as a heartbeat) and SIGHUPs Alloy on change.
func runEnrolled(logger *log.Logger, o enrolledOptions) int {
	e, err := enroll.Load(o.stateDir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "run --enrolled:", err)
		return 1
	}
	tok, orgID := e.DaemonToken()
	client := enroll.NewClient(e.ControlPlane, tok, orgID)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	fetchCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	remote, err := client.FetchConfig(fetchCtx, e.NodeID)
	cancel()
	if err != nil {
		fmt.Fprintln(os.Stderr, "run --enrolled: fetch config:", err)
		return 1
	}

	det := e.Detection
	r := agent.New(agent.Config{
		Target:           e.NodeName,
		RemoteWriteURL:   remote.RemoteWriteURL,
		Instance:         e.NodeName,
		ExporterBin:      o.exporterBin,
		AlloyBin:         o.alloyBin,
		ExporterPort:     o.exporterPort,
		ExporterURL:      o.exporterURL,
		StateDir:         o.stateDir,
		MetricsAddr:      o.metricsAddr,
		AlloyUIAddr:      o.alloyUIAddr,
		DisableReporting: o.disableReporting,
	}, logger)
	if err := r.PrepareRemote(&det, remote.AlloyConfig); err != nil {
		fmt.Fprintln(os.Stderr, "run --enrolled:", err)
		return 1
	}
	logger.Printf("agent: enrolled as %q (tenant %s); polling %s every %s",
		e.NodeName, e.TenantID, e.ControlPlane, o.pollInterval)

	if o.pollInterval <= 0 {
		o.pollInterval = 60 * time.Second
	}
	go func() {
		t := time.NewTicker(o.pollInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				pctx, pcancel := context.WithTimeout(ctx, 30*time.Second)
				latest, err := client.FetchConfig(pctx, e.NodeID)
				pcancel()
				if err != nil {
					logger.Printf("agent: config poll failed: %v", err)
					continue
				}
				if err := r.ApplyRemoteConfig(latest.AlloyConfig); err != nil {
					logger.Printf("agent: config apply failed: %v", err)
				}
			}
		}
	}()

	if err := r.Start(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "run --enrolled:", err)
		return 1
	}
	return 0
}
