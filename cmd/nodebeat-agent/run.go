package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/nodebeat/agent/internal/agent"
	"github.com/nodebeat/agent/internal/detect"
	"github.com/nodebeat/agent/internal/enroll"
)

// runRun starts the supervised agent. SIGINT/SIGTERM stop it gracefully;
// SIGHUP re-detects the target and reloads the Alloy pipeline.
//
// Both modes detect and render the pipeline locally with the same code.
// Standalone takes the sink from flags; --enrolled takes it from
// enrollment.json (pinned at enroll) and heartbeats the control plane,
// which may only rename the node.
func runRun(args []string) int {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	target := fs.String("target", "", "node hostname or IP to monitor (required; never defaults to localhost)")
	remoteWriteURL := fs.String("remote-write-url", "", "ingest endpoint, e.g. https://ingest:8428/api/v1/write (required unless --enrolled)")
	instance := fs.String("instance", "", "instance label for all series (default: --target; --enrolled uses the portal node name)")
	exporterBin := fs.String("exporter-bin", "", "Cosmos exporter binary (resolved via PATH; default cosmos-validator-watcher). Ethereum is polled in-process")
	alloyBin := fs.String("alloy-bin", "alloy", "Alloy binary (resolved via PATH)")
	exporterPort := fs.Int("exporter-port", 9090, "loopback port for the Cosmos exporter on the agent host")
	exporterURL := fs.String("exporter-url", "", "chain metrics URL Alloy scrapes (default: Ethereum http://<metrics-addr>/chain/metrics, Cosmos http://127.0.0.1:<exporter-port>/metrics)")
	stateDir := fs.String("state-dir", ".nodebeat", "agent state dir; the only path the agent writes to")
	metricsAddr := fs.String("metrics-addr", "127.0.0.1:19090", "localhost diagnostics address for /metrics and /manifest")
	alloyUIAddr := fs.String("alloy-ui-addr", "127.0.0.1:12345", "localhost address for the Alloy UI")
	ingestTokenFlag := fs.String("ingest-token", "", "remote-write Bearer token, saved to <state-dir>/ingest-token (prefer NODEBEAT_INGEST_TOKEN env: flags are visible in ps); default: that file if present, else no auth")
	disableReporting := fs.Bool("disable-reporting", true, "pass --disable-reporting to Alloy (no usage telemetry)")
	enrolled := fs.Bool("enrolled", false, "take the sink from enrollment.json in --state-dir (created by `enroll`) and heartbeat the control plane")
	pollInterval := fs.Duration("poll-interval", 60*time.Second, "heartbeat interval in enrolled mode")
	chainFlag := fs.String("chain", "", "restrict detection to ethereum or cosmos (empty = auto; use per instance on mixed hosts)")
	validatorsFlag := fs.String("validators", "", "validators to track duties for, comma-separated (default $NB_VALIDATORS; empty = no duty alerts): Ethereum indices or 0x pubkeys; Cosmos consensus addresses (hex or ...valcons1...)")
	var ports detect.Ports
	detect.BindPortFlags(fs, &ports)
	if err := fs.Parse(args); err != nil {
		return 2
	}
	usage := func(msg string) int {
		fmt.Fprintln(os.Stderr, "run:", msg)
		return 2
	}
	chain, err := detect.ParseChainFilter(*chainFlag)
	if err != nil {
		return usage(err.Error())
	}
	validators := agent.SplitValidators(firstNonEmpty(*validatorsFlag, os.Getenv("NB_VALIDATORS")))
	if *target == "" {
		return usage("--target is required")
	}
	if *pollInterval <= 0 {
		return usage("--poll-interval must be positive")
	}
	dir, err := filepath.Abs(*stateDir)
	if err != nil {
		return usage(err.Error())
	}
	token, err := ingestToken(*ingestTokenFlag, dir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "run:", err)
		return 1
	}

	cfg := agent.Config{
		Target:           *target,
		RemoteWriteURL:   *remoteWriteURL,
		Instance:         *instance,
		ExporterBin:      *exporterBin,
		AlloyBin:         *alloyBin,
		ExporterPort:     *exporterPort,
		ExporterURL:      *exporterURL,
		StateDir:         dir,
		MetricsAddr:      *metricsAddr,
		AlloyUIAddr:      *alloyUIAddr,
		DisableReporting: *disableReporting,
		Ports:            ports,
		Chain:            chain,
		Validators:       validators,
	}
	if token != "" {
		cfg.IngestTokenFile = enroll.TokenPath(dir)
	}
	var e enroll.Enrollment
	if *enrolled {
		if *remoteWriteURL != "" || *instance != "" {
			return usage("--remote-write-url and --instance come from the enrollment with --enrolled")
		}
		if e, err = enroll.Load(dir); err != nil {
			fmt.Fprintln(os.Stderr, "run --enrolled:", err)
			return 1
		}
		if token == "" {
			fmt.Fprintf(os.Stderr, "run --enrolled: no ingest token in %s (run `enroll` again)\n", enroll.TokenPath(dir))
			return 1
		}
		cfg.RemoteWriteURL, cfg.Instance, cfg.TenantID = e.RemoteWriteURL, e.NodeName, e.TenantID
		if cfg.Chain, err = enrolledChain(chain, e.Chain); err != nil {
			return usage(err.Error())
		}
	} else if *remoteWriteURL == "" {
		return usage("--remote-write-url is required (or use --enrolled)")
	}
	// Formats depend on the chain: checked here when it is already known
	// (--chain or the enrollment), otherwise after detection
	// (NormalizeValidators).
	if cfg.Chain != "" {
		if _, err := agent.NormalizeValidators(cfg.Chain, validators); err != nil {
			return usage("--validators: " + err.Error())
		}
	}

	logger := log.New(os.Stderr, "", log.LstdFlags)
	r := agent.New(cfg, logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	defer signal.Stop(hup)

	if *enrolled {
		logger.Printf("agent: enrolled as %q (tenant %s); heartbeat to %s every %s",
			e.NodeName, e.TenantID, e.ControlPlane, *pollInterval)
		go heartbeat(ctx, enroll.NewClient(e.ControlPlane, token), e, r, *pollInterval, logger)
	}
	return runWithSignals(ctx, hup, r, logger)
}

// enrolledChain is the chain an enrolled run watches: the one detected at
// enroll, so a host serving both chains keeps the node's chain without
// --chain. A --chain that contradicts the enrollment is an error (the
// node's series would land under the wrong chain); enrollments written
// before the chain was recorded fall back to flag / auto-detection.
func enrolledChain(flagChain, enrolled string) (string, error) {
	switch {
	case enrolled == "":
		return flagChain, nil
	case flagChain == "" || flagChain == enrolled:
		return enrolled, nil
	default:
		return "", fmt.Errorf("--chain %s contradicts the enrollment (chain %s); re-run `enroll --chain %s` to change it",
			flagChain, enrolled, flagChain)
	}
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

// heartbeat polls the control plane (which records last_seen) and applies
// the one parameter that may change after enroll: the node name. The
// remote-write URL is pinned at enroll and never taken from a poll.
func heartbeat(ctx context.Context, c *enroll.Client, e enroll.Enrollment, r *agent.Runner, every time.Duration, logger *log.Logger) {
	t := time.NewTicker(every)
	defer t.Stop()
	refused := "" // last refused remote-write URL, logged once
	for {
		pctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		p, err := c.Heartbeat(pctx, e.NodeID)
		cancel()
		switch {
		case err != nil:
			logger.Printf("agent: heartbeat failed: %v", err)
		default:
			if p.RemoteWriteURL != "" && p.RemoteWriteURL != e.RemoteWriteURL && p.RemoteWriteURL != refused {
				refused = p.RemoteWriteURL
				logger.Printf("agent: control plane reports remote-write %s; keeping %s pinned at enroll (re-run enroll to change it)",
					p.RemoteWriteURL, e.RemoteWriteURL)
			}
			if err := r.SetInstance(p.NodeName); err != nil {
				logger.Printf("agent: rename to %q failed: %v", p.NodeName, err)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// ingestToken saves a token given by flag or env to <stateDir>/ingest-token
// (the single copy Alloy and the heartbeat read) and returns the token in
// that file, "" when there is none.
func ingestToken(flagVal, stateDir string) (string, error) {
	if t := firstNonEmpty(flagVal, os.Getenv("NODEBEAT_INGEST_TOKEN"), os.Getenv("NB_INGEST_TOKEN")); t != "" {
		if err := enroll.SaveToken(stateDir, t); err != nil {
			return "", err
		}
	}
	return enroll.LoadToken(stateDir)
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
