// Package agent is the nodebeat-agent supervisor: detect the chain
// client(s) on a target host, render an Alloy pipeline, and run the bundled
// Alloy + chain exporter binaries as supervised child processes.
//
// Trust model: the agent only supervises its own children and writes inside
// its state dir. It never SSHes anywhere, never restarts the validator, and
// the agent binary itself opens no inbound ports beyond localhost
// diagnostics. NOTE: the supervised chain exporter has no bind-address flag
// and listens on 0.0.0.0:9090 (upstream limitation) — operators must restrict
// it with the host firewall (packaging/firewall/).
package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/nodebeat/agent/internal/alloycfg"
	"github.com/nodebeat/agent/internal/detect"
	"github.com/nodebeat/agent/internal/supervise"
	"github.com/nodebeat/agent/internal/version"
)

// Child names, used for metrics, logs and reload signaling.
const (
	ChildExporter = "exporter"
	ChildAlloy    = "alloy"
)

// Config tunes one agent run. Zero values select documented defaults via
// withDefaults, except DisableReporting which the CLI defaults to true.
type Config struct {
	Target         string
	RemoteWriteURL string
	Instance       string
	// IngestToken renders the remote_write Bearer block (standalone agents
	// writing through the auth-enforcing ingest proxy). Empty omits it.
	IngestToken      string
	ExporterBin      string // default: "ethereum-metrics-exporter" or "cosmos-validator-watcher" per chain
	AlloyBin         string // default "alloy"
	ExporterPort     int    // default 9090
	ExporterURL      string // default http://127.0.0.1:<port>/metrics
	StateDir         string // default ".nodebeat"; the only writable path
	MetricsAddr      string // default "127.0.0.1:19090" (localhost only)
	AlloyUIAddr      string // default "127.0.0.1:12345" (localhost only)
	DisableReporting bool   // pass --disable-reporting to Alloy
	// Ports overrides target probing (devnet ephemeral host ports).
	// Zero value = standard ports (metrics 0 = per-client default).
	Ports         detect.Ports
	SuperviseOpts supervise.Options
}

func (c Config) withDefaults() Config {
	// ExporterBin intentionally has no default here — materializeWithConfig
	// selects "ethereum-metrics-exporter" vs "cosmos-validator-watcher" per
	// detected chain when empty, so Cosmos nodes are not forced onto the
	// Ethereum exporter.
	if c.AlloyBin == "" {
		c.AlloyBin = "alloy"
	}
	if c.ExporterPort == 0 {
		c.ExporterPort = 9090
	}
	if c.ExporterURL == "" {
		c.ExporterURL = "http://127.0.0.1:" + strconv.Itoa(c.ExporterPort) + "/metrics"
	}
	if c.StateDir == "" {
		c.StateDir = ".nodebeat"
	}
	if c.MetricsAddr == "" {
		c.MetricsAddr = "127.0.0.1:19090"
	}
	if c.AlloyUIAddr == "" {
		c.AlloyUIAddr = "127.0.0.1:12345"
	}
	if c.Instance == "" {
		c.Instance = c.Target
	}
	return c
}

// ScrapeJob describes one Alloy scrape job for the data manifest.
type ScrapeJob struct {
	Name     string   `json:"name"`
	Interval string   `json:"interval"`
	Targets  []string `json:"targets"`
}

// Manifest states exactly what the agent collects and where it goes. It is
// written to the state dir and served on /manifest.
type Manifest struct {
	AgentVersion   string      `json:"agent_version"`
	GeneratedAt    time.Time   `json:"generated_at"`
	Target         string      `json:"target"`
	Chain          string      `json:"chain"`
	ELClient       string      `json:"el_client,omitempty"`
	CLClient       string      `json:"cl_client,omitempty"`
	RemoteWriteURL string      `json:"remote_write_url"`
	Exporter       string      `json:"exporter"`
	AlloyConfig    string      `json:"alloy_config"`
	ScrapeJobs     []ScrapeJob `json:"scrape_jobs"`
	StateDir       string      `json:"state_dir"`
}

// Runner holds the prepared state of one agent run.
type Runner struct {
	cfg Config
	log *log.Logger
	// mu guards det, children, manifest and sup: Reload/ApplyRemoteConfig
	// (SIGHUP goroutine, enrolled poll loop) race with Run/Start and the
	// diagnostics server. Take the lock for every access, never across
	// disk/network IO.
	mu       sync.RWMutex
	det      *detect.Result
	children []supervise.Child
	manifest Manifest
	sup      *supervise.Supervisor
	registry *prometheus.Registry
}

// New builds a Runner. Call Prepare, then Start (or Run for both).
func New(cfg Config, logger *log.Logger) *Runner {
	if logger == nil {
		logger = log.New(os.Stderr, "", log.LstdFlags)
	}
	return &Runner{cfg: cfg.withDefaults(), log: logger}
}

// ChildrenStats returns supervisor stats, or nil before Start.
func (r *Runner) ChildrenStats() []supervise.Stats {
	r.mu.RLock()
	sup := r.sup
	r.mu.RUnlock()
	if sup == nil {
		return nil
	}
	return sup.Stats()
}

// Manifest returns the manifest from the last Prepare/Reload.
func (r *Runner) Manifest() Manifest {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.manifest
}

// ConfigPath is where the rendered Alloy config lives (inside the state dir).
func (r *Runner) ConfigPath() string {
	return filepath.Join(r.cfg.StateDir, "config.alloy")
}

// ManifestPath is where manifest.json lives (inside the state dir).
func (r *Runner) ManifestPath() string {
	return filepath.Join(r.cfg.StateDir, "manifest.json")
}

// Prepare detects the target, renders the Alloy config and writes the state
// dir (config.alloy 0600 plus manifest.json). Nothing is written outside the
// state dir.
func (r *Runner) Prepare(ctx context.Context) error {
	dctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	det, err := detect.DetectWithPorts(dctx, r.cfg.Target, r.cfg.Ports)
	if err != nil {
		return err
	}
	return r.materialize(det)
}

// materialize writes config + manifest + child specs for an already-known
// detection. Split from Prepare so tests can run it without network access.
func (r *Runner) materialize(det *detect.Result) error {
	rendered, err := alloycfg.Render(det, alloycfg.Options{
		RemoteWriteURL:     r.cfg.RemoteWriteURL,
		ExporterMetricsURL: r.cfg.ExporterURL,
		Instance:           r.cfg.Instance,
		IngestToken:        r.cfg.IngestToken,
	})
	if err != nil {
		return err
	}
	return r.materializeWithConfig(det, rendered)
}

// PrepareRemote writes a server-rendered Alloy pipeline (from `enroll` +
// GET /nodes/:id/config) instead of rendering locally. The exporter child is
// still resolved and supervised from the stored detection.
func (r *Runner) PrepareRemote(det *detect.Result, alloyConfig string) error {
	if alloyConfig == "" {
		return fmt.Errorf("empty remote alloy config")
	}
	return r.materializeWithConfig(det, alloyConfig)
}

// ApplyRemoteConfig rewrites config.alloy when the server pipeline changed
// and SIGHUPs Alloy. Used by the `run --enrolled` poll loop (which doubles
// as a heartbeat).
func (r *Runner) ApplyRemoteConfig(alloyConfig string) error {
	r.mu.RLock()
	sup := r.sup
	r.mu.RUnlock()
	if sup == nil {
		return fmt.Errorf("apply before start")
	}
	if alloyConfig == "" {
		return fmt.Errorf("empty remote alloy config")
	}
	before, err := os.ReadFile(r.ConfigPath())
	if err != nil {
		return err
	}
	if string(before) == alloyConfig {
		return nil
	}
	if err := os.WriteFile(r.ConfigPath(), []byte(alloyConfig), 0o600); err != nil {
		return fmt.Errorf("write alloy config: %w", err)
	}
	r.log.Print("agent: remote pipeline changed, signaling alloy to reload")
	return sup.Signal(ChildAlloy, syscall.SIGHUP)
}

// materializeWithConfig writes config + manifest + child specs for det using
// an already-rendered Alloy pipeline.
func (r *Runner) materializeWithConfig(det *detect.Result, rendered string) error {
	// Select exporter binary based on detected chain
	exporterBinName := r.cfg.ExporterBin
	if exporterBinName == "" {
		switch det.Chain {
		case detect.ChainCosmos:
			exporterBinName = "cosmos-validator-watcher"
		default:
			exporterBinName = "ethereum-metrics-exporter"
		}
	}
	exporterBin, err := exec.LookPath(exporterBinName)
	if err != nil {
		return fmt.Errorf("exporter binary %q: %w", exporterBinName, err)
	}
	alloyBin, err := exec.LookPath(r.cfg.AlloyBin)
	if err != nil {
		return fmt.Errorf("alloy binary %q: %w", r.cfg.AlloyBin, err)
	}
	if err := os.MkdirAll(r.cfg.StateDir, 0o750); err != nil {
		return fmt.Errorf("create state dir: %w", err)
	}

	if err := os.WriteFile(r.ConfigPath(), []byte(rendered), 0o600); err != nil {
		return fmt.Errorf("write alloy config: %w", err)
	}

	exporterArgs := exporterArgs(det, r.cfg.ExporterPort)
	alloyArgs := []string{
		"run",
		"--storage.path=" + filepath.Join(r.cfg.StateDir, "alloy-wal"),
		"--server.http.listen-addr=" + r.cfg.AlloyUIAddr,
	}
	if r.cfg.DisableReporting {
		alloyArgs = append(alloyArgs, "--disable-reporting")
	}
	alloyArgs = append(alloyArgs, r.ConfigPath())

	children := []supervise.Child{
		{Name: ChildExporter, Path: exporterBin, Args: exporterArgs, Dir: r.cfg.StateDir},
		{Name: ChildAlloy, Path: alloyBin, Args: alloyArgs, Dir: r.cfg.StateDir},
	}
	manifest := buildManifest(det, r.cfg, exporterBin, exporterArgs)
	manifestJSON, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	// Disk first, then a single atomic in-memory commit: a failed write
	// leaves the previous state untouched (no partial mutation, so a later
	// reload can still recover).
	if err := os.WriteFile(r.ManifestPath(), manifestJSON, 0o644); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}
	r.mu.Lock()
	r.det = det
	r.children = children
	r.manifest = manifest
	r.mu.Unlock()
	r.log.Printf("agent: detected el=%s cl=%s on %s; wrote %s",
		det.ELClient, det.CLClient, det.Target, r.ConfigPath())
	return nil
}

// Reload re-detects the target and, when the rendered pipeline changed,
// rewrites it and SIGHUPs Alloy (which reloads from disk and keeps running
// on the last valid config if the new one is broken).
func (r *Runner) Reload(ctx context.Context) error {
	r.mu.RLock()
	sup := r.sup
	r.mu.RUnlock()
	if sup == nil {
		return fmt.Errorf("reload before start")
	}
	dctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	det, err := detect.DetectWithPorts(dctx, r.cfg.Target, r.cfg.Ports)
	if err != nil {
		return err
	}
	return r.applyDetection(det)
}

// applyDetection rewrites config + manifest for det and SIGHUPs Alloy when
// the pipeline changed. Split from Reload so tests can drive it directly.
func (r *Runner) applyDetection(det *detect.Result) error {
	before, err := os.ReadFile(r.ConfigPath())
	if err != nil {
		return err
	}
	r.mu.RLock()
	prevDet := r.det
	prevChildren := r.children
	prevManifest := r.manifest
	sup := r.sup
	r.mu.RUnlock()
	// Detect chain switch: exporter binary depends on chain. A live switch
	// (ethereum <-> cosmos) requires a full agent restart — Alloy reload
	// cannot change the exporter child binary/args.
	if prevDet != nil && det.Chain != "" && prevDet.Chain != "" && det.Chain != prevDet.Chain {
		return fmt.Errorf("agent: chain changed %q -> %q; restart the agent", prevDet.Chain, det.Chain)
	}
	// On materialize failure the previous in-memory state is untouched
	// (disk-first commit), so just return the error: the next reload can
	// still recover.
	if err := r.materialize(det); err != nil {
		return err
	}
	// Child processes are stable across reloads: keep the original supervised
	// specs (exporter args, alloy bin) and only reload Alloy config via SIGHUP.
	// materialize updated r.manifest + r.det; restore children but keep the
	// new config on disk. Manifest's exporter field is reverted to match the
	// actually-running child.
	r.mu.Lock()
	r.children = prevChildren
	// Revert manifest exporter to reflect the still-running child, but keep
	// chain/target and scrape jobs from the new detection.
	runningExporter := ""
	if len(prevChildren) > 0 {
		for _, c := range prevChildren {
			if c.Name == ChildExporter {
				runningExporter = c.Path + " " + strings.Join(c.Args, " ")
				break
			}
		}
	}
	if runningExporter != "" {
		r.manifest.Exporter = runningExporter
	} else {
		r.manifest = prevManifest
	}
	manifest := r.manifest
	r.mu.Unlock()
	// Persist the reverted manifest.
	if b, err := json.MarshalIndent(manifest, "", "  "); err == nil {
		_ = os.WriteFile(r.ManifestPath(), b, 0o644)
	}
	after, err := os.ReadFile(r.ConfigPath())
	if err != nil {
		return err
	}
	if bytes.Equal(before, after) {
		r.log.Print("agent: reload found no changes")
		return nil
	}
	r.log.Print("agent: pipeline changed, signaling alloy to reload")
	return sup.Signal(ChildAlloy, syscall.SIGHUP)
}

// Start launches the children and serves /metrics + /manifest until ctx ends.
// Prepare must have run first.
func (r *Runner) Start(ctx context.Context) error {
	r.mu.RLock()
	children := r.children
	det := r.det
	r.mu.RUnlock()
	if len(children) == 0 {
		return fmt.Errorf("start before prepare")
	}
	r.registry = prometheus.NewRegistry()
	r.registry.MustRegister(prometheus.NewGoCollector())
	r.registry.MustRegister(newStatsCollector(r.ChildrenStats))
	info := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "nodebeat_agent_info",
		Help: "Agent identity and detected clients.",
	}, []string{"version", "target", "chain", "el_client", "cl_client"})
	info.WithLabelValues(version.Version, det.Target, det.Chain, det.ELClient, det.CLClient).Set(1)
	r.registry.MustRegister(info)
	up := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "nodebeat_agent_up",
		Help: "1 while the agent supervisor is running.",
	})
	up.Set(1)
	r.registry.MustRegister(up)

	r.mu.Lock()
	r.sup = supervise.New(r.log, r.cfg.SuperviseOpts, children...)
	r.mu.Unlock()

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(r.registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/manifest", func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(r.Manifest())
	})
	srv := &http.Server{Addr: r.cfg.MetricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			r.log.Printf("agent: diagnostics server: %v", err)
		}
	}()

	r.log.Printf("agent: diagnostics on http://%s/metrics http://%s/manifest",
		r.cfg.MetricsAddr, r.cfg.MetricsAddr)
	return r.sup.Run(ctx)
}

// Run prepares once, then starts supervision.
func (r *Runner) Run(ctx context.Context) error {
	if err := r.Prepare(ctx); err != nil {
		return err
	}
	return r.Start(ctx)
}

// exporterArgs maps detection endpoints to the appropriate chain exporter flags.
func exporterArgs(det *detect.Result, port int) []string {
	args := []string{"--metrics-port", strconv.Itoa(port)}
	switch det.Chain {
	case detect.ChainEthereum:
		if u := firstURL(det, detect.KindELRPC); u != "" {
			args = append(args, "--execution-url", u)
		}
		if u := firstURL(det, detect.KindCLBeacon); u != "" {
			args = append(args, "--consensus-url", u)
		}
	case detect.ChainCosmos:
		if u := firstURL(det, detect.KindCosmosRPC); u != "" {
			args = append(args, "--node", u)
		}
	}
	return args
}

func firstURL(det *detect.Result, kind string) string {
	for _, e := range det.Endpoints {
		if e.Kind == kind {
			return e.URL
		}
	}
	return ""
}

func buildManifest(det *detect.Result, cfg Config, exporterBin string, exporterArgs []string) Manifest {
	// Manifest target must match the Prometheus instance label (cfg.Instance,
	// defaulting to the detection target), not the raw target IP.
	target := cfg.Instance
	if target == "" {
		target = det.Target
	}
	hot := []string{cfg.ExporterURL}
	for _, e := range det.Endpoints {
		if e.Kind == detect.KindCLMetrics || e.Kind == detect.KindCosmosMetrics {
			hot = append(hot, e.URL)
		}
	}
	var standard []string
	for _, e := range det.Endpoints {
		if e.Kind == detect.KindELMetrics {
			standard = append(standard, e.URL)
		}
	}
	return Manifest{
		AgentVersion:   version.Version,
		GeneratedAt:    time.Now().UTC(),
		Target:         target,
		Chain:          det.Chain,
		ELClient:       det.ELClient,
		CLClient:       det.CLClient,
		RemoteWriteURL: cfg.RemoteWriteURL,
		Exporter:       strings.Join(append([]string{exporterBin}, exporterArgs...), " "),
		AlloyConfig:    filepath.Join(cfg.StateDir, "config.alloy"),
		ScrapeJobs: []ScrapeJob{
			{Name: "hot", Interval: alloycfg.HotInterval, Targets: hot},
			{Name: "standard", Interval: alloycfg.StandardInterval, Targets: standard},
			{Name: "node", Interval: alloycfg.NodeInterval, Targets: []string{"embedded prometheus.exporter.unix"}},
		},
		StateDir: cfg.StateDir,
	}
}
