// Package agent is the nodebeat-agent supervisor: detect the chain
// client(s) on a target host, render an Alloy pipeline, and run Alloy (plus,
// for Cosmos, the bundled validator watcher) as supervised child processes.
// Ethereum chain state comes from the in-process ethpoll poller instead of
// a child exporter.
//
// Trust model: the agent only supervises its own children and writes inside
// its state dir. It never SSHes anywhere, never restarts the validator, and
// neither it nor its children listen beyond loopback: the poller is served
// on the agent's own localhost diagnostics server and the Cosmos watcher is
// bound to 127.0.0.1 (only the local Alloy scrapes either).
package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
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
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/nodebeat/agent/internal/alloycfg"
	"github.com/nodebeat/agent/internal/detect"
	"github.com/nodebeat/agent/internal/ethpoll"
	"github.com/nodebeat/agent/internal/supervise"
	"github.com/nodebeat/agent/internal/version"
)

// Child names, used for metrics, logs and reload signaling.
const (
	ChildExporter = "exporter"
	ChildAlloy    = "alloy"
)

// ChainMetricsPath serves the Ethereum poller on the diagnostics server;
// ChainDutiesPath its validator duty series (only with Validators set).
const (
	ChainMetricsPath = "/chain/metrics"
	ChainDutiesPath  = "/chain/duties"
)

// Config tunes one agent run. Zero values select documented defaults via
// withDefaults, except DisableReporting which the CLI defaults to true.
type Config struct {
	Target         string
	RemoteWriteURL string
	// Instance labels every series; default Target. Enrolled agents use the
	// portal node name and follow renames via SetInstance.
	Instance string
	// TenantID stamps tenant_id on every series (enrolled agents; the
	// ingest proxy enforces the tenant from the token regardless).
	TenantID string
	// IngestTokenFile holds the remote_write Bearer token; Alloy reads it,
	// so config.alloy holds no secret. Empty = no auth block.
	IngestTokenFile  string
	ExporterBin      string // Cosmos only; default "cosmos-validator-watcher"
	AlloyBin         string // default "alloy"
	ExporterPort     int    // Cosmos watcher port; default 9090
	ExporterURL      string // chain metrics URL; default per chain, see ChainMetricsURL
	StateDir         string // default ".nodebeat", made absolute; the only writable path
	MetricsAddr      string // default "127.0.0.1:19090" (localhost only)
	AlloyUIAddr      string // default "127.0.0.1:12345" (localhost only)
	DisableReporting bool   // pass --disable-reporting to Alloy
	// Ports overrides target probing (devnet ephemeral host ports).
	// Zero value = standard ports (metrics 0 = per-client default).
	Ports detect.Ports
	// Chain restricts detection to one family ("ethereum"/"cosmos") on
	// mixed hosts where each agent instance handles one chain. Empty =
	// probe all families.
	Chain string
	// Validators turns on validator duty tracking: Ethereum indices or 0x
	// pubkeys for the poller (US-1.12), Cosmos consensus addresses for the
	// watcher's --validator (US-1.7/1.13). Normalized per chain once
	// detection is known; see NormalizeValidators.
	Validators    []string
	SuperviseOpts supervise.Options
}

func (c Config) withDefaults() Config {
	// ExporterBin and ExporterURL depend on the detected chain, so they are
	// resolved in materializeWithConfig / chainMetricsURL.
	if c.AlloyBin == "" {
		c.AlloyBin = "alloy"
	}
	if c.ExporterPort == 0 {
		c.ExporterPort = 9090
	}
	if c.StateDir == "" {
		c.StateDir = ".nodebeat"
	}
	// Absolute: children run with the state dir as working directory and
	// get paths inside it as arguments.
	if abs, err := filepath.Abs(c.StateDir); err == nil {
		c.StateDir = abs
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

// ChainMetricsURL is where Alloy's hot job scrapes chain metrics: override
// when set; for Ethereum the poller on the agent's diagnostics server
// (metricsAddr); otherwise the Cosmos watcher's loopback port. Enrolled
// agents send it on config polls so the server-rendered pipeline scrapes it.
func ChainMetricsURL(chain string, port int, metricsAddr, override string) string {
	if override != "" {
		return override
	}
	if chain == detect.ChainEthereum {
		return agentURL(metricsAddr, ChainMetricsPath)
	}
	return "http://127.0.0.1:" + strconv.Itoa(port) + "/metrics"
}

// ChainDutiesURL is where Alloy's standard job scrapes validator duties:
// the poller's duty endpoint on the diagnostics server, or "" when no
// validators are configured (no target, no series).
func ChainDutiesURL(chain, metricsAddr string, validators []string) string {
	if chain != detect.ChainEthereum || len(validators) == 0 {
		return ""
	}
	return agentURL(metricsAddr, ChainDutiesPath)
}

func agentURL(metricsAddr, path string) string {
	host, p, err := net.SplitHostPort(metricsAddr)
	if err != nil {
		host, p = "127.0.0.1", "19090"
	}
	if ip := net.ParseIP(host); host == "" || (ip != nil && ip.IsUnspecified()) {
		host = "127.0.0.1"
	}
	return "http://" + net.JoinHostPort(host, p) + path
}

func (r *Runner) chainMetricsURL(det *detect.Result) string {
	return ChainMetricsURL(det.Chain, r.cfg.ExporterPort, r.cfg.MetricsAddr, r.cfg.ExporterURL)
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
	AgentVersion   string    `json:"agent_version"`
	GeneratedAt    time.Time `json:"generated_at"`
	Target         string    `json:"target"`
	Chain          string    `json:"chain"`
	ELClient       string    `json:"el_client,omitempty"`
	CLClient       string    `json:"cl_client,omitempty"`
	RemoteWriteURL string    `json:"remote_write_url"`
	Exporter       string    `json:"exporter"`
	// ChainAPICalls lists every request the Ethereum poller makes against
	// the node (empty for Cosmos), including duty calls when Validators
	// are set.
	ChainAPICalls []string `json:"chain_api_calls,omitempty"`
	// Validators are the configured validators whose duties are tracked.
	Validators  []string    `json:"validators,omitempty"`
	AlloyConfig string      `json:"alloy_config"`
	ScrapeJobs  []ScrapeJob `json:"scrape_jobs"`
	StateDir    string      `json:"state_dir"`
}

// Runner holds the prepared state of one agent run.
type Runner struct {
	cfg Config
	log *log.Logger
	// writeMu serializes apply, the only writer of the pipeline files.
	writeMu sync.Mutex
	// mu guards instance, det, children, manifest, sup and the poller
	// fields: Reload (SIGHUP goroutine) and SetInstance (enrolled heartbeat
	// loop) race with Start and the diagnostics server. Take the lock for
	// every access, never across disk/network IO. cfg is only written by
	// fixChildren, before Start.
	mu       sync.RWMutex
	instance string
	det      *detect.Result
	children []supervise.Child
	manifest Manifest
	sup      *supervise.Supervisor
	registry *prometheus.Registry
	// poller serves ChainMetricsPath (Ethereum only); pollCancel stops it
	// and runCtx is Start's context, so reloads can replace the poller.
	poller     *ethpoll.Poller
	pollCancel context.CancelFunc
	runCtx     context.Context
}

// New builds a Runner. Call Prepare, then Start (or Run for both).
func New(cfg Config, logger *log.Logger) *Runner {
	if logger == nil {
		logger = log.New(os.Stderr, "", log.LstdFlags)
	}
	cfg = cfg.withDefaults()
	return &Runner{cfg: cfg, instance: cfg.Instance, log: logger}
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
	det, err := r.detect(ctx)
	if err != nil {
		return err
	}
	return r.materialize(det)
}

func (r *Runner) detect(ctx context.Context) (*detect.Result, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	return detect.Detect(ctx, r.cfg.Target, r.cfg.Ports, r.cfg.Chain)
}

// materialize renders and writes the pipeline for det. Split from Prepare
// so tests can run it without network access.
func (r *Runner) materialize(det *detect.Result) error {
	_, err := r.apply(det)
	return err
}

// apply is the only writer of the pipeline: it renders the Alloy config for
// det locally (never from the network), then writes config.alloy (0600) and
// manifest.json and commits det + manifest in memory. Disk first: a failed
// write leaves the previous in-memory state untouched, so a later reload
// can still recover. changed reports whether config.alloy differs from
// what was there before. Callers are serialized by writeMu.
func (r *Runner) apply(det *detect.Result) (changed bool, err error) {
	r.writeMu.Lock()
	defer r.writeMu.Unlock()
	if err := r.fixChildren(det); err != nil {
		return false, err
	}
	r.mu.RLock()
	instance, children := r.instance, r.children
	r.mu.RUnlock()
	rendered, err := alloycfg.Render(det, alloycfg.Options{
		RemoteWriteURL:     r.cfg.RemoteWriteURL,
		IngestTokenFile:    r.cfg.IngestTokenFile,
		ExporterMetricsURL: r.chainMetricsURL(det),
		DutiesMetricsURL:   ChainDutiesURL(det.Chain, r.cfg.MetricsAddr, r.cfg.Validators),
		Instance:           instance,
		TenantID:           r.cfg.TenantID,
	})
	if err != nil {
		return false, err
	}
	if err := os.MkdirAll(r.cfg.StateDir, 0o750); err != nil {
		return false, fmt.Errorf("create state dir: %w", err)
	}
	before, _ := os.ReadFile(r.ConfigPath())
	if err := os.WriteFile(r.ConfigPath(), []byte(rendered), 0o600); err != nil {
		return false, fmt.Errorf("write alloy config: %w", err)
	}
	manifest := buildManifest(det, r.cfg, instance, r.chainMetricsURL(det), children)
	manifestJSON, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return false, err
	}
	if err := os.WriteFile(r.ManifestPath(), manifestJSON, 0o644); err != nil {
		return false, fmt.Errorf("write manifest: %w", err)
	}
	r.mu.Lock()
	r.det = det
	r.manifest = manifest
	r.mu.Unlock()
	r.log.Printf("agent: detected el=%s cl=%s on %s; wrote %s",
		det.ELClient, det.CLClient, det.Target, r.ConfigPath())
	return string(before) != rendered, nil
}

// fixChildren runs once, on the first prepare: it normalizes --validators
// for the detected chain and resolves the child processes. Both stay fixed
// for the life of the Runner (an Alloy reload cannot change a child's
// binary or args), so r.cfg is never written after this.
func (r *Runner) fixChildren(det *detect.Result) error {
	r.mu.RLock()
	done := r.children != nil
	r.mu.RUnlock()
	if done {
		return nil
	}
	vals, err := NormalizeValidators(det.Chain, r.cfg.Validators)
	if err != nil {
		return fmt.Errorf("--validators: %w", err)
	}

	var children []supervise.Child
	// Ethereum is polled in-process; only Cosmos runs a child exporter.
	if det.Chain == detect.ChainCosmos {
		name := r.cfg.ExporterBin
		if name == "" {
			name = "cosmos-validator-watcher"
		}
		bin, err := exec.LookPath(name)
		if err != nil {
			return fmt.Errorf("exporter binary %q: %w", name, err)
		}
		children = append(children, supervise.Child{Name: ChildExporter, Path: bin,
			Args: exporterArgs(det, r.cfg.ExporterPort, vals), Dir: r.cfg.StateDir})
	}
	alloyBin, err := exec.LookPath(r.cfg.AlloyBin)
	if err != nil {
		return fmt.Errorf("alloy binary %q: %w", r.cfg.AlloyBin, err)
	}
	alloyArgs := []string{
		"run",
		"--storage.path=" + filepath.Join(r.cfg.StateDir, "alloy-wal"),
		"--server.http.listen-addr=" + r.cfg.AlloyUIAddr,
	}
	if r.cfg.DisableReporting {
		alloyArgs = append(alloyArgs, "--disable-reporting")
	}
	alloyArgs = append(alloyArgs, r.ConfigPath())
	children = append(children, supervise.Child{Name: ChildAlloy, Path: alloyBin, Args: alloyArgs, Dir: r.cfg.StateDir})

	r.cfg.Validators = vals
	r.mu.Lock()
	r.children = children
	r.mu.Unlock()
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
	det, err := r.detect(ctx)
	if err != nil {
		return err
	}
	return r.applyDetection(det)
}

// applyDetection rewrites config + manifest for det and SIGHUPs Alloy when
// the pipeline changed. Children are fixed (see fixChildren), so a chain
// switch needs an agent restart; the in-process poller has no such limit and
// is replaced when the endpoints move.
func (r *Runner) applyDetection(det *detect.Result) error {
	r.mu.RLock()
	prev := r.det
	r.mu.RUnlock()
	if prev != nil && det.Chain != prev.Chain {
		return fmt.Errorf("agent: chain changed %q -> %q; restart the agent", prev.Chain, det.Chain)
	}
	changed, err := r.apply(det)
	if err != nil {
		return err
	}
	if det.Chain == detect.ChainEthereum && !sameEndpoints(prev, det) {
		r.startPoller(det)
	}
	return r.reloadAlloy(changed)
}

// SetInstance changes the instance label (an enrolled node renamed in the
// portal) and reloads Alloy when the pipeline changed.
func (r *Runner) SetInstance(name string) error {
	r.mu.Lock()
	same := name == "" || name == r.instance
	if !same {
		r.instance = name
	}
	det := r.det
	r.mu.Unlock()
	if same || det == nil { // not prepared yet: Prepare renders the new name
		return nil
	}
	r.log.Printf("agent: instance renamed to %q", name)
	changed, err := r.apply(det)
	if err != nil {
		return err
	}
	return r.reloadAlloy(changed)
}

// reloadAlloy SIGHUPs Alloy after a config change (nothing to do before
// Start: Alloy reads the file when it starts).
func (r *Runner) reloadAlloy(changed bool) error {
	r.mu.RLock()
	sup := r.sup
	r.mu.RUnlock()
	if !changed {
		r.log.Print("agent: pipeline unchanged")
		return nil
	}
	if sup == nil {
		return nil
	}
	r.log.Print("agent: pipeline changed, signaling alloy to reload")
	return sup.Signal(ChildAlloy, syscall.SIGHUP)
}

// Start launches the children and serves the read-only diagnostics
// (/metrics, /manifest and the poller's series) on MetricsAddr until ctx
// ends. Prepare must have run first.
func (r *Runner) Start(ctx context.Context) error {
	r.mu.RLock()
	children, det := r.children, r.det
	r.mu.RUnlock()
	if len(children) == 0 || det == nil {
		return fmt.Errorf("start before prepare")
	}
	r.registry = prometheus.NewRegistry()
	r.registry.MustRegister(collectors.NewGoCollector())
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
	r.runCtx = ctx
	r.mu.Unlock()
	if det.Chain == detect.ChainEthereum {
		r.startPoller(det)
	}

	// pollerHandler serves one of the current poller's handlers (the poller
	// is replaced on reloads), or 404 when there is none.
	pollerHandler := func(h func(*ethpoll.Poller) http.Handler) http.HandlerFunc {
		return func(w http.ResponseWriter, req *http.Request) {
			r.mu.RLock()
			p := r.poller
			r.mu.RUnlock()
			if p == nil || h(p) == nil {
				http.NotFound(w, req)
				return
			}
			h(p).ServeHTTP(w, req)
		}
	}
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(r.registry, promhttp.HandlerOpts{}))
	mux.Handle("GET "+ChainMetricsPath, pollerHandler((*ethpoll.Poller).Handler))
	mux.Handle("GET "+ChainDutiesPath, pollerHandler((*ethpoll.Poller).DutiesHandler))
	mux.HandleFunc("GET /manifest", func(w http.ResponseWriter, req *http.Request) {
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

// startPoller (re)starts the Ethereum poller for det's endpoints. Called
// from Start and on reloads that change endpoints; the old poller stops.
func (r *Runner) startPoller(det *detect.Result) {
	p := ethpoll.New(ethpoll.Config{
		BeaconURL:    firstURL(det, detect.KindCLBeacon),
		ExecutionURL: firstURL(det, detect.KindELRPC),
		Validators:   r.cfg.Validators,
	})
	r.mu.Lock()
	if r.runCtx == nil {
		r.mu.Unlock()
		return
	}
	if r.pollCancel != nil {
		r.pollCancel()
	}
	ctx, cancel := context.WithCancel(r.runCtx)
	r.poller, r.pollCancel = p, cancel
	r.mu.Unlock()
	go p.Run(ctx)
}

func sameEndpoints(a, b *detect.Result) bool {
	if a == nil || b == nil {
		return a == b
	}
	return firstURL(a, detect.KindCLBeacon) == firstURL(b, detect.KindCLBeacon) &&
		firstURL(a, detect.KindELRPC) == firstURL(b, detect.KindELRPC)
}

// Run prepares once, then starts supervision.
func (r *Runner) Run(ctx context.Context) error {
	if err := r.Prepare(ctx); err != nil {
		return err
	}
	return r.Start(ctx)
}

// exporterArgs maps detection endpoints to cosmos-validator-watcher flags.
// It serves /metrics on --http-addr (default :8080); we pin
// 127.0.0.1:<port> to match the chain metrics URL — only the co-located
// Alloy scrapes it.
//
// Without --validator the watcher tracks no validator and emits chain-level
// series only, so the jail / missed-block / voting-power rules have nothing
// to read: each configured consensus address is passed through.
func exporterArgs(det *detect.Result, port int, validators []string) []string {
	args := []string{"--http-addr", "127.0.0.1:" + strconv.Itoa(port)}
	if u := firstURL(det, detect.KindCosmosRPC); u != "" {
		args = append(args, "--node", u)
	}
	for _, v := range validators {
		args = append(args, "--validator", v)
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

// buildManifest describes the pipeline for det. Its target is the
// instance label, not the raw target IP.
func buildManifest(det *detect.Result, cfg Config, instance, chainURL string, children []supervise.Child) Manifest {
	hot := []string{chainURL}
	for _, e := range det.Endpoints {
		if e.Kind == detect.KindCLMetrics || e.Kind == detect.KindCosmosMetrics {
			hot = append(hot, e.URL)
		}
	}
	var standard []string
	if u := ChainDutiesURL(det.Chain, cfg.MetricsAddr, cfg.Validators); u != "" {
		standard = append(standard, u)
	}
	for _, e := range det.Endpoints {
		if e.Kind == detect.KindELMetrics {
			standard = append(standard, e.URL)
		}
	}
	var exporter string
	var calls []string
	if det.Chain == detect.ChainEthereum {
		exporter = fmt.Sprintf("built-in ethpoll (beacon=%s execution=%s, every %s)",
			firstURL(det, detect.KindCLBeacon), firstURL(det, detect.KindELRPC), ethpoll.DefaultInterval)
		calls = ethpoll.Calls
		if len(cfg.Validators) > 0 {
			calls = append(append([]string(nil), calls...), ethpoll.DutyCalls...)
		}
	}
	for _, c := range children {
		if c.Name == ChildExporter {
			exporter = strings.Join(append([]string{c.Path}, c.Args...), " ")
		}
	}
	return Manifest{
		AgentVersion:   version.Version,
		GeneratedAt:    time.Now().UTC(),
		Target:         instance,
		Chain:          det.Chain,
		ELClient:       det.ELClient,
		CLClient:       det.CLClient,
		RemoteWriteURL: cfg.RemoteWriteURL,
		Exporter:       exporter,
		ChainAPICalls:  calls,
		Validators:     cfg.Validators,
		AlloyConfig:    filepath.Join(cfg.StateDir, "config.alloy"),
		ScrapeJobs: []ScrapeJob{
			{Name: "hot", Interval: alloycfg.HotInterval, Targets: hot},
			{Name: "standard", Interval: alloycfg.StandardInterval, Targets: standard},
			{Name: "node", Interval: alloycfg.NodeInterval, Targets: []string{"embedded prometheus.exporter.unix"}},
		},
		StateDir: cfg.StateDir,
	}
}
