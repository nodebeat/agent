// Package alloycfg renders a Grafana Alloy configuration from a detection
// result. The generated pipeline mirrors the Increment 1 alerting design:
//
//   - hot path (5s scrape): consensus-critical metrics (chain state from the
//     agent's Ethereum poller or the Cosmos watcher + native CL metrics)
//   - standard path (15s scrape): execution metrics
//   - node path (15s scrape): host metrics via Alloy's embedded node_exporter
//
// All three forward to a single prometheus.remote_write endpoint; Alloy owns
// WAL, retry and TLS so the agent never hand-rolls remote-write.
package alloycfg

import (
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"text/template"

	"github.com/nodebeat/agent/internal/detect"
)

// Options tunes the rendered pipeline.
type Options struct {
	// RemoteWriteURL is the ingest endpoint, e.g.
	// https://ingest:8428/api/v1/write. Required.
	RemoteWriteURL string
	// IngestTokenFile is the file holding the per-node Bearer token the
	// SaaS ingest proxy validates. Alloy reads it, so the rendered config
	// holds no secret. Empty (and no IngestToken) omits the authorization
	// block (standalone sinks without auth).
	IngestTokenFile string
	// IngestToken inlines the token instead. Only the control plane's
	// server-rendered pipeline for older agents uses it; agents render
	// with IngestTokenFile.
	IngestToken string
	// ExporterMetricsURL is the chain metrics endpoint on the agent host:
	// the Ethereum poller (http://127.0.0.1:19090/chain/metrics) or the
	// Cosmos watcher. Defaults to http://127.0.0.1:9090/metrics, which is
	// what agents too old to send exporter_url still run.
	ExporterMetricsURL string
	// DutiesMetricsURL is the Ethereum poller's validator duty endpoint
	// (http://127.0.0.1:19090/chain/duties), scraped on the standard path:
	// duties change once per epoch. Empty = no validators configured.
	DutiesMetricsURL string
	// Instance labels every series (normally the target host). Defaults to
	// the detection target.
	Instance string
	// TenantID stamps tenant_id on every series (SaaS multi-tenancy:
	// per-org VM accounts, usage metering, alert routing). Empty omits
	// the label (standalone/dev single-tenant mode).
	TenantID string
}

const defaultExporterMetricsURL = "http://127.0.0.1:9090/metrics"

// Scrape intervals shared by the renderer, the agent manifest and the docs.
// Hot carries consensus-critical metrics for the <10s paging path.
//
// ChainPollInterval overrides the hot interval for the Ethereum poller's
// target only (per-target __scrape_interval__, so the job label and series
// identity stay the same): its ~25 series are what the fastest pages read,
// while the ~10k native CL series on the same job stay at 5s.
const (
	HotInterval       = "5s"
	HotTimeout        = "4s"
	ChainPollInterval = "1s"
	StandardInterval  = "15s"
	NodeInterval      = "15s"
)

// label is one static Prometheus label on a discovery target.
type label struct {
	Key   string
	Value string
}

// staticTarget is one entry in a discovery.targets list.
type staticTarget struct {
	Address string
	Scheme  string
	Path    string
	Labels  []label
}

type pipeline struct {
	Instance         string
	Chain            string
	TenantID         string
	RemoteWriteURL   string
	IngestToken      string
	IngestTokenFile  string
	Hot              []staticTarget
	Standard         []staticTarget
	HotInterval      string
	HotTimeout       string
	StandardInterval string
	NodeInterval     string
	NodeJob          string
}

// Render builds the Alloy configuration for det.
func Render(det *detect.Result, opts Options) (string, error) {
	if det == nil {
		return "", errors.New("nil detection result")
	}
	if opts.RemoteWriteURL == "" {
		return "", errors.New("remote-write URL is required")
	}
	exporterURL := opts.ExporterMetricsURL
	if exporterURL == "" {
		exporterURL = defaultExporterMetricsURL
	}
	instance := opts.Instance
	if instance == "" {
		instance = det.Target
	}

	base := []label{
		{Key: "chain", Value: det.Chain},
		{Key: "instance", Value: instance},
	}
	if opts.TenantID != "" {
		base = append(base, label{Key: "tenant_id", Value: opts.TenantID})
	}

	p := pipeline{
		Instance:         instance,
		Chain:            det.Chain,
		TenantID:         opts.TenantID,
		RemoteWriteURL:   opts.RemoteWriteURL,
		IngestToken:      opts.IngestToken,
		IngestTokenFile:  opts.IngestTokenFile,
		HotInterval:      HotInterval,
		HotTimeout:       HotTimeout,
		StandardInterval: StandardInterval,
		NodeInterval:     NodeInterval,
		NodeJob:          "prometheus.scrape.node",
	}

	// withRole returns a fresh label set: base + role + extra.
	withRole := func(role string, extra ...label) []label {
		ls := append(append([]label(nil), base...), label{Key: "role", Value: role})
		return append(ls, extra...)
	}
	add := func(dst *[]staticTarget, what, u string, labels []label) error {
		t, err := targetFromURL(u, labels)
		if err != nil {
			return fmt.Errorf("bad %s URL %q: %w", what, u, err)
		}
		*dst = append(*dst, t)
		return nil
	}

	// Chain metrics (Ethereum poller or cosmos-validator-watcher). The
	// Ethereum poller target is scraped at ChainPollInterval.
	var chainExtra []label
	if det.Chain == detect.ChainEthereum {
		chainExtra = []label{
			{Key: "__scrape_interval__", Value: ChainPollInterval},
			{Key: "__scrape_timeout__", Value: ChainPollInterval},
		}
	}
	if err := add(&p.Hot, "exporter metrics", exporterURL, withRole("duties", chainExtra...)); err != nil {
		return "", err
	}

	switch det.Chain {
	case detect.ChainEthereum:
		// Validator duties change once per epoch: standard path.
		if opts.DutiesMetricsURL != "" {
			if err := add(&p.Standard, "duties metrics", opts.DutiesMetricsURL, withRole("validators")); err != nil {
				return "", err
			}
		}
		// CL metrics on the hot path, EL metrics on the standard path.
		for _, u := range det.MetricsURLs(detect.KindCLMetrics) {
			if err := add(&p.Hot, "CL metrics", u, withRole("consensus")); err != nil {
				return "", err
			}
		}
		for _, u := range det.MetricsURLs(detect.KindELMetrics) {
			if err := add(&p.Standard, "EL metrics", u, withRole("execution")); err != nil {
				return "", err
			}
		}
	case detect.ChainCosmos:
		// CometBFT metrics are consensus-critical: hot path.
		for _, u := range det.MetricsURLs(detect.KindCosmosMetrics) {
			if err := add(&p.Hot, "CometBFT metrics", u, withRole("consensus")); err != nil {
				return "", err
			}
		}
	}

	var sb strings.Builder
	if err := configTemplate.Execute(&sb, p); err != nil {
		return "", err
	}
	return sb.String(), nil
}

func targetFromURL(raw string, labels []label) (staticTarget, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return staticTarget{}, err
	}
	if u.Host == "" {
		return staticTarget{}, fmt.Errorf("no host in %q", raw)
	}
	scheme := u.Scheme
	if scheme == "" {
		scheme = "http"
	}
	path := u.EscapedPath()
	if path == "" {
		path = "/metrics"
	}
	sort.Slice(labels, func(i, j int) bool { return labels[i].Key < labels[j].Key })
	return staticTarget{Address: u.Host, Scheme: scheme, Path: path, Labels: labels}, nil
}

// Every interpolated value goes through q (strconv.Quote): Alloy string
// literals use Go's escape syntax, so no value can break out of its string.
var configTemplate = template.Must(template.New("config").Funcs(template.FuncMap{
	"q": strconv.Quote,
}).Parse(`// Generated by nodebeat-agent for instance {{q .Instance}} ({{q .Chain}}). DO NOT EDIT.
// Hot path (5s): consensus-critical metrics. Standard path (15s): execution.
// Node path (15s): host metrics via the embedded node_exporter.

prometheus.exporter.unix "node" {
}

prometheus.remote_write "default" {
	endpoint {
		url = {{q .RemoteWriteURL}}

		// Hot-path budget: flush at least every 1s (default 5s) so a
		// scraped fault reaches ingest in ~1s. Costs more requests, not
		// more samples.
		queue_config {
			batch_send_deadline = "1s"
		}
{{- if .IngestTokenFile}}
		authorization {
			type             = "Bearer"
			credentials_file = {{q .IngestTokenFile}}
		}
{{- else if .IngestToken}}
		authorization {
			type        = "Bearer"
			credentials = {{q .IngestToken}}
		}
{{- end}}
	}
}

prometheus.scrape "hot" {
	targets = [
{{- range .Hot}}
		{ __address__ = {{q .Address}}, __scheme__ = {{q .Scheme}}, __metrics_path__ = {{q .Path}}{{range .Labels}}, {{.Key}} = {{q .Value}}{{end}} },
{{- end}}
	]
	forward_to      = [prometheus.remote_write.default.receiver]
	scrape_interval = {{q .HotInterval}}
	scrape_timeout  = {{q .HotTimeout}}
}

prometheus.scrape "standard" {
	targets = [
{{- range .Standard}}
		{ __address__ = {{q .Address}}, __scheme__ = {{q .Scheme}}, __metrics_path__ = {{q .Path}}{{range .Labels}}, {{.Key}} = {{q .Value}}{{end}} },
{{- end}}
	]
	forward_to      = [prometheus.remote_write.default.receiver]
	scrape_interval = {{q .StandardInterval}}
}

// The unix exporter presets job="integrations/unix" and instance=<hostname>
// on its targets. Normalize to our convention so host metrics join the same
// instance label as the hot/standard jobs.
discovery.relabel "node" {
	targets = prometheus.exporter.unix.node.targets

	rule {
		target_label = "instance"
		replacement  = {{q .Instance}}
	}

	rule {
		target_label = "job"
		replacement  = {{q .NodeJob}}
	}
{{- if .TenantID}}

	rule {
		target_label = "tenant_id"
		replacement  = {{q .TenantID}}
	}
{{- end}}
}

prometheus.scrape "node" {
	targets         = discovery.relabel.node.output
	forward_to      = [prometheus.remote_write.default.receiver]
	scrape_interval = {{q .NodeInterval}}
}
`))
