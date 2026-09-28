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
	"strings"
	"text/template"

	"github.com/nodebeat/agent/internal/detect"
)

// Options tunes the rendered pipeline.
type Options struct {
	// RemoteWriteURL is the ingest endpoint, e.g.
	// https://ingest:8428/api/v1/write. Required.
	// SaaS ingest goes through the nginx proxy, which validates a per-node
	// Bearer token (see nodetoken + /internal/ingest/verify). Pass it as
	// IngestToken to render the authorization block; empty omits it
	// (dev loopback without the proxy, never public).
	RemoteWriteURL string
	IngestToken    string
	// ExporterMetricsURL is the chain metrics endpoint on the agent host:
	// the Ethereum poller (http://127.0.0.1:19090/chain/metrics) or the
	// Cosmos watcher. Defaults to http://127.0.0.1:9090/metrics, which is
	// what agents too old to send exporter_url still run.
	ExporterMetricsURL string
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
	HasTenantID      bool
	RemoteWriteURL   string
	IngestToken      string
	HasIngestToken   bool
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
		HasTenantID:      opts.TenantID != "",
		RemoteWriteURL:   opts.RemoteWriteURL,
		IngestToken:      opts.IngestToken,
		HasIngestToken:   opts.IngestToken != "",
		HotInterval:      HotInterval,
		HotTimeout:       HotTimeout,
		StandardInterval: StandardInterval,
		NodeInterval:     NodeInterval,
		NodeJob:          "prometheus.scrape.node",
	}

	// Chain-agnostic: chain metrics (Ethereum poller or cosmos-validator-watcher)
	duties := append(append([]label(nil), base...), label{Key: "role", Value: "duties"})
	if det.Chain == detect.ChainEthereum {
		duties = append(duties,
			label{Key: "__scrape_interval__", Value: ChainPollInterval},
			label{Key: "__scrape_timeout__", Value: ChainPollInterval})
	}
	if t, err := targetFromURL(exporterURL, duties); err == nil {
		p.Hot = append(p.Hot, t)
	} else {
		return "", fmt.Errorf("bad exporter metrics URL: %w", err)
	}

	switch det.Chain {
	case detect.ChainEthereum:
		// Ethereum: CL metrics on hot path, EL metrics on standard path
		for _, u := range det.MetricsURLs(detect.KindCLMetrics) {
			t, err := targetFromURL(u, append(base, label{Key: "role", Value: "consensus"}))
			if err != nil {
				return "", fmt.Errorf("bad CL metrics URL %q: %w", u, err)
			}
			p.Hot = append(p.Hot, t)
		}
		for _, u := range det.MetricsURLs(detect.KindELMetrics) {
			t, err := targetFromURL(u, append(base, label{Key: "role", Value: "execution"}))
			if err != nil {
				return "", fmt.Errorf("bad EL metrics URL %q: %w", u, err)
			}
			p.Standard = append(p.Standard, t)
		}
	case detect.ChainCosmos:
		// Cosmos: CometBFT metrics on hot path (consensus-critical)
		for _, u := range det.MetricsURLs(detect.KindCosmosMetrics) {
			t, err := targetFromURL(u, append(base, label{Key: "role", Value: "consensus"}))
			if err != nil {
				return "", fmt.Errorf("bad CometBFT metrics URL %q: %w", u, err)
			}
			p.Hot = append(p.Hot, t)
		}
		// Cosmos RPC/REST don't have native metrics endpoints we scrape
		// Standard path could include Cosmos REST metrics if needed
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
	ls := make([]label, len(labels))
	copy(ls, labels)
	sort.Slice(ls, func(i, j int) bool { return ls[i].Key < ls[j].Key })
	return staticTarget{Address: u.Host, Scheme: scheme, Path: path, Labels: ls}, nil
}

func alloyQuote(s string) string {
	r := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)
	return `"` + r.Replace(s) + `"`
}

var configTemplate = template.Must(template.New("config").Funcs(template.FuncMap{
	"q": alloyQuote,
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
{{- if .HasIngestToken}}
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
{{- if .HasTenantID}}

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
