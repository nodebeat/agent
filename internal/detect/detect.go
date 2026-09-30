// Package detect identifies the chain clients running on a target host
// by probing well-known ports. Supports Ethereum (EL/CL) and Cosmos (CometBFT).
//
// Every probe is a read-only HTTP request to the explicit target host (never
// an assumed localhost): web3_clientVersion over JSON-RPC, GET
// /eth/v1/node/version, GET /status, GET /cosmos/base/tendermint/v1beta1/node_info,
// and a GET of each native metrics URL. Redirects are not followed and
// response bodies are capped.
package detect

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Chain constants.
const (
	ChainEthereum = "ethereum"
	ChainCosmos   = "cosmos"
)

// Endpoint kinds reported in a Result.
const (
	// Ethereum
	KindELRPC     = "el-rpc"
	KindELMetrics = "el-metrics"
	KindCLBeacon  = "cl-beacon"
	KindCLMetrics = "cl-metrics"
	// Cosmos (CometBFT/Cosmos SDK)
	KindCosmosRPC     = "cosmos-rpc"
	KindCosmosMetrics = "cosmos-metrics"
	KindCosmosREST    = "cosmos-rest"
)

const (
	probeTimeout = 3 * time.Second
	maxBody      = 1 << 20
)

// Ports holds the discovery ports for Ethereum and Cosmos.
// Discovery, P2P and Cosmos-metrics fields match the standard client layout.
// ELMetrics/CLMetrics/CosmosMetrics are overrides: 0 means "per detected
// client default" (EL 6060/9001/9545, CL 5054/8008/8080, CometBFT 26660).
// The override exists for Docker/Kurtosis devnets, where the host-side ports
// are ephemeral. Production keeps the defaults (unset = standard).
type Ports struct {
	ELRPC         int // Ethereum JSON-RPC (default 8545)
	Beacon        int // Beacon Node API (default 5052)
	ELMetrics     int // EL native metrics override (default 0 = per-client default)
	CLMetrics     int // CL native metrics override (default 0 = per-client default)
	ELP2P         int // EL P2P TCP (default 30303; onboard check only)
	CLP2P         int // CL P2P TCP (default 9000; onboard check only)
	CosmosRPC     int // CometBFT RPC (default 26657)
	CosmosREST    int // Cosmos REST API (default 1317)
	CosmosMetrics int // CometBFT metrics override (default 0 = 26660)
	CosmosP2P     int // CometBFT P2P TCP (default 26656; onboard check only)
}

// DefaultPorts returns the standard discovery ports (metrics overrides 0).
func DefaultPorts() Ports {
	return Ports{
		ELRPC:      8545,
		Beacon:     5052,
		ELP2P:      30303,
		CLP2P:      9000,
		CosmosRPC:  26657,
		CosmosREST: 1317,
		CosmosP2P:  26656,
	}
}

// WithDefaults fills every zero discovery/P2P field with its standard port.
// Metrics overrides keep 0 (= per-client default).
func (p Ports) WithDefaults() Ports {
	d := DefaultPorts()
	for _, f := range []struct {
		v *int
		d int
	}{
		{&p.ELRPC, d.ELRPC}, {&p.Beacon, d.Beacon}, {&p.ELP2P, d.ELP2P}, {&p.CLP2P, d.CLP2P},
		{&p.CosmosRPC, d.CosmosRPC}, {&p.CosmosREST, d.CosmosREST}, {&p.CosmosP2P, d.CosmosP2P},
	} {
		if *f.v == 0 {
			*f.v = f.d
		}
	}
	return p
}

// BindPortFlags registers the --*-port override flags on fs, writing into p.
// One helper so detect/run/enroll/onboard expose identical flags.
func BindPortFlags(fs *flag.FlagSet, p *Ports) {
	d := DefaultPorts()
	fs.IntVar(&p.ELRPC, "el-rpc-port", d.ELRPC, "EL JSON-RPC port on the target host")
	fs.IntVar(&p.Beacon, "beacon-port", d.Beacon, "CL Beacon API port on the target host")
	fs.IntVar(&p.ELMetrics, "el-metrics-port", 0, "EL native metrics port override (0 = per detected client default)")
	fs.IntVar(&p.CLMetrics, "cl-metrics-port", 0, "CL native metrics port override (0 = per detected client default)")
	fs.IntVar(&p.ELP2P, "el-p2p-port", d.ELP2P, "EL P2P TCP port on the target host (onboard check)")
	fs.IntVar(&p.CLP2P, "cl-p2p-port", d.CLP2P, "CL P2P TCP port on the target host (onboard check)")
	fs.IntVar(&p.CosmosRPC, "cosmos-rpc-port", d.CosmosRPC, "CometBFT RPC port on the target host")
	fs.IntVar(&p.CosmosREST, "cosmos-rest-port", d.CosmosREST, "Cosmos REST port on the target host")
	fs.IntVar(&p.CosmosMetrics, "cosmos-metrics-port", 0, "CometBFT metrics port override (0 = 26660)")
	fs.IntVar(&p.CosmosP2P, "cosmos-p2p-port", d.CosmosP2P, "CometBFT P2P TCP port on the target host (onboard check)")
}

// metricsEndpoint is a client's native Prometheus endpoint.
type metricsEndpoint struct {
	Port int
	Path string
}

// Known EL native metrics endpoints (port + path).
var elMetricsEndpoints = map[string]metricsEndpoint{
	"geth":       {Port: 6060, Path: "/debug/metrics/prometheus"},
	"reth":       {Port: 9001, Path: "/metrics"},
	"nethermind": {Port: 6060, Path: "/metrics"},
	"erigon":     {Port: 6060, Path: "/metrics"},
	"besu":       {Port: 9545, Path: "/metrics"},
}

// Known CL native metrics endpoints (port + path).
var clMetricsEndpoints = map[string]metricsEndpoint{
	"lighthouse": {Port: 5054, Path: "/metrics"},
	"teku":       {Port: 8008, Path: "/metrics"},
	"prysm":      {Port: 8080, Path: "/metrics"},
	"nimbus":     {Port: 8008, Path: "/metrics"},
	"lodestar":   {Port: 8008, Path: "/metrics"},
	"grandine":   {Port: 8008, Path: "/metrics"},
}

// CometBFT (and legacy Tendermint) all use the same metrics layout. /status
// often reports a bare version ("1.0.0", verified live on cometbft v1.0.0),
// so the client is not identified: an answering RPC is enough.
var cometbftMetrics = metricsEndpoint{Port: 26660, Path: "/metrics"}

// Endpoint is a single reachable chain endpoint on the target host.
type Endpoint struct {
	Kind string `json:"kind"`
	URL  string `json:"url"`
}

// Result is the outcome of probing one target host.
type Result struct {
	Target   string `json:"target"`
	Chain    string `json:"chain"`
	ELClient string `json:"el_client,omitempty"`
	CLClient string `json:"cl_client,omitempty"`
	// ELVersion/CLVersion are the raw web3_clientVersion and Beacon node
	// version strings (CleanVersion applied), kept even when the client is
	// not identified: they tell us which clients to support next.
	ELVersion string     `json:"el_version,omitempty"`
	CLVersion string     `json:"cl_version,omitempty"`
	Endpoints []Endpoint `json:"endpoints"`
}

// MaxVersionLen caps a reported client version string.
const MaxVersionLen = 256

// CleanVersion makes a client-reported string safe to store and show: it
// keeps printable ASCII only, trims spaces and caps the length at
// MaxVersionLen. The agent applies it before sending and the control plane
// again on receipt (the node is not trusted).
func CleanVersion(v string) string {
	b := make([]byte, 0, len(v))
	for i := 0; i < len(v); i++ {
		if c := v[i]; c >= 0x20 && c < 0x7f {
			b = append(b, c)
		}
	}
	out := strings.TrimSpace(string(b))
	if len(out) > MaxVersionLen {
		out = strings.TrimSpace(out[:MaxVersionLen])
	}
	return out
}

// MetricsURLs returns the endpoint URLs of the given kind.
func (r *Result) MetricsURLs(kind string) []string {
	var out []string
	for _, e := range r.Endpoints {
		if e.Kind == kind {
			out = append(out, e.URL)
		}
	}
	return out
}

// IdentifyEL maps a web3_clientVersion string to a known EL client name,
// or "" when unknown.
func IdentifyEL(clientVersion string) string {
	return identify(clientVersion, "nethermind", "ethereumjs", "erigon", "besu", "reth", "geth")
}

// IdentifyCL maps a Beacon /eth/v1/node/version string to a known CL client
// name, or "" when unknown.
func IdentifyCL(nodeVersion string) string {
	return identify(nodeVersion, "lighthouse", "grandine", "lodestar", "nimbus", "prysm", "teku")
}

func identify(version string, names ...string) string {
	v := strings.ToLower(version)
	for _, name := range names {
		if strings.Contains(v, name) {
			return name
		}
	}
	return ""
}

// ParseChainFilter normalizes a --chain flag for Detect: "" (auto-detect
// all families), "ethereum", or "cosmos".
func ParseChainFilter(c string) (string, error) {
	switch c = strings.ToLower(strings.TrimSpace(c)); c {
	case "", ChainEthereum, ChainCosmos:
		return c, nil
	default:
		return "", fmt.Errorf("unsupported chain %q: use ethereum or cosmos (empty = auto)", c)
	}
}

// Detect probes target and reports what it finds. Zero ports select the
// standard ones (see Ports). A non-empty chain ("ethereum"/"cosmos", see
// ParseChainFilter) restricts probing to that family, for hosts running
// both where each agent instance handles one chain. It returns an error
// only when nothing chain-related is detected at all.
func Detect(ctx context.Context, target string, ports Ports, chain string) (*Result, error) {
	ports = ports.WithDefaults()
	c := &http.Client{
		Timeout:       probeTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	base := func(port int) string { return "http://" + net.JoinHostPort(target, strconv.Itoa(port)) }
	metrics := func(me metricsEndpoint, override int) string {
		if override != 0 {
			me.Port = override
		}
		return base(me.Port) + me.Path
	}
	res := &Result{Target: target}
	add := func(kind, u string) { res.Endpoints = append(res.Endpoints, Endpoint{Kind: kind, URL: u}) }

	if chain == "" || chain == ChainEthereum {
		rpcURL := base(ports.ELRPC)
		var el struct {
			Result string `json:"result"`
		}
		rpcBody := `{"jsonrpc":"2.0","id":1,"method":"web3_clientVersion","params":[]}`
		if getJSON(ctx, c, http.MethodPost, rpcURL, rpcBody, &el) == nil && el.Result != "" {
			res.Chain = ChainEthereum
			add(KindELRPC, rpcURL)
			res.ELVersion = CleanVersion(el.Result)
			res.ELClient = IdentifyEL(el.Result)
			if me, ok := elMetricsEndpoints[res.ELClient]; ok {
				if u := metrics(me, ports.ELMetrics); reachable(ctx, c, u) {
					add(KindELMetrics, u)
				}
			}
		}

		beaconURL := base(ports.Beacon)
		var cl struct {
			Data struct {
				Version string `json:"version"`
			} `json:"data"`
		}
		if getJSON(ctx, c, http.MethodGet, beaconURL+"/eth/v1/node/version", "", &cl) == nil && cl.Data.Version != "" {
			res.Chain = ChainEthereum
			add(KindCLBeacon, beaconURL)
			res.CLVersion = CleanVersion(cl.Data.Version)
			res.CLClient = IdentifyCL(cl.Data.Version)
			if me, ok := clMetricsEndpoints[res.CLClient]; ok {
				if u := metrics(me, ports.CLMetrics); reachable(ctx, c, u) {
					add(KindCLMetrics, u)
				}
			}
		}
	}

	if chain == "" || chain == ChainCosmos {
		rpcURL := base(ports.CosmosRPC)
		var status struct {
			Result struct {
				NodeInfo struct {
					Version string `json:"version"`
				} `json:"node_info"`
			} `json:"result"`
		}
		if getJSON(ctx, c, http.MethodGet, rpcURL+"/status", "", &status) == nil && status.Result.NodeInfo.Version != "" {
			if res.Chain == "" {
				res.Chain = ChainCosmos
			}
			add(KindCosmosRPC, rpcURL)
			if u := metrics(cometbftMetrics, ports.CosmosMetrics); reachable(ctx, c, u) {
				add(KindCosmosMetrics, u)
			}
			restURL := base(ports.CosmosREST)
			var info struct {
				DefaultNodeInfo struct {
					Version string `json:"version"`
				} `json:"default_node_info"`
			}
			if getJSON(ctx, c, http.MethodGet, restURL+"/cosmos/base/tendermint/v1beta1/node_info", "", &info) == nil && info.DefaultNodeInfo.Version != "" {
				add(KindCosmosREST, restURL)
			}
		}
	}

	if len(res.Endpoints) == 0 {
		return nil, fmt.Errorf("no ethereum or cosmos endpoint detected on %s", target)
	}
	return res, nil
}

// getJSON sends one request (a JSON body when body is non-empty) and decodes
// a 200 JSON response into out.
func getJSON(ctx context.Context, c *http.Client, method, url, body string, out any) error {
	req, err := http.NewRequestWithContext(ctx, method, url, strings.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s %s: %s", method, url, resp.Status)
	}
	return json.NewDecoder(io.LimitReader(resp.Body, maxBody)).Decode(out)
}

// reachable reports whether a URL answers HTTP at all. Any status (even 404)
// counts: the port is open and serving.
func reachable(ctx context.Context, c *http.Client, url string) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false
	}
	resp, err := c.Do(req)
	if err != nil {
		return false
	}
	resp.Body.Close()
	return true
}
