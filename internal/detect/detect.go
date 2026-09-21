// Package detect identifies the chain clients running on a target host
// by probing well-known ports. Supports Ethereum (EL/CL) and Cosmos (CometBFT).
//
// The target host is always explicit: test and customer nodes run on machines
// other than the one running OpenCode, so this package must never assume
// localhost.
package detect

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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

// Ports holds the discovery ports for Ethereum and Cosmos.
// Defaults match the common client layout; tests override them.
type Ports struct {
	ELRPC      int // Ethereum JSON-RPC (default 8545)
	Beacon     int // Beacon Node API (default 5052)
	CosmosRPC  int // CometBFT RPC (default 26657)
	CosmosREST int // Cosmos REST API (default 1317)
}

// DefaultPorts returns the standard discovery ports.
func DefaultPorts() Ports {
	return Ports{
		ELRPC:      8545,
		Beacon:     5052,
		CosmosRPC:  26657,
		CosmosREST: 1317,
	}
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

// Known CometBFT native metrics endpoints (port + path).
var cometbftMetricsEndpoints = map[string]metricsEndpoint{
	"cometbft": {Port: 26660, Path: "/metrics"},
}

// Endpoint is a single reachable chain endpoint on the target host.
type Endpoint struct {
	Kind string `json:"kind"`
	URL  string `json:"url"`
}

// Result is the outcome of probing one target host.
type Result struct {
	Target    string     `json:"target"`
	Chain     string     `json:"chain"`
	ELClient  string     `json:"el_client,omitempty"`
	CLClient  string     `json:"cl_client,omitempty"`
	Endpoints []Endpoint `json:"endpoints"`
}

// MetricsURLs returns the native metrics endpoint URLs by kind.
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
	v := strings.ToLower(clientVersion)
	for _, name := range []string{"nethermind", "ethereumjs", "erigon", "besu", "reth", "geth"} {
		if strings.Contains(v, name) {
			return name
		}
	}
	return ""
}

// IdentifyCL maps a Beacon /eth/v1/node/version string to a known CL client
// name, or "" when unknown.
func IdentifyCL(nodeVersion string) string {
	v := strings.ToLower(nodeVersion)
	for _, name := range []string{"lighthouse", "grandine", "lodestar", "nimbus", "prysm", "teku"} {
		if strings.Contains(v, name) {
			return name
		}
	}
	return ""
}

// IdentifyCosmos maps a CometBFT /status node info to a known client name,
// or "" when unknown.
func IdentifyCosmos(version string) string {
	v := strings.ToLower(version)
	// CometBFT is the only one currently, but keep extensible
	if strings.Contains(v, "cometbft") {
		return "cometbft"
	}
	if strings.Contains(v, "tendermint") {
		return "tendermint"
	}
	return ""
}

// Detect probes target with the default ports and reports what it finds.
// It returns an error only when nothing chain-related is detected at all.
func Detect(ctx context.Context, target string) (*Result, error) {
	return detectWith(ctx, target, DefaultPorts(), nil, nil, nil)
}

func detectWith(ctx context.Context, target string, ports Ports, customEL, customCL, customCometBFT map[string]metricsEndpoint) (*Result, error) {
	// Use custom maps if provided, otherwise use defaults
	elM := elMetricsEndpoints
	if customEL != nil {
		elM = customEL
	}
	clM := clMetricsEndpoints
	if customCL != nil {
		clM = customCL
	}
	cometBFTM := cometbftMetricsEndpoints
	if customCometBFT != nil {
		cometBFTM = customCometBFT
	}
	httpClient := &http.Client{Timeout: 3 * time.Second}
	res := &Result{Target: target}

	// Try Ethereum EL
	rpcURL := "http://" + net.JoinHostPort(target, strconv.Itoa(ports.ELRPC))
	if version, err := probeJSONRPCClientVersion(ctx, httpClient, rpcURL); err == nil {
		res.Chain = ChainEthereum
		res.Endpoints = append(res.Endpoints, Endpoint{Kind: KindELRPC, URL: rpcURL})
		if name := IdentifyEL(version); name != "" {
			res.ELClient = name
			if me, ok := elM[name]; ok {
				if u := metricsURL(target, me); reachable(ctx, httpClient, u) {
					res.Endpoints = append(res.Endpoints, Endpoint{Kind: KindELMetrics, URL: u})
				}
			}
		}
	}

	// Try Ethereum CL
	beaconBase := "http://" + net.JoinHostPort(target, strconv.Itoa(ports.Beacon))
	if version, err := probeBeaconNodeVersion(ctx, httpClient, beaconBase+"/eth/v1/node/version"); err == nil {
		res.Chain = ChainEthereum
		res.Endpoints = append(res.Endpoints, Endpoint{Kind: KindCLBeacon, URL: beaconBase})
		if name := IdentifyCL(version); name != "" {
			res.CLClient = name
			if me, ok := clM[name]; ok {
				if u := metricsURL(target, me); reachable(ctx, httpClient, u) {
					res.Endpoints = append(res.Endpoints, Endpoint{Kind: KindCLMetrics, URL: u})
				}
			}
		}
	}

	// Try Cosmos (CometBFT) - probe RPC first
	cosmosRPC := "http://" + net.JoinHostPort(target, strconv.Itoa(ports.CosmosRPC))
	if version, err := probeCometBFTRPC(ctx, httpClient, cosmosRPC); err == nil {
		if res.Chain == "" {
			res.Chain = ChainCosmos
		}
		res.Endpoints = append(res.Endpoints, Endpoint{Kind: KindCosmosRPC, URL: cosmosRPC})
		if name := IdentifyCosmos(version); name != "" {
			if me, ok := cometBFTM[name]; ok {
				if u := metricsURL(target, me); reachable(ctx, httpClient, u) {
					res.Endpoints = append(res.Endpoints, Endpoint{Kind: KindCosmosMetrics, URL: u})
				}
			}
		}
		// Also probe REST for additional info
		cosmosREST := "http://" + net.JoinHostPort(target, strconv.Itoa(ports.CosmosREST))
		if version2, err := probeCosmosREST(ctx, httpClient, cosmosREST); err == nil {
			res.Endpoints = append(res.Endpoints, Endpoint{Kind: KindCosmosREST, URL: cosmosREST})
			if name := IdentifyCosmos(version2); name != "" {
				// REST doesn't have native metrics endpoint beyond what RPC gives
			}
		}
	}

	if len(res.Endpoints) == 0 {
		return nil, fmt.Errorf("no ethereum or cosmos endpoint detected on %s", target)
	}
	return res, nil
}

func metricsURL(target string, me metricsEndpoint) string {
	return "http://" + net.JoinHostPort(target, strconv.Itoa(me.Port)) + me.Path
}

// probeJSONRPCClientVersion calls web3_clientVersion on an EL JSON-RPC URL.
func probeJSONRPCClientVersion(ctx context.Context, httpClient *http.Client, url string) (string, error) {
	body, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "web3_clientVersion",
		"params":  []any{},
	})
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("json-rpc %s returned %s", url, resp.Status)
	}
	var out struct {
		Result string `json:"result"`
		Error  *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", fmt.Errorf("decode json-rpc response from %s: %w", url, err)
	}
	if out.Error != nil {
		return "", fmt.Errorf("json-rpc error from %s: %s", url, out.Error.Message)
	}
	if out.Result == "" {
		return "", fmt.Errorf("empty web3_clientVersion from %s", url)
	}
	return out.Result, nil
}

// probeBeaconNodeVersion reads the version field of the Beacon
// /eth/v1/node/version endpoint.
func probeBeaconNodeVersion(ctx context.Context, httpClient *http.Client, url string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("beacon %s returned %s", url, resp.Status)
	}
	var out struct {
		Data struct {
			Version string `json:"version"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", fmt.Errorf("decode beacon response from %s: %w", url, err)
	}
	if out.Data.Version == "" {
		return "", fmt.Errorf("empty node version from %s", url)
	}
	return out.Data.Version, nil
}

// probeCometBFTRPC calls /status on a CometBFT RPC endpoint.
func probeCometBFTRPC(ctx context.Context, httpClient *http.Client, url string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url+"/status", nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("cometbft rpc %s returned %s", url, resp.Status)
	}
	var out struct {
		Result struct {
			NodeInfo struct {
				Version string `json:"version"`
				Network string `json:"network"`
			} `json:"node_info"`
		} `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", fmt.Errorf("decode cometbft response from %s: %w", url, err)
	}
	if out.Result.NodeInfo.Version == "" {
		return "", fmt.Errorf("empty cometbft version from %s", url)
	}
	return out.Result.NodeInfo.Version, nil
}

// probeCosmosREST calls /cosmos/base/tendermint/v1beta1/node_info on Cosmos REST.
func probeCosmosREST(ctx context.Context, httpClient *http.Client, url string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url+"/cosmos/base/tendermint/v1beta1/node_info", nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("cosmos rest %s returned %s", url, resp.Status)
	}
	var out struct {
		DefaultNodeInfo struct {
			Version string `json:"version"`
			Network string `json:"network"`
		} `json:"default_node_info"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", fmt.Errorf("decode cosmos rest response from %s: %w", url, err)
	}
	if out.DefaultNodeInfo.Version == "" {
		return "", fmt.Errorf("empty cosmos rest version from %s", url)
	}
	return out.DefaultNodeInfo.Version, nil
}

// reachable reports whether a URL answers HTTP at all. Any status (even 404)
// counts: the port is open and serving.
func reachable(ctx context.Context, httpClient *http.Client, url string) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return false
	}
	resp.Body.Close()
	return true
}
