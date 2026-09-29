// Package ethpoll is the agent's read-only Ethereum chain-state poller
// (spec §2.1 A). It reads the standard Beacon API and execution JSON-RPC,
// which every client implements, and exposes the eth_con_* / eth_exe_*
// series the alert rules and dashboards use. Names and labels match what
// ethereum-metrics-exporter emitted, so existing series continue across the
// upgrade instead of going stale (which would page EthereumTelemetryStale).
//
// These are the only health calls it makes (also listed in the agent
// manifest; duty calls for configured validators are in duties.go):
//
//	GET  <beacon>/eth/v1/node/syncing
//	GET  <beacon>/eth/v1/node/peer_count
//	GET  <beacon>/eth/v1/events?topics=chain_reorg   (SSE stream)
//	POST <execution> JSON-RPC batch: eth_syncing,
//	     eth_getBlockByNumber("latest", false), net_peerCount, eth_gasPrice
//
// On a failed poll the last values are kept and health_up drops to 0 at
// once: health pages immediately, and the head-stall rules still see a
// frozen head instead of a vanished series.
package ethpoll

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math/big"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Defaults: poll at the 1s scrape of the poller target so a fault is on the
// next scrape; the timeout turns a hung client into health_up=0 within 2s
// without flagging a slow-but-healthy node.
const (
	DefaultInterval = 1 * time.Second
	DefaultTimeout  = 2 * time.Second
	maxBody         = 4 << 20
)

// Calls lists every request the poller can make, for the data manifest.
var Calls = []string{
	"GET <beacon>/eth/v1/node/syncing",
	"GET <beacon>/eth/v1/node/peer_count",
	"GET <beacon>/eth/v1/events?topics=chain_reorg (SSE)",
	`POST <execution> JSON-RPC: eth_syncing, eth_getBlockByNumber("latest", false), net_peerCount, eth_gasPrice`,
}

// Config selects the endpoints. An empty URL disables that side: its
// series are not exported at all.
type Config struct {
	BeaconURL    string
	ExecutionURL string
	Interval     time.Duration
	Timeout      time.Duration
	// Validators (indices or 0x pubkeys, see ParseValidator) turns on
	// duty tracking; needs BeaconURL. Served by DutiesHandler.
	Validators []string
	// Client overrides the HTTP client (tests). It must not set Timeout:
	// the SSE stream is long-lived; per-poll deadlines come from contexts.
	Client *http.Client
}

// Poller polls one node. Create with New, run with Run, serve Handler.
type Poller struct {
	cfg    Config
	client *http.Client
	reg    *prometheus.Registry

	conUp, conHead, conDist, conSyncing, conPct prometheus.Gauge
	conPeers                                    *prometheus.GaugeVec
	reorgCount, reorgDepth                      prometheus.Counter

	exeUp, exeHead, exeTx, exeGas, exePeers, exeSyncing, exePct prometheus.Gauge

	duties *duties
}

func New(cfg Config) *Poller {
	if cfg.Interval <= 0 {
		cfg.Interval = DefaultInterval
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = DefaultTimeout
	}
	cfg.BeaconURL = strings.TrimSuffix(cfg.BeaconURL, "/")
	client := cfg.Client
	if client == nil {
		client = &http.Client{
			// Read-only against the endpoints we were given: never follow a
			// redirect somewhere else.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		}
	}
	p := &Poller{cfg: cfg, client: client, reg: prometheus.NewRegistry()}

	con := func(module, name, help string) prometheus.Gauge {
		g := prometheus.NewGauge(prometheus.GaugeOpts{Name: name, Help: help,
			ConstLabels: prometheus.Labels{"module": module, "node": "consensus"}})
		p.reg.MustRegister(g)
		return g
	}
	exe := func(module, name, help string, extra prometheus.Labels) prometheus.Gauge {
		l := prometheus.Labels{"module": module, "ethereum_role": "execution", "node_name": "execution"}
		maps.Copy(l, extra)
		g := prometheus.NewGauge(prometheus.GaugeOpts{Name: name, Help: help, ConstLabels: l})
		p.reg.MustRegister(g)
		return g
	}

	if cfg.BeaconURL != "" {
		p.conUp = con("health", "eth_con_health_up", "1 if the last Beacon API poll succeeded.")
		p.conHead = con("sync", "eth_con_sync_head_slot", "Beacon node head slot.")
		p.conDist = con("sync", "eth_con_sync_distance", "Slots between the head and the wall-clock slot.")
		p.conSyncing = con("sync", "eth_con_sync_is_syncing", "1 if the beacon node reports syncing.")
		p.conPct = con("sync", "eth_con_sync_percentage", "head_slot / (head_slot + sync_distance) * 100.")
		p.conPeers = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "eth_con_peers",
			Help:        "Beacon node peers by state.",
			ConstLabels: prometheus.Labels{"module": "general", "node": "consensus"}}, []string{"state"})
		p.reg.MustRegister(p.conPeers)
		counter := func(name, help string) prometheus.Counter {
			c := prometheus.NewCounter(prometheus.CounterOpts{Name: name, Help: help,
				ConstLabels: prometheus.Labels{"module": "beacon", "node": "consensus"}})
			p.reg.MustRegister(c)
			return c
		}
		p.reorgCount = counter("eth_con_beacon_reorg_count", "chain_reorg events seen.")
		p.reorgDepth = counter("eth_con_beacon_reorg_depth", "Sum of chain_reorg depths seen.")
	}
	if cfg.ExecutionURL != "" {
		p.exeUp = exe("health", "eth_exe_health_up", "1 if the last JSON-RPC poll succeeded.", nil)
		p.exeHead = exe("block", "eth_exe_block_most_recent_number", "Latest execution block number.",
			prometheus.Labels{"identifier": "head"})
		p.exeTx = exe("block", "eth_exe_block_head_transactions_in_block", "Transactions in the latest block.", nil)
		p.exeGas = exe("general", "eth_exe_gas_price_gwei", "eth_gasPrice in gwei.", nil)
		p.exePeers = exe("web3", "eth_exe_net_peer_count", "net_peerCount.", nil)
		p.exeSyncing = exe("sync", "eth_exe_sync_is_syncing", "1 if eth_syncing reports progress.", nil)
		p.exePct = exe("sync", "eth_exe_sync_percentage", "currentBlock / highestBlock * 100 (100 when not syncing).", nil)
	}
	if cfg.BeaconURL != "" && len(cfg.Validators) > 0 {
		p.duties = newDuties(p, cfg.Validators)
	}
	return p
}

// Handler serves the poller's series in Prometheus text format.
func (p *Poller) Handler() http.Handler {
	return promhttp.HandlerFor(p.reg, promhttp.HandlerOpts{})
}

// Run polls until ctx ends.
func (p *Poller) Run(ctx context.Context) {
	var wg sync.WaitGroup
	if p.cfg.BeaconURL != "" {
		wg.Go(func() { p.reorgStream(ctx) })
	}
	if p.duties != nil {
		wg.Go(func() { p.duties.run(ctx) })
	}
	t := time.NewTicker(p.cfg.Interval)
	defer t.Stop()
	for {
		p.PollOnce(ctx)
		select {
		case <-ctx.Done():
			wg.Wait()
			return
		case <-t.C:
		}
	}
}

// PollOnce runs one consensus + execution poll concurrently.
func (p *Poller) PollOnce(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, p.cfg.Timeout)
	defer cancel()
	var wg sync.WaitGroup
	if p.cfg.BeaconURL != "" {
		wg.Go(func() { p.pollConsensus(ctx) })
	}
	if p.cfg.ExecutionURL != "" {
		wg.Go(func() { p.pollExecution(ctx) })
	}
	wg.Wait()
}

func (p *Poller) pollConsensus(ctx context.Context) {
	var syncing struct {
		Data struct {
			HeadSlot     string `json:"head_slot"`
			SyncDistance string `json:"sync_distance"`
			IsSyncing    bool   `json:"is_syncing"`
		} `json:"data"`
	}
	if err := p.getJSON(ctx, "/eth/v1/node/syncing", &syncing); err != nil {
		p.conUp.Set(0)
		return
	}
	head, err1 := strconv.ParseUint(syncing.Data.HeadSlot, 10, 64)
	dist, err2 := strconv.ParseUint(syncing.Data.SyncDistance, 10, 64)
	if err1 != nil || err2 != nil {
		p.conUp.Set(0)
		return
	}
	p.conUp.Set(1)
	p.conHead.Set(float64(head))
	p.conDist.Set(float64(dist))
	p.conSyncing.Set(b2f(syncing.Data.IsSyncing))
	p.conPct.Set(pct(float64(head), float64(head+dist)))

	// Peers are informational: a failure here does not mark the node down.
	var peers struct {
		Data map[string]string `json:"data"`
	}
	if err := p.getJSON(ctx, "/eth/v1/node/peer_count", &peers); err == nil {
		for _, state := range []string{"connected", "connecting", "disconnected", "disconnecting"} {
			if n, err := strconv.ParseUint(peers.Data[state], 10, 64); err == nil {
				p.conPeers.WithLabelValues(state).Set(float64(n))
			}
		}
	}
}

func (p *Poller) getJSON(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.cfg.BeaconURL+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// 206 = "syncing" on some endpoints; the body is still valid.
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("GET %s: %s", path, resp.Status)
	}
	return json.NewDecoder(io.LimitReader(resp.Body, maxBody)).Decode(out)
}

type rpcReq struct {
	JSONRPC string `json:"jsonrpc"`
	ID      int    `json:"id"`
	Method  string `json:"method"`
	Params  []any  `json:"params"`
}

type rpcResp struct {
	ID     int             `json:"id"`
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Message string `json:"message"`
	} `json:"error"`
}

const (
	idSyncing = iota + 1
	idBlock
	idPeers
	idGas
)

func (p *Poller) pollExecution(ctx context.Context) {
	batch := []rpcReq{
		{"2.0", idSyncing, "eth_syncing", []any{}},
		{"2.0", idBlock, "eth_getBlockByNumber", []any{"latest", false}},
		{"2.0", idPeers, "net_peerCount", []any{}},
		{"2.0", idGas, "eth_gasPrice", []any{}},
	}
	res, err := p.rpcBatch(ctx, batch)
	if err != nil {
		p.exeUp.Set(0)
		return
	}
	var block struct {
		Number       string            `json:"number"`
		Transactions []json.RawMessage `json:"transactions"`
	}
	raw, ok := res[idBlock]
	if !ok || json.Unmarshal(raw, &block) != nil {
		p.exeUp.Set(0)
		return
	}
	num, err := hexUint(block.Number)
	if err != nil {
		p.exeUp.Set(0)
		return
	}
	p.exeUp.Set(1)
	p.exeHead.Set(float64(num))
	p.exeTx.Set(float64(len(block.Transactions)))

	if raw, ok := res[idSyncing]; ok {
		var progress struct {
			CurrentBlock string `json:"currentBlock"`
			HighestBlock string `json:"highestBlock"`
		}
		switch {
		case bytes.Equal(bytes.TrimSpace(raw), []byte("false")):
			p.exeSyncing.Set(0)
			p.exePct.Set(100)
		case json.Unmarshal(raw, &progress) == nil:
			cur, err1 := hexUint(progress.CurrentBlock)
			high, err2 := hexUint(progress.HighestBlock)
			p.exeSyncing.Set(1)
			if err1 == nil && err2 == nil {
				p.exePct.Set(pct(float64(cur), float64(high)))
			}
		}
	}
	if n, err := hexResult(res, idPeers); err == nil {
		p.exePeers.Set(float64(n.Uint64()))
	}
	if wei, err := hexResult(res, idGas); err == nil {
		gwei, _ := new(big.Float).Quo(new(big.Float).SetInt(wei), big.NewFloat(1e9)).Float64()
		p.exeGas.Set(gwei)
	}
}

// rpcBatch returns successful results by id; per-call errors are dropped
// (e.g. a node with the net namespace disabled still reports blocks).
func (p *Poller) rpcBatch(ctx context.Context, batch []rpcReq) (map[int]json.RawMessage, error) {
	body, err := json.Marshal(batch)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.cfg.ExecutionURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("JSON-RPC: %s", resp.Status)
	}
	var out []rpcResp
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxBody)).Decode(&out); err != nil {
		return nil, err
	}
	res := make(map[int]json.RawMessage, len(out))
	for _, r := range out {
		if r.Error == nil && len(r.Result) > 0 && string(r.Result) != "null" {
			res[r.ID] = r.Result
		}
	}
	return res, nil
}

// reorgStream follows the Beacon SSE chain_reorg topic, reconnecting with
// backoff. Reorgs cannot be seen by polling the head.
func (p *Poller) reorgStream(ctx context.Context) {
	backoff := time.Second
	for ctx.Err() == nil {
		start := time.Now()
		_ = p.readReorgs(ctx)
		if time.Since(start) > time.Minute {
			backoff = time.Second
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func (p *Poller) readReorgs(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.cfg.BeaconURL+"/eth/v1/events?topics=chain_reorg", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "text/event-stream")
	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("events: %s", resp.Status)
	}
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	event := ""
	for sc.Scan() {
		line := sc.Text()
		switch {
		case line == "":
			event = ""
		case strings.HasPrefix(line, "event:"):
			event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		case strings.HasPrefix(line, "data:") && event == "chain_reorg":
			var ev struct {
				Depth string `json:"depth"`
			}
			if json.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(line, "data:"))), &ev) != nil {
				continue
			}
			p.reorgCount.Inc()
			if d, err := strconv.ParseUint(ev.Depth, 10, 64); err == nil {
				p.reorgDepth.Add(float64(d))
			}
		}
	}
	if err := sc.Err(); err != nil {
		return err
	}
	return errors.New("events: stream closed")
}

func hexResult(res map[int]json.RawMessage, id int) (*big.Int, error) {
	raw, ok := res[id]
	if !ok {
		return nil, errors.New("missing")
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return nil, err
	}
	n, ok := new(big.Int).SetString(strings.TrimPrefix(s, "0x"), 16)
	if !ok || !strings.HasPrefix(s, "0x") {
		return nil, fmt.Errorf("bad quantity %q", s)
	}
	return n, nil
}

func hexUint(s string) (uint64, error) {
	if !strings.HasPrefix(s, "0x") {
		return 0, fmt.Errorf("bad quantity %q", s)
	}
	return strconv.ParseUint(s[2:], 16, 64)
}

func pct(part, whole float64) float64 {
	if whole <= 0 {
		return 100
	}
	return part / whole * 100
}

func b2f(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
