package detect

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
)

func TestIdentifyEL(t *testing.T) {
	cases := map[string]string{
		"Geth/v1.14.0-stable-362d3a89/linux-amd64/go1.22.1": "geth",
		"Reth/v1.1.0-2c1c981/x86_64-unknown-linux-gnu":      "reth",
		"Nethermind/v1.25.0+8a5d7dc8/linux-x64/dotnet8.0.7": "nethermind",
		"erigon/v2.60.0/linux-amd64/go1.22.1":               "erigon",
		"besu/v24.7.0/linux-x86_64/openjdk-java-21":         "besu",
		"EthereumJS/1.2.3/darwin-arm64/nodejs":              "ethereumjs",
		"":                                                  "",
		"SomeFutureClient/v9.9.9":                           "",
	}
	for in, want := range cases {
		if got := IdentifyEL(in); got != want {
			t.Errorf("IdentifyEL(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestIdentifyCL(t *testing.T) {
	cases := map[string]string{
		"Lighthouse/v5.1.0-aa022f1/x86_64-linux":               "lighthouse",
		"teku/v24.8.0/linux-amd64/-privatebuild":               "teku",
		"Prysm/v5.1.0/1c3a5f9d63a44d3630d4e2e008d1b0a82b4e29f": "prysm",
		"Nimbus/v24.8.0-abc1234/linux-amd64":                   "nimbus",
		"Lodestar/v1.22.0/linux-x64/nodejs":                    "lodestar",
		"Grandine/v1.0.0/linux-amd64":                          "grandine",
		"":                                                     "",
		"MysteryClient/v9.9.9":                                 "",
	}
	for in, want := range cases {
		if got := IdentifyCL(in); got != want {
			t.Errorf("IdentifyCL(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestIdentifyCosmos(t *testing.T) {
	cases := map[string]string{
		"cometbft/1.0.0":       "cometbft",
		"tendermint/0.34.0":    "tendermint",
		"CometBFT/v1.0.0":      "cometbft",
		"Tendermint/v0.34.0":   "tendermint",
		"":                     "",
		"UnknownClient/v9.9.9": "",
	}
	for in, want := range cases {
		if got := IdentifyCosmos(in); got != want {
			t.Errorf("IdentifyCosmos(%q) = %q, want %q", in, got, want)
		}
	}
}

func portOf(t *testing.T, raw string) int {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	_, p, err := net.SplitHostPort(u.Host)
	if err != nil {
		t.Fatal(err)
	}
	n, err := strconv.Atoi(p)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return portOf(t, "http://"+l.Addr().String())
}

func rpcServer(t *testing.T, clientVersion string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      1,
			"result":  clientVersion,
		})
	}))
}

func beaconServer(t *testing.T, nodeVersion string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/eth/v1/node/version" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"data": map[string]any{"version": nodeVersion},
		})
	}))
}

func cometBFTRPCServer(t *testing.T, version string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/status" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"result": map[string]any{
				"node_info": map[string]any{
					"version": version,
					"network": "testnet",
				},
			},
		})
	}))
}

func cosmosRESTServer(t *testing.T, version string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/cosmos/base/tendermint/v1beta1/node_info" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"default_node_info": map[string]any{
				"version": version,
				"network": "testnet",
			},
		})
	}))
}

func metricsServer(t *testing.T, path string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != path {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		w.Write([]byte("# HELP test_metric\n# TYPE test_metric gauge\ntest_metric 1\n"))
	}))
}

func endpointKinds(res *Result) map[string]string {
	out := map[string]string{}
	for _, e := range res.Endpoints {
		out[e.Kind] = e.URL
	}
	return out
}

func TestDetectFullNode(t *testing.T) {
	rpc := rpcServer(t, "Geth/v1.14.0-stable/linux-amd64/go1.22")
	defer rpc.Close()
	elM := metricsServer(t, "/debug/metrics/prometheus")
	defer elM.Close()
	beacon := beaconServer(t, "Lighthouse/v5.1.0/x86_64-linux")
	defer beacon.Close()
	clM := metricsServer(t, "/metrics")
	defer clM.Close()

	res, err := detectWith(context.Background(), "127.0.0.1",
		Ports{ELRPC: portOf(t, rpc.URL), Beacon: portOf(t, beacon.URL)},
		map[string]metricsEndpoint{
			"geth": {Port: portOf(t, elM.URL), Path: "/debug/metrics/prometheus"},
		},
		map[string]metricsEndpoint{
			"lighthouse": {Port: portOf(t, clM.URL), Path: "/metrics"},
		},
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("detected: chain=%s el_client=%s cl_client=%s endpoints=%v", res.Chain, res.ELClient, res.CLClient, res.Endpoints)
	if res.Chain != ChainEthereum {
		t.Errorf("chain = %q, want %q", res.Chain, ChainEthereum)
	}
	if res.ELClient != "geth" {
		t.Errorf("el_client = %q, want geth", res.ELClient)
	}
	if res.CLClient != "lighthouse" {
		t.Errorf("cl_client = %q, want lighthouse", res.CLClient)
	}
	kinds := endpointKinds(res)
	for _, k := range []string{KindELRPC, KindELMetrics, KindCLBeacon, KindCLMetrics} {
		if _, ok := kinds[k]; !ok {
			t.Errorf("missing endpoint kind %q (have %v)", k, kinds)
		}
	}
}

func TestDetectELOnlyMetricsClosed(t *testing.T) {
	rpc := rpcServer(t, "Reth/v1.1.0/x86_64-unknown-linux-gnu")
	defer rpc.Close()

	res, err := detectWith(context.Background(), "127.0.0.1",
		Ports{ELRPC: portOf(t, rpc.URL), Beacon: freePort(t)},
		map[string]metricsEndpoint{
			"reth": {Port: freePort(t), Path: "/metrics"},
		},
		nil,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if res.ELClient != "reth" {
		t.Errorf("el_client = %q, want reth", res.ELClient)
	}
	if res.CLClient != "" {
		t.Errorf("cl_client = %q, want empty", res.CLClient)
	}
	kinds := endpointKinds(res)
	if _, ok := kinds[KindELRPC]; !ok {
		t.Errorf("missing %q (have %v)", KindELRPC, kinds)
	}
	if _, ok := kinds[KindELMetrics]; ok {
		t.Errorf("unexpected %q for closed port (have %v)", KindELMetrics, kinds)
	}
}

func TestDetectUnknownClientsStillRecorded(t *testing.T) {
	beacon := beaconServer(t, "MysteryClient/v9.9.9")
	defer beacon.Close()

	res, err := detectWith(context.Background(), "127.0.0.1",
		Ports{ELRPC: freePort(t), Beacon: portOf(t, beacon.URL)},
		nil,
		nil,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if res.CLClient != "" {
		t.Errorf("cl_client = %q, want empty for unknown client", res.CLClient)
	}
	kinds := endpointKinds(res)
	if _, ok := kinds[KindCLBeacon]; !ok {
		t.Errorf("missing %q (have %v)", KindCLBeacon, kinds)
	}
}

func TestDetectNothing(t *testing.T) {
	_, err := detectWith(context.Background(), "127.0.0.1",
		Ports{ELRPC: freePort(t), Beacon: freePort(t)},
		nil,
		nil,
		nil,
	)
	if err == nil {
		t.Error("expected error when nothing is detected, got nil")
	}
}

func TestDetectCosmosFullNode(t *testing.T) {
	cosmosRPC := cometBFTRPCServer(t, "cometbft/1.0.0")
	defer cosmosRPC.Close()
	cosmosREST := cosmosRESTServer(t, "cometbft/1.0.0")
	defer cosmosREST.Close()
	cosmosM := metricsServer(t, "/metrics")
	defer cosmosM.Close()

	res, err := detectWith(context.Background(), "127.0.0.1",
		Ports{
			CosmosRPC:  portOf(t, cosmosRPC.URL),
			CosmosREST: portOf(t, cosmosREST.URL),
		},
		nil,
		nil,
		map[string]metricsEndpoint{
			"cometbft": {Port: portOf(t, cosmosM.URL), Path: "/metrics"},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if res.Chain != ChainCosmos {
		t.Errorf("chain = %q, want %q", res.Chain, ChainCosmos)
	}
	kinds := endpointKinds(res)
	for _, k := range []string{KindCosmosRPC, KindCosmosREST, KindCosmosMetrics} {
		if _, ok := kinds[k]; !ok {
			t.Errorf("missing endpoint kind %q (have %v)", k, kinds)
		}
	}
}

func TestDetectCosmosRPCOnly(t *testing.T) {
	cosmosRPC := cometBFTRPCServer(t, "tendermint/0.34.0")
	defer cosmosRPC.Close()

	res, err := detectWith(context.Background(), "127.0.0.1",
		Ports{CosmosRPC: portOf(t, cosmosRPC.URL), CosmosREST: freePort(t)},
		nil,
		nil,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if res.Chain != ChainCosmos {
		t.Errorf("chain = %q, want %q", res.Chain, ChainCosmos)
	}
	kinds := endpointKinds(res)
	if _, ok := kinds[KindCosmosRPC]; !ok {
		t.Errorf("missing %q", KindCosmosRPC)
	}
	if _, ok := kinds[KindCosmosREST]; ok {
		t.Errorf("unexpected %q for closed port", KindCosmosREST)
	}
}
