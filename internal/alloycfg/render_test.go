package alloycfg

import (
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nodebeat/agent/internal/detect"
)

var update = flag.Bool("update", false, "rewrite golden files")

func fullDetection() *detect.Result {
	return &detect.Result{
		Target:   "node1.example",
		Chain:    detect.ChainEthereum,
		ELClient: "geth",
		CLClient: "lighthouse",
		Endpoints: []detect.Endpoint{
			{Kind: detect.KindELRPC, URL: "http://node1.example:8545"},
			{Kind: detect.KindELMetrics, URL: "http://node1.example:6060/debug/metrics/prometheus"},
			{Kind: detect.KindCLBeacon, URL: "http://node1.example:5052"},
			{Kind: detect.KindCLMetrics, URL: "http://node1.example:5054/metrics"},
		},
	}
}

func partialDetection() *detect.Result {
	return &detect.Result{
		Target:   "node2.example",
		Chain:    detect.ChainEthereum,
		ELClient: "reth",
		Endpoints: []detect.Endpoint{
			{Kind: detect.KindELRPC, URL: "http://node2.example:8545"},
			{Kind: detect.KindELMetrics, URL: "http://node2.example:9001/metrics"},
		},
	}
}

func cosmosDetection() *detect.Result {
	return &detect.Result{
		Target:   "node3.example",
		Chain:    detect.ChainCosmos,
		Endpoints: []detect.Endpoint{
			{Kind: detect.KindCosmosRPC, URL: "http://node3.example:26657"},
			{Kind: detect.KindCosmosREST, URL: "http://node3.example:1317"},
			{Kind: detect.KindCosmosMetrics, URL: "http://node3.example:26660/metrics"},
		},
	}
}

func TestRenderValidation(t *testing.T) {
	if _, err := Render(nil, Options{RemoteWriteURL: "http://x:8428/api/v1/write"}); err == nil {
		t.Error("expected error for nil detection, got nil")
	}
	if _, err := Render(fullDetection(), Options{}); err == nil {
		t.Error("expected error for missing remote-write URL, got nil")
	}
	bad := fullDetection()
	bad.Endpoints = append(bad.Endpoints, detect.Endpoint{Kind: detect.KindCLMetrics, URL: "://no-host"})
	if _, err := Render(bad, Options{RemoteWriteURL: "http://x:8428/api/v1/write"}); err == nil {
		t.Error("expected error for malformed metrics URL, got nil")
	}
}

func TestRenderTenantID(t *testing.T) {
	got, err := Render(fullDetection(), Options{
		RemoteWriteURL: "https://ingest.example:8428/api/v1/write",
		TenantID:       "org_123",
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`tenant_id = "org_123"`, `target_label = "tenant_id"`} {
		if !strings.Contains(got, want) {
			t.Errorf("expected %q in rendered config\n%s", want, got)
		}
	}
	// Without TenantID the label must be absent entirely.
	plain, err := Render(fullDetection(), Options{RemoteWriteURL: "https://ingest.example:8428/api/v1/write"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(plain, "tenant_id") {
		t.Errorf("tenant_id label must be absent when TenantID is empty\n%s", plain)
	}
}

func TestRenderGolden(t *testing.T) {
	cases := []struct {
		name string
		det  *detect.Result
		opts Options
	}{
		{
			name: "full_geth_lighthouse",
			det:  fullDetection(),
			opts: Options{RemoteWriteURL: "https://ingest.example:8428/api/v1/write"},
		},
		{
			name: "partial_reth",
			det:  partialDetection(),
			opts: Options{
				RemoteWriteURL:     "https://ingest.example:8428/api/v1/write",
				ExporterMetricsURL: "http://127.0.0.1:9091/metrics",
				Instance:           "reth-node-2",
			},
		},
		{
			name: "cosmos_full",
			det:  cosmosDetection(),
			opts: Options{RemoteWriteURL: "https://ingest.example:8428/api/v1/write"},
		},
		{
			name: "full_with_token",
			det:  fullDetection(),
			opts: Options{
				RemoteWriteURL: "https://ingest.example:8428/api/v1/write",
				IngestToken:    "nb_ingest_TESTTOKEN0123456789abcdef",
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := Render(c.det, c.opts)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join("testdata", c.name+".alloy.golden")
			if *update {
				if err := os.MkdirAll("testdata", 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
					t.Fatal(err)
				}
				return
			}
			want, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read golden: %v (run with -update to create)", err)
			}
			if got != string(want) {
				t.Errorf("rendered config differs from golden %s (run with -update after review)", path)
			}
		})
	}
}
