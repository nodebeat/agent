package agent

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nodebeat/agent/internal/detect"
)

func testLogger() *log.Logger {
	return log.New(io.Discard, "", 0)
}

func fullFixture() *detect.Result {
	return &detect.Result{
		Target:   "node9.example",
		Chain:    detect.ChainEthereum,
		ELClient: "geth",
		CLClient: "lighthouse",
		Endpoints: []detect.Endpoint{
			{Kind: detect.KindELRPC, URL: "http://node9.example:8545"},
			{Kind: detect.KindELMetrics, URL: "http://node9.example:6060/debug/metrics/prometheus"},
			{Kind: detect.KindCLBeacon, URL: "http://node9.example:5052"},
			{Kind: detect.KindCLMetrics, URL: "http://node9.example:5054/metrics"},
		},
	}
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// fakeBin writes an executable shell script. When hupTrap is set the script
// ignores SIGHUP (emulating Alloy's reload behavior in reload tests).
// Invocations append their argv to $ARGS_LOG.
func fakeBin(t *testing.T, dir, name string, hupTrap bool) string {
	t.Helper()
	trap := ""
	if hupTrap {
		trap = "trap '' HUP\n"
	}
	body := "#!/bin/sh\n" + trap + "echo \"$@\" >> \"$ARGS_LOG\"\nexec sleep 30\n"
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestChainMetricsURL(t *testing.T) {
	for _, c := range []struct {
		chain, addr, override, want string
	}{
		{detect.ChainEthereum, "127.0.0.1:19090", "", "http://127.0.0.1:19090/chain/metrics"},
		{detect.ChainEthereum, ":19091", "", "http://127.0.0.1:19091/chain/metrics"},
		{detect.ChainEthereum, "0.0.0.0:19090", "", "http://127.0.0.1:19090/chain/metrics"},
		{detect.ChainEthereum, "[::1]:19090", "", "http://[::1]:19090/chain/metrics"},
		{detect.ChainCosmos, "127.0.0.1:19090", "", "http://127.0.0.1:9095/metrics"},
		{detect.ChainEthereum, "127.0.0.1:19090", "http://x:1/m", "http://x:1/m"},
	} {
		if got := ChainMetricsURL(c.chain, 9095, c.addr, c.override); got != c.want {
			t.Errorf("ChainMetricsURL(%s, %s, %q) = %s, want %s", c.chain, c.addr, c.override, got, c.want)
		}
	}
}

func TestExporterArgsCosmos(t *testing.T) {
	det := &detect.Result{
		Target: "node3.example",
		Chain:  detect.ChainCosmos,
		Endpoints: []detect.Endpoint{
			{Kind: detect.KindCosmosRPC, URL: "http://node3.example:26657"},
			{Kind: detect.KindCosmosMetrics, URL: "http://node3.example:26660/metrics"},
		},
	}
	joined := strings.Join(exporterArgs(det, 9090), " ")
	for _, want := range []string{
		"--http-addr 127.0.0.1:9090",
		"--node http://node3.example:26657",
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("cosmos exporter args %q missing %q", joined, want)
		}
	}
	if strings.Contains(joined, "--metrics-port") {
		t.Errorf("cosmos watcher has no --metrics-port flag, got %q", joined)
	}
	if strings.Contains(joined, "--execution-url") || strings.Contains(joined, "--consensus-url") {
		t.Errorf("cosmos args must not carry ethereum flags, got %q", joined)
	}
}

func TestManifestContents(t *testing.T) {
	m := buildManifest(fullFixture(), Config{
		RemoteWriteURL: "https://ingest:8428/api/v1/write",
		StateDir:       "s",
	}, "http://127.0.0.1:19090/chain/metrics", "", nil)
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "remote_write_auth") {
		t.Errorf("manifest should not contain remote_write_auth: %s", raw)
	}
	if m.RemoteWriteURL != "https://ingest:8428/api/v1/write" {
		t.Errorf("remote_write_url = %q", m.RemoteWriteURL)
	}
	if len(m.ScrapeJobs) != 3 {
		t.Errorf("expected 3 scrape jobs, got %d", len(m.ScrapeJobs))
	}
	if !strings.HasPrefix(m.Exporter, "built-in ethpoll (beacon=http://node9.example:5052 execution=http://node9.example:8545") {
		t.Errorf("exporter = %q, want the built-in poller with its endpoints", m.Exporter)
	}
	if len(m.ChainAPICalls) == 0 || !strings.Contains(strings.Join(m.ChainAPICalls, "\n"), "/eth/v1/node/syncing") {
		t.Errorf("manifest must list the poller's calls, got %v", m.ChainAPICalls)
	}
	if m.ScrapeJobs[0].Targets[0] != "http://127.0.0.1:19090/chain/metrics" {
		t.Errorf("hot job should scrape the poller first, got %v", m.ScrapeJobs[0].Targets)
	}
}

func TestMaterializeWritesStateDir(t *testing.T) {
	dir := t.TempDir()
	r := New(Config{
		Target:         "node9.example",
		RemoteWriteURL: "https://ingest:8428/api/v1/write",
		AlloyBin:       "/bin/true",
		StateDir:       dir,
	}, testLogger())

	if err := r.materialize(fullFixture()); err != nil {
		t.Fatal(err)
	}
	cfg := readFile(t, r.ConfigPath())
	for _, want := range []string{"node9.example:5054", `"127.0.0.1:19090", __scheme__ = "http", __metrics_path__ = "/chain/metrics"`, "remote_write"} {
		if !strings.Contains(cfg, want) {
			t.Errorf("config.alloy missing %q", want)
		}
	}
	if fi, err := os.Stat(r.ConfigPath()); err != nil || fi.Mode().Perm() != 0o600 {
		t.Errorf("config.alloy should be 0600, got %v, %v", fi, err)
	}
	if len(r.children) != 1 || r.children[0].Name != ChildAlloy {
		t.Errorf("ethereum should supervise only alloy (poller is in-process), got %+v", r.children)
	}
	raw := readFile(t, r.ManifestPath())
	var m Manifest
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		t.Fatal(err)
	}
	if m.ELClient != "geth" || m.CLClient != "lighthouse" {
		t.Errorf("unexpected manifest clients: %+v", m)
	}
	for _, p := range []string{r.ConfigPath(), r.ManifestPath()} {
		abs, err := filepath.Abs(p)
		if err != nil {
			t.Fatal(err)
		}
		absDir, err := filepath.Abs(dir)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(abs, absDir) {
			t.Errorf("agent wrote outside state dir: %s", abs)
		}
	}
}

// fakeEthNode answers the poller's Beacon API and JSON-RPC calls.
func fakeEthNode(t *testing.T) (beaconURL, rpcURL string) {
	t.Helper()
	b := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/eth/v1/node/syncing":
			io.WriteString(w, `{"data":{"head_slot":"4242","sync_distance":"0","is_syncing":false}}`)
		case "/eth/v1/node/peer_count":
			io.WriteString(w, `{"data":{"connected":"7","connecting":"0","disconnected":"0","disconnecting":"0"}}`)
		default: // events stream: hold until the client goes away
			<-r.Context().Done()
		}
	}))
	e := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `[{"jsonrpc":"2.0","id":2,"result":{"number":"0x10","transactions":[]}}]`)
	}))
	t.Cleanup(func() { b.CloseClientConnections(); b.Close(); e.Close() })
	return b.URL, e.URL
}

func runUntilCancel(t *testing.T, r *Runner) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Start(ctx) }()
	return func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("expected clean shutdown, got: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("agent did not stop after cancel")
		}
	}
}

func TestFakeBinaryE2E(t *testing.T) {
	dir := t.TempDir()
	binDir := t.TempDir()
	argsLog := filepath.Join(dir, "args.log")
	t.Setenv("ARGS_LOG", argsLog)
	alloyBin := fakeBin(t, binDir, "alloy.sh", false)
	beaconURL, rpcURL := fakeEthNode(t)
	det := fullFixture()
	det.Endpoints = []detect.Endpoint{
		{Kind: detect.KindELRPC, URL: rpcURL},
		{Kind: detect.KindCLBeacon, URL: beaconURL},
		{Kind: detect.KindCLMetrics, URL: "http://node9.example:5054/metrics"},
	}

	metricsPort := freePort(t)
	r := New(Config{
		Target:           "node9.example",
		RemoteWriteURL:   "https://ingest:8428/api/v1/write",
		AlloyBin:         alloyBin,
		StateDir:         dir,
		MetricsAddr:      "127.0.0.1:" + itoa(metricsPort),
		AlloyUIAddr:      "127.0.0.1:12345",
		DisableReporting: true,
	}, testLogger())
	if err := r.materialize(det); err != nil {
		t.Fatal(err)
	}
	stop := runUntilCancel(t, r)
	defer stop()

	waitFor(t, 5*time.Second, "alloy running", func() bool {
		st := r.ChildrenStats()
		return len(st) == 1 && st[0].Running
	})

	base := "http://127.0.0.1:" + itoa(metricsPort)
	var body string
	waitFor(t, 5*time.Second, "/metrics served", func() bool {
		b, err := tryGet(base + "/metrics")
		if err != nil {
			return false
		}
		body = b
		return true
	})
	for _, want := range []string{"nodebeat_agent_up 1", "nodebeat_agent_info", `child="alloy"`} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics missing %q", want)
		}
	}
	if strings.Contains(body, `child="exporter"`) || strings.Contains(body, "eth_con_") {
		t.Errorf("/metrics should carry neither an exporter child nor chain series:\n%s", body)
	}
	// The chain series are on their own path, polled from the fake node.
	waitFor(t, 5*time.Second, "chain metrics polled", func() bool {
		b, err := tryGet(base + ChainMetricsPath)
		return err == nil && strings.Contains(b, `eth_con_sync_head_slot{module="sync",node="consensus"} 4242`) &&
			strings.Contains(b, `eth_exe_block_most_recent_number{ethereum_role="execution",identifier="head",module="block",node_name="execution"} 16`)
	})
	var m Manifest
	waitFor(t, 5*time.Second, "/manifest served", func() bool {
		b, err := tryGet(base + "/manifest")
		if err != nil {
			return false
		}
		return json.Unmarshal([]byte(b), &m) == nil
	})
	if m.Target != "node9.example" || m.RemoteWriteURL != "https://ingest:8428/api/v1/write" {
		t.Errorf("unexpected /manifest: %+v", m)
	}

	argv := readFile(t, argsLog)
	for _, want := range []string{
		"run",
		"--storage.path=" + dir + "/alloy-wal",
		"--server.http.listen-addr=127.0.0.1:12345",
		"--disable-reporting",
		dir + "/config.alloy",
	} {
		if !strings.Contains(argv, want) {
			t.Errorf("alloy argv missing %q (got %q)", want, argv)
		}
	}
}

func TestFakeBinaryE2ECosmos(t *testing.T) {
	dir := t.TempDir()
	binDir := t.TempDir()
	argsLog := filepath.Join(dir, "args.log")
	t.Setenv("ARGS_LOG", argsLog)
	r := New(Config{
		Target:         "node3.example",
		RemoteWriteURL: "https://ingest:8428/api/v1/write",
		ExporterBin:    fakeBin(t, binDir, "watcher.sh", false),
		AlloyBin:       fakeBin(t, binDir, "alloy.sh", false),
		ExporterPort:   9095,
		StateDir:       dir,
		MetricsAddr:    "127.0.0.1:" + itoa(freePort(t)),
	}, testLogger())
	det := &detect.Result{
		Target: "node3.example",
		Chain:  detect.ChainCosmos,
		Endpoints: []detect.Endpoint{
			{Kind: detect.KindCosmosRPC, URL: "http://node3.example:26657"},
			{Kind: detect.KindCosmosMetrics, URL: "http://node3.example:26660/metrics"},
		},
	}
	if err := r.materialize(det); err != nil {
		t.Fatal(err)
	}
	if cfg := readFile(t, r.ConfigPath()); !strings.Contains(cfg, `"127.0.0.1:9095", __scheme__ = "http", __metrics_path__ = "/metrics"`) {
		t.Errorf("cosmos config should scrape the watcher on loopback:\n%s", cfg)
	}
	stop := runUntilCancel(t, r)
	defer stop()
	waitFor(t, 5*time.Second, "watcher + alloy running", func() bool {
		st := r.ChildrenStats()
		return len(st) == 2 && st[0].Running && st[1].Running
	})
	waitFor(t, 5*time.Second, "watcher argv", func() bool {
		b, err := os.ReadFile(argsLog)
		return err == nil && strings.Contains(string(b), "--http-addr 127.0.0.1:9095 --node http://node3.example:26657")
	})
}

func TestApplyDetectionReloadsAlloy(t *testing.T) {
	dir := t.TempDir()
	binDir := t.TempDir()
	argsLog := filepath.Join(dir, "args.log")
	t.Setenv("ARGS_LOG", argsLog)
	alloyBin := fakeBin(t, binDir, "alloy.sh", true) // traps SIGHUP like Alloy

	r := New(Config{
		Target:         "node9.example",
		RemoteWriteURL: "https://ingest:8428/api/v1/write",
		AlloyBin:       alloyBin,
		StateDir:       dir,
		MetricsAddr:    "127.0.0.1:" + itoa(freePort(t)),
	}, testLogger())
	if err := r.materialize(fullFixture()); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Start(ctx) }()
	waitFor(t, 5*time.Second, "alloy running", func() bool {
		st := r.ChildrenStats()
		return len(st) == 1 && st[0].Running
	})
	r.mu.RLock()
	firstPoller := r.poller
	r.mu.RUnlock()

	// CL disappears: config must drop the consensus target, alloy must stay
	// up (SIGHUP reload, not restart).
	changed := fullFixture()
	changed.CLClient = ""
	changed.Endpoints = []detect.Endpoint{
		{Kind: detect.KindELRPC, URL: "http://node9.example:8545"},
		{Kind: detect.KindELMetrics, URL: "http://node9.example:6060/debug/metrics/prometheus"},
	}
	if err := r.applyDetection(changed); err != nil {
		t.Fatal(err)
	}
	if cfg := readFile(t, r.ConfigPath()); strings.Contains(cfg, ":5054") {
		t.Errorf("reloaded config should drop consensus target:\n%s", cfg)
	}
	if m := r.Manifest(); m.CLClient != "" || strings.Contains(m.Exporter, ":5052") {
		t.Errorf("manifest should drop cl_client and the beacon URL, got %+v", m)
	}
	r.mu.RLock()
	replaced := r.poller != firstPoller
	r.mu.RUnlock()
	if !replaced {
		t.Error("poller should be replaced when endpoints change")
	}
	for _, st := range r.ChildrenStats() {
		if st.Name == ChildAlloy && st.Restarts != 0 {
			t.Errorf("alloy restarted on reload (restarts=%d); want SIGHUP reload", st.Restarts)
		}
	}

	// No-op reload must also succeed without restarts and keep the poller.
	r.mu.RLock()
	secondPoller := r.poller
	r.mu.RUnlock()
	if err := r.applyDetection(changed); err != nil {
		t.Fatal(err)
	}
	r.mu.RLock()
	kept := r.poller == secondPoller
	r.mu.RUnlock()
	if !kept {
		t.Error("no-op reload replaced the poller")
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected clean shutdown, got: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("agent did not stop after cancel")
	}
}

func tryGet(url string) (string, error) {
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", &httpError{url, resp.Status}
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

type httpError struct {
	url, status string
}

func (e *httpError) Error() string {
	return "GET " + e.url + ": " + e.status
}

func itoa(n int) string {
	return strconv.Itoa(n)
}
