package agent

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
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

func TestExporterArgs(t *testing.T) {
	full := fullFixture()
	args := exporterArgs(full, 9090)
	joined := strings.Join(args, " ")
	for _, want := range []string{
		"--metrics-port 9090",
		"--execution-url http://node9.example:8545",
		"--consensus-url http://node9.example:5052",
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("exporter args %q missing %q", joined, want)
		}
	}

	elOnly := &detect.Result{
		Target:   "node9.example",
		Chain:    detect.ChainEthereum,
		ELClient: "reth",
		Endpoints: []detect.Endpoint{
			{Kind: detect.KindELRPC, URL: "http://node9.example:8545"},
		},
	}
	joined = strings.Join(exporterArgs(elOnly, 9091), " ")
	if strings.Contains(joined, "--consensus-url") {
		t.Errorf("EL-only args should omit --consensus-url, got %q", joined)
	}
	if !strings.Contains(joined, "--metrics-port 9091") {
		t.Errorf("expected custom metrics port, got %q", joined)
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
		"--http-addr :9090",
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
		ExporterURL:    "http://127.0.0.1:9090/metrics",
	}, "/bin/exporter", []string{"--metrics-port", "9090"})
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
}

func TestMaterializeWritesStateDir(t *testing.T) {
	dir := t.TempDir()
	r := New(Config{
		Target:         "node9.example",
		RemoteWriteURL: "https://ingest:8428/api/v1/write",
		ExporterBin:    "/bin/true",
		AlloyBin:       "/bin/true",
		StateDir:       dir,
	}, testLogger())

	if err := r.materialize(fullFixture()); err != nil {
		t.Fatal(err)
	}
	cfg := readFile(t, r.ConfigPath())
	for _, want := range []string{"node9.example:5054", "127.0.0.1:9090", "remote_write"} {
		if !strings.Contains(cfg, want) {
			t.Errorf("config.alloy missing %q", want)
		}
	}
	if fi, err := os.Stat(r.ConfigPath()); err != nil || fi.Mode().Perm() != 0o600 {
		t.Errorf("config.alloy should be 0600, got %v, %v", fi, err)
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

func TestFakeBinaryE2E(t *testing.T) {
	dir := t.TempDir()
	binDir := t.TempDir()
	argsLog := filepath.Join(dir, "args.log")
	t.Setenv("ARGS_LOG", argsLog)
	exporterBin := fakeBin(t, binDir, "exporter.sh", false)
	alloyBin := fakeBin(t, binDir, "alloy.sh", false)

	metricsPort := freePort(t)
	r := New(Config{
		Target:           "node9.example",
		RemoteWriteURL:   "https://ingest:8428/api/v1/write",
		ExporterBin:      exporterBin,
		AlloyBin:         alloyBin,
		ExporterPort:     9090,
		StateDir:         dir,
		MetricsAddr:      "127.0.0.1:" + itoa(metricsPort),
		AlloyUIAddr:      "127.0.0.1:12345",
		DisableReporting: true,
	}, testLogger())
	if err := r.materialize(fullFixture()); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Start(ctx) }()

	waitFor(t, 5*time.Second, "both children running", func() bool {
		st := r.ChildrenStats()
		if len(st) != 2 {
			return false
		}
		return st[0].Running && st[1].Running
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
	for _, want := range []string{"nodebeat_agent_up 1", "nodebeat_agent_info", `child="alloy"`, `child="exporter"`} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics missing %q", want)
		}
	}
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
		"--metrics-port 9090",
		"--execution-url http://node9.example:8545",
		"--consensus-url http://node9.example:5052",
	} {
		if !strings.Contains(argv, want) {
			t.Errorf("child argv missing %q (got %q)", want, argv)
		}
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

func TestApplyDetectionReloadsAlloy(t *testing.T) {
	dir := t.TempDir()
	binDir := t.TempDir()
	argsLog := filepath.Join(dir, "args.log")
	t.Setenv("ARGS_LOG", argsLog)
	exporterBin := fakeBin(t, binDir, "exporter.sh", false)
	alloyBin := fakeBin(t, binDir, "alloy.sh", true) // traps SIGHUP like Alloy

	r := New(Config{
		Target:         "node9.example",
		RemoteWriteURL: "https://ingest:8428/api/v1/write",
		ExporterBin:    exporterBin,
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
	waitFor(t, 5*time.Second, "children running", func() bool {
		st := r.ChildrenStats()
		return len(st) == 2 && st[0].Running && st[1].Running
	})

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
	if m := r.Manifest(); m.CLClient != "" {
		t.Errorf("manifest should drop cl_client, got %+v", m)
	}
	for _, st := range r.ChildrenStats() {
		if st.Name == ChildAlloy && st.Restarts != 0 {
			t.Errorf("alloy restarted on reload (restarts=%d); want SIGHUP reload", st.Restarts)
		}
	}

	// No-op reload must also succeed without restarts.
	if err := r.applyDetection(changed); err != nil {
		t.Fatal(err)
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
