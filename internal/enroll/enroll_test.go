package enroll

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/nodebeat/agent/internal/detect"
)

// server answers every call with resp (as JSON) and records the request.
func server(t *testing.T, resp any, auth, body *string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if auth != nil {
			*auth = r.Header.Get("Authorization")
		}
		if body != nil {
			b, _ := io.ReadAll(r.Body)
			*body = string(b)
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestActivate(t *testing.T) {
	var gotAuth, gotBody string
	srv := server(t, map[string]string{
		"node_id": "node-1", "node_name": "n1", "tenant_id": "org_1",
		"remote_write_url": "https://ingest/w", "alloy_config": "ignored{}",
	}, &gotAuth, &gotBody)

	out, err := NewClient(srv.URL, "nb_ingest_x").Activate(context.Background(), ActivateRequest{
		Chain: "ethereum", ELClient: "geth", Hostname: "h1",
		Endpoints: []detect.Endpoint{{Kind: detect.KindELRPC, URL: "http://127.0.0.1:8545"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if gotAuth != "Bearer nb_ingest_x" {
		t.Errorf("auth = %q, want Bearer nb_ingest_x", gotAuth)
	}
	for _, want := range []string{`"chain":"ethereum"`, `"hostname":"h1"`, `"kind":"el-rpc"`} {
		if !strings.Contains(gotBody, want) {
			t.Errorf("body %s missing %s", gotBody, want)
		}
	}
	if out != (NodeParams{NodeID: "node-1", NodeName: "n1", TenantID: "org_1", RemoteWriteURL: "https://ingest/w"}) {
		t.Errorf("unexpected response: %+v", out)
	}
}

func TestActivateRejectsBadRemoteWriteURL(t *testing.T) {
	for _, rw := range []string{"", "ftp://ingest/w", "https://user:pw@ingest/w", "/relative"} {
		srv := server(t, map[string]string{"node_id": "n", "node_name": "n", "remote_write_url": rw}, nil, nil)
		if _, err := NewClient(srv.URL, "t").Activate(context.Background(), ActivateRequest{Chain: "ethereum"}); err == nil {
			t.Errorf("remote-write URL %q accepted", rw)
		}
	}
}

func TestActivateError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte(`{"error":"node not activated"}`))
	}))
	defer srv.Close()
	_, err := NewClient(srv.URL, "bad").Activate(context.Background(), ActivateRequest{Chain: "ethereum"})
	if err == nil || !strings.Contains(err.Error(), "403") || !strings.Contains(err.Error(), "node not activated") {
		t.Errorf("err = %v, want HTTP 403 with the server message", err)
	}
}

func TestRedirectNotFollowed(t *testing.T) {
	var leaked bool
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { leaked = true }))
	defer target.Close()
	srv := httptest.NewServer(http.RedirectHandler(target.URL+"/api/v1/nodes/n1/config", http.StatusFound))
	defer srv.Close()
	if _, err := NewClient(srv.URL, "tok").Heartbeat(context.Background(), "n1"); err == nil || leaked {
		t.Errorf("redirect followed (leaked=%v, err=%v)", leaked, err)
	}
}

func TestSaveLoadRoundtrip(t *testing.T) {
	dir := t.TempDir()
	e := Enrollment{ControlPlane: "https://cp", NodeID: "n1", RemoteWriteURL: "https://ingest/w", Chain: "cosmos"}
	if err := Save(dir, e); err != nil {
		t.Fatal(err)
	}
	got, err := Load(dir)
	if err != nil || got != e {
		t.Fatalf("roundtrip = %+v, %v", got, err)
	}
	if err := SaveToken(dir, "nb_ingest_x\n"); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{Path(dir), TokenPath(dir)} {
		if fi, err := os.Stat(p); err != nil || fi.Mode().Perm() != 0o600 {
			t.Errorf("%s not 0600: %v %v", p, fi, err)
		}
	}
	if raw, _ := os.ReadFile(Path(dir)); strings.Contains(string(raw), "nb_ingest") {
		t.Errorf("enrollment.json must not hold the token: %s", raw)
	}
	if tok, err := LoadToken(dir); err != nil || tok != "nb_ingest_x" {
		t.Errorf("LoadToken = %q, %v", tok, err)
	}
	if tok, err := LoadToken(t.TempDir()); err != nil || tok != "" {
		t.Errorf("missing token file = %q, %v; want empty, nil", tok, err)
	}
	if err := SaveToken(dir, "two words"); err == nil {
		t.Error("token with whitespace accepted")
	}
}

// Old enrollments (token inside enrollment.json, no remote-write URL
// pinned) must be re-enrolled, not half-loaded.
func TestLoadRejectsIncomplete(t *testing.T) {
	dir := t.TempDir()
	_ = os.WriteFile(Path(dir), []byte(`{"control_plane":"https://cp","node_id":"n1","ingest_token":"x"}`), 0o600)
	if _, err := Load(dir); err == nil {
		t.Error("incomplete enrollment loaded")
	}
}

func TestHeartbeat(t *testing.T) {
	var gotAuth string
	srv := server(t, map[string]string{"node_id": "n1", "node_name": "renamed", "remote_write_url": "https://ingest/w"}, &gotAuth, nil)
	p, err := NewClient(srv.URL, "tok").Heartbeat(context.Background(), "n1")
	if err != nil || p.NodeName != "renamed" || gotAuth != "Bearer tok" {
		t.Errorf("Heartbeat = %+v, %v (auth %q)", p, err, gotAuth)
	}
}
