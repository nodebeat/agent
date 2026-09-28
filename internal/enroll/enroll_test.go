package enroll

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/nodebeat/agent/internal/detect"
)

func TestActivate(t *testing.T) {
	var gotAuth, gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		var body ActivateRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode body: %v", err)
		}
		raw, _ := json.Marshal(body)
		gotBody = string(raw)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(ActivateResponse{
			NodeID: "node-1", NodeName: "n1", TenantID: "org_1",
			RemoteWriteURL: "https://ingest/w", AlloyConfig: "config{}",
		})
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "nb_ingest_x", "")
	out, err := c.Activate(context.Background(), ActivateRequest{
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
		if !contains(gotBody, want) {
			t.Errorf("body %s missing %s", gotBody, want)
		}
	}
	if out.NodeID != "node-1" || out.AlloyConfig != "config{}" {
		t.Errorf("unexpected response: %+v", out)
	}
}

func TestActivateError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte(`{"error":"node not activated"}`))
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "bad", "")
	if _, err := c.Activate(context.Background(), ActivateRequest{Chain: "ethereum"}); err == nil {
		t.Error("expected error on 403, got nil")
	}
}

func TestSaveLoadRoundtrip(t *testing.T) {
	dir := t.TempDir()
	e := Enrollment{ControlPlane: "https://cp", NodeID: "n1", IngestToken: "nb_ingest_x"}
	if err := Save(dir, e); err != nil {
		t.Fatal(err)
	}
	if fi, err := os.Stat(filepath.Join(dir, Filename)); err != nil || fi.Mode().Perm() != 0o600 {
		t.Errorf("enrollment not 0600: %v %v", fi, err)
	}
	got, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got.NodeID != "n1" || got.IngestToken != "nb_ingest_x" {
		t.Errorf("roundtrip mismatch: %+v", got)
	}
	if tok, _ := got.DaemonToken(); tok != "nb_ingest_x" {
		t.Errorf("DaemonToken = %q", tok)
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (func() bool {
		for i := 0; i+len(sub) <= len(s); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
		return false
	})()
}

// FetchConfig forwards the agent's exporter URL so the server renders the
// port the exporter actually runs on; empty sends no query (server default).
func TestFetchConfigSendsExporterURL(t *testing.T) {
	var got []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = append(got, r.URL.Query().Get("exporter_url"))
		_ = json.NewEncoder(w).Encode(NodeConfig{NodeID: "n1", AlloyConfig: "cfg"})
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "tok", "")
	for _, u := range []string{"http://127.0.0.1:9091/metrics", ""} {
		if _, err := c.FetchConfig(context.Background(), "n1", u); err != nil {
			t.Fatal(err)
		}
	}
	if len(got) != 2 || got[0] != "http://127.0.0.1:9091/metrics" || got[1] != "" {
		t.Fatalf("exporter_url sent = %q", got)
	}
}
