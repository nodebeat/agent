package main

import (
	"os"
	"testing"

	"github.com/nodebeat/agent/internal/enroll"
)

// A token from env is stored once in <state-dir>/ingest-token (0600) and
// later runs without one reuse that file.
func TestIngestTokenStoredOnce(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("NB_INGEST_TOKEN", "")
	t.Setenv("NODEBEAT_INGEST_TOKEN", "nb_ingest_env")
	if tok, err := ingestToken("", dir); err != nil || tok != "nb_ingest_env" {
		t.Fatalf("ingestToken = %q, %v", tok, err)
	}
	if fi, err := os.Stat(enroll.TokenPath(dir)); err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("token file: %v %v", fi, err)
	}
	t.Setenv("NODEBEAT_INGEST_TOKEN", "")
	if tok, err := ingestToken("", dir); err != nil || tok != "nb_ingest_env" {
		t.Errorf("stored token not reused: %q, %v", tok, err)
	}
	if tok, err := ingestToken("", t.TempDir()); err != nil || tok != "" {
		t.Errorf("no token anywhere = %q, %v; want empty", tok, err)
	}
}

// run --enrolled watches the chain recorded at enroll unless --chain names
// it; a contradicting --chain is refused; pre-chain enrollments keep the
// flag (or auto-detection).
func TestEnrolledChain(t *testing.T) {
	for _, tc := range []struct {
		flag, enrolled, want string
		wantErr              bool
	}{
		{"", "cosmos", "cosmos", false},
		{"cosmos", "cosmos", "cosmos", false},
		{"ethereum", "cosmos", "", true},
		{"", "", "", false},
		{"ethereum", "", "ethereum", false},
	} {
		got, err := enrolledChain(tc.flag, tc.enrolled)
		if got != tc.want || (err != nil) != tc.wantErr {
			t.Errorf("enrolledChain(%q, %q) = %q, %v; want %q, err %v", tc.flag, tc.enrolled, got, err, tc.want, tc.wantErr)
		}
	}
}
