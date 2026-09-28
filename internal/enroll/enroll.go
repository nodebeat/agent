// Package enroll implements the SaaS enrollment flow for nodebeat-agent:
//
// Portal creates a pending node (name only) and shows its ingest token once.
// enroll (on the node host): detect → POST /api/v1/nodes/activate → save
// enrollment.json. Activation uploads detection (chain, clients, endpoints,
// hostname), flips the node to 'active', and returns the rendered pipeline.
//
//	run --enrolled: load enrollment.json → GET /nodes/:id/config → supervise
//
// Auth throughout is the node's own ingest token (nb_ingest_..., Bearer):
// it authorizes activation, config polls, and remote_write (via the nginx
// ingest proxy). No session JWT ever touches the node host. The ingest
// token is stored 0600 alongside the enrollment. Treat the state dir as
// secret.
package enroll

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/nodebeat/agent/internal/detect"
)

// Filename is the enrollment file inside the agent state dir.
const Filename = "enrollment.json"

// Enrollment is the persisted result of `nodebeat-agent enroll`.
type Enrollment struct {
	ControlPlane   string        `json:"control_plane"`
	NodeID         string        `json:"node_id"`
	NodeName       string        `json:"node_name"`
	TenantID       string        `json:"tenant_id"`
	RemoteWriteURL string        `json:"remote_write_url"`
	OrgID          string        `json:"org_id,omitempty"`       // legacy dev-bypass enrollments only
	Token          string        `json:"token,omitempty"`        // legacy: Clerk JWT (prefer IngestToken)
	IngestToken    string        `json:"ingest_token,omitempty"` // per-node credential (nb_ingest_...)
	Detection      detect.Result `json:"detection"`
	EnrolledAt     time.Time     `json:"enrolled_at"`
}

// DaemonToken returns the credential run --enrolled should use: the node
// ingest token, falling back to legacy fields for old enrollment files.
func (e Enrollment) DaemonToken() (token, orgID string) {
	if e.IngestToken != "" {
		return e.IngestToken, ""
	}
	return e.Token, e.OrgID
}

// Path returns the enrollment file path inside stateDir.
func Path(stateDir string) string {
	return filepath.Join(stateDir, Filename)
}

// Save writes the enrollment 0600 (it may hold a token).
func Save(stateDir string, e Enrollment) error {
	if err := os.MkdirAll(stateDir, 0o750); err != nil {
		return fmt.Errorf("create state dir: %w", err)
	}
	buf, err := json.MarshalIndent(e, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(Path(stateDir), buf, 0o600); err != nil {
		return fmt.Errorf("write enrollment: %w", err)
	}
	// WriteFile keeps the mode of a pre-existing file; force 0600 so a
	// re-enroll over a loosely-permissioned file cannot leak the token.
	if err := os.Chmod(Path(stateDir), 0o600); err != nil {
		return fmt.Errorf("chmod enrollment: %w", err)
	}
	return nil
}

// Load reads the enrollment file.
func Load(stateDir string) (Enrollment, error) {
	var e Enrollment
	buf, err := os.ReadFile(Path(stateDir))
	if err != nil {
		return e, fmt.Errorf("read enrollment (run `enroll` first?): %w", err)
	}
	if err := json.Unmarshal(buf, &e); err != nil {
		return e, fmt.Errorf("parse enrollment: %w", err)
	}
	if e.NodeID == "" || e.ControlPlane == "" {
		return e, fmt.Errorf("enrollment incomplete (run `enroll` again)")
	}
	return e, nil
}

// Client talks to the control-plane API.
type Client struct {
	base  string
	token string
	orgID string
	http  *http.Client
}

// NewClient builds a client. token (the node's ingest token, nb_ingest_...)
// is always sent as Bearer; orgID is sent as X-Org-ID for the legacy
// session-authed calls (activation and polls resolve the org from the node
// token instead).
func NewClient(controlPlane, token, orgID string) *Client {
	return &Client{
		base:  strings.TrimSuffix(controlPlane, "/"),
		token: token,
		orgID: orgID,
		http:  &http.Client{Timeout: 30 * time.Second},
	}
}

func (c *Client) auth(req *http.Request) {
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	if c.orgID != "" {
		req.Header.Set("X-Org-ID", c.orgID)
	}
}

func (c *Client) do(ctx context.Context, method, path string, body any) (*http.Response, error) {
	var rdr *bytes.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		rdr = bytes.NewReader(buf)
	} else {
		rdr = bytes.NewReader(nil)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, rdr)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	c.auth(req)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		defer resp.Body.Close()
		var errBody map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&errBody)
		return nil, fmt.Errorf("control-plane %s %s: HTTP %d (%v)", method, path, resp.StatusCode, errBody["error"])
	}
	return resp, nil
}

// ActivateRequest is the agent's detection uploaded to activate a
// portal-created (pending) node. The presented ingest token identifies the
// node, so no name or id is needed.
type ActivateRequest struct {
	Chain     string            `json:"chain"`
	ELClient  string            `json:"el_client,omitempty"`
	CLClient  string            `json:"cl_client,omitempty"`
	Endpoints []detect.Endpoint `json:"endpoints,omitempty"`
	Hostname  string            `json:"hostname,omitempty"`
}

// ActivateResponse carries everything enroll persists into enrollment.json.
type ActivateResponse struct {
	NodeID         string `json:"node_id"`
	NodeName       string `json:"node_name"`
	TenantID       string `json:"tenant_id"`
	RemoteWriteURL string `json:"remote_write_url"`
	AlloyConfig    string `json:"alloy_config"`
}

// Activate uploads detection for the token's node, flipping it to 'active'.
// The server renders the pipeline from the submitted detection (full
// fidelity) and returns it with the node's identity — one round trip.
func (c *Client) Activate(ctx context.Context, req ActivateRequest) (ActivateResponse, error) {
	var out ActivateResponse
	resp, err := c.do(ctx, "POST", "/api/v1/nodes/activate", req)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return out, err
	}
	if out.NodeID == "" {
		return out, fmt.Errorf("control-plane returned no node id")
	}
	if out.AlloyConfig == "" {
		return out, fmt.Errorf("control-plane returned empty alloy config")
	}
	return out, nil
}

// NodeConfig is the agent-facing config payload.
type NodeConfig struct {
	NodeID         string `json:"node_id"`
	TenantID       string `json:"tenant_id"`
	RemoteWriteURL string `json:"remote_write_url"`
	AlloyConfig    string `json:"alloy_config"`
}

// FetchConfig polls the rendered Alloy pipeline. Each poll doubles as a
// heartbeat (the server refreshes last_seen).
func (c *Client) FetchConfig(ctx context.Context, nodeID string) (NodeConfig, error) {
	var out NodeConfig
	resp, err := c.do(ctx, "GET", "/api/v1/nodes/"+nodeID+"/config", nil)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return out, err
	}
	if out.AlloyConfig == "" {
		return out, fmt.Errorf("control-plane returned empty alloy config")
	}
	return out, nil
}
