// Package enroll implements the SaaS enrollment flow for nodebeat-agent:
//
//	enroll: detect (optional) → POST /api/v1/nodes → save enrollment.json
//	run --enrolled: load enrollment.json → GET /nodes/:id/config → supervise
//
// Auth: a session JWT (--token, sent as Bearer; Clerk in prod, devjwt-minted
// in dev) plus --org-id (sent as X-Org-ID, selects the tenant). The server
// verifies the JWT on every call and resolves the org against memberships —
// there is no header-only mode. The server returns a per-node opaque ingest
// token (nb_ingest_...) which is the daemon's only credential: it authorizes
// config polls and remote_write (via the nginx ingest proxy). The user JWT
// is never persisted; the ingest token is stored 0600 alongside the
// enrollment. Treat the state dir as secret.
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

// NewClient builds a client. token (session JWT) is always sent as Bearer;
// orgID (our org id or slug) is sent as X-Org-ID to select the tenant.
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

// RegisterNode creates the node server-side and returns its id, tenant,
// ingest URL and per-node ingest token (shown once — persist it, it is
// never returned again except via rotate).
func (c *Client) RegisterNode(ctx context.Context, name, chain, elEndpoint, clEndpoint string) (nodeID, tenantID, remoteWriteURL, ingestToken string, err error) {
	resp, err := c.do(ctx, "POST", "/api/v1/nodes", map[string]any{
		"name": name, "chain": chain, "el_endpoint": elEndpoint, "cl_endpoint": clEndpoint,
	})
	if err != nil {
		return "", "", "", "", err
	}
	defer resp.Body.Close()
	var out struct {
		ID             string `json:"id"`
		TenantID       string `json:"tenant_id"`
		RemoteWriteURL string `json:"remote_write_url"`
		IngestToken    string `json:"ingest_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", "", "", "", err
	}
	if out.ID == "" {
		return "", "", "", "", fmt.Errorf("control-plane returned no node id")
	}
	if out.IngestToken == "" {
		return "", "", "", "", fmt.Errorf("control-plane returned no ingest token")
	}
	return out.ID, out.TenantID, out.RemoteWriteURL, out.IngestToken, nil
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
