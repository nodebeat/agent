// Package enroll implements the SaaS enrollment flow for nodebeat-agent:
//
// Portal creates a pending node (name only) and shows its ingest token once.
// enroll (on the node host): detect → POST /api/v1/nodes/activate → save
// ingest-token + enrollment.json. Activation uploads detection (chain,
// clients, endpoints, hostname) and flips the node to 'active'.
//
//	run --enrolled: load enrollment.json → render the pipeline locally →
//	supervise; GET /nodes/:id/config periodically as a heartbeat
//
// The control plane only supplies parameters (node name, tenant id,
// remote-write URL), never pipeline config: the agent renders its Alloy
// config itself, so the server cannot make the host run anything. The
// remote-write URL is pinned at enroll time; a later poll cannot move it.
//
// Auth throughout is the node's own ingest token (nb_ingest_..., Bearer):
// it authorizes activation, heartbeats, and remote_write (via the ingest
// proxy). No session JWT ever touches the node host. The token is stored
// once, in <state-dir>/ingest-token (0600), which Alloy also reads.
package enroll

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/nodebeat/agent/internal/detect"
)

// Files inside the agent state dir.
const (
	Filename      = "enrollment.json"
	TokenFilename = "ingest-token"
)

// Enrollment is the persisted result of `nodebeat-agent enroll`. It holds
// no secret: the token lives in TokenPath.
type Enrollment struct {
	ControlPlane   string    `json:"control_plane"`
	NodeID         string    `json:"node_id"`
	NodeName       string    `json:"node_name"`
	TenantID       string    `json:"tenant_id"`
	RemoteWriteURL string    `json:"remote_write_url"` // pinned at enroll
	Chain          string    `json:"chain,omitempty"`  // detected at enroll; empty in pre-chain enrollments
	EnrolledAt     time.Time `json:"enrolled_at"`
}

// Path returns the enrollment file path inside stateDir.
func Path(stateDir string) string {
	return filepath.Join(stateDir, Filename)
}

// TokenPath returns the ingest token file inside stateDir.
func TokenPath(stateDir string) string {
	return filepath.Join(stateDir, TokenFilename)
}

// Save writes the enrollment (0600, no secrets, but only the agent needs it).
func Save(stateDir string, e Enrollment) error {
	buf, err := json.MarshalIndent(e, "", "  ")
	if err != nil {
		return err
	}
	return writePrivate(stateDir, Path(stateDir), buf)
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
	if e.NodeID == "" || e.ControlPlane == "" || e.RemoteWriteURL == "" {
		return e, fmt.Errorf("enrollment incomplete (run `enroll` again)")
	}
	return e, nil
}

// SaveToken writes the ingest token file (0600).
func SaveToken(stateDir, token string) error {
	token = strings.TrimSpace(token)
	if token == "" || strings.ContainsAny(token, " \t\r\n") {
		return errors.New("invalid ingest token")
	}
	return writePrivate(stateDir, TokenPath(stateDir), []byte(token))
}

// LoadToken reads the ingest token file; "" when there is none.
func LoadToken(stateDir string) (string, error) {
	b, err := os.ReadFile(TokenPath(stateDir))
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read ingest token: %w", err)
	}
	return strings.TrimSpace(string(b)), nil
}

func writePrivate(stateDir, path string, buf []byte) error {
	if err := os.MkdirAll(stateDir, 0o750); err != nil {
		return fmt.Errorf("create state dir: %w", err)
	}
	if err := os.WriteFile(path, buf, 0o600); err != nil {
		return fmt.Errorf("write %s: %w", filepath.Base(path), err)
	}
	// WriteFile keeps the mode of a pre-existing file; force 0600 so a
	// rewrite over a loosely-permissioned file cannot leak it.
	return os.Chmod(path, 0o600)
}

// Client talks to the control-plane API. Its only credential is the node
// ingest token, sent as Bearer on every call.
type Client struct {
	base  string
	token string
	http  *http.Client
}

// NewClient builds a client for controlPlane authenticated by token.
// Redirects are not followed, so the token is only ever sent to base.
func NewClient(controlPlane, token string) *Client {
	return &Client{
		base:  strings.TrimSuffix(controlPlane, "/"),
		token: token,
		http: &http.Client{
			Timeout:       30 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}
}

// maxBody caps control-plane responses.
const maxBody = 1 << 20

// do sends body (JSON, when non-nil) and decodes a 2xx JSON response into out.
func (c *Client) do(ctx context.Context, method, path string, body, out any) error {
	var buf []byte
	if body != nil {
		var err error
		if buf, err = json.Marshal(body); err != nil {
			return err
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, bytes.NewReader(buf))
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	rdr := io.LimitReader(resp.Body, maxBody)
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		var e struct {
			Error string `json:"error"`
		}
		_ = json.NewDecoder(rdr).Decode(&e)
		return fmt.Errorf("control-plane %s %s: HTTP %d (%s)", method, path, resp.StatusCode, e.Error)
	}
	return json.NewDecoder(rdr).Decode(out)
}

// ActivateRequest is the agent's detection uploaded to activate a
// portal-created (pending) node. The presented ingest token identifies the
// node, so no name or id is needed.
type ActivateRequest struct {
	Chain     string            `json:"chain"`
	ELClient  string            `json:"el_client,omitempty"`
	CLClient  string            `json:"cl_client,omitempty"`
	ELVersion string            `json:"el_version,omitempty"`
	CLVersion string            `json:"cl_version,omitempty"`
	Endpoints []detect.Endpoint `json:"endpoints,omitempty"`
	Hostname  string            `json:"hostname,omitempty"`
}

// NodeParams are the only things the control plane tells the agent. Any
// other field in the response (e.g. alloy_config, kept for older agents)
// is ignored.
type NodeParams struct {
	NodeID         string `json:"node_id"`
	NodeName       string `json:"node_name"`
	TenantID       string `json:"tenant_id"`
	RemoteWriteURL string `json:"remote_write_url"`
}

// Activate uploads detection for the token's node, flipping it to 'active',
// and returns the node's parameters. The remote-write URL must be https
// when the control plane is (so the token never travels in clear).
func (c *Client) Activate(ctx context.Context, req ActivateRequest) (NodeParams, error) {
	var out NodeParams
	if err := c.do(ctx, http.MethodPost, "/api/v1/nodes/activate", req, &out); err != nil {
		return out, err
	}
	if out.NodeID == "" || out.NodeName == "" {
		return out, errors.New("control-plane returned no node id or name")
	}
	u, err := url.Parse(out.RemoteWriteURL)
	if err != nil || u.Host == "" || u.User != nil || (u.Scheme != "https" && u.Scheme != "http") {
		return out, fmt.Errorf("control-plane returned an invalid remote-write URL %q", out.RemoteWriteURL)
	}
	if strings.HasPrefix(c.base, "https://") && u.Scheme != "https" {
		return out, fmt.Errorf("control-plane returned a plain-http remote-write URL %q", out.RemoteWriteURL)
	}
	return out, nil
}

// Heartbeat polls the node's parameters; the server records last_seen.
// det, when known, reports the currently detected clients and their raw
// versions (all four always sent, so the server also learns when a client
// became unidentified); nil sends none.
func (c *Client) Heartbeat(ctx context.Context, nodeID string, det *detect.Result) (NodeParams, error) {
	path := "/api/v1/nodes/" + url.PathEscape(nodeID) + "/config"
	if det != nil {
		q := url.Values{}
		q.Set("el_client", det.ELClient)
		q.Set("cl_client", det.CLClient)
		q.Set("el_version", det.ELVersion)
		q.Set("cl_version", det.CLVersion)
		path += "?" + q.Encode()
	}
	var out NodeParams
	err := c.do(ctx, http.MethodGet, path, nil, &out)
	return out, err
}
