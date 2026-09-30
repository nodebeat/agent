package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/nodebeat/agent/internal/detect"
	"github.com/nodebeat/agent/internal/enroll"
)

// runEnroll activates a portal-created (pending) node from the node host:
//
//	detect --target → POST /api/v1/nodes/activate → save ingest-token + enrollment.json
//
// Auth is the node's own ingest token (shown once at Add Node / rotation),
// via NODEBEAT_INGEST_TOKEN env or --ingest-token, or the token already in
// --state-dir (re-enroll). It is saved to <state-dir>/ingest-token (0600);
// enrollment.json holds no secret. Activation uploads detection (chain,
// clients, endpoints, hostname) and flips the node to 'active'; the reply
// carries only parameters (node name, tenant, remote-write URL, pinned
// here). Afterwards `run --enrolled` renders and supervises the pipeline
// locally. Exit codes: 0 ok, 1 failure, 2 flag usage error.
func runEnroll(args []string) int {
	fs := flag.NewFlagSet("enroll", flag.ContinueOnError)
	controlPlane := fs.String("control-plane", "", "control-plane base URL, e.g. https://app-dev.nodebeat.stream (required)")
	ingestTokenFlag := fs.String("ingest-token", "", "node ingest token nb_ingest_... (prefer NODEBEAT_INGEST_TOKEN env; default: the token already in --state-dir)")
	target := fs.String("target", "", "node hostname or IP to auto-detect (required)")
	hostname := fs.String("hostname", "", "reported hostname shown in the portal (default: OS hostname)")
	chainFlag := fs.String("chain", "", "restrict detection to ethereum or cosmos (empty = auto; use per instance on mixed hosts)")
	stateDir := fs.String("state-dir", ".nodebeat", "agent state dir; holds ingest-token and enrollment.json (0600)")
	var ports detect.Ports
	detect.BindPortFlags(fs, &ports)
	if err := fs.Parse(args); err != nil {
		return 2
	}
	usage := func(msg string) int {
		fmt.Fprintln(os.Stderr, "enroll:", msg)
		fs.Usage()
		return 2
	}
	chain, err := detect.ParseChainFilter(*chainFlag)
	if err != nil {
		return usage(err.Error())
	}
	if *controlPlane == "" {
		return usage("--control-plane is required")
	}
	if *target == "" {
		return usage("--target is required (activation uploads detection of that host)")
	}
	host := *hostname
	if host == "" {
		if host, err = os.Hostname(); err != nil || host == "" {
			return usage("--hostname is required (OS hostname unavailable)")
		}
	}
	dir, err := filepath.Abs(*stateDir)
	if err != nil {
		return usage(err.Error())
	}
	token, err := ingestToken(*ingestTokenFlag, dir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "enroll:", err)
		return 1
	}
	if token == "" {
		return usage("a node ingest token is required (NODEBEAT_INGEST_TOKEN env or --ingest-token; shown once at Add Node / rotation)")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	dctx, dcancel := context.WithTimeout(ctx, 30*time.Second)
	det, err := detect.Detect(dctx, *target, ports, chain)
	dcancel()
	if err != nil {
		fmt.Fprintln(os.Stderr, "enroll: detection failed:", err)
		return 1
	}
	fmt.Fprintf(os.Stderr, "enroll: detected chain=%s el=%s cl=%s endpoints=%d\n",
		det.Chain, det.ELClient, det.CLClient, len(det.Endpoints))

	p, err := enroll.NewClient(*controlPlane, token).Activate(ctx, enroll.ActivateRequest{
		Chain: det.Chain, ELClient: det.ELClient, CLClient: det.CLClient,
		ELVersion: det.ELVersion, CLVersion: det.CLVersion,
		Endpoints: det.Endpoints, Hostname: host,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "enroll: activate:", err)
		return 1
	}
	if err := enroll.Save(dir, enroll.Enrollment{
		ControlPlane:   *controlPlane,
		NodeID:         p.NodeID,
		NodeName:       p.NodeName,
		TenantID:       p.TenantID,
		RemoteWriteURL: p.RemoteWriteURL,
		Chain:          det.Chain,
		EnrolledAt:     time.Now().UTC(),
	}); err != nil {
		fmt.Fprintln(os.Stderr, "enroll:", err)
		return 1
	}

	fmt.Printf("activated node %q (id %s, chain %s, tenant %s)\n", p.NodeName, p.NodeID, det.Chain, p.TenantID)
	fmt.Printf("remote-write: %s (pinned)\n", p.RemoteWriteURL)
	fmt.Fprintf(os.Stderr, "enrollment saved to %s; start with: nodebeat-agent run --enrolled --target %s --state-dir %s\n",
		enroll.Path(dir), *target, dir)
	return 0
}
