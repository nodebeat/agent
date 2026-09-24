package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/nodebeat/agent/internal/detect"
	"github.com/nodebeat/agent/internal/enroll"
)

// runEnroll activates a portal-created (pending) node from the node host:
//
//	detect --target → POST /api/v1/nodes/activate → save enrollment.json
//
// Auth is the node's own ingest token (shown once at Add Node / rotation),
// via --ingest-token or NODEBEAT_INGEST_TOKEN env — never a session JWT and
// never a flag that lands in shell history if env is used. Activation
// uploads detection (chain, clients, endpoints, hostname), flips the node
// to 'active', and returns the rendered pipeline in one round trip.
//
// Afterwards `run --enrolled` supervises Alloy + exporter from the
// server-rendered pipeline. Exit codes: 0 ok, 1 failure, 2 flag usage error.
func runEnroll(args []string) int {
	fs := flag.NewFlagSet("enroll", flag.ContinueOnError)
	controlPlane := fs.String("control-plane", "", "control-plane base URL, e.g. https://app-dev.nodebeat.stream (required)")
	ingestToken := fs.String("ingest-token", "", "node ingest token nb_ingest_... (or NODEBEAT_INGEST_TOKEN env); shown once at Add Node / rotation")
	target := fs.String("target", "", "node hostname or IP to auto-detect (required)")
	hostname := fs.String("hostname", "", "reported hostname shown in the portal (default: OS hostname)")
	chainFlag := fs.String("chain", "", "restrict detection to ethereum or cosmos (empty = auto; use per instance on mixed hosts)")
	stateDir := fs.String("state-dir", ".nodebeat", "agent state dir; holds enrollment.json (0600)")
	var ports detect.Ports
	detect.BindPortFlags(fs, &ports)
	if err := fs.Parse(args); err != nil {
		return 2
	}
	chain, err := detect.ParseChainFilter(*chainFlag)
	if err != nil {
		fmt.Fprintln(os.Stderr, "enroll:", err)
		return 2
	}
	if *controlPlane == "" {
		fmt.Fprintln(os.Stderr, "enroll: --control-plane is required")
		fs.Usage()
		return 2
	}
	token := firstNonEmpty(*ingestToken, os.Getenv("NODEBEAT_INGEST_TOKEN"))
	if token == "" {
		fmt.Fprintln(os.Stderr, "enroll: a node ingest token is required (--ingest-token or NODEBEAT_INGEST_TOKEN env; shown once at Add Node / rotation)")
		fs.Usage()
		return 2
	}
	if *target == "" {
		fmt.Fprintln(os.Stderr, "enroll: --target is required (activation uploads detection of that host)")
		fs.Usage()
		return 2
	}
	host := *hostname
	if host == "" {
		hn, err := os.Hostname()
		if err != nil || hn == "" {
			fmt.Fprintln(os.Stderr, "enroll: --hostname is required (OS hostname unavailable)")
			return 2
		}
		host = hn
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	dctx, dcancel := context.WithTimeout(ctx, 30*time.Second)
	det, err := detect.DetectFiltered(dctx, *target, ports, chain)
	dcancel()
	if err != nil {
		fmt.Fprintln(os.Stderr, "enroll: detection failed:", err)
		return 1
	}
	fmt.Fprintf(os.Stderr, "enroll: detected chain=%s el=%s cl=%s endpoints=%d\n",
		det.Chain, det.ELClient, det.CLClient, len(det.Endpoints))

	client := enroll.NewClient(*controlPlane, token, "")
	act, err := client.Activate(ctx, enroll.ActivateRequest{
		Chain: det.Chain, ELClient: det.ELClient, CLClient: det.CLClient,
		Endpoints: det.Endpoints, Hostname: host,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "enroll: activate:", err)
		return 1
	}
	if err := enroll.Save(*stateDir, enroll.Enrollment{
		ControlPlane:   *controlPlane,
		NodeID:         act.NodeID,
		NodeName:       act.NodeName,
		TenantID:       act.TenantID,
		RemoteWriteURL: act.RemoteWriteURL,
		IngestToken:    token,
		Detection:      *det,
		EnrolledAt:     time.Now().UTC(),
	}); err != nil {
		fmt.Fprintln(os.Stderr, "enroll:", err)
		return 1
	}

	fmt.Printf("activated node %q (id %s, chain %s, tenant %s)\n", act.NodeName, act.NodeID, det.Chain, act.TenantID)
	fmt.Printf("remote-write: %s\n", act.RemoteWriteURL)
	fmt.Printf("alloy config: %d bytes (server-rendered)\n", len(act.AlloyConfig))
	fmt.Fprintf(os.Stderr, "enrollment saved to %s; start with: nodebeat-agent run --enrolled --state-dir %s\n",
		enroll.Path(*stateDir), *stateDir)
	return 0
}
