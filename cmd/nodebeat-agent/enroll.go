package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/nodebeat/agent/internal/detect"
	"github.com/nodebeat/agent/internal/enroll"
)

// runEnroll registers this host with the SaaS control plane:
//
//	detect (optional) → POST /api/v1/nodes → save enrollment.json → fetch config
//
// Afterwards `run --enrolled` supervises Alloy + exporter from the
// server-rendered pipeline. Exit codes: 0 ok, 1 failure, 2 flag usage error.
func runEnroll(args []string) int {
	fs := flag.NewFlagSet("enroll", flag.ContinueOnError)
	controlPlane := fs.String("control-plane", "", "control-plane base URL, e.g. http://localhost:18080 (required)")
	token := fs.String("token", "", "Clerk session JWT (Bearer auth)")
	orgID := fs.String("org-id", "", "org id for dev/self-hosted planes without Clerk (X-Org-ID header)")
	name := fs.String("name", "", "node name (default: OS hostname)")
	target := fs.String("target", "", "node hostname or IP to auto-detect (optional; skips probing when empty)")
	chain := fs.String("chain", "", "chain: ethereum or cosmos (required when --target is empty)")
	elEndpoint := fs.String("el-endpoint", "", "EL JSON-RPC URL (optional; auto-filled by detection)")
	clEndpoint := fs.String("cl-endpoint", "", "CL Beacon API base URL (optional; auto-filled by detection)")
	stateDir := fs.String("state-dir", ".nodebeat", "agent state dir; holds enrollment.json (0600)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *controlPlane == "" {
		fmt.Fprintln(os.Stderr, "enroll: --control-plane is required")
		fs.Usage()
		return 2
	}
	if *token == "" && *orgID == "" {
		fmt.Fprintln(os.Stderr, "enroll: one of --token or --org-id is required")
		fs.Usage()
		return 2
	}
	nodeName := *name
	if nodeName == "" {
		hn, err := os.Hostname()
		if err != nil || hn == "" {
			fmt.Fprintln(os.Stderr, "enroll: --name is required (hostname unavailable)")
			return 2
		}
		nodeName = hn
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Auto-detect chain + endpoints when a target is given; explicit flags
	// always win so air-gapped or already-known nodes can skip probing.
	det := &detect.Result{Target: nodeName, Chain: strings.ToLower(*chain)}
	if *target != "" {
		dctx, dcancel := context.WithTimeout(ctx, 30*time.Second)
		res, err := detect.Detect(dctx, *target)
		dcancel()
		if err != nil {
			fmt.Fprintln(os.Stderr, "enroll: detection failed (pass --chain/--el-endpoint/--cl-endpoint explicitly):", err)
			return 1
		}
		det = res
		fmt.Fprintf(os.Stderr, "enroll: detected chain=%s el=%s cl=%s endpoints=%d\n",
			det.Chain, det.ELClient, det.CLClient, len(det.Endpoints))
	}
	if det.Chain == "" {
		fmt.Fprintln(os.Stderr, "enroll: chain unknown (pass --target for auto-detect or --chain explicitly)")
		return 2
	}
	el := firstEndpoint(det, detect.KindELRPC, detect.KindCosmosRPC)
	if *elEndpoint != "" {
		el = *elEndpoint
	}
	cl := firstEndpoint(det, detect.KindCLBeacon)
	if *clEndpoint != "" {
		cl = *clEndpoint
	}

	client := enroll.NewClient(*controlPlane, *token, *orgID)
	nodeID, tenantID, rwURL, ingestToken, err := client.RegisterNode(ctx, nodeName, det.Chain, el, cl)
	if err != nil {
		fmt.Fprintln(os.Stderr, "enroll: register:", err)
		return 1
	}
	// Steady state uses the node token (narrow scope, never expires).
	// The Clerk JWT authorized this call only and is never persisted.
	nodeClient := enroll.NewClient(*controlPlane, ingestToken, "")
	cfg, err := nodeClient.FetchConfig(ctx, nodeID)
	if err != nil {
		fmt.Fprintln(os.Stderr, "enroll: fetch config:", err)
		return 1
	}
	if err := enroll.Save(*stateDir, enroll.Enrollment{
		ControlPlane:   *controlPlane,
		NodeID:         nodeID,
		NodeName:       nodeName,
		TenantID:       tenantID,
		RemoteWriteURL: rwURL,
		IngestToken:    ingestToken,
		Detection:      *det,
		EnrolledAt:     time.Now().UTC(),
	}); err != nil {
		fmt.Fprintln(os.Stderr, "enroll:", err)
		return 1
	}

	fmt.Printf("enrolled node %q (id %s, chain %s, tenant %s)\n", nodeName, nodeID, det.Chain, tenantID)
	fmt.Printf("remote-write: %s\n", rwURL)
	fmt.Printf("ingest token: %s (shown once — stored in %s; rotate via portal)\n", ingestToken, enroll.Path(*stateDir))
	fmt.Printf("alloy config: %d bytes (server-rendered)\n", len(cfg.AlloyConfig))
	fmt.Fprintf(os.Stderr, "enrollment saved to %s; start with: nodebeat-agent run --enrolled --state-dir %s\n",
		enroll.Path(*stateDir), *stateDir)
	return 0
}

func firstEndpoint(det *detect.Result, kinds ...string) string {
	for _, k := range kinds {
		for _, e := range det.Endpoints {
			if e.Kind == k {
				return e.URL
			}
		}
	}
	return ""
}
