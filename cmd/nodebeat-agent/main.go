package main

import (
	"fmt"
	"os"

	"github.com/nodebeat/agent/internal/version"
)

func usage() {
	fmt.Fprintln(os.Stderr, `nodebeat-agent <command> [flags]

Commands:
  version   print the agent version
  detect    probe a node host for chain clients (Ethereum/Cosmos) and render an Alloy config
  enroll    register this host with the SaaS control plane (writes enrollment.json)
  run       supervise Alloy (+ cosmos-validator-watcher for Cosmos; Ethereum is polled in-process) against a node host (or --enrolled)`)
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "version", "--version", "-v":
		fmt.Println("nodebeat-agent", version.Version)
	case "detect":
		os.Exit(runDetect(os.Args[2:]))
	case "enroll":
		os.Exit(runEnroll(os.Args[2:]))
	case "run":
		os.Exit(runRun(os.Args[2:]))
	case "-h", "--help", "help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "nodebeat-agent: unknown command %q\n", os.Args[1])
		usage()
		os.Exit(2)
	}
}
