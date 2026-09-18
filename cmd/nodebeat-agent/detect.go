package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/nodebeat/agent/internal/alloycfg"
	"github.com/nodebeat/agent/internal/detect"
)

// runDetect probes --target for Ethereum clients and writes an Alloy config.
// Exit codes: 0 ok, 1 detection/render failure, 2 flag usage error.
func runDetect(args []string) int {
	fs := flag.NewFlagSet("detect", flag.ContinueOnError)
	target := fs.String("target", "", "node hostname or IP to probe (required; never defaults to localhost)")
	remoteWriteURL := fs.String("remote-write-url", "", "ingest endpoint, e.g. https://ingest:8428/api/v1/write (required)")
	exporterURL := fs.String("exporter-url", "", "bundled chain exporter /metrics URL on the agent host (default http://127.0.0.1:9090/metrics)")
	instance := fs.String("instance", "", "instance label for all series (default: --target)")
	out := fs.String("out", "config.alloy", "path to write the generated Alloy config")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *target == "" || *remoteWriteURL == "" {
		fmt.Fprintln(os.Stderr, "detect: --target and --remote-write-url are required")
		fs.Usage()
		return 2
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	res, err := detect.Detect(ctx, *target)
	if err != nil {
		fmt.Fprintln(os.Stderr, "detect:", err)
		return 1
	}
	cfg, err := alloycfg.Render(res, alloycfg.Options{
		RemoteWriteURL:     *remoteWriteURL,
		ExporterMetricsURL: *exporterURL,
		Instance:           *instance,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "detect:", err)
		return 1
	}
	if err := os.WriteFile(*out, []byte(cfg), 0o644); err != nil {
		fmt.Fprintln(os.Stderr, "detect:", err)
		return 1
	}

	summary, _ := json.Marshal(res)
	fmt.Println(string(summary))
	fmt.Fprintf(os.Stderr, "wrote Alloy config to %s\n", *out)
	return 0
}
