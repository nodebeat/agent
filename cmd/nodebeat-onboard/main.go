package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/nodebeat/agent/internal/detect"
)

type OnboardCheck struct {
	Name     string
	Required bool
	Check    func(target string, params map[string]any) (bool, string)
	Params   map[string]any
}

func main() {
	var (
		target         = flag.String("target", "127.0.0.1", "target host to check")
		checkDisk      = flag.Bool("check-disk", true, "check disk space")
		diskThreshold  = flag.Float64("disk-threshold", 15.0, "disk free threshold percent")
		checkNTP       = flag.Bool("check-ntp", true, "check NTP/time sync")
		ntpThreshold   = flag.Float64("ntp-threshold", 1.0, "max NTP offset in seconds")
		checkP2P       = flag.Bool("check-p2p", true, "check P2P ports")
		checkStaticIP  = flag.Bool("check-static-ip", true, "check target resolves to a routable (non-loopback) IP")
		chain          = flag.String("chain", "ethereum", "chain type: ethereum or cosmos")
		verbose        = flag.Bool("v", false, "verbose output")
		nonInteractive = flag.Bool("non-interactive", false, "exit with code 1 on any failure")
	)
	var ports detect.Ports
	detect.BindPortFlags(flag.CommandLine, &ports)
	flag.Parse()

	if *verbose {
		log.SetFlags(log.LstdFlags | log.Lshortfile)
	}
	ports = ports.WithDefaults()
	chainName, err := detect.ParseChainFilter(*chain)
	if err != nil || chainName == "" {
		fmt.Fprintf(os.Stderr, "onboard: --chain must be ethereum or cosmos, got %q\n", *chain)
		os.Exit(2)
	}
	*chain = chainName

	checks := []OnboardCheck{
		{Name: "disk", Required: *checkDisk, Check: doCheckDiskSpace, Params: map[string]any{"threshold": *diskThreshold}},
		{Name: "ntp", Required: *checkNTP, Check: doCheckNTP, Params: map[string]any{"threshold": *ntpThreshold}},
		{Name: "p2p", Required: *checkP2P, Check: doCheckP2PPorts, Params: map[string]any{"chain": *chain, "elP2P": ports.ELP2P, "clP2P": ports.CLP2P, "cosmosP2P": ports.CosmosP2P}},
		{Name: "static-ip", Required: *checkStaticIP, Check: doCheckStaticIP, Params: nil},
	}

	passed := 0
	failed := 0

	fmt.Println("=== NodeBeat Node Onboarding Checks ===")
	fmt.Printf("Target: %s\n", *target)
	fmt.Printf("Chain: %s\n", *chain)
	fmt.Println()

	for _, check := range checks {
		if !check.Required {
			fmt.Printf("  [%s] SKIPPED\n", check.Name)
			continue
		}

		fmt.Printf("  [%s] ", check.Name)
		ok, msg := check.Check(*target, check.Params)
		if ok {
			fmt.Printf("PASS - %s\n", msg)
			passed++
		} else {
			fmt.Printf("FAIL - %s\n", msg)
			failed++
		}
	}

	// Chain-specific detection
	if *chain == "cosmos" {
		fmt.Println()
		fmt.Println("=== Cosmos Detection ===")
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		det, err := detect.DetectFiltered(ctx, *target, ports, detect.ChainCosmos)
		if err != nil {
			fmt.Printf("  [cosmos-detect] FAIL - %v\n", err)
			failed++
		} else {
			fmt.Printf("  [cosmos-detect] PASS - chain=%s cl=%s endpoints=%d\n",
				det.Chain, det.CLClient, len(det.Endpoints))
		}
	} else {
		fmt.Println()
		fmt.Println("=== Ethereum Detection ===")
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		det, err := detect.DetectFiltered(ctx, *target, ports, detect.ChainEthereum)
		if err != nil {
			fmt.Printf("  [eth-detect] FAIL - %v\n", err)
			failed++
		} else {
			fmt.Printf("  [eth-detect] PASS - chain=%s el=%s cl=%s endpoints=%d\n",
				det.Chain, det.ELClient, det.CLClient, len(det.Endpoints))
		}
	}

	fmt.Println()
	fmt.Printf("=== Summary: %d passed, %d failed ===\n", passed, failed)

	if *nonInteractive && failed > 0 {
		os.Exit(1)
	}
}

// CheckDiskSpace checks if the root filesystem has enough free space.
// threshold is the minimum free percentage (e.g., 15.0 for 15%).
// isRemote indicates whether the target is a remote host (check runs locally).
func CheckDiskSpace(threshold float64, isRemote bool) (bool, string) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs("/", &stat); err != nil {
		return false, fmt.Sprintf("statfs failed: %v", err)
	}

	total := float64(stat.Blocks) * float64(stat.Bsize)
	free := float64(stat.Bavail) * float64(stat.Bsize)
	pct := (free / total) * 100
	suffix := ""
	if isRemote {
		suffix = " (local host only; remote disk not checked — run onboard on the node host for accurate result)"
	}
	if pct < threshold {
		return false, fmt.Sprintf("disk free %.1f%% (threshold %.1f%%)%s", pct, threshold, suffix)
	}
	return true, fmt.Sprintf("disk free %.1f%% (threshold %.1f%%)%s", pct, threshold, suffix)
}

func doCheckDiskSpace(target string, params map[string]any) (bool, string) {
	threshold := 15.0
	if v, ok := params["threshold"]; ok {
		if f, ok := v.(float64); ok {
			threshold = f
		}
	}

	isRemote := target != "" && target != "127.0.0.1" && target != "localhost" && target != "::1"
	return CheckDiskSpace(threshold, isRemote)
}

// CheckNTP checks if an NTP service is active.
// services is the list of service names to check (default: ntp, chronyd, systemd-timesyncd).
// isRemote indicates whether the target is a remote host.
func CheckNTP(services []string, isRemote bool) (bool, string) {
	for _, svc := range services {
		output, err := exec.Command("systemctl", "is-active", svc).Output()
		if err == nil && strings.TrimSpace(string(output)) == "active" {
			if isRemote {
				return true, fmt.Sprintf("%s service active (local host only; remote NTP not checked)", svc)
			}
			return true, fmt.Sprintf("%s service active", svc)
		}
	}
	if isRemote {
		return false, "no NTP service found running locally (ntp/chronyd/systemd-timesyncd) — remote NTP not checked; run onboard on the node host"
	}
	return false, "no NTP service found running (ntp/chronyd/systemd-timesyncd)"
}

func doCheckNTP(target string, params map[string]any) (bool, string) {
	services := []string{"ntp", "chronyd", "systemd-timesyncd"}
	isRemote := target != "" && target != "127.0.0.1" && target != "localhost" && target != "::1"
	return CheckNTP(services, isRemote)
}

// CheckP2PPorts checks if the required P2P ports are reachable on the target.
// chain specifies the chain type ("ethereum" or "cosmos").
// ports is a map of port names to port numbers (elP2P, clP2P, cosmosP2P).
func CheckP2PPorts(target, chain string, ports map[string]int) (bool, string) {
	var portList []int
	switch strings.ToLower(chain) {
	case "ethereum":
		portList = []int{ports["elP2P"], ports["clP2P"]}
	case "cosmos":
		portList = []int{ports["cosmosP2P"]}
	default:
		portList = []int{ports["elP2P"], ports["cosmosP2P"], ports["clP2P"]}
	}

	var failed []string
	for _, port := range portList {
		conn, err := net.DialTimeout("tcp", net.JoinHostPort(target, strconv.Itoa(port)), 3*time.Second)
		if err != nil {
			failed = append(failed, fmt.Sprintf("%d", port))
			continue
		}
		conn.Close()
	}

	if len(failed) > 0 {
		return false, fmt.Sprintf("P2P ports unreachable: %s", strings.Join(failed, ", "))
	}
	return true, fmt.Sprintf("all P2P ports reachable: %v", portList)
}

func doCheckP2PPorts(target string, params map[string]any) (bool, string) {
	chain := "ethereum"
	if v, ok := params["chain"]; ok {
		if s, ok := v.(string); ok {
			chain = s
		}
	}
	elP2P, _ := params["elP2P"].(int)
	if elP2P == 0 {
		elP2P = 30303
	}
	clP2P, _ := params["clP2P"].(int)
	if clP2P == 0 {
		clP2P = 9000
	}
	cosmosP2P, _ := params["cosmosP2P"].(int)
	if cosmosP2P == 0 {
		cosmosP2P = 26656
	}

	return CheckP2PPorts(target, chain, map[string]int{
		"elP2P":     elP2P,
		"clP2P":     clP2P,
		"cosmosP2P": cosmosP2P,
	})
}

// CheckStaticIP checks if the target resolves to a routable (non-loopback) IP.
// This does not verify static assignment (DHCP vs static); that requires host/provider confirmation.
func CheckStaticIP(target string) (bool, string) {
	if target == "" || target == "127.0.0.1" || target == "localhost" || target == "::1" {
		return true, "loopback target: public-IP check skipped (pass the node's public hostname to verify)"
	}
	ips, err := net.LookupIP(target)
	if err != nil {
		return false, fmt.Sprintf("DNS lookup failed: %v", err)
	}

	for _, ip := range ips {
		if !ip.IsLoopback() && !ip.IsLinkLocalUnicast() && !ip.IsLinkLocalMulticast() && !ip.IsUnspecified() {
			return true, fmt.Sprintf("target resolves to routable IP: %s (static assignment not verifiable — confirm with your host/network provider)", ip.String())
		}
	}
	return false, "no suitable routable IP found (only loopback/link-local)"
}

func doCheckStaticIP(target string, params map[string]any) (bool, string) {
	return CheckStaticIP(target)
}
