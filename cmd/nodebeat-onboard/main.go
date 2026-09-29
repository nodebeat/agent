package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/nodebeat/agent/internal/detect"
)

// Pre-flight checks for a node host. Everything here is read-only: statfs,
// `systemctl is-active` + the time daemon's status command, TCP connects to
// the P2P ports, a DNS lookup, and the same read-only probes as `detect`.
func main() {
	var (
		target         = flag.String("target", "127.0.0.1", "target host to check")
		checkDisk      = flag.Bool("check-disk", true, "check disk space")
		diskThreshold  = flag.Float64("disk-threshold", 15.0, "disk free threshold percent")
		checkNTP       = flag.Bool("check-ntp", true, "check NTP/time sync")
		ntpThreshold   = flag.Float64("ntp-threshold", 1.0, "max NTP offset in seconds")
		checkP2P       = flag.Bool("check-p2p", true, "check P2P ports")
		checkStaticIP  = flag.Bool("check-static-ip", true, "check target resolves to a routable (non-loopback) IP")
		chainFlag      = flag.String("chain", "ethereum", "chain type: ethereum or cosmos")
		nonInteractive = flag.Bool("non-interactive", false, "exit with code 1 on any failure")
	)
	var ports detect.Ports
	detect.BindPortFlags(flag.CommandLine, &ports)
	flag.Parse()

	ports = ports.WithDefaults()
	chain, err := detect.ParseChainFilter(*chainFlag)
	if err != nil || chain == "" {
		fmt.Fprintf(os.Stderr, "onboard: --chain must be ethereum or cosmos, got %q\n", *chainFlag)
		os.Exit(2)
	}
	remote := !isLocal(*target)

	checks := []struct {
		name    string
		enabled bool
		run     func() (bool, string)
	}{
		{"disk", *checkDisk, func() (bool, string) { return CheckDiskSpace(*diskThreshold, remote) }},
		{"ntp", *checkNTP, func() (bool, string) { return CheckNTP(ntpServices, *ntpThreshold, remote) }},
		{"p2p", *checkP2P, func() (bool, string) { return CheckP2PPorts(*target, p2pPorts(chain, ports)) }},
		{"static-ip", *checkStaticIP, func() (bool, string) { return CheckStaticIP(*target) }},
	}

	passed, failed := 0, 0
	fmt.Println("=== NodeBeat Node Onboarding Checks ===")
	fmt.Printf("Target: %s\n", *target)
	fmt.Printf("Chain: %s\n", chain)
	fmt.Println()
	for _, c := range checks {
		if !c.enabled {
			fmt.Printf("  [%s] SKIPPED\n", c.name)
			continue
		}
		if ok, msg := c.run(); ok {
			fmt.Printf("  [%s] PASS - %s\n", c.name, msg)
			passed++
		} else {
			fmt.Printf("  [%s] FAIL - %s\n", c.name, msg)
			failed++
		}
	}

	// Chain detection, restricted to the requested family.
	label := map[string]string{detect.ChainEthereum: "eth-detect", detect.ChainCosmos: "cosmos-detect"}[chain]
	fmt.Println()
	fmt.Printf("=== %s%s Detection ===\n", strings.ToUpper(chain[:1]), chain[1:])
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	det, err := detect.Detect(ctx, *target, ports, chain)
	cancel()
	if err != nil {
		fmt.Printf("  [%s] FAIL - %v\n", label, err)
		failed++
	} else {
		fmt.Printf("  [%s] PASS - chain=%s el=%s cl=%s endpoints=%d\n",
			label, det.Chain, det.ELClient, det.CLClient, len(det.Endpoints))
	}

	fmt.Println()
	fmt.Printf("=== Summary: %d passed, %d failed ===\n", passed, failed)
	if *nonInteractive && failed > 0 {
		os.Exit(1)
	}
}

// isLocal reports whether target is this host. Disk and NTP checks always
// run locally; for a remote target their result says so.
func isLocal(target string) bool {
	switch target {
	case "", "127.0.0.1", "localhost", "::1":
		return true
	}
	return false
}

// p2pPorts are the P2P ports the chain's clients must expose.
func p2pPorts(chain string, p detect.Ports) []int {
	if chain == detect.ChainCosmos {
		return []int{p.CosmosP2P}
	}
	return []int{p.ELP2P, p.CLP2P}
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

// runCommand runs a local command and returns stdout (swapped in tests).
var runCommand = func(name string, args ...string) ([]byte, error) {
	return exec.Command(name, args...).Output()
}

// ntpOffsetProbe asks a time daemon for its measured clock offset.
type ntpOffsetProbe struct {
	cmd   []string
	parse func(out string) (time.Duration, bool)
}

// ntpOffsetProbes maps each supported NTP service to the command that
// reports its current offset from the reference clock.
var ntpOffsetProbes = map[string]ntpOffsetProbe{
	"chronyd":           {[]string{"chronyc", "tracking"}, parseChronyOffset},
	"systemd-timesyncd": {[]string{"timedatectl", "timesync-status"}, parseTimesyncdOffset},
	"ntp":               {[]string{"ntpq", "-c", "rv"}, parseNtpqOffset},
}

// parseChronyOffset reads `chronyc tracking`:
// "System time     : 0.000012345 seconds fast of NTP time".
func parseChronyOffset(out string) (time.Duration, bool) {
	for _, line := range strings.Split(out, "\n") {
		k, v, ok := strings.Cut(line, ":")
		if !ok || strings.TrimSpace(k) != "System time" {
			continue
		}
		fields := strings.Fields(v)
		if len(fields) == 0 {
			return 0, false
		}
		secs, err := strconv.ParseFloat(fields[0], 64)
		if err != nil {
			return 0, false
		}
		return time.Duration(secs * float64(time.Second)), true
	}
	return 0, false
}

// parseTimesyncdOffset reads `timedatectl timesync-status`: "Offset: +1.234ms".
func parseTimesyncdOffset(out string) (time.Duration, bool) {
	for _, line := range strings.Split(out, "\n") {
		k, v, ok := strings.Cut(line, ":")
		if !ok || strings.TrimSpace(k) != "Offset" {
			continue
		}
		d, err := time.ParseDuration(strings.TrimSpace(v))
		if err != nil {
			return 0, false
		}
		return d, true
	}
	return 0, false
}

// parseNtpqOffset reads `ntpq -c rv`: "..., offset=0.123, ..." (milliseconds).
func parseNtpqOffset(out string) (time.Duration, bool) {
	for _, field := range strings.FieldsFunc(out, func(r rune) bool { return r == ',' || r == '\n' }) {
		k, v, ok := strings.Cut(strings.TrimSpace(field), "=")
		if !ok || k != "offset" {
			continue
		}
		ms, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0, false
		}
		return time.Duration(ms * float64(time.Millisecond)), true
	}
	return 0, false
}

// CheckNTP checks that an NTP service is active and that its measured clock
// offset is within threshold seconds.
// services is the list of service names to check (default: ntp, chronyd, systemd-timesyncd).
// isRemote indicates whether the target is a remote host (check runs locally).
func CheckNTP(services []string, threshold float64, isRemote bool) (bool, string) {
	suffix := ""
	if isRemote {
		suffix = " (local host only; remote NTP not checked)"
	}
	for _, svc := range services {
		output, err := runCommand("systemctl", "is-active", svc)
		if err != nil || strings.TrimSpace(string(output)) != "active" {
			continue
		}
		probe, known := ntpOffsetProbes[svc]
		if !known {
			return true, fmt.Sprintf("%s service active; offset not measurable (threshold %.3fs not checked)%s", svc, threshold, suffix)
		}
		out, err := runCommand(probe.cmd[0], probe.cmd[1:]...)
		offset, ok := probe.parse(string(out))
		if err != nil || !ok {
			return true, fmt.Sprintf("%s service active; offset unavailable from `%s` (threshold %.3fs not checked)%s",
				svc, strings.Join(probe.cmd, " "), threshold, suffix)
		}
		if abs := offset.Abs().Seconds(); abs > threshold {
			return false, fmt.Sprintf("%s service active but clock offset %s exceeds threshold %.3fs%s", svc, offset, threshold, suffix)
		}
		return true, fmt.Sprintf("%s service active, clock offset %s (threshold %.3fs)%s", svc, offset, threshold, suffix)
	}
	if isRemote {
		return false, "no NTP service found running locally (ntp/chronyd/systemd-timesyncd) — remote NTP not checked; run onboard on the node host"
	}
	return false, "no NTP service found running (ntp/chronyd/systemd-timesyncd)"
}

// ntpServices are the time daemons CheckNTP looks for, in order.
var ntpServices = []string{"ntp", "chronyd", "systemd-timesyncd"}

// CheckP2PPorts checks that each port accepts a TCP connection on target.
func CheckP2PPorts(target string, ports []int) (bool, string) {
	var failed []string
	for _, port := range ports {
		conn, err := net.DialTimeout("tcp", net.JoinHostPort(target, strconv.Itoa(port)), 3*time.Second)
		if err != nil {
			failed = append(failed, strconv.Itoa(port))
			continue
		}
		conn.Close()
	}
	if len(failed) > 0 {
		return false, fmt.Sprintf("P2P ports unreachable: %s", strings.Join(failed, ", "))
	}
	return true, fmt.Sprintf("all P2P ports reachable: %v", ports)
}

// CheckStaticIP checks if the target resolves to a routable (non-loopback) IP.
// This does not verify static assignment (DHCP vs static); that requires host/provider confirmation.
func CheckStaticIP(target string) (bool, string) {
	if isLocal(target) {
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
