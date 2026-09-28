package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"

	"github.com/nodebeat/agent/internal/detect"
)

func TestCheckDiskSpace(t *testing.T) {
	// We can't easily mock syscall.Statfs, so we just test the logic
	// with a known-good threshold. This will pass on any reasonable host.
	ok, msg := CheckDiskSpace(0.1, false) // 0.1% threshold should always pass
	if !ok {
		t.Errorf("CheckDiskSpace(0.1) should pass: %s", msg)
	}
	t.Logf("disk check: %s", msg)

	ok, msg = CheckDiskSpace(100.0, false) // 100% threshold should fail
	if ok {
		t.Errorf("CheckDiskSpace(100.0) should fail")
	}
	t.Logf("disk check (impossible threshold): %s", msg)

	// Test remote suffix
	ok, msg = CheckDiskSpace(0.1, true)
	if ok {
		if !strings.Contains(msg, "local host only") {
			t.Errorf("remote check should mention local host only: %s", msg)
		}
	}
}

func TestCheckNTP(t *testing.T) {
	// On most systems, at least one of these is not running.
	// We test that the function doesn't panic and returns a reasonable result.
	services := []string{"ntp", "chronyd", "systemd-timesyncd", "definitely-not-a-service-12345"}
	ok, msg := CheckNTP(services, false)
	// Result depends on system state; just verify it returns something sensible
	if !ok {
		if !strings.Contains(msg, "no NTP service found") {
			t.Errorf("unexpected fail message: %s", msg)
		}
	} else {
		if !strings.Contains(msg, "service active") {
			t.Errorf("unexpected pass message: %s", msg)
		}
	}
	t.Logf("ntp check: %s", msg)

	// Test remote suffix
	ok, msg = CheckNTP(services, true)
	if !ok {
		if !strings.Contains(msg, "remote NTP not checked") {
			t.Errorf("remote fail should mention remote not checked: %s", msg)
		}
	} else {
		if !strings.Contains(msg, "local host only") {
			t.Errorf("remote pass should mention local host only: %s", msg)
		}
	}
}

func TestCheckP2PPorts(t *testing.T) {
	// Start a test listener on a random port to simulate a reachable port
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	_, portStr, _ := net.SplitHostPort(l.Addr().String())
	var port int
	fmt.Sscanf(portStr, "%d", &port)

	// Test ethereum with both ports reachable (use same port for both)
	ok, msg := CheckP2PPorts("127.0.0.1", "ethereum", map[string]int{"elP2P": port, "clP2P": port})
	if !ok {
		t.Errorf("reachable ports should pass: %s", msg)
	}
	t.Logf("p2p check (ethereum reachable): %s", msg)

	// Test unreachable ports
	ok, msg = CheckP2PPorts("127.0.0.1", "ethereum", map[string]int{"elP2P": 1, "clP2P": 2})
	if ok {
		t.Errorf("unreachable ports should fail")
	}
	if !strings.Contains(msg, "unreachable") {
		t.Errorf("fail message should mention unreachable: %s", msg)
	}
	t.Logf("p2p check (unreachable): %s", msg)

	// Test cosmos chain with reachable port
	ok, msg = CheckP2PPorts("127.0.0.1", "cosmos", map[string]int{"cosmosP2P": port})
	if !ok {
		t.Errorf("cosmos with reachable port should pass: %s", msg)
	}
	t.Logf("p2p check (cosmos): %s", msg)

	// Test cosmos with unreachable port
	ok, msg = CheckP2PPorts("127.0.0.1", "cosmos", map[string]int{"cosmosP2P": 1})
	if ok {
		t.Errorf("cosmos unreachable should fail")
	}
	t.Logf("p2p check (cosmos unreachable): %s", msg)
}

func TestCheckStaticIP(t *testing.T) {
	// Test loopback - should skip
	ok, msg := CheckStaticIP("127.0.0.1")
	if !ok {
		t.Errorf("loopback should pass (skipped): %s", msg)
	}
	if !strings.Contains(msg, "skipped") {
		t.Errorf("loopback message should mention skipped: %s", msg)
	}
	t.Logf("static ip (loopback): %s", msg)

	ok, msg = CheckStaticIP("localhost")
	if !ok {
		t.Errorf("localhost should pass (skipped): %s", msg)
	}

	// Test a known hostname that resolves - may or may not work depending on DNS
	// We just verify it doesn't panic
	ok, msg = CheckStaticIP("google.com")
	t.Logf("static ip (google.com): ok=%v msg=%s", ok, msg)
}

func TestDoCheckDiskSpace(t *testing.T) {
	// Test the wrapper that matches OnboardCheck.Check signature
	ok, msg := doCheckDiskSpace("127.0.0.1", map[string]any{"threshold": 0.1})
	if !ok {
		t.Errorf("doCheckDiskSpace should pass: %s", msg)
	}

	ok, msg = doCheckDiskSpace("192.168.1.100", map[string]any{"threshold": 0.1})
	if !ok {
		t.Errorf("doCheckDiskSpace remote should pass: %s", msg)
	}
	if !strings.Contains(msg, "local host only") {
		t.Errorf("remote check should mention local host only: %s", msg)
	}
}

func TestDoCheckNTP(t *testing.T) {
	ok, msg := doCheckNTP("127.0.0.1", nil)
	t.Logf("doCheckNTP local: ok=%v msg=%s", ok, msg)

	ok, msg = doCheckNTP("192.168.1.100", nil)
	t.Logf("doCheckNTP remote: ok=%v msg=%s", ok, msg)
	if !ok {
		if !strings.Contains(msg, "remote NTP not checked") {
			t.Errorf("remote fail should mention remote not checked: %s", msg)
		}
	}
}

func TestDoCheckP2PPorts(t *testing.T) {
	// Start a test listener
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	_, portStr, _ := net.SplitHostPort(l.Addr().String())
	var port int
	fmt.Sscanf(portStr, "%d", &port)

	ok, msg := doCheckP2PPorts("127.0.0.1", map[string]any{"chain": "ethereum", "elP2P": port, "clP2P": port})
	if !ok {
		t.Errorf("doCheckP2PPorts ethereum should pass: %s", msg)
	}

	ok, msg = doCheckP2PPorts("127.0.0.1", map[string]any{"chain": "cosmos", "cosmosP2P": port})
	if !ok {
		t.Errorf("doCheckP2PPorts cosmos should pass: %s", msg)
	}

	// Test with defaults (port 0 means use default) - should fail unless defaults happen to be open
	ok, msg = doCheckP2PPorts("127.0.0.1", map[string]any{"chain": "ethereum", "elP2P": 0, "clP2P": 0})
	// Should fail with default ports (30303, 9000) unless those happen to be open
	t.Logf("doCheckP2PPorts defaults: ok=%v msg=%s", ok, msg)
}

func TestDoCheckStaticIP(t *testing.T) {
	ok, msg := doCheckStaticIP("127.0.0.1", nil)
	if !ok {
		t.Errorf("doCheckStaticIP loopback should pass: %s", msg)
	}

	ok, msg = doCheckStaticIP("google.com", nil)
	t.Logf("doCheckStaticIP google.com: ok=%v msg=%s", ok, msg)
}

// Integration test: run the full onboarding flow with default flags
func TestOnboardIntegration(t *testing.T) {
	// This test runs the actual main() logic but with a short timeout
	// We can't easily capture stdout from main, so we test the check functions directly
	// The integration is tested manually via `make e2e` which runs onboarding

	// Verify all check functions exist and are callable
	checks := []struct {
		name string
		fn   func(string, map[string]any) (bool, string)
	}{
		{"disk", doCheckDiskSpace},
		{"ntp", doCheckNTP},
		{"p2p", doCheckP2PPorts},
		{"static-ip", doCheckStaticIP},
	}

	for _, c := range checks {
		ok, msg := c.fn("127.0.0.1", nil)
		t.Logf("check %s: ok=%v msg=%s", c.name, ok, msg)
		if msg == "" {
			t.Errorf("check %s returned empty message", c.name)
		}
	}
}

// Test that Ports.WithDefaults fills in expected defaults
func TestPortsWithDefaults(t *testing.T) {
	ports := detect.Ports{}.WithDefaults()
	if ports.ELRPC != 8545 {
		t.Errorf("ELRPC default = %d, want 8545", ports.ELRPC)
	}
	if ports.Beacon != 5052 {
		t.Errorf("Beacon default = %d, want 5052", ports.Beacon)
	}
	if ports.ELP2P != 30303 {
		t.Errorf("ELP2P default = %d, want 30303", ports.ELP2P)
	}
	if ports.CLP2P != 9000 {
		t.Errorf("CLP2P default = %d, want 9000", ports.CLP2P)
	}
	if ports.CosmosRPC != 26657 {
		t.Errorf("CosmosRPC default = %d, want 26657", ports.CosmosRPC)
	}
	if ports.CosmosREST != 1317 {
		t.Errorf("CosmosREST default = %d, want 1317", ports.CosmosREST)
	}
	if ports.CosmosP2P != 26656 {
		t.Errorf("CosmosP2P default = %d, want 26656", ports.CosmosP2P)
	}
}

// Test flag parsing with custom ports (only port flags from detect.BindPortFlags)
func TestFlagParsing(t *testing.T) {
	// Save original args
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()

	os.Args = []string{"nodebeat-onboard", "--el-rpc-port", "8546", "--beacon-port", "5053"}

	var ports detect.Ports
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	detect.BindPortFlags(fs, &ports)
	if err := fs.Parse(os.Args[1:]); err != nil {
		t.Fatal(err)
	}
	ports = ports.WithDefaults()

	if ports.ELRPC != 8546 {
		t.Errorf("ELRPC = %d, want 8546", ports.ELRPC)
	}
	if ports.Beacon != 5053 {
		t.Errorf("Beacon = %d, want 5053", ports.Beacon)
	}
	if ports.CosmosP2P != 26656 { // default not overridden
		t.Errorf("CosmosP2P = %d, want 26656", ports.CosmosP2P)
	}
}
