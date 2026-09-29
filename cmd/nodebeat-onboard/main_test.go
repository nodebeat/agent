package main

import (
	"errors"
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

// fakeCommands swaps runCommand for canned outputs keyed by the joined argv.
func fakeCommands(t *testing.T, outputs map[string]string) {
	t.Helper()
	orig := runCommand
	runCommand = func(name string, args ...string) ([]byte, error) {
		if out, ok := outputs[strings.Join(append([]string{name}, args...), " ")]; ok {
			return []byte(out), nil
		}
		return nil, errors.New("inactive")
	}
	t.Cleanup(func() { runCommand = orig })
}

func TestCheckNTP(t *testing.T) {
	services := []string{"ntp", "chronyd", "systemd-timesyncd"}
	timesyncd := func(offset string) map[string]string {
		return map[string]string{
			"systemctl is-active systemd-timesyncd": "active\n",
			"timedatectl timesync-status":           "       Server: 1.2.3.4\n       Offset: " + offset + "\n        Delay: 1ms\n",
		}
	}
	cases := []struct {
		name      string
		outputs   map[string]string
		threshold float64
		remote    bool
		wantOK    bool
		wantMsg   string
	}{
		{"no service", nil, 1, false, false, "no NTP service found"},
		{"no service remote", nil, 1, true, false, "remote NTP not checked"},
		{"within threshold", timesyncd("+621.861ms"), 1, false, true, "offset 621.861ms"},
		{"exceeds threshold", timesyncd("+621.861ms"), 0.5, false, false, "exceeds threshold 0.500s"},
		{"negative offset exceeds", timesyncd("-2.5s"), 1, false, false, "exceeds threshold"},
		{"remote pass", timesyncd("12us"), 1, true, true, "local host only"},
		{"chrony", map[string]string{
			"systemctl is-active chronyd": "active\n",
			"chronyc tracking":            "Reference ID    : A9FEA97B\nSystem time     : 1.500000000 seconds slow of NTP time\n",
		}, 1, false, false, "exceeds threshold"},
		{"ntpd", map[string]string{
			"systemctl is-active ntp": "active\n",
			"ntpq -c rv":              "associd=0 status=0615 leap_none, sync_ntp,\nstratum=2, offset=-0.412, sys_jitter=0.1\n",
		}, 1, false, true, "offset -412µs"},
		{"offset unavailable", map[string]string{
			"systemctl is-active chronyd": "active\n",
		}, 1, false, true, "threshold 1.000s not checked"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fakeCommands(t, tc.outputs)
			ok, msg := CheckNTP(services, tc.threshold, tc.remote)
			if ok != tc.wantOK || !strings.Contains(msg, tc.wantMsg) {
				t.Errorf("CheckNTP = %v, %q; want %v containing %q", ok, msg, tc.wantOK, tc.wantMsg)
			}
		})
	}
}

func TestCheckP2PPorts(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	port := l.Addr().(*net.TCPAddr).Port

	if ok, msg := CheckP2PPorts("127.0.0.1", []int{port, port}); !ok {
		t.Errorf("reachable ports should pass: %s", msg)
	}
	ok, msg := CheckP2PPorts("127.0.0.1", []int{port, 1})
	if ok || !strings.Contains(msg, "unreachable: 1") {
		t.Errorf("an unreachable port should fail and be named: %v %s", ok, msg)
	}
}

func TestP2PPortsPerChain(t *testing.T) {
	p := detect.DefaultPorts()
	if got := fmt.Sprint(p2pPorts(detect.ChainEthereum, p)); got != "[30303 9000]" {
		t.Errorf("ethereum p2p ports = %s", got)
	}
	if got := fmt.Sprint(p2pPorts(detect.ChainCosmos, p)); got != "[26656]" {
		t.Errorf("cosmos p2p ports = %s", got)
	}
}

func TestIsLocal(t *testing.T) {
	for target, want := range map[string]bool{"": true, "127.0.0.1": true, "localhost": true, "::1": true, "192.168.1.100": false, "node.example": false} {
		if got := isLocal(target); got != want {
			t.Errorf("isLocal(%q) = %v, want %v", target, got, want)
		}
	}
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
