package supervise

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type memLogger struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (l *memLogger) Printf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.buf.WriteString(fmt.Sprintf(format, args...))
	l.buf.WriteString("\n")
}

func (l *memLogger) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func testOpts() Options {
	return Options{
		RestartBase:            time.Millisecond,
		RestartMax:             10 * time.Millisecond,
		StableAfter:            time.Hour, // disabled unless a test opts in
		MaxConsecutiveFailures: 3,
		StopGrace:              2 * time.Second,
	}
}

func TestGivesUpOnCrashLoop(t *testing.T) {
	log := &memLogger{}
	sup := New(log, Options{
		RestartBase:            time.Millisecond,
		RestartMax:             5 * time.Millisecond,
		StableAfter:            time.Hour,
		MaxConsecutiveFailures: 2,
		StopGrace:              time.Second,
	}, Child{Name: "crasher", Path: "/bin/sh", Args: []string{"-c", "exit 1"}})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := sup.Run(ctx)
	if err == nil {
		t.Fatal("expected supervisor to fail on crash loop, got nil")
	}
	if !strings.Contains(err.Error(), "crasher") {
		t.Errorf("error should name the child, got: %v", err)
	}
	stats := sup.Stats()
	if len(stats) != 1 || stats[0].Restarts < 1 {
		t.Errorf("expected >=1 restart, got %+v", stats)
	}
}

func TestRestartsFlakyChildThenStopsCleanly(t *testing.T) {
	dir := t.TempDir()
	flag := filepath.Join(dir, "started")
	script := `if [ -f "$1" ]; then exec sleep 30; fi; touch "$1"; exit 1`
	log := &memLogger{}
	sup := New(log, testOpts(),
		Child{Name: "flaky", Path: "/bin/sh", Args: []string{"-c", script, "sh", flag}, Dir: dir})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- sup.Run(ctx) }()

	waitFor(t, 5*time.Second, "flaky child to restart", func() bool {
		st := sup.Stats()
		return len(st) == 1 && st[0].Running && st[0].Restarts >= 1
	})

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected clean shutdown, got: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("supervisor did not stop after cancel")
	}
	if _, err := os.Stat(flag); err != nil {
		t.Errorf("expected first run to leave flag file: %v", err)
	}
}

func TestChildOutputIsLogged(t *testing.T) {
	log := &memLogger{}
	sup := New(log, testOpts(),
		Child{Name: "chatty", Path: "/bin/sh", Args: []string{"-c", "echo hello-out; echo hello-err >&2; exec sleep 30"}})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- sup.Run(ctx) }()

	waitFor(t, 5*time.Second, "child output in logs", func() bool {
		out := log.String()
		return strings.Contains(out, "[chatty] hello-out") &&
			strings.Contains(out, "[chatty] hello-err")
	})

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected clean shutdown, got: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("supervisor did not stop after cancel")
	}
}

func TestStableRunResetsFailureStreak(t *testing.T) {
	dir := t.TempDir()
	flag := filepath.Join(dir, "started")
	// First run crashes fast; the rerun sleeps past StableAfter, so a later
	// crash must not trip a budget of 2.
	script := `if [ -f "$1" ]; then exec sleep 30; fi; touch "$1"; exit 1`
	log := &memLogger{}
	sup := New(log, Options{
		RestartBase:            time.Millisecond,
		RestartMax:             5 * time.Millisecond,
		StableAfter:            50 * time.Millisecond,
		MaxConsecutiveFailures: 2,
		StopGrace:              time.Second,
	}, Child{Name: "flaky", Path: "/bin/sh", Args: []string{"-c", script, "sh", flag}, Dir: dir})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- sup.Run(ctx) }()

	// If the streak were not reset, the supervisor would fail within ~10ms.
	// Surviving 300ms with the child running proves the reset.
	waitFor(t, 2*time.Second, "child running after stable period", func() bool {
		st := sup.Stats()
		return len(st) == 1 && st[0].Running && st[0].Restarts >= 1 &&
			time.Since(st[0].LastStart) > 100*time.Millisecond
	})
	select {
	case err := <-done:
		t.Fatalf("supervisor gave up despite stable run: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
}

// A child whose binary cannot start must still exhaust the failure budget.
// Regression: lastStart was only set on successful starts, so every failed
// start looked like a long stable run and reset the streak forever.
func TestGivesUpWhenChildCannotStart(t *testing.T) {
	log := &memLogger{}
	opts := testOpts()
	opts.StableAfter = 50 * time.Millisecond
	sup := New(log, opts, Child{Name: "ghost", Path: filepath.Join(t.TempDir(), "missing-binary")})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := sup.Run(ctx)
	if err == nil || !strings.Contains(err.Error(), "giving up") {
		t.Fatalf("Run() = %v, want give-up error (log:\n%s)", err, log.String())
	}
}

// A child that ignores SIGTERM is killed once StopGrace has passed.
func TestKillsChildIgnoringSIGTERM(t *testing.T) {
	log := &memLogger{}
	opts := testOpts()
	opts.StopGrace = 200 * time.Millisecond
	sup := New(log, opts, Child{Name: "stubborn", Path: "/bin/sh",
		Args: []string{"-c", `trap "" TERM; echo ready; while :; do sleep 1; done`}})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- sup.Run(ctx) }()
	waitFor(t, 5*time.Second, "child running", func() bool {
		return strings.Contains(log.String(), "[stubborn] ready")
	})

	start := time.Now()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected clean shutdown, got: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("supervisor did not kill a child ignoring SIGTERM")
	}
	if d := time.Since(start); d < opts.StopGrace {
		t.Errorf("stopped after %s, before the %s grace period", d, opts.StopGrace)
	}
}

// Children never inherit the ingest token from the agent's environment.
func TestChildEnvDropsIngestToken(t *testing.T) {
	t.Setenv("NB_INGEST_TOKEN", "secret-a")
	t.Setenv("NODEBEAT_INGEST_TOKEN", "secret-b")
	t.Setenv("NB_KEEP", "kept")
	env := strings.Join(childEnv(), "\n")
	if strings.Contains(env, "secret-") {
		t.Errorf("ingest token leaked into child env")
	}
	if !strings.Contains(env, "NB_KEEP=kept") {
		t.Errorf("unrelated variable dropped from child env")
	}
}
