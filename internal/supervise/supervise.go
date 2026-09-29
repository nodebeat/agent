// Package supervise runs and watches child processes: start, stream logs,
// restart crashed children with backoff, and stop everything gracefully.
//
// It supervises only the agent's own bundled binaries (Alloy, chain
// exporters). It never touches the customer validator or node processes.
package supervise

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Logger receives child lifecycle and output lines.
type Logger interface {
	Printf(format string, args ...any)
}

// Child describes one supervised process.
type Child struct {
	Name string
	Path string
	Args []string
	// Dir is the child's working directory (the agent state dir).
	Dir string
}

// Options tunes restart behavior. Zero values select the defaults.
type Options struct {
	// RestartBase is the first restart delay. Default 1s.
	RestartBase time.Duration
	// RestartMax caps the exponential backoff. Default 30s.
	RestartMax time.Duration
	// StableAfter resets the consecutive-failure counter once a child has
	// stayed up this long. Default 60s.
	StableAfter time.Duration
	// MaxConsecutiveFailures gives up on a child after this many crashes in
	// a row and fails the supervisor (so systemd can restart the agent).
	// Default 5.
	MaxConsecutiveFailures int
	// StopGrace is how long to wait after SIGTERM before SIGKILL. Default 10s.
	StopGrace time.Duration
}

func (o Options) withDefaults() Options {
	if o.RestartBase <= 0 {
		o.RestartBase = time.Second
	}
	if o.RestartMax <= 0 {
		o.RestartMax = 30 * time.Second
	}
	if o.StableAfter <= 0 {
		o.StableAfter = time.Minute
	}
	if o.MaxConsecutiveFailures <= 0 {
		o.MaxConsecutiveFailures = 5
	}
	if o.StopGrace <= 0 {
		o.StopGrace = 10 * time.Second
	}
	return o
}

// Stats is a point-in-time snapshot of one child.
type Stats struct {
	Name      string
	Running   bool
	Restarts  int
	Failures  int
	LastExit  string
	LastStart time.Time
}

type childState struct {
	spec Child
	mu   sync.Mutex
	// proc is the live process, nil while the child is not running.
	proc      *os.Process
	restarts  int
	failures  int
	lastExit  string
	lastStart time.Time
}

func (c *childState) stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Stats{
		Name:      c.spec.Name,
		Running:   c.proc != nil,
		Restarts:  c.restarts,
		Failures:  c.failures,
		LastExit:  c.lastExit,
		LastStart: c.lastStart,
	}
}

// Supervisor runs a fixed set of children until the context is cancelled.
type Supervisor struct {
	opts     Options
	log      Logger
	children []*childState
}

// New builds a supervisor for the given children.
func New(log Logger, opts Options, children ...Child) *Supervisor {
	s := &Supervisor{opts: opts.withDefaults(), log: log}
	for _, c := range children {
		s.children = append(s.children, &childState{spec: c})
	}
	return s
}

// Signal sends sig to the named child (used for config reloads, e.g. SIGHUP
// to Alloy). It errors when the child is unknown or not currently running.
func (s *Supervisor) Signal(name string, sig os.Signal) error {
	for _, c := range s.children {
		if c.spec.Name != name {
			continue
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.proc == nil {
			return fmt.Errorf("supervise: %s is not running", name)
		}
		return c.proc.Signal(sig)
	}
	return fmt.Errorf("supervise: unknown child %q", name)
}

// Stats returns a snapshot for every child.
func (s *Supervisor) Stats() []Stats {
	out := make([]Stats, 0, len(s.children))
	for _, c := range s.children {
		out = append(out, c.stats())
	}
	return out
}

// Run starts all children and blocks until ctx is cancelled (graceful stop,
// returns nil) or a child exceeds its failure budget (returns an error after
// stopping the rest). Stopping is only ever done by cancelling the shared
// context: each child's own runOnce then terminates its process.
func (s *Supervisor) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		wg      sync.WaitGroup
		errOnce sync.Once
		failErr error
	)
	for _, c := range s.children {
		wg.Go(func() {
			if err := s.keepAlive(ctx, c); err != nil {
				errOnce.Do(func() { failErr = err })
				cancel()
			}
		})
	}
	wg.Wait()
	return failErr
}

// keepAlive restarts c with exponential backoff until ctx ends (nil) or c
// crashes MaxConsecutiveFailures times in a row (error).
func (s *Supervisor) keepAlive(ctx context.Context, c *childState) error {
	delay := s.opts.RestartBase
	for {
		s.runOnce(ctx, c)
		if ctx.Err() != nil {
			return nil
		}

		c.mu.Lock()
		c.restarts++
		// A run that stayed up past StableAfter breaks the crash streak:
		// only rapid consecutive failures count toward the budget, and the
		// backoff restarts from the base delay.
		if time.Since(c.lastStart) >= s.opts.StableAfter {
			c.failures = 0
			delay = s.opts.RestartBase
		}
		c.failures++
		restarts, failures, lastExit := c.restarts, c.failures, c.lastExit
		c.mu.Unlock()

		s.log.Printf("supervise: %s exited (%s); restart %d in %s", c.spec.Name, lastExit, restarts, delay)
		if failures >= s.opts.MaxConsecutiveFailures {
			return fmt.Errorf("supervise: %s failed %d times in a row, giving up", c.spec.Name, failures)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(delay):
		}
		delay = min(delay*2, s.opts.RestartMax)
	}
}

// runOnce starts the child and waits for it to exit. When ctx ends, the
// standard library sends SIGTERM (cmd.Cancel) and, after StopGrace, SIGKILL
// (cmd.WaitDelay).
func (s *Supervisor) runOnce(ctx context.Context, c *childState) {
	out := &lineLogger{log: s.log, prefix: "[" + c.spec.Name + "] "}
	cmd := exec.CommandContext(ctx, c.spec.Path, c.spec.Args...)
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	cmd.WaitDelay = s.opts.StopGrace
	cmd.Dir = c.spec.Dir
	cmd.Env = childEnv()
	cmd.Stdout, cmd.Stderr = out, out

	// lastStart is the attempt time, also for a failed start: left stale,
	// keepAlive would read every failed start as a long stable run, reset
	// the streak, and retry at base delay forever.
	c.mu.Lock()
	c.lastStart = time.Now()
	c.mu.Unlock()
	if err := cmd.Start(); err != nil {
		c.setExited(fmt.Sprintf("start failed: %v", err))
		return
	}
	c.mu.Lock()
	c.proc = cmd.Process
	c.mu.Unlock()

	err := cmd.Wait()
	out.flush()
	exit := "exit 0"
	if cmd.ProcessState != nil {
		exit = "exit " + cmd.ProcessState.String()
	} else if err != nil {
		exit = err.Error()
	}
	c.setExited(exit)
}

func (c *childState) setExited(s string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.proc = nil
	c.lastExit = s
}

// childEnv is the agent's environment minus the ingest token: Alloy reads
// it from its 0600 config file, and no other child needs it.
func childEnv() []string {
	var env []string
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "NB_INGEST_TOKEN=") && !strings.HasPrefix(kv, "NODEBEAT_INGEST_TOKEN=") {
			env = append(env, kv)
		}
	}
	return env
}

// lineLogger forwards a child's output to the logger line by line. The same
// instance is both Stdout and Stderr, so exec serializes its Write calls.
type lineLogger struct {
	log    Logger
	prefix string
	buf    []byte
}

func (l *lineLogger) Write(p []byte) (int, error) {
	l.buf = append(l.buf, p...)
	for {
		i := bytes.IndexByte(l.buf, '\n')
		if i < 0 {
			break
		}
		l.log.Printf("%s%s", l.prefix, l.buf[:i])
		l.buf = l.buf[i+1:]
	}
	if len(l.buf) > 64<<10 { // no newline in 64 KiB: emit what we have
		l.flush()
	}
	return len(p), nil
}

func (l *lineLogger) flush() {
	if len(l.buf) > 0 {
		l.log.Printf("%s%s", l.prefix, l.buf)
		l.buf = nil
	}
}
