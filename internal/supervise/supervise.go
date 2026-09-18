// Package supervise runs and watches child processes: start, stream logs,
// restart crashed children with backoff, and stop everything gracefully.
//
// It supervises only the agent's own bundled binaries (Alloy, chain
// exporters). It never touches the customer validator or node processes.
package supervise

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
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
	// Env holds extra VAR=value entries appended to the current environment.
	Env []string
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
	// StopGrace is how long to wait for SIGTERM before SIGKILL. Default 10s.
	StopGrace time.Duration
}

func (o *Options) withDefaults() Options {
	out := *o
	if out.RestartBase <= 0 {
		out.RestartBase = time.Second
	}
	if out.RestartMax <= 0 {
		out.RestartMax = 30 * time.Second
	}
	if out.StableAfter <= 0 {
		out.StableAfter = time.Minute
	}
	if out.MaxConsecutiveFailures <= 0 {
		out.MaxConsecutiveFailures = 5
	}
	if out.StopGrace <= 0 {
		out.StopGrace = 10 * time.Second
	}
	return out
}

// Stats is a point-in-time snapshot of one child.
type Stats struct {
	Name      string
	Running   bool
	Restarts  int
	Failures  int
	LastExit  string
	LastStart time.Time
	StartedAt time.Time
}

type childState struct {
	spec      Child
	mu        sync.Mutex
	cmd       *exec.Cmd
	running   bool
	restarts  int
	failures  int
	lastExit  string
	lastStart time.Time
}

func (s *childState) stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Stats{
		Name:      s.spec.Name,
		Running:   s.running,
		Restarts:  s.restarts,
		Failures:  s.failures,
		LastExit:  s.lastExit,
		LastStart: s.lastStart,
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
		if c.cmd == nil || c.cmd.Process == nil {
			return fmt.Errorf("supervise: %s is not running", name)
		}
		return c.cmd.Process.Signal(sig)
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
// stopping the rest).
func (s *Supervisor) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	failed := make(chan error, len(s.children))
	var wg sync.WaitGroup
	for _, c := range s.children {
		wg.Add(1)
		go func(c *childState) {
			defer wg.Done()
			if err := s.keepAlive(ctx, c); err != nil {
				select {
				case failed <- err:
				default:
				}
				cancel()
			}
		}(c)
	}

	select {
	case err := <-failed:
		s.stopAll()
		wg.Wait()
		return err
	case <-ctx.Done():
		s.stopAll()
		wg.Wait()
		return nil
	}
}

func (s *Supervisor) keepAlive(ctx context.Context, c *childState) error {
	delay := s.opts.RestartBase
	for {
		// runOnce returns nil on crash (restart accounting below),
		// ctx.Err() on shutdown (swallowed: shutdown is not a failure),
		// or a fatal setup error.
		if err := s.runOnce(ctx, c); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		c.mu.Lock()
		c.restarts++
		// A run that stayed up past StableAfter breaks the crash streak:
		// only rapid consecutive failures count toward the budget.
		if time.Since(c.lastStart) >= s.opts.StableAfter {
			c.failures = 0
		}
		c.failures++
		failures := c.failures
		c.mu.Unlock()

		s.log.Printf("supervise: %s exited (%s); restart %d in %s",
			c.spec.Name, c.stats().LastExit, c.restarts, delay)
		if failures >= s.opts.MaxConsecutiveFailures {
			return fmt.Errorf("supervise: %s failed %d times in a row, giving up",
				c.spec.Name, failures)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(delay):
		}
		delay *= 2
		if delay > s.opts.RestartMax {
			delay = s.opts.RestartMax
		}
	}
}

// runOnce starts the child and waits for it to exit or for ctx to end.
// A nil return always means "proceed" (either clean shutdown or crash;
// crash accounting happens in keepAlive).
func (s *Supervisor) runOnce(ctx context.Context, c *childState) error {
	cmd := exec.Command(c.spec.Path, c.spec.Args...)
	cmd.Dir = c.spec.Dir
	cmd.Env = append([]string{}, envOrOS(c.spec.Env)...)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("supervise: %s stdout pipe: %w", c.spec.Name, err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("supervise: %s stderr pipe: %w", c.spec.Name, err)
	}
	if err := cmd.Start(); err != nil {
		c.setExited(fmt.Sprintf("start failed: %v", err))
		return nil
	}
	c.setRunning(cmd)

	go scanLines(stdout, func(line string) {
		s.log.Printf("[%s] %s", c.spec.Name, line)
	})
	go scanLines(stderr, func(line string) {
		s.log.Printf("[%s] %s", c.spec.Name, line)
	})

	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	select {
	case <-ctx.Done():
		terminate(cmd, s.opts.StopGrace)
		err := <-done
		c.setExited(exitString(cmd, err))
		return ctx.Err()
	case err := <-done:
		c.setExited(exitString(cmd, err))
		return nil
	}
}

func (s *Supervisor) stopAll() {
	for _, c := range s.children {
		c.mu.Lock()
		cmd := c.cmd
		c.mu.Unlock()
		if cmd != nil && cmd.Process != nil {
			terminate(cmd, s.opts.StopGrace)
		}
	}
}

func (c *childState) setRunning(cmd *exec.Cmd) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cmd = cmd
	c.running = true
	c.lastStart = time.Now()
}

func (c *childState) setExited(s string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cmd = nil
	c.running = false
	c.lastExit = s
}

// terminate asks nicely (SIGTERM), then insists (SIGKILL) after grace.
// It does not call Wait — the caller (runOnce) owns Wait on the exec.Cmd.
func terminate(cmd *exec.Cmd, grace time.Duration) {
	if cmd.Process == nil {
		return
	}
	_ = cmd.Process.Signal(syscall.SIGTERM)
	// Poll ProcessState: runOnce's cmd.Wait() will set it. Polling avoids a
	// second Wait (which would race with cmd.Wait).
	deadline := time.Now().Add(grace)
	for time.Now().Before(deadline) {
		if cmd.ProcessState != nil && cmd.ProcessState.Exited() {
			return
		}
		time.Sleep(50 * time.Millisecond)
		if cmd.ProcessState != nil && cmd.ProcessState.Exited() {
			return
		}
		// Check via Signal(0) whether process still exists.
		if err := cmd.Process.Signal(syscall.Signal(0)); err != nil {
			return
		}
	}
	_ = cmd.Process.Kill()
}

func exitString(cmd *exec.Cmd, err error) string {
	if err == nil {
		return "exit 0"
	}
	if cmd.ProcessState != nil {
		return "exit " + cmd.ProcessState.String()
	}
	return err.Error()
}

func scanLines(r io.Reader, emit func(string)) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	for sc.Scan() {
		emit(sc.Text())
	}
}

func envOrOS(extra []string) []string {
	return append(os.Environ(), extra...)
}
