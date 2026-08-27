package swift

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

const (
	helperEnabled = "SYNCHRO_SWIFT_SESSION_HELPER"
	helperMode    = "SYNCHRO_SWIFT_SESSION_HELPER_MODE"
	helperStarted = "SYNCHRO_SWIFT_SESSION_HELPER_STARTED"
	helperRelease = "SYNCHRO_SWIFT_SESSION_HELPER_RELEASE"
)

func TestMain(m *testing.M) {
	if os.Getenv(helperEnabled) == "1" {
		runRunnerHelper()
		return
	}
	os.Exit(m.Run())
}

func runRunnerHelper() {
	mode := os.Getenv(helperMode)
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 4096), MaximumMessageBytes)
	requestCount := 0
	for scanner.Scan() {
		requestCount++
		switch mode {
		case "block":
			for {
				time.Sleep(time.Hour)
			}
		case "gate":
			if requestCount == 1 {
				_ = os.WriteFile(os.Getenv(helperStarted), []byte("started"), 0o600)
				for {
					if _, err := os.Stat(os.Getenv(helperRelease)); err == nil {
						break
					}
					time.Sleep(5 * time.Millisecond)
				}
			}
		case "stderr":
			_, _ = os.Stderr.Write(bytes.Repeat([]byte{'s'}, maximumRunnerStderr*2))
		}
		_, _ = fmt.Fprintln(os.Stdout, helperResponse(requestCount))
	}
}

func helperResponse(requestCount int) string {
	return fmt.Sprintf(`{"schema_version":1,"outcome":"passed","result":{"call_id":"call_%d","state":"completed","completion":"idle","transport_observations":{"observations":[],"overflowed":false,"sequence_checkpoint":0}},"error_code":null}`, requestCount)
}

func TestSessionStartsExecutesAndClosesRunner(t *testing.T) {
	t.Setenv(helperEnabled, "1")
	session := startTestSession(t, "")
	defer closeTestSession(t, session)

	result, err := session.Execute(context.Background(), Request{
		Operation: "begin-call",
		CallID:    "sync_cycle",
		Method:    "start",
	})
	if err != nil {
		t.Fatalf("execute command: %v", err)
	}
	if result.CallID == nil || *result.CallID != "call_1" {
		t.Fatalf("unexpected result: %#v", result.CallID)
	}
}

func TestSessionSerializesRequestsAndRechecksCancellation(t *testing.T) {
	t.Setenv(helperEnabled, "1")
	started := filepath.Join(t.TempDir(), "started")
	release := filepath.Join(t.TempDir(), "release")
	t.Setenv(helperMode, "gate")
	t.Setenv(helperStarted, started)
	t.Setenv(helperRelease, release)
	session := startTestSession(t, "gate")
	defer closeTestSession(t, session)

	firstDone := make(chan error, 1)
	go func() {
		_, err := session.Execute(context.Background(), Request{Operation: "capture"})
		firstDone <- err
	}()
	waitForFile(t, started)

	secondContext, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	secondDone := make(chan error, 1)
	go func() {
		_, err := session.Execute(secondContext, Request{Operation: "capture"})
		secondDone <- err
	}()
	select {
	case err := <-secondDone:
		t.Fatalf("second request completed before first request was released: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	if err := os.WriteFile(release, []byte("release"), 0o600); err != nil {
		t.Fatalf("release helper: %v", err)
	}
	if err := <-firstDone; err != nil {
		t.Fatalf("first request: %v", err)
	}
	if err := <-secondDone; !errorsIsContext(err) {
		t.Fatalf("second request error = %v, want context cancellation", err)
	}
}

func TestSessionCancellationKillsUnresponsiveRunner(t *testing.T) {
	t.Setenv(helperEnabled, "1")
	session := startTestSession(t, "block")

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := session.Execute(ctx, Request{Operation: "capture"}); !errorsIsContext(err) {
		t.Fatalf("execute error = %v, want context cancellation", err)
	}
	if _, err := session.Execute(context.Background(), Request{Operation: "capture"}); err == nil {
		t.Fatal("executed a command after cancellation killed the runner")
	}
	if err := session.Close(context.Background()); err != nil {
		t.Fatalf("close canceled runner: %v", err)
	}
}

func TestSessionBoundsStderr(t *testing.T) {
	t.Setenv(helperEnabled, "1")
	session := startTestSession(t, "stderr")
	defer closeTestSession(t, session)

	if _, err := session.Execute(context.Background(), Request{Operation: "capture"}); err != nil {
		t.Fatalf("execute stderr command: %v", err)
	}
	if size := session.process.stderrSize(); size != maximumRunnerStderr {
		t.Fatalf("stderr size = %d, want %d", size, maximumRunnerStderr)
	}
}

func TestSessionKillConfirmsChildTermination(t *testing.T) {
	t.Setenv(helperEnabled, "1")
	session := startTestSession(t, "")
	process := session.process
	process.mu.Lock()
	command := process.command
	process.mu.Unlock()
	if err := session.Kill(context.Background()); err != nil {
		t.Fatalf("kill runner: %v", err)
	}
	status, ok := command.ProcessState.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatal("killed runner did not retain its SIGKILL status")
	}
	if err := session.Close(context.Background()); err != nil {
		t.Fatalf("close killed runner: %v", err)
	}
}

func startTestSession(t *testing.T, mode string) *Session {
	t.Helper()
	if mode != "" {
		t.Setenv(helperMode, mode)
	}
	session, err := StartSession(context.Background(), Config{RunnerPath: os.Args[0]})
	if err != nil {
		t.Fatalf("start session: %v", err)
	}
	return session
}

func closeTestSession(t *testing.T, session *Session) {
	t.Helper()
	if err := session.Close(context.Background()); err != nil {
		t.Fatalf("close session: %v", err)
	}
}

func waitForFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", path)
}

func errorsIsContext(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
