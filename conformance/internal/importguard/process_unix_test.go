//go:build unix

package importguard

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestModulePolicyCancellationKillsDescendants(t *testing.T) {
	bin := t.TempDir()
	childPIDPath := filepath.Join(t.TempDir(), "child.pid")
	goPath := filepath.Join(bin, "go")
	script := "#!/bin/sh\nsleep 30 &\nchild=$!\nprintf '%s' \"$child\" > \"$SYNCHRO_CHILD_PID_FILE\"\nwait \"$child\"\n"
	if err := os.WriteFile(goPath, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("SYNCHRO_CHILD_PID_FILE", childPIDPath)
	root := tempModule(t, map[string]string{"go.mod": testModuleFile})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- CheckModulePolicy(ctx, root)
	}()

	var rawPID []byte
	startDeadline := time.NewTimer(5 * time.Second)
	defer startDeadline.Stop()
	startPoll := time.NewTicker(10 * time.Millisecond)
	defer startPoll.Stop()
	for len(rawPID) == 0 {
		var err error
		rawPID, err = os.ReadFile(childPIDPath)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			cancel()
			<-result
			t.Fatal(err)
		}
		select {
		case err := <-result:
			t.Fatalf("module policy returned before its child started: %v", err)
		case <-startDeadline.C:
			cancel()
			<-result
			t.Fatal("timed out waiting for module-policy child process")
		case <-startPoll.C:
		}
	}
	cancel()
	err := <-result
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(rawPID)))
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		err = syscall.Kill(pid, 0)
		if errors.Is(err, syscall.ESRCH) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	_ = syscall.Kill(pid, syscall.SIGKILL)
	t.Fatalf("descendant process %d survived context cancellation", pid)
}
