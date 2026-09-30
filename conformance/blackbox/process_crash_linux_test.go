//go:build linux

package blackbox

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// TestOwnedBackendCrashOwnerIdentityRetainedUntilWait starts a harmless owned
// child that exits when its FIFO input closes. The test sends no signal.
func TestOwnedBackendCrashOwnerIdentityRetainedUntilWait(t *testing.T) {
	fifo := filepath.Join(t.TempDir(), "input")
	if err := unix.Mkfifo(fifo, 0o600); err != nil {
		t.Fatalf("create owned child input: %v", err)
	}
	child, err := startOwnedProcess("/bin/sh", []string{"-c", `read -r line < "$1"`, "sh", fifo}, nil, 4096, nil)
	if err != nil {
		t.Fatalf("start owned child: %v", err)
	}
	t.Cleanup(func() { releaseOwnedChildInput(t, fifo, child) })

	input := openOwnedChildInput(t, fifo, child)
	defer input.Close()
	if err := ownedProcessAlive(child); err != nil {
		t.Fatalf("owner check on the running owned child: %v", err)
	}
	if err := input.Close(); err != nil {
		t.Fatalf("close owned child input: %v", err)
	}
	select {
	case <-child.done:
	case <-time.After(10 * time.Second):
		t.Fatal("owned child did not exit after its input closed")
	}
	if err := ownedProcessAlive(child); !errors.Is(err, errRetainedProcessIdentityUnavailable) {
		t.Fatalf("owner check after Wait = %v, want unavailable identity", err)
	}
}

// openOwnedChildInput opens the FIFO writer after the child opens the reader.
func openOwnedChildInput(t *testing.T, fifo string, child *ownedProcess) *os.File {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		input, err := os.OpenFile(fifo, os.O_WRONLY|syscall.O_NONBLOCK, 0)
		if err == nil {
			return input
		}
		if !errors.Is(err, syscall.ENXIO) || child.Exited() || time.Now().After(deadline) {
			t.Fatalf("open owned child input: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// releaseOwnedChildInput ends a child that still waits on its input. A reader
// returns end of file after the last writer closes, so no signal is needed.
func releaseOwnedChildInput(t *testing.T, fifo string, child *ownedProcess) {
	deadline := time.After(10 * time.Second)
	for {
		if input, err := os.OpenFile(fifo, os.O_WRONLY|syscall.O_NONBLOCK, 0); err == nil {
			input.Close()
		}
		select {
		case <-child.done:
			// Wait has returned, so the context cancel cannot signal the child.
			child.cancel()
			return
		case <-deadline:
			t.Error("owned child still waits on its input after cleanup")
			return
		case <-time.After(10 * time.Millisecond):
		}
	}
}
