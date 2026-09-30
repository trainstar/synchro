//go:build unix

package importguard

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// The descendant holds the only write end of a test-owned FIFO. End of file
// on the read end proves that the descendant is gone without a process ID, so
// the test never signals a process that it cannot identify. The descendant
// also has a bounded lifetime, so a failed cancellation cannot leave it
// running for long.
func TestModulePolicyCancellationKillsDescendants(t *testing.T) {
	bin := t.TempDir()
	fifoPath := filepath.Join(t.TempDir(), "descendant.fifo")
	if err := syscall.Mkfifo(fifoPath, 0o600); err != nil {
		t.Fatal(err)
	}
	reader, err := os.OpenFile(fifoPath, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	// Fd puts the reader in blocking mode, so reads wait for the descendant.
	_ = reader.Fd()

	goPath := filepath.Join(bin, "go")
	script := "#!/bin/sh\n(printf x >&9; exec sleep 30) 9>\"$SYNCHRO_DESCENDANT_FIFO\" </dev/null >/dev/null 2>&1 &\nwait\n"
	if err := os.WriteFile(goPath, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("SYNCHRO_DESCENDANT_FIFO", fifoPath)
	root := tempModule(t, map[string]string{"go.mod": testModuleFile})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- CheckModulePolicy(ctx, root)
	}()

	started := make(chan error, 1)
	go func() {
		marker := make([]byte, 1)
		for {
			count, err := reader.Read(marker)
			if count == 1 {
				started <- nil
				return
			}
			if err != nil && !errors.Is(err, io.EOF) {
				started <- err
				return
			}
			// End of file before the marker means the descendant has not opened the FIFO yet.
			time.Sleep(10 * time.Millisecond)
		}
	}()
	select {
	case err := <-started:
		if err != nil {
			cancel()
			<-result
			t.Fatal(err)
		}
	case err := <-result:
		t.Fatalf("module policy returned before its descendant started: %v", err)
	case <-time.After(5 * time.Second):
		cancel()
		<-result
		t.Fatal("timed out waiting for module-policy descendant process")
	}

	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}
	closed := make(chan error, 1)
	go func() {
		_, err := reader.Read(make([]byte, 1))
		closed <- err
	}()
	select {
	case err := <-closed:
		if !errors.Is(err, io.EOF) {
			t.Fatalf("descendant FIFO read after cancellation = %v, want end of file", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("descendant process survived context cancellation")
	}
}
