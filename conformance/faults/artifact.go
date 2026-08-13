package faults

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
)

const maximumArtifactBytes = 64 << 20

// Artifact temporarily replaces one regular file and restores it on Close.
//
// It retains replacement bytes only while the fault is active. It does not
// export those bytes or emit them in a trace.
type Artifact struct {
	mu sync.Mutex

	path        string
	existed     bool
	mode        fs.FileMode
	original    []byte
	replacement []byte
	applied     bool
	closed      bool

	done       chan struct{}
	stop       func() bool
	unregister func()
	closeOnce  sync.Once
	closeErr   error
}

// TamperArtifact replaces path with replacement until Close restores it.
//
// The target must be a regular file or a currently absent path. Symlinks are
// rejected so temporary fault cleanup cannot target a different artifact.
func TamperArtifact(ctx context.Context, owner *Controller, path string, replacement []byte) (*Artifact, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	existed, original, mode, err := captureArtifact(path)
	if err != nil {
		return nil, err
	}

	artifact := &Artifact{
		path:        path,
		existed:     existed,
		mode:        mode,
		original:    append([]byte(nil), original...),
		replacement: append([]byte(nil), replacement...),
		done:        make(chan struct{}),
	}
	artifact.mu.Lock()
	artifact.stop = context.AfterFunc(ctx, func() {
		_ = artifact.Close()
	})
	artifact.mu.Unlock()
	if owner != nil {
		unregister, err := owner.register(artifact)
		if err != nil {
			_ = artifact.Close()
			return nil, err
		}
		artifact.setUnregister(unregister)
	}

	artifact.mu.Lock()
	if artifact.closed {
		artifact.mu.Unlock()
		_ = artifact.Close()
		return nil, ErrFaultClosed
	}
	err = writeArtifact(path, artifact.replacement, mode)
	if err == nil {
		artifact.applied = true
	}
	artifact.mu.Unlock()
	if err != nil {
		_ = artifact.Close()
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		_ = artifact.Close()
		return nil, err
	}
	return artifact, nil
}

// Restore restores the original file state. It is equivalent to Close.
func (a *Artifact) Restore() error {
	return a.Close()
}

// Close restores the original file state exactly once.
func (a *Artifact) Close() error {
	if a == nil {
		return ErrFaultClosed
	}
	a.closeOnce.Do(func() {
		a.mu.Lock()
		a.closed = true
		stop := a.stop
		a.stop = nil
		if a.applied {
			if a.existed {
				a.closeErr = writeArtifact(a.path, a.original, a.mode)
			} else {
				err := os.Remove(a.path)
				if err != nil && !errors.Is(err, os.ErrNotExist) {
					a.closeErr = err
				}
			}
			a.applied = false
		}
		zero(a.original)
		zero(a.replacement)
		a.original = nil
		a.replacement = nil
		unregister := a.unregister
		a.unregister = nil
		a.mu.Unlock()
		if stop != nil {
			stop()
		}

		if unregister != nil {
			unregister()
		}
		close(a.done)
	})
	return a.closeErr
}

// Done closes after artifact restoration completes.
func (a *Artifact) Done() <-chan struct{} {
	if a == nil {
		return nil
	}
	return a.done
}

func (a *Artifact) setUnregister(unregister func()) {
	a.mu.Lock()
	closed := a.closed
	if !closed {
		a.unregister = unregister
	}
	a.mu.Unlock()
	if closed && unregister != nil {
		unregister()
	}
}

func captureArtifact(path string) (bool, []byte, fs.FileMode, error) {
	if path == "" {
		return false, nil, 0, ErrInvalidArtifact
	}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil, 0o600, nil
	}
	if err != nil {
		return false, nil, 0, fmt.Errorf("inspect artifact: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return false, nil, 0, ErrInvalidArtifact
	}
	if info.Size() > maximumArtifactBytes {
		return false, nil, 0, ErrArtifactTooLarge
	}
	file, err := os.Open(path)
	if err != nil {
		return false, nil, 0, fmt.Errorf("open artifact: %w", err)
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil {
		return false, nil, 0, fmt.Errorf("inspect opened artifact: %w", err)
	}
	if !opened.Mode().IsRegular() {
		return false, nil, 0, ErrInvalidArtifact
	}
	data, err := readArtifact(file)
	if err != nil {
		return false, nil, 0, err
	}
	return true, data, opened.Mode(), nil
}

func readArtifact(reader io.Reader) ([]byte, error) {
	var output bytes.Buffer
	buffer := make([]byte, 32*1024)
	for {
		count, err := reader.Read(buffer)
		if count > 0 {
			if output.Len()+count > maximumArtifactBytes {
				return nil, ErrArtifactTooLarge
			}
			_, _ = output.Write(buffer[:count])
		}
		if errors.Is(err, io.EOF) {
			return output.Bytes(), nil
		}
		if err != nil {
			return nil, fmt.Errorf("read artifact: %w", err)
		}
	}
}

func writeArtifact(path string, data []byte, mode fs.FileMode) error {
	directory := filepath.Dir(path)
	base := filepath.Base(path)
	temporary, err := os.CreateTemp(directory, "."+base+".fault-")
	if err != nil {
		return fmt.Errorf("create temporary artifact: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)

	if err := temporary.Chmod(restoredMode(mode)); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("set temporary artifact mode: %w", err)
	}
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write temporary artifact: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sync temporary artifact: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close temporary artifact: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("replace artifact: %w", err)
	}
	return nil
}

func restoredMode(mode fs.FileMode) fs.FileMode {
	return mode & (fs.ModePerm | fs.ModeSetuid | fs.ModeSetgid | fs.ModeSticky)
}

func zero(data []byte) {
	for index := range data {
		data[index] = 0
	}
}
