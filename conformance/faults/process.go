package faults

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"sync"

	"github.com/trainstar/synchro/conformance/barriers"
)

// Process is a started child process with cancellation-safe cleanup.
type Process struct {
	command *exec.Cmd
	owner   *Controller

	mu           sync.Mutex
	finished     bool
	waitErr      error
	terminated   bool
	terminateErr error
	unregister   func()

	done chan struct{}
	stop func() bool

	terminateOnce sync.Once
	closeOnce     sync.Once
	closeErr      error
}

// StartProcess starts command and kills it when ctx or owner is canceled.
func StartProcess(ctx context.Context, owner *Controller, command *exec.Cmd) (*Process, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if command == nil {
		return nil, ErrNilCommand
	}
	if err := command.Start(); err != nil {
		return nil, err
	}

	process := &Process{
		command: command,
		owner:   owner,
		done:    make(chan struct{}),
	}
	process.mu.Lock()
	process.stop = context.AfterFunc(ctx, func() {
		_ = process.Close()
	})
	process.mu.Unlock()
	go process.wait()
	if owner != nil {
		unregister, err := owner.register(process)
		if err != nil {
			_ = process.Close()
			return nil, err
		}
		process.setUnregister(unregister)
	}
	if err := ctx.Err(); err != nil {
		_ = process.Close()
		return nil, err
	}
	return process, nil
}

// Terminate sends a nonrecoverable termination signal to the child process.
func (p *Process) Terminate() error {
	if p == nil {
		return ErrFaultClosed
	}
	p.terminateOnce.Do(func() {
		p.mu.Lock()
		p.terminated = true
		command := p.command
		p.mu.Unlock()
		if command == nil || command.Process == nil {
			p.terminateErr = ErrFaultClosed
			return
		}
		if err := command.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			p.terminateErr = err
		}
	})
	return p.terminateErr
}

// Wait waits for process completion or context cancellation.
func (p *Process) Wait(ctx context.Context) error {
	if p == nil {
		return ErrFaultClosed
	}
	if err := checkContext(ctx); err != nil {
		return err
	}
	select {
	case <-p.done:
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.waitErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

// TerminateAt kills the process when the named barrier releases.
func (p *Process) TerminateAt(ctx context.Context, controller barriers.Controller, id barriers.BarrierID, participant string) (*Termination, error) {
	if p == nil {
		return nil, ErrFaultClosed
	}
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if controller == nil {
		return nil, ErrNilBarrierController
	}

	armedContext, cancel := context.WithCancel(ctx)
	termination := &Termination{
		process: p,
		cancel:  cancel,
		done:    make(chan struct{}),
	}
	termination.mu.Lock()
	termination.stop = context.AfterFunc(ctx, func() {
		_ = termination.Close()
	})
	termination.mu.Unlock()
	go func() {
		err := controller.Await(armedContext, id, participant)
		if err == nil {
			err = p.Terminate()
		}
		termination.finish(err)
	}()
	if p.owner != nil {
		unregister, err := p.owner.register(termination)
		if err != nil {
			_ = termination.Close()
			return nil, err
		}
		termination.setUnregister(unregister)
	}

	if err := ctx.Err(); err != nil {
		_ = termination.Close()
		return nil, err
	}
	return termination, nil
}

// Close kills the process and waits for its child-reaping goroutine.
func (p *Process) Close() error {
	if p == nil {
		return ErrFaultClosed
	}
	p.closeOnce.Do(func() {
		p.mu.Lock()
		stop := p.stop
		p.stop = nil
		p.mu.Unlock()
		if stop != nil {
			stop()
		}
		p.closeErr = p.Terminate()
		<-p.done
		p.removeUnregister()
	})
	return p.closeErr
}

// Done closes after the process has exited and Wait has completed.
func (p *Process) Done() <-chan struct{} {
	if p == nil {
		return nil
	}
	return p.done
}

func (p *Process) wait() {
	err := p.command.Wait()
	p.mu.Lock()
	stop := p.stop
	p.stop = nil
	p.waitErr = err
	p.finished = true
	unregister := p.unregister
	p.unregister = nil
	p.mu.Unlock()
	if stop != nil {
		stop()
	}
	close(p.done)
	if unregister != nil {
		unregister()
	}
}

func (p *Process) setUnregister(unregister func()) {
	p.mu.Lock()
	finished := p.finished
	if !finished {
		p.unregister = unregister
	}
	p.mu.Unlock()
	if finished && unregister != nil {
		unregister()
	}
}

func (p *Process) removeUnregister() {
	p.mu.Lock()
	unregister := p.unregister
	p.unregister = nil
	p.mu.Unlock()
	if unregister != nil {
		unregister()
	}
}

// Termination is a named-barrier process termination handle.
type Termination struct {
	process *Process
	cancel  context.CancelFunc

	mu         sync.Mutex
	finished   bool
	result     error
	unregister func()
	done       chan struct{}
	stop       func() bool

	closeOnce sync.Once
	closeErr  error
}

// Wait waits for the barrier release and termination request.
func (t *Termination) Wait(ctx context.Context) error {
	if t == nil {
		return ErrFaultClosed
	}
	if err := checkContext(ctx); err != nil {
		return err
	}
	select {
	case <-t.done:
		t.mu.Lock()
		defer t.mu.Unlock()
		return t.result
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close cancels a pending barrier wait and terminates the child process.
func (t *Termination) Close() error {
	if t == nil {
		return ErrFaultClosed
	}
	t.closeOnce.Do(func() {
		t.mu.Lock()
		stop := t.stop
		t.stop = nil
		t.mu.Unlock()
		if stop != nil {
			stop()
		}
		if t.cancel != nil {
			t.cancel()
		}
		<-t.done
		if t.process != nil {
			t.closeErr = t.process.Close()
		}
		t.removeUnregister()
	})
	return t.closeErr
}

// Done closes after the barrier wait resolves.
func (t *Termination) Done() <-chan struct{} {
	if t == nil {
		return nil
	}
	return t.done
}

func (t *Termination) finish(err error) {
	t.mu.Lock()
	if t.finished {
		t.mu.Unlock()
		return
	}
	t.finished = true
	t.result = err
	stop := t.stop
	t.stop = nil
	unregister := t.unregister
	t.unregister = nil
	t.mu.Unlock()
	if t.cancel != nil {
		t.cancel()
	}
	if stop != nil {
		stop()
	}
	close(t.done)
	if unregister != nil {
		unregister()
	}
}

func (t *Termination) setUnregister(unregister func()) {
	t.mu.Lock()
	finished := t.finished
	if !finished {
		t.unregister = unregister
	}
	t.mu.Unlock()
	if finished && unregister != nil {
		unregister()
	}
}

func (t *Termination) removeUnregister() {
	t.mu.Lock()
	unregister := t.unregister
	t.unregister = nil
	t.mu.Unlock()
	if unregister != nil {
		unregister()
	}
}
