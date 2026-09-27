package blackbox

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
)

const (
	crashFixturePostmasterPID = 41_000
	crashFixtureWorkerPID     = 41_001
	crashFixtureSiblingPID    = 41_002
	crashFixtureUnrelatedPID  = 42_000
	crashFixtureNamespace     = "pid:[4026531836]"
	crashFixtureDataDirectory = "/owned/synchro-conformance/postgres"
)

// crashFixtureProcess is one process instance. A later instance can reuse its
// process ID, but a handle stays bound to this instance.
type crashFixtureProcess struct {
	parent       int
	pidNamespace string
	exited       bool
	delivered    []syscall.Signal
}

type crashFixtureHandle struct {
	process    *crashFixtureProcess
	attempts   int
	closed     bool
	beforeKill func()
}

func (handle *crashFixtureHandle) kill() error {
	handle.attempts++
	if handle.beforeKill != nil {
		handle.beforeKill()
	}
	if handle.process.exited {
		return syscall.ESRCH
	}
	handle.process.delivered = append(handle.process.delivered, syscall.SIGKILL)
	return nil
}

func (handle *crashFixtureHandle) close() error {
	handle.closed = true
	return nil
}

// crashFixtureSystem models a process table where each numeric process ID
// names its current instance.
type crashFixtureSystem struct {
	processes    map[int]*crashFixtureProcess
	all          []*crashFixtureProcess
	observations []walWorkerObservation
	viewErr      error
	ownerErr     error
	afterOpen    func(*crashFixtureHandle)
	handles      []*crashFixtureHandle
	observeCalls int
}

func newCrashFixture() (*Harness, *crashFixtureSystem) {
	harness := &Harness{
		names:       HarnessNames{Database: "synchro_conformance_owned"},
		dataDir:     crashFixtureDataDirectory,
		sourceReady: true,
		postgres: &ownedProcess{
			command: &exec.Cmd{Process: &os.Process{Pid: crashFixturePostmasterPID}},
			done:    make(chan struct{}),
		},
	}
	fixture := &crashFixtureSystem{processes: map[int]*crashFixtureProcess{}}
	fixture.start(crashFixtureWorkerPID, crashFixturePostmasterPID, crashFixtureNamespace)
	fixture.start(crashFixtureSiblingPID, crashFixturePostmasterPID, crashFixtureNamespace)
	fixture.start(crashFixtureUnrelatedPID, 1, crashFixtureNamespace)
	fixture.observations = []walWorkerObservation{crashFixtureObservation(harness, crashFixtureWorkerPID)}
	return harness, fixture
}

func (fixture *crashFixtureSystem) start(pid, parent int, namespace string) *crashFixtureProcess {
	process := &crashFixtureProcess{parent: parent, pidNamespace: namespace}
	fixture.processes[pid] = process
	fixture.all = append(fixture.all, process)
	return process
}

// replace ends the current instance of pid and starts a new instance that
// passes every numeric check a stale target could pass.
func (fixture *crashFixtureSystem) replace(pid int) *crashFixtureProcess {
	fixture.processes[pid].exited = true
	return fixture.start(pid, crashFixturePostmasterPID, crashFixtureNamespace)
}

func crashFixtureObservation(harness *Harness, pid int) walWorkerObservation {
	return walWorkerObservation{database: harness.names.Database, dataDirectory: harness.dataDir, workers: 1, pid: pid}
}

func (fixture *crashFixtureSystem) system() backendCrashSystem {
	return backendCrashSystem{
		localProcesses: func() (localProcessView, error) {
			if fixture.viewErr != nil {
				return localProcessView{}, fixture.viewErr
			}
			return localProcessView{pidNamespace: crashFixtureNamespace, open: fixture.open, identity: fixture.identity}, nil
		},
		observeWorker: func(context.Context) (walWorkerObservation, error) {
			fixture.observeCalls++
			index := min(fixture.observeCalls, len(fixture.observations)) - 1
			return fixture.observations[index], nil
		},
		ownerAlive: func(*ownedProcess) error {
			return fixture.ownerErr
		},
	}
}

func (fixture *crashFixtureSystem) open(pid int) (stableProcessHandle, error) {
	process, found := fixture.processes[pid]
	if !found || process.exited {
		return nil, syscall.ESRCH
	}
	handle := &crashFixtureHandle{process: process}
	fixture.handles = append(fixture.handles, handle)
	if fixture.afterOpen != nil {
		fixture.afterOpen(handle)
	}
	return handle, nil
}

func (fixture *crashFixtureSystem) identity(pid int) (localProcessIdentity, error) {
	process, found := fixture.processes[pid]
	if !found || process.exited {
		return localProcessIdentity{}, syscall.ESRCH
	}
	return localProcessIdentity{parent: process.parent, pidNamespace: process.pidNamespace}, nil
}

func (fixture *crashFixtureSystem) killAttempts() int {
	attempts := 0
	for _, handle := range fixture.handles {
		attempts += handle.attempts
	}
	return attempts
}

func (fixture *crashFixtureSystem) requireNoDelivery(t *testing.T) {
	t.Helper()
	for index, process := range fixture.all {
		if len(process.delivered) != 0 {
			t.Fatalf("process instance %d received %v", index, process.delivered)
		}
	}
}

func (fixture *crashFixtureSystem) requireHandlesClosed(t *testing.T) {
	t.Helper()
	for _, handle := range fixture.handles {
		if !handle.closed {
			t.Fatal("backend crash left a process handle open")
		}
	}
}

func TestOwnedBackendCrashSignalsOnlyTheOwnedWALWorker(t *testing.T) {
	harness, fixture := newCrashFixture()
	worker := fixture.processes[crashFixtureWorkerPID]
	pid, err := harness.crashWALWorkerBackend(context.Background(), fixture.system())
	if err != nil {
		t.Fatalf("owned WAL worker backend crash failed: %v", err)
	}
	if pid != crashFixtureWorkerPID {
		t.Fatalf("crashed backend = %d, want %d", pid, crashFixtureWorkerPID)
	}
	if len(worker.delivered) != 1 || worker.delivered[0] != syscall.SIGKILL {
		t.Fatalf("owned WAL worker received %v, want one SIGKILL", worker.delivered)
	}
	for index, process := range fixture.all {
		if process != worker && len(process.delivered) != 0 {
			t.Fatalf("process instance %d received %v", index, process.delivered)
		}
	}
	if len(fixture.handles) != 1 || fixture.handles[0].process != worker {
		t.Fatalf("backend crash opened %d handles, want one handle for the WAL worker", len(fixture.handles))
	}
	fixture.requireHandlesClosed(t)
}

func TestOwnedBackendCrashRejectsUnownedTargetsWithoutSignal(t *testing.T) {
	tests := []struct {
		name           string
		missingContext bool
		// beforeLookup requires rejection before any database lookup or handle.
		beforeLookup bool
		mutate       func(*Harness, *crashFixtureSystem)
	}{
		{name: "missing context", missingContext: true, beforeLookup: true},
		{name: "source not ready", beforeLookup: true, mutate: func(h *Harness, _ *crashFixtureSystem) {
			h.sourceReady = false
		}},
		{name: "attached remote database", beforeLookup: true, mutate: func(h *Harness, _ *crashFixtureSystem) {
			h.attached = true
			h.attachHost = "database.example.invalid"
		}},
		{name: "missing owner", beforeLookup: true, mutate: func(h *Harness, _ *crashFixtureSystem) {
			h.postgres = nil
		}},
		{name: "exited owner", beforeLookup: true, mutate: func(h *Harness, _ *crashFixtureSystem) {
			close(h.postgres.done)
		}},
		{name: "unsupported stable identity", beforeLookup: true, mutate: func(_ *Harness, f *crashFixtureSystem) {
			f.viewErr = errors.New("stable process identity is unsupported")
		}},
		{name: "other database", mutate: func(_ *Harness, f *crashFixtureSystem) {
			f.observations[0].database = "postgres"
		}},
		{name: "other data directory", mutate: func(_ *Harness, f *crashFixtureSystem) {
			f.observations[0].dataDirectory = filepath.Join(filepath.Dir(f.observations[0].dataDirectory), "other")
		}},
		{name: "missing worker", mutate: func(_ *Harness, f *crashFixtureSystem) {
			f.observations[0].workers = 0
			f.observations[0].pid = 0
		}},
		{name: "ambiguous worker", mutate: func(_ *Harness, f *crashFixtureSystem) {
			f.observations[0].workers = 2
		}},
		{name: "stale worker process", mutate: func(_ *Harness, f *crashFixtureSystem) {
			f.processes[crashFixtureWorkerPID].exited = true
		}},
		{name: "reported process of another parent", mutate: func(h *Harness, f *crashFixtureSystem) {
			f.observations[0] = crashFixtureObservation(h, crashFixtureUnrelatedPID)
		}},
		{name: "reparented worker", mutate: func(_ *Harness, f *crashFixtureSystem) {
			f.processes[crashFixtureWorkerPID].parent = 1
		}},
		{name: "other process namespace", mutate: func(_ *Harness, f *crashFixtureSystem) {
			f.processes[crashFixtureWorkerPID].pidNamespace = "pid:[4026532999]"
		}},
		{name: "worker changed after handle", mutate: func(h *Harness, f *crashFixtureSystem) {
			f.observations = append(f.observations, crashFixtureObservation(h, crashFixtureSiblingPID))
		}},
		{name: "worker exited after handle", mutate: func(_ *Harness, f *crashFixtureSystem) {
			f.afterOpen = func(handle *crashFixtureHandle) {
				handle.process.exited = true
			}
		}},
		{name: "owner exited during verification", mutate: func(_ *Harness, f *crashFixtureSystem) {
			f.ownerErr = os.ErrProcessDone
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			harness, fixture := newCrashFixture()
			ctx := context.Background()
			if test.missingContext {
				ctx = nil
			}
			if test.mutate != nil {
				test.mutate(harness, fixture)
			}
			if _, err := harness.crashWALWorkerBackend(ctx, fixture.system()); err == nil {
				t.Fatal("backend crash accepted an unowned target")
			}
			if attempts := fixture.killAttempts(); attempts != 0 {
				t.Fatalf("rejected backend crash made %d signal attempts", attempts)
			}
			fixture.requireNoDelivery(t)
			fixture.requireHandlesClosed(t)
			if test.beforeLookup && (fixture.observeCalls != 0 || len(fixture.handles) != 0) {
				t.Fatalf("rejection ran %d database lookups and opened %d handles", fixture.observeCalls, len(fixture.handles))
			}
		})
	}
	var missingHarness *Harness
	if _, err := missingHarness.CrashWALWorkerBackend(context.Background()); err == nil {
		t.Fatal("backend crash accepted a missing harness")
	}
}

func TestOwnedBackendCrashHandleCannotSignalAReusedPID(t *testing.T) {
	for _, test := range []struct {
		name         string
		beforeSignal bool
	}{
		{name: "reused before verification"},
		{name: "reused before signal", beforeSignal: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			harness, fixture := newCrashFixture()
			var replacement *crashFixtureProcess
			reuse := func() { replacement = fixture.replace(crashFixtureWorkerPID) }
			fixture.afterOpen = func(handle *crashFixtureHandle) {
				if test.beforeSignal {
					handle.beforeKill = reuse
					return
				}
				reuse()
			}
			if _, err := harness.crashWALWorkerBackend(context.Background(), fixture.system()); err == nil {
				t.Fatal("backend crash reported a signal after its process was replaced")
			}
			if replacement == nil {
				t.Fatal("fixture did not replace the WAL worker process")
			}
			if len(fixture.handles) != 1 || fixture.handles[0].attempts != 1 {
				t.Fatalf("backend crash opened %d handles, want one signal attempt on the first handle", len(fixture.handles))
			}
			fixture.requireNoDelivery(t)
			fixture.requireHandlesClosed(t)
		})
	}
}

func TestOwnedBackendCrashLocalProcessViewMatchesPlatform(t *testing.T) {
	view, err := openLocalProcessView()
	if runtime.GOOS != "linux" {
		if err == nil {
			t.Fatal("stable backend identity was accepted without Linux pidfd")
		}
		return
	}
	if err != nil {
		t.Fatalf("open Linux process view: %v", err)
	}
	identity, err := view.identity(os.Getpid())
	if err != nil {
		t.Fatalf("read own procfs identity: %v", err)
	}
	if identity.parent != os.Getppid() || identity.pidNamespace != view.pidNamespace {
		t.Fatalf("own procfs identity = %+v, want parent %d in namespace %q", identity, os.Getppid(), view.pidNamespace)
	}
	handle, err := view.open(os.Getpid())
	if err != nil {
		t.Fatalf("open own pidfd: %v", err)
	}
	if err := handle.close(); err != nil {
		t.Fatalf("close own pidfd: %v", err)
	}
}
