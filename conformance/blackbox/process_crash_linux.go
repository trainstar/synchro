//go:build linux

package blackbox

import (
	"errors"
	"os"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

type pidfdHandle int

func (handle pidfdHandle) kill() error {
	return unix.PidfdSendSignal(int(handle), unix.SIGKILL, nil, 0)
}

func (handle pidfdHandle) close() error {
	return unix.Close(int(handle))
}

// openLocalProcessView requires pidfd support for a stable process handle. It
// also requires procfs in the PID namespace of this process, so that pidfd and
// procfs name the same process for one process ID.
func openLocalProcessView() (localProcessView, error) {
	unsupported := errors.New("owned backend crash requires Linux pidfd and procfs process identity")
	probe, err := unix.PidfdOpen(os.Getpid(), 0)
	if err != nil {
		return localProcessView{}, unsupported
	}
	if err := unix.Close(probe); err != nil {
		return localProcessView{}, unsupported
	}
	status, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return localProcessView{}, unsupported
	}
	// NSpid has one entry only when procfs uses the namespace of this process.
	namespacePID, err := procfsStatusField(status, "NSpid")
	if err != nil || namespacePID != strconv.Itoa(os.Getpid()) {
		return localProcessView{}, unsupported
	}
	namespace, err := os.Readlink("/proc/self/ns/pid")
	if err != nil || namespace == "" {
		return localProcessView{}, unsupported
	}
	return localProcessView{pidNamespace: namespace, open: openPIDFD, identity: readProcfsIdentity}, nil
}

func openPIDFD(pid int) (stableProcessHandle, error) {
	fd, err := unix.PidfdOpen(pid, 0)
	if err != nil {
		return nil, err
	}
	return pidfdHandle(fd), nil
}

func readProcfsIdentity(pid int) (localProcessIdentity, error) {
	directory := "/proc/" + strconv.Itoa(pid)
	status, err := os.ReadFile(directory + "/status")
	if err != nil {
		return localProcessIdentity{}, err
	}
	value, err := procfsStatusField(status, "PPid")
	if err != nil {
		return localProcessIdentity{}, err
	}
	parent, err := strconv.Atoi(value)
	if err != nil || parent <= 0 {
		return localProcessIdentity{}, errors.New("procfs process parent is invalid")
	}
	namespace, err := os.Readlink(directory + "/ns/pid")
	if err != nil {
		return localProcessIdentity{}, err
	}
	return localProcessIdentity{parent: parent, pidNamespace: namespace}, nil
}

func procfsStatusField(status []byte, name string) (string, error) {
	for _, line := range strings.Split(string(status), "\n") {
		if value, found := strings.CutPrefix(line, name+":"); found {
			return strings.TrimSpace(value), nil
		}
	}
	return "", errors.New("procfs process status field is unavailable")
}
