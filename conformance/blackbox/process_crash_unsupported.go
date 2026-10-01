//go:build !linux

package blackbox

import "errors"

// openLocalProcessView fails because only the Linux pidfd handle keeps a
// backend signal on one process instance.
func openLocalProcessView() (localProcessView, error) {
	return localProcessView{}, errors.New("owned backend crash requires Linux pidfd and procfs process identity")
}

// retainedProcessIdentity is always unavailable outside Linux, because only a
// pidfd keeps an owner check on one process instance.
type retainedProcessIdentity struct{}

func openRetainedProcessIdentity(int) retainedProcessIdentity {
	return retainedProcessIdentity{}
}

func (*retainedProcessIdentity) alive() error {
	return errRetainedProcessIdentityUnavailable
}

func (*retainedProcessIdentity) close() error {
	return nil
}
