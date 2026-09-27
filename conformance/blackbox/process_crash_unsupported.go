//go:build !linux

package blackbox

import "errors"

// openLocalProcessView fails because only the Linux pidfd handle keeps a
// backend signal on one process instance.
func openLocalProcessView() (localProcessView, error) {
	return localProcessView{}, errors.New("owned backend crash requires Linux pidfd and procfs process identity")
}
