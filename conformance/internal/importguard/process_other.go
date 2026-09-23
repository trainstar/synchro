//go:build !unix

package importguard

import (
	"os/exec"
	"time"
)

func configureCommand(command *exec.Cmd) {
	command.WaitDelay = 2 * time.Second
}
