//go:build !windows

package main

import (
	"os/exec"
	"syscall"
)

// ownProcessGroup starts cmd in a process group of its own, and cancelling it
// kills the whole group, so nothing the command starts outlives it.
func ownProcessGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
}
