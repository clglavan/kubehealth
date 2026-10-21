//go:build !windows

package main

import (
	"os/exec"
	"syscall"
)

// ownProcessGroup starts cmd in a process group of its own, and cancelling it
// kills the whole group, so nothing the command starts outlives it.
func ownProcessGroup(cmd *exec.Cmd) {
	newProcessGroup(cmd)
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
}

// newProcessGroup starts cmd in a process group of its own.
func newProcessGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

// stopProcessGroup signals cmd's whole group: SIGTERM, or SIGKILL with force.
func stopProcessGroup(cmd *exec.Cmd, force bool) {
	sig := syscall.SIGTERM
	if force {
		sig = syscall.SIGKILL
	}
	syscall.Kill(-cmd.Process.Pid, sig)
}

// shellCommand runs a command line the way the user's terminal would.
func shellCommand(line string) *exec.Cmd {
	return exec.Command("/bin/sh", "-c", line)
}
