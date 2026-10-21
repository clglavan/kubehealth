//go:build windows

package main

import "os/exec"

// ownProcessGroup: on Windows, cancelling kills the process itself (the
// exec.Cmd default).
func ownProcessGroup(cmd *exec.Cmd) {}

func newProcessGroup(cmd *exec.Cmd) {}

// stopProcessGroup kills the process; Windows has no process groups to signal.
func stopProcessGroup(cmd *exec.Cmd, force bool) {
	cmd.Process.Kill()
}

// shellCommand runs a command line the way the user's terminal would.
func shellCommand(line string) *exec.Cmd {
	return exec.Command("cmd", "/C", line)
}
