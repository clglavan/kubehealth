//go:build windows

package main

import "os/exec"

// ownProcessGroup: on Windows, cancelling kills the process itself (the
// exec.Cmd default).
func ownProcessGroup(cmd *exec.Cmd) {}
