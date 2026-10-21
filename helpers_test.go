package main

import (
	"os"
	"testing"
)

// TestMain lets the test binary stand in for programs KubeHealth runs: with
// GO_WANT_FAKE_CLAUDE it acts as claude -p (claudecode_test.go), with
// GO_WANT_FAKE_TUNNEL as a tunnel command (tunnels_test.go).
func TestMain(m *testing.M) {
	switch {
	case os.Getenv("GO_WANT_FAKE_CLAUDE") == "1":
		os.Exit(fakeClaude(os.Args[1:]))
	case os.Getenv("GO_WANT_FAKE_TUNNEL") == "1":
		os.Exit(fakeTunnel())
	}
	os.Exit(m.Run())
}
