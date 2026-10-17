package main

import (
	"strings"
	"testing"
	"time"
)

// An RCA on a Deployment starts from its Pods' warnings, not just its own
// events, and reads the newest failing Pod's logs.
func TestMemberEvidence(t *testing.T) {
	c := testCluster()
	now := time.Now()
	c.nsCache.Store("orders", &nsEntry{events: []EventSummary{
		warn("orders", "Pod", "orders-worker-67576b7f96-5bdql", "BackOff", 8, now.Add(-5*time.Minute), now.Add(-time.Minute)),
		warn("orders", "Pod", "orders-worker-67576b7f96-h5zlx", "BackOff", 10, now.Add(-5*time.Minute), now.Add(-10*time.Second)),
		warn("orders", "ReplicaSet", "orders-worker-67576b7f96", "FailedCreate", 1, now, now),
		// Another workload in the same namespace.
		warn("orders", "Pod", "orders-api-5fc8fd6d99-mhkpt", "Unhealthy", 1, now, now),
		// The anchor's own warning is already in the prompt.
		warn("orders", "Deployment", "orders-worker", "ProgressDeadlineExceeded", 1, now, now),
	}})

	lines, pods := memberEvidence(c, "orders", "Deployment", "orders-worker")
	if len(lines) != 3 {
		t.Fatalf("lines = %q; want the two Pods' and the ReplicaSet's warnings", lines)
	}
	for _, l := range lines {
		if strings.Contains(l, "orders-api") || strings.Contains(l, "ProgressDeadlineExceeded") {
			t.Errorf("unexpected line %q", l)
		}
	}
	if len(pods) != 2 || pods[0] != "orders-worker-67576b7f96-h5zlx" {
		t.Errorf("pods = %v; want the newest failing Pod first", pods)
	}

	// A Pod owns nothing.
	if lines, pods := memberEvidence(c, "orders", "Pod", "orders-worker-67576b7f96-h5zlx"); len(lines) != 0 || len(pods) != 0 {
		t.Errorf("pod anchor: lines=%q pods=%v", lines, pods)
	}
}
