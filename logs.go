package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type ContainerLogs struct {
	Container string `json:"container"`
	Logs      string `json:"logs"`
	Error     string `json:"error,omitempty"`
}

type PodLogsResponse struct {
	Pod        string          `json:"pod"`
	Namespace  string          `json:"namespace"`
	Containers []ContainerLogs `json:"containers"`
}

// containerInMsgRe matches patterns like "container php", "container \"php\"", "Stopping container php"
var containerInMsgRe = regexp.MustCompile(`(?i)container\s+"?([a-zA-Z0-9][a-zA-Z0-9_.-]*)"?`)

func extractContainerFromMsg(msg string) string {
	if m := containerInMsgRe.FindStringSubmatch(msg); len(m) > 1 {
		return m[1]
	}
	return ""
}

func jsonError(w http.ResponseWriter, ns, pod, msg string) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(PodLogsResponse{
		Pod:        pod,
		Namespace:  ns,
		Containers: []ContainerLogs{{Container: "-", Error: msg}},
	})
}

func handleAPILogs(w http.ResponseWriter, r *http.Request) {
	// /api/logs/{namespace}/{pod}
	path := strings.TrimPrefix(r.URL.Path, "/api/logs/")
	idx := strings.Index(path, "/")
	if idx < 0 || path[:idx] == "" || path[idx+1:] == "" {
		jsonError(w, "", "", "invalid path: expected /api/logs/{namespace}/{pod}")
		return
	}
	ns, podName := path[:idx], path[idx+1:]
	cl := apiCluster(w, r)
	if cl == nil {
		return
	}

	// Explicit container override via query string
	containerHint := r.URL.Query().Get("container")

	// Auto-detect container from cached events when no explicit hint
	if containerHint == "" {
		for _, ev := range cl.namespaceEvents(ns) {
			if ev.ObjectName == podName && strings.EqualFold(ev.Kind, "Pod") {
				if c := extractContainerFromMsg(ev.Message); c != "" {
					containerHint = c
					break
				}
			}
		}
	}

	ctx := context.Background()
	pod, err := cl.client.CoreV1().Pods(ns).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		jsonError(w, ns, podName, fmt.Sprintf("pod not found: %v", err))
		return
	}

	// Determine container list: use hint if it matches a real container, else all containers
	var containers []string
	if containerHint != "" {
		for _, c := range pod.Spec.Containers {
			if c.Name == containerHint {
				containers = []string{containerHint}
				break
			}
		}
	}
	if len(containers) == 0 {
		for _, c := range pod.Spec.Containers {
			containers = append(containers, c.Name)
		}
	}

	tail := getLogTailLines()
	resp := PodLogsResponse{Pod: podName, Namespace: ns}

	for _, cName := range containers {
		content := streamPodLogs(ctx, cl, ns, podName, cName, tail, false)
		previous := false
		if strings.TrimSpace(content) == "" {
			content = streamPodLogs(ctx, cl, ns, podName, cName, tail, true)
			if strings.TrimSpace(content) != "" {
				previous = true
			}
		}
		cl := ContainerLogs{Container: cName}
		if previous {
			cl.Logs = "[previous container run]\n" + content
		} else if content != "" {
			cl.Logs = content
		} else {
			cl.Error = "no logs available (pod may not be scheduled or has not produced output)"
		}
		resp.Containers = append(resp.Containers, cl)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// containerStatePrefix starts the line fetchLogsForAI puts above the logs.
const containerStatePrefix = "Container state: "

// containerStateLine says why a Pod's containers keep stopping, which their
// logs often don't: a container killed for using too much memory logs
// nothing about it. Empty when every container is running without restarts.
func containerStateLine(p *corev1.Pod) string {
	reasonOr := func(r, dflt string) string {
		if r == "" {
			return dflt
		}
		return r
	}
	memLimit := map[string]string{}
	for _, c := range p.Spec.Containers {
		if q, ok := c.Resources.Limits[corev1.ResourceMemory]; ok {
			memLimit[c.Name] = q.String()
		}
	}
	// The exit code alone doesn't say why; the signal behind it often does.
	// Runtimes don't always report OOMKilled (nested or cgroup v2 setups), so
	// a SIGKILL next to a memory limit is worth spelling out.
	ended := func(container string, t *corev1.ContainerStateTerminated) string {
		s := fmt.Sprintf("%s (exit code %d", reasonOr(t.Reason, "Error"), t.ExitCode)
		switch t.ExitCode {
		case 137:
			s += ", SIGKILL"
			if t.Reason != "OOMKilled" {
				s += ": usually out of memory"
			}
		case 143:
			s += ", SIGTERM"
		case 139:
			s += ", SIGSEGV"
		}
		if lim := memLimit[container]; lim != "" && (t.ExitCode == 137 || t.Reason == "OOMKilled") {
			s += ", memory limit " + lim
		}
		return s + ")"
	}
	var parts []string
	for _, cs := range p.Status.ContainerStatuses {
		var bits []string
		if cs.RestartCount > 0 {
			bits = append(bits, fmt.Sprintf("restarted %d times", cs.RestartCount))
		}
		if t := cs.LastTerminationState.Terminated; t != nil {
			bits = append(bits, "last run ended "+ended(cs.Name, t))
		}
		switch {
		case cs.State.Waiting != nil && cs.State.Waiting.Reason != "":
			w := cs.State.Waiting
			now := "now " + w.Reason
			// CrashLoopBackOff's message only repeats the back-off; others say why.
			if w.Message != "" && w.Reason != "CrashLoopBackOff" {
				now += ": " + truncate(w.Message, 200)
			}
			bits = append(bits, now)
		case cs.State.Terminated != nil:
			bits = append(bits, "now stopped: "+ended(cs.Name, cs.State.Terminated))
		}
		if len(bits) > 0 {
			parts = append(parts, cs.Name+" "+strings.Join(bits, ", "))
		}
	}
	if len(parts) == 0 {
		return ""
	}
	return containerStatePrefix + strings.Join(parts, "; ") + "."
}

// fetchLogsForAI fetches pod logs and returns them as a plain string for
// inclusion in AI prompts, after a line with the containers' state.
func fetchLogsForAI(cl *clusterState, ns, pod string) (string, error) {
	containerHint := ""
	for _, ev := range cl.namespaceEvents(ns) {
		if ev.ObjectName == pod && strings.EqualFold(ev.Kind, "Pod") {
			if c := extractContainerFromMsg(ev.Message); c != "" {
				containerHint = c
				break
			}
		}
	}

	ctx := context.Background()
	podObj, err := cl.client.CoreV1().Pods(ns).Get(ctx, pod, metav1.GetOptions{})
	if err != nil {
		return "", err
	}

	var containers []string
	if containerHint != "" {
		for _, c := range podObj.Spec.Containers {
			if c.Name == containerHint {
				containers = []string{containerHint}
				break
			}
		}
	}
	if len(containers) == 0 {
		for _, c := range podObj.Spec.Containers {
			containers = append(containers, c.Name)
		}
	}

	tail := getLogTailLines()
	var sb strings.Builder
	if state := containerStateLine(podObj); state != "" {
		sb.WriteString(state + "\n")
	}
	for _, cName := range containers {
		content := streamPodLogs(ctx, cl, ns, pod, cName, tail, false)
		if strings.TrimSpace(content) == "" {
			// Fallback: previous terminated container — critical for CrashLoopBackOff pods
			prev := streamPodLogs(ctx, cl, ns, pod, cName, tail, true)
			if strings.TrimSpace(prev) != "" {
				content = "[previous container run]\n" + prev
			}
		}
		if content == "" {
			continue
		}
		if len(containers) > 1 {
			fmt.Fprintf(&sb, "=== container: %s ===\n", cName)
		}
		sb.WriteString(content)
		if !strings.HasSuffix(sb.String(), "\n") {
			sb.WriteByte('\n')
		}
	}
	return sb.String(), nil
}

// streamPodLogs reads logs for one container; returns empty string on any error.
func streamPodLogs(ctx context.Context, cl *clusterState, ns, pod, container string, tail int64, previous bool) string {
	stream, err := cl.client.CoreV1().Pods(ns).GetLogs(pod, &corev1.PodLogOptions{
		Container: container,
		TailLines: &tail,
		Previous:  previous,
	}).Stream(ctx)
	if err != nil {
		return ""
	}
	b, _ := io.ReadAll(stream)
	stream.Close()
	return string(b)
}
