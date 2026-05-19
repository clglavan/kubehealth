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

	// Explicit container override via query string
	containerHint := r.URL.Query().Get("container")

	// Auto-detect container from cached events when no explicit hint
	if containerHint == "" {
		if v, ok := nsCache.Load(ns); ok {
			e := v.(*nsEntry)
			e.RLock()
			for _, ev := range e.events {
				if ev.ObjectName == podName && strings.EqualFold(ev.Kind, "Pod") {
					if c := extractContainerFromMsg(ev.Message); c != "" {
						containerHint = c
						break
					}
				}
			}
			e.RUnlock()
		}
	}

	ctx := context.Background()
	pod, err := client.CoreV1().Pods(ns).Get(ctx, podName, metav1.GetOptions{})
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
		stream, err := client.CoreV1().Pods(ns).GetLogs(podName, &corev1.PodLogOptions{
			Container: cName,
			TailLines: &tail,
		}).Stream(ctx)
		if err != nil {
			resp.Containers = append(resp.Containers, ContainerLogs{Container: cName, Error: err.Error()})
			continue
		}
		logBytes, readErr := io.ReadAll(stream)
		stream.Close()
		if readErr != nil {
			resp.Containers = append(resp.Containers, ContainerLogs{Container: cName, Error: readErr.Error()})
			continue
		}
		resp.Containers = append(resp.Containers, ContainerLogs{Container: cName, Logs: string(logBytes)})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// fetchLogsForAI fetches pod logs and returns them as a plain string for inclusion in AI prompts.
func fetchLogsForAI(ns, pod string) (string, error) {
	containerHint := ""
	if v, ok := nsCache.Load(ns); ok {
		e := v.(*nsEntry)
		e.RLock()
		for _, ev := range e.events {
			if ev.ObjectName == pod && strings.EqualFold(ev.Kind, "Pod") {
				if c := extractContainerFromMsg(ev.Message); c != "" {
					containerHint = c
					break
				}
			}
		}
		e.RUnlock()
	}

	ctx := context.Background()
	podObj, err := client.CoreV1().Pods(ns).Get(ctx, pod, metav1.GetOptions{})
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
	for _, cName := range containers {
		stream, err := client.CoreV1().Pods(ns).GetLogs(pod, &corev1.PodLogOptions{
			Container: cName,
			TailLines: &tail,
		}).Stream(ctx)
		if err != nil {
			sb.WriteString(fmt.Sprintf("=== container: %s — error: %v ===\n", cName, err))
			continue
		}
		logBytes, _ := io.ReadAll(stream)
		stream.Close()
		if len(containers) > 1 {
			sb.WriteString(fmt.Sprintf("=== container: %s ===\n", cName))
		}
		sb.Write(logBytes)
		if !strings.HasSuffix(sb.String(), "\n") {
			sb.WriteByte('\n')
		}
	}
	return sb.String(), nil
}
