package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	k8stesting "k8s.io/client-go/testing"
	"sigs.k8s.io/yaml"
)

// A loaded snapshot: its metadata, objects (as recorded, timestamps not yet
// moved), API discovery and container logs.
type benchSnapshot struct {
	dir       string
	meta      snapshotMeta
	objects   []map[string]any
	discovery []*metav1.APIResourceList
	logs      map[string]string // logKey → recorded log text
}

func logKey(ns, pod, container string, previous bool) string {
	k := ns + "/" + pod + "/" + container
	if previous {
		k += ".previous"
	}
	return k
}

func loadSnapshot(dir string) (*benchSnapshot, error) {
	meta, err := readSnapshotMeta(dir)
	if err != nil {
		return nil, err
	}
	s := &benchSnapshot{dir: dir, meta: meta, logs: map[string]string{}}
	files, _ := filepath.Glob(filepath.Join(dir, "cluster", "*.yaml"))
	nsFiles, _ := filepath.Glob(filepath.Join(dir, "namespaces", "*", "*.yaml"))
	files = append(files, nsFiles...)
	sort.Strings(files)
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			return nil, err
		}
		var list struct {
			Items []map[string]any `json:"items"`
		}
		if err := yaml.Unmarshal(b, &list); err != nil {
			return nil, fmt.Errorf("%s: %w", f, err)
		}
		s.objects = append(s.objects, list.Items...)
	}
	if b, err := os.ReadFile(filepath.Join(dir, "discovery.yaml")); err == nil {
		var d struct {
			Resources []*metav1.APIResourceList `json:"resources"`
		}
		if err := yaml.Unmarshal(b, &d); err != nil {
			return nil, fmt.Errorf("discovery.yaml: %w", err)
		}
		s.discovery = d.Resources
	}
	logRoot := filepath.Join(dir, "logs")
	err = filepath.WalkDir(logRoot, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".log") {
			return err
		}
		rel, _ := filepath.Rel(logRoot, path)
		parts := strings.Split(filepath.ToSlash(rel), "/")
		if len(parts) != 3 {
			return nil
		}
		container := strings.TrimSuffix(parts[2], ".log")
		container, previous := strings.CutSuffix(container, ".previous")
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		s.logs[logKey(parts[0], parts[1], container, previous)] = string(b)
		return nil
	})
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	return s, nil
}

// timeFields are the object fields that hold points in time. Replay moves
// all of them by the same amount, so every age stays what it was.
var timeFields = map[string]bool{
	"creationTimestamp": true, "deletionTimestamp": true,
	"firstTimestamp": true, "lastTimestamp": true, "eventTime": true, "lastObservedTime": true,
	"startTime": true, "completionTime": true, "startedAt": true, "finishedAt": true,
	"lastScheduleTime": true, "lastSuccessfulTime": true,
	"lastTransitionTime": true, "lastUpdateTime": true, "lastProbeTime": true, "lastHeartbeatTime": true,
	"lastPhaseTransitionTime": true, "lastScaleTime": true,
}

// shiftTimes moves every timestamp in an object (see timeFields) by d,
// keeping each one's format: RFC 3339 seconds, or microseconds for
// MicroTime fields such as an event's eventTime.
func shiftTimes(v any, d time.Duration) {
	switch t := v.(type) {
	case map[string]any:
		for k, val := range t {
			if s, ok := val.(string); ok && timeFields[k] {
				t[k] = shiftTimestamp(s, d)
				continue
			}
			shiftTimes(val, d)
		}
	case []any:
		for _, val := range t {
			shiftTimes(val, d)
		}
	}
}

func shiftTimestamp(s string, d time.Duration) string {
	ts, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return s
	}
	ts = ts.Add(d).UTC()
	if strings.Contains(s, ".") {
		return ts.Format(metav1.RFC3339Micro)
	}
	return ts.Format(time.RFC3339)
}

// deepCopyJSON copies a decoded JSON/YAML object.
func deepCopyJSON(m map[string]any) map[string]any {
	return runtime.DeepCopyJSON(m)
}

// cluster builds an in-memory cluster from the snapshot as it would look at
// time at: every timestamp moved by (at − recorded time). The clients are
// client-go fakes (typed and dynamic) holding the recorded objects; logs come
// from the recording through clusterState.podLogs.
func (s *benchSnapshot) cluster(at time.Time) (*clusterState, error) {
	shift := at.Sub(s.meta.RecordedAt)
	var typed, untyped []runtime.Object
	decoder := clientgoscheme.Codecs.UniversalDeserializer()
	for _, raw := range s.objects {
		obj := deepCopyJSON(raw)
		shiftTimes(obj, shift)
		b, err := json.Marshal(obj)
		if err != nil {
			return nil, err
		}
		t, _, err := decoder.Decode(b, nil, nil)
		if err != nil {
			return nil, fmt.Errorf("%s/%s: %w", obj["kind"], nmStr(nmMap(obj, "metadata"), "name"), err)
		}
		// Decoded like the dynamic client decodes a response: integers
		// become int64, as describe output expects.
		u := &unstructured.Unstructured{}
		if err := u.UnmarshalJSON(b); err != nil {
			return nil, err
		}
		typed = append(typed, t)
		untyped = append(untyped, u)
	}
	cs := fake.NewSimpleClientset(typed...)
	cs.Fake.Resources = s.discovery
	cs.PrependReactor("list", "*", listLikeAPIServer(cs.Tracker()))
	dyn := dynamicfake.NewSimpleDynamicClient(clientgoscheme.Scheme, untyped...)
	c := &clusterState{name: s.meta.Name, server: "bench://" + s.meta.Name, client: cs, dynamic: dyn}
	c.podLogs = s.podLogs
	return c, nil
}

// listLikeAPIServer answers list calls the way an API server does, which the
// fake's own reaction doesn't: events filtered by field selector (the warning
// feed asks for type=Warning) and pages of ListOptions.Limit items with a
// continue token (the sweep's 200-event cap per namespace, the warning feed's
// pages), in namespace/name order. Label selectors are applied by the fake
// clients themselves.
func listLikeAPIServer(tracker k8stesting.ObjectTracker) k8stesting.ReactionFunc {
	return func(a k8stesting.Action) (bool, runtime.Object, error) {
		la, ok := a.(k8stesting.ListActionImpl)
		if !ok {
			return false, nil, nil
		}
		obj, err := tracker.List(la.GetResource(), la.GetKind(), la.GetNamespace())
		if err != nil {
			return true, nil, err
		}
		items, err := meta.ExtractList(obj)
		if err != nil {
			return true, nil, err
		}
		key := func(o runtime.Object) string {
			m, _ := meta.Accessor(o)
			return m.GetNamespace() + "/" + m.GetName()
		}
		sort.SliceStable(items, func(i, j int) bool { return key(items[i]) < key(items[j]) })
		if sel := la.GetListRestrictions().Fields; sel != nil && !sel.Empty() {
			var kept []runtime.Object
			for _, it := range items {
				if e, ok := it.(*corev1.Event); ok && !sel.Matches(eventFields(*e)) {
					continue
				}
				kept = append(kept, it)
			}
			items = kept
		}
		opts := la.ListOptions
		start := 0
		if opts.Continue != "" {
			if start, err = strconv.Atoi(opts.Continue); err != nil || start < 0 || start > len(items) {
				return true, nil, fmt.Errorf("bad continue token %q", opts.Continue)
			}
		}
		end := len(items)
		cont := ""
		if opts.Limit > 0 && int64(end-start) > opts.Limit {
			end = start + int(opts.Limit)
			cont = strconv.Itoa(end)
		}
		if err := meta.SetList(obj, items[start:end]); err != nil {
			return true, nil, err
		}
		if lm, err := meta.ListAccessor(obj); err == nil {
			lm.SetContinue(cont)
		}
		return true, obj, nil
	}
}

// eventFields are the fields the API server lets you select events by.
func eventFields(e corev1.Event) fields.Set {
	return fields.Set{
		"metadata.name":                  e.Name,
		"metadata.namespace":             e.Namespace,
		"type":                           e.Type,
		"reason":                         e.Reason,
		"source":                         e.Source.Component,
		"reportingComponent":             e.ReportingController,
		"involvedObject.kind":            e.InvolvedObject.Kind,
		"involvedObject.name":            e.InvolvedObject.Name,
		"involvedObject.namespace":       e.InvolvedObject.Namespace,
		"involvedObject.uid":             string(e.InvolvedObject.UID),
		"involvedObject.apiVersion":      e.InvolvedObject.APIVersion,
		"involvedObject.resourceVersion": e.InvolvedObject.ResourceVersion,
		"involvedObject.fieldPath":       e.InvolvedObject.FieldPath,
	}
}

// podLogs answers a log request from the recording, as the kubelet would:
// the last tail lines, or an error when the recording has none (the API
// refused that request when the snapshot was taken).
func (s *benchSnapshot) podLogs(ns, pod, container string, tail int64, previous bool) (string, error) {
	text, ok := s.logs[logKey(ns, pod, container, previous)]
	if !ok {
		what := "logs"
		if previous {
			what = "previous logs"
		}
		return "", fmt.Errorf("no recorded %s for %s/%s container %s", what, ns, pod, container)
	}
	return tailLines(text, tail), nil
}

// tailLines keeps the last n lines of text (n <= 0: all of it).
func tailLines(text string, n int64) string {
	if n <= 0 || text == "" {
		return text
	}
	body := strings.TrimSuffix(text, "\n")
	lines := strings.Split(body, "\n")
	if int64(len(lines)) <= n {
		return text
	}
	out := strings.Join(lines[int64(len(lines))-n:], "\n")
	if strings.HasSuffix(text, "\n") {
		out += "\n"
	}
	return out
}

// benchWarm fills a replayed cluster's caches the way a running KubeHealth
// does: the sweep lists every namespace's events, the warning feed lists all
// warnings, and the owner resolver confirms what the problem list asks it to
// until nothing is left to confirm.
func (c *clusterState) benchWarm(ctx context.Context, now time.Time) error {
	namespaces, err := c.resolveNamespaces(ctx)
	if err != nil {
		return err
	}
	for _, ns := range namespaces {
		c.refreshNamespace(ctx, ns)
	}
	c.refreshWarnings(ctx)
	c.feed.RLock()
	feedErr := c.feed.lastErr
	c.feed.RUnlock()
	if feedErr != "" {
		return fmt.Errorf("warning feed: %s", feedErr)
	}
	for round := 0; round < 50; round++ {
		c.buildProblems(c.allNamespaceEvents(), now)
		c.owners.lockInit()
		queue := c.owners.queue
		c.owners.queue = nil
		c.owners.Unlock()
		if len(queue) == 0 {
			return nil
		}
		// resolveOwner queues each owner's parent, which the next round takes.
		for _, j := range queue {
			c.resolveOwner(ctx, j)
		}
	}
	return nil
}
