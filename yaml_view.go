package main

// The YAML panel's server side: an object as YAML with its Secret values
// masked, one Secret value on request, and the fields that explain why the
// object is failing (from its events and its containers' state).

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"

	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/yaml"
)

// handleAPIYAML serves the YAML panel: GET /api/yaml/{namespace}/{kind}/{name}
// ("_cluster" for cluster-scoped objects). With ?reveal=<key> on a Secret it
// returns that one value, decoded.
func handleAPIYAML(w http.ResponseWriter, r *http.Request) {
	parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/api/yaml/"), "/", 3)
	if len(parts) < 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		http.Error(w, "invalid path", http.StatusBadRequest)
		return
	}
	c := apiCluster(w, r)
	if c == nil {
		return
	}
	ns, kind, name := parts[0], parts[1], parts[2]
	w.Header().Set("Content-Type", "application/json")
	fail := func(err error) {
		status := http.StatusBadGateway
		if k8serrors.IsNotFound(err) {
			status = http.StatusNotFound
		}
		w.WriteHeader(status)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
	}
	obj, err := c.fetchObject(ns, kind, name)
	if err != nil {
		fail(err)
		return
	}

	if key := r.URL.Query().Get("reveal"); key != "" {
		v, binary, err := secretValue(obj, r.URL.Query().Get("field"), key)
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"key": key, "value": v, "binary": binary})
		return
	}

	issues := c.yamlIssues(ns, kind, name, obj)
	keys := maskSecret(obj)
	y, err := toYAML(obj)
	if err != nil {
		fail(err)
		return
	}
	json.NewEncoder(w).Encode(map[string]any{
		"yaml": y, "namespace": ns, "kind": kind, "name": name,
		"secret_keys": keys, "issues": issues,
	})
}

// fetchObjectYAML is an object's YAML with its Secret values masked.
func (c *clusterState) fetchObjectYAML(namespace, kind, name string) (string, error) {
	obj, err := c.fetchObject(namespace, kind, name)
	if err != nil {
		return "", err
	}
	maskSecret(obj)
	return toYAML(obj)
}

func (c *clusterState) fetchObject(namespace, kind, name string) (map[string]any, error) {
	namespace = apiNamespace(namespace)
	gvr, err := c.kindToGVR(kind)
	if err != nil {
		return nil, err
	}
	res := c.dynamic.Resource(gvr)
	if namespace == "" || namespace == "_" {
		o, err := res.Get(context.Background(), name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		return o.Object, nil
	}
	o, err := res.Namespace(namespace).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return o.Object, nil
}

func toYAML(obj map[string]any) (string, error) {
	b, err := json.Marshal(obj)
	if err != nil {
		return "", err
	}
	y, err := yaml.JSONToYAML(b)
	if err != nil {
		return "", err
	}
	return string(y), nil
}

// ---- Secrets -----------------------------------------------------------------

// secretHidden replaces a Secret's values in the YAML the page gets.
const secretHidden = "(hidden)"

type secretKey struct {
	Field string `json:"field"` // data or stringData
	Key   string `json:"key"`
	Bytes int    `json:"bytes"` // the decoded value's size
}

// maskSecret replaces a Secret's values with secretHidden, so they never
// reach the page unless one is asked for, and returns what it hid. The
// last-applied annotation holds a copy of them, so it is masked too.
func maskSecret(obj map[string]any) []secretKey {
	if k, _ := obj["kind"].(string); k != "Secret" {
		return nil
	}
	var keys []secretKey
	for _, field := range []string{"data", "stringData"} {
		m, ok := obj[field].(map[string]any)
		if !ok {
			continue
		}
		for k, v := range m {
			s, _ := v.(string)
			n := len(s)
			if field == "data" {
				if b, err := base64.StdEncoding.DecodeString(s); err == nil {
					n = len(b)
				}
			}
			m[k] = secretHidden
			keys = append(keys, secretKey{field, k, n})
		}
	}
	if md, ok := obj["metadata"].(map[string]any); ok {
		if an, ok := md["annotations"].(map[string]any); ok {
			if _, ok := an[lastAppliedAnnotation]; ok {
				an[lastAppliedAnnotation] = secretHidden
			}
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Field != keys[j].Field {
			return keys[i].Field < keys[j].Field
		}
		return keys[i].Key < keys[j].Key
	})
	return keys
}

// secretValue is one value of a Secret, decoded; binary values come back
// base64-encoded.
func secretValue(obj map[string]any, field, key string) (value string, binary bool, err error) {
	if k, _ := obj["kind"].(string); k != "Secret" {
		return "", false, fmt.Errorf("only a Secret's values can be revealed")
	}
	if field == "" {
		field = "data"
	}
	if field != "data" && field != "stringData" {
		return "", false, fmt.Errorf("unknown field %q", field)
	}
	m, _ := obj[field].(map[string]any)
	s, ok := m[key].(string)
	if !ok {
		return "", false, fmt.Errorf("the Secret has no key %q in %s", key, field)
	}
	if field == "stringData" {
		return s, false, nil
	}
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return s, true, nil
	}
	if !utf8.Valid(b) {
		return s, true, nil
	}
	return string(b), false, nil
}

// ---- Why it fails ------------------------------------------------------------

// yamlIssue points the YAML panel at a field that explains why the object is
// failing: the panel highlights the line and shows Text next to it.
type yamlIssue struct {
	// Suffix: the field's path ends with these keys ("[]" for a list item);
	// Fallback paths are tried in order when no line matches.
	Suffix    []string   `json:"suffix,omitempty"`
	Fallback  [][]string `json:"fallback,omitempty"`
	Container string     `json:"container,omitempty"` // only inside this container
	Value     string     `json:"value,omitempty"`     // only where the value is this
	// Or: the line that refers to this object (a ConfigMap, a Secret …).
	RefKind  string `json:"ref_kind,omitempty"`
	RefName  string `json:"ref_name,omitempty"`
	Severity string `json:"severity"` // crit or warn
	Text     string `json:"text"`
}

const yamlIssuesMax = 10

var (
	refNotFoundRe  = regexp.MustCompile(`(?i)\b(configmap|secret|persistentvolumeclaim|serviceaccount)s?\s+"([^"]+)"\s+not found`)
	refKeyRe       = regexp.MustCompile(`(?i)couldn't find key (\S+) in (Secret|ConfigMap) [^/\s]+/(\S+)`)
	pullSecretsRe  = regexp.MustCompile(`(?i)image pull secrets \(([^)]+)\)`)
	pullImageRe    = regexp.MustCompile(`(?i)pull(?:ing)? image "([^"]+)"`)
	storageClassRe = regexp.MustCompile(`(?i)storageclass[^"]*"([^"]+)" not found`)
)

var refKinds = map[string]string{
	"configmap": "ConfigMap", "secret": "Secret",
	"persistentvolumeclaim": "PersistentVolumeClaim", "serviceaccount": "ServiceAccount",
}

// refInMessage finds the object a message says is missing.
func refInMessage(msg string) (kind, name string, ok bool) {
	if m := refKeyRe.FindStringSubmatch(msg); m != nil {
		return refKinds[strings.ToLower(m[2])], m[3], true
	}
	if m := refNotFoundRe.FindStringSubmatch(msg); m != nil {
		return refKinds[strings.ToLower(m[1])], m[2], true
	}
	if m := pullSecretsRe.FindStringSubmatch(msg); m != nil {
		return "Secret", strings.TrimSpace(strings.Split(m[1], ",")[0]), true
	}
	return "", "", false
}

func isWorkloadKind(kind string) bool {
	switch strings.ToLower(kind) {
	case "deployment", "statefulset", "daemonset", "replicaset", "job", "cronjob":
		return true
	}
	return false
}

// yamlIssues reads the object's warnings (a workload's include its Pods'),
// and its Pod's containers (a workload's newest failing Pod), into fields to
// highlight. obj is the object as fetched, before masking.
func (c *clusterState) yamlIssues(ns, kind, name string, obj map[string]any) []yamlIssue {
	var events []EventSummary
	for _, ev := range objectEvents(c, ns, kind, name) {
		if ev.Type == "Warning" {
			events = append(events, ev)
		}
	}
	var pod *corev1.Pod
	switch {
	case strings.EqualFold(kind, "Pod"):
		var p corev1.Pod
		if runtime.DefaultUnstructuredConverter.FromUnstructured(obj, &p) == nil {
			pod = &p
		}
	case isWorkloadKind(kind) && ns != clusterScopeNS:
		evs, pods := memberWarnings(c, ns, kind, name)
		events = append(events, evs...)
		if len(pods) > 0 && c.client != nil {
			if p, err := c.client.CoreV1().Pods(apiNamespace(ns)).Get(context.Background(), pods[0], metav1.GetOptions{}); err == nil {
				pod = p
			}
		}
	}
	return issuesFrom(events, pod)
}

// issuesFrom is the rule set: container state first (the most specific), then
// events, without repeats.
func issuesFrom(events []EventSummary, pod *corev1.Pod) []yamlIssue {
	var out []yamlIssue
	seen := map[string]bool{}
	add := func(is yamlIssue) {
		key := strings.Join(is.Suffix, ".") + "|" + is.Container + "|" + is.Value + "|" + is.RefKind + "/" + is.RefName
		if seen[key] || len(out) >= yamlIssuesMax {
			return
		}
		seen[key] = true
		is.Text = truncate(oneLine(is.Text), 240)
		out = append(out, is)
	}
	if pod != nil {
		containerIssues(pod, add)
	}
	for _, ev := range events {
		eventIssues(ev, add)
	}
	return out
}

func containerIssues(pod *corev1.Pod, add func(yamlIssue)) {
	limits := map[string]string{}
	for _, c := range append(append([]corev1.Container{}, pod.Spec.InitContainers...), pod.Spec.Containers...) {
		if q, ok := c.Resources.Limits[corev1.ResourceMemory]; ok {
			limits[c.Name] = q.String()
		}
	}
	check := func(list string, st corev1.ContainerStatus) {
		f := func(keys ...string) []string { return append([]string{list, "[]"}, keys...) }
		if w := st.State.Waiting; w != nil {
			switch w.Reason {
			case "ImagePullBackOff", "ErrImagePull", "InvalidImageName", "ErrImageNeverPull":
				add(yamlIssue{Suffix: f("image"), Container: st.Name, Severity: "crit", Text: w.Reason + ": " + w.Message})
			case "CreateContainerConfigError", "CreateContainerError":
				if k, n, ok := refInMessage(w.Message); ok {
					add(yamlIssue{RefKind: k, RefName: n, Severity: "crit", Text: w.Reason + ": " + w.Message})
				} else {
					add(yamlIssue{Suffix: f("name"), Container: st.Name, Severity: "crit", Text: w.Reason + ": " + w.Message})
				}
			}
		}
		t := st.LastTerminationState.Terminated
		if t == nil {
			t = st.State.Terminated
		}
		if t == nil {
			return
		}
		lim := limits[st.Name]
		switch {
		case t.Reason == "OOMKilled" || (t.ExitCode == 137 && lim != ""):
			text := fmt.Sprintf("Last run ended %s (exit %d)", t.Reason, t.ExitCode)
			if lim != "" {
				text += ", over its " + lim + " memory limit"
			}
			add(yamlIssue{Suffix: f("resources", "limits", "memory"), Fallback: [][]string{f("resources"), f("name")},
				Container: st.Name, Severity: "crit", Text: text})
		case t.ExitCode != 0 && (st.RestartCount > 0 || pod.Status.Phase == corev1.PodFailed):
			text := fmt.Sprintf("Crash-looping: last run ended %s (exit %d), restarted %d times", t.Reason, t.ExitCode, st.RestartCount)
			if st.RestartCount == 0 {
				text = fmt.Sprintf("Exited with %s (exit %d)", t.Reason, t.ExitCode)
			}
			add(yamlIssue{Suffix: f("name"), Container: st.Name, Severity: "warn", Text: text})
		}
	}
	for _, st := range pod.Status.InitContainerStatuses {
		check("initContainers", st)
	}
	for _, st := range pod.Status.ContainerStatuses {
		check("containers", st)
	}
}

func eventIssues(ev EventSummary, add func(yamlIssue)) {
	m, text := ev.Message, ev.Reason+": "+ev.Message
	has := func(s string) bool { return strings.Contains(strings.ToLower(m), strings.ToLower(s)) }
	path := func(p ...string) []string { return p }
	switch ev.Reason {
	case "BackOff":
		if im := pullImageRe.FindStringSubmatch(m); im != nil {
			add(yamlIssue{Suffix: path("containers", "[]", "image"), Value: im[1], Fallback: [][]string{path("initContainers", "[]", "image")}, Severity: "crit", Text: text})
		} else if c := extractContainerFromMsg(m); c != "" {
			add(yamlIssue{Suffix: path("containers", "[]", "name"), Fallback: [][]string{path("initContainers", "[]", "name")}, Container: c, Severity: "warn", Text: text})
		}
	case "Unhealthy", "Killing":
		for _, p := range []struct{ prefix, field string }{
			{"Readiness probe", "readinessProbe"}, {"Liveness probe", "livenessProbe"}, {"Startup probe", "startupProbe"},
		} {
			if has(p.prefix) {
				add(yamlIssue{Suffix: path(p.field), Container: extractContainerFromMsg(m), Severity: "warn", Text: text})
			}
		}
	case "FailedScheduling":
		switch {
		case has("Insufficient cpu"):
			add(yamlIssue{Suffix: path("resources", "requests", "cpu"), Fallback: [][]string{path("resources", "limits", "cpu")}, Severity: "crit", Text: text})
		case has("Insufficient memory"):
			add(yamlIssue{Suffix: path("resources", "requests", "memory"), Fallback: [][]string{path("resources", "limits", "memory")}, Severity: "crit", Text: text})
		case has("unbound immediate PersistentVolumeClaims") || has("persistentvolumeclaim"):
			add(yamlIssue{Suffix: path("persistentVolumeClaim", "claimName"), Fallback: [][]string{path("volumeClaimTemplates")}, Severity: "crit", Text: text})
		case has("anti-affinity"):
			add(yamlIssue{Suffix: path("affinity", "podAntiAffinity"), Fallback: [][]string{path("affinity")}, Severity: "warn", Text: text})
		case has("untolerated taint") || has("node affinity/selector") || has("node selector"):
			add(yamlIssue{Suffix: path("nodeSelector"), Fallback: [][]string{path("affinity", "nodeAffinity"), path("tolerations")}, Severity: "warn", Text: text})
		}
	case "FailedMount", "FailedAttachVolume", "Failed", "FailedToRetrieveImagePullSecret":
		if k, n, ok := refInMessage(m); ok {
			is := yamlIssue{RefKind: k, RefName: n, Severity: "crit", Text: text}
			if pullSecretsRe.MatchString(m) {
				// Not in the Pod spec: it comes from the ServiceAccount.
				is.Fallback = [][]string{path("serviceAccountName")}
			}
			add(is)
		} else if im := pullImageRe.FindStringSubmatch(m); im != nil {
			add(yamlIssue{Suffix: path("containers", "[]", "image"), Value: im[1], Fallback: [][]string{path("initContainers", "[]", "image")}, Severity: "crit", Text: text})
		}
	case "ProvisioningFailed":
		if sc := storageClassRe.FindStringSubmatch(m); sc != nil {
			add(yamlIssue{Suffix: path("storageClassName"), Value: sc[1], Severity: "crit", Text: text})
		} else if has("access mode") || has("ReadWriteMany") || has("only supports") {
			add(yamlIssue{Suffix: path("accessModes"), Severity: "crit", Text: text})
		} else {
			add(yamlIssue{Suffix: path("storageClassName"), Severity: "warn", Text: text})
		}
	case "FailedCreate":
		switch {
		case has("exceeded quota"):
			add(yamlIssue{Suffix: path("replicas"), Fallback: [][]string{path("resources")}, Severity: "crit", Text: text})
		case has("maximum memory usage per Container"):
			add(yamlIssue{Suffix: path("resources", "limits", "memory"), Severity: "crit", Text: text})
		case has("maximum cpu usage per Container"):
			add(yamlIssue{Suffix: path("resources", "limits", "cpu"), Severity: "crit", Text: text})
		case has("minimum memory usage per Container"):
			add(yamlIssue{Suffix: path("resources", "requests", "memory"), Severity: "crit", Text: text})
		case has("minimum cpu usage per Container"):
			add(yamlIssue{Suffix: path("resources", "requests", "cpu"), Severity: "crit", Text: text})
		default:
			if k, n, ok := refInMessage(m); ok {
				add(yamlIssue{RefKind: k, RefName: n, Severity: "crit", Text: text})
			}
		}
	case "BackoffLimitExceeded":
		add(yamlIssue{Suffix: path("backoffLimit"), Severity: "warn", Text: text})
	case "DeadlineExceeded":
		add(yamlIssue{Suffix: path("activeDeadlineSeconds"), Severity: "crit", Text: text})
	case "VolumeFailedDelete", "VolumeFailedRecycle":
		add(yamlIssue{Suffix: path("persistentVolumeReclaimPolicy"), Severity: "crit", Text: text})
	}
}
