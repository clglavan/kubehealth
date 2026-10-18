package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/yaml"
)

// benchRecordedAt is when the test cluster is recorded; its objects' times
// are relative to it.
var benchRecordedAt = time.Date(2026, 10, 18, 9, 0, 0, 0, time.UTC)

const (
	benchPod    = "worker-5d8f7b6c9d-x7k2p"
	benchSecret = "hunter2"
	benchEnvVal = "s3cr3t-literal"
)

// benchSourceObjects is a small cluster: a crash-looping Deployment, a PV
// whose reclaim failed, a Secret, a ConfigMap and a kube-system Pod that the
// recording skips.
func benchSourceObjects() []runtime.Object {
	at := func(d time.Duration) metav1.Time { return metav1.NewTime(benchRecordedAt.Add(-d)) }
	ctrl := true
	ev := func(ns, name, kind, obj, objNS, typ, reason, msg string, count int32, first, last time.Duration) *corev1.Event {
		return &corev1.Event{
			ObjectMeta:     metav1.ObjectMeta{Name: name, Namespace: ns, CreationTimestamp: at(first)},
			InvolvedObject: corev1.ObjectReference{Kind: kind, Name: obj, Namespace: objNS},
			Type:           typ, Reason: reason, Message: msg, Count: count,
			FirstTimestamp: at(first), LastTimestamp: at(last),
		}
	}
	return []runtime.Object{
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "orders", CreationTimestamp: at(2 * time.Hour)}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "default", CreationTimestamp: at(2 * time.Hour)}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "kube-system", CreationTimestamp: at(2 * time.Hour)}},
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "worker", Namespace: "orders", CreationTimestamp: at(10 * time.Minute)},
			Spec: appsv1.DeploymentSpec{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "worker"}}}},
		&appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: "worker-5d8f7b6c9d", Namespace: "orders", CreationTimestamp: at(10 * time.Minute),
			OwnerReferences: []metav1.OwnerReference{{Kind: "Deployment", Name: "worker", Controller: &ctrl}}}},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: benchPod, Namespace: "orders", CreationTimestamp: at(10 * time.Minute),
				Annotations:     map[string]string{lastAppliedAnnotation: `{"env":"` + benchEnvVal + `"}`},
				ManagedFields:   []metav1.ManagedFieldsEntry{{Manager: "kubectl"}},
				OwnerReferences: []metav1.OwnerReference{{Kind: "ReplicaSet", Name: "worker-5d8f7b6c9d", Controller: &ctrl}}},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "worker", Image: "acme/worker:1",
				Env: []corev1.EnvVar{{Name: "TOKEN", Value: benchEnvVal},
					{Name: "DB_PASSWORD", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: "db-creds"}, Key: "password"}}}}}}},
			Status: corev1.PodStatus{Phase: corev1.PodRunning, StartTime: &metav1.Time{Time: benchRecordedAt.Add(-10 * time.Minute)},
				ContainerStatuses: []corev1.ContainerStatus{{Name: "worker", RestartCount: 4,
					State:                corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}},
					LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, Reason: "Error", FinishedAt: at(time.Minute)}}}}},
		},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "db-creds", Namespace: "orders",
			Annotations: map[string]string{lastAppliedAnnotation: `{"data":{"password":"` + base64.StdEncoding.EncodeToString([]byte(benchSecret)) + `"}}`}},
			Type: corev1.SecretTypeOpaque, Data: map[string][]byte{"password": []byte(benchSecret)}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "orders"}, Data: map[string]string{"log-level": benchSecret}},
		&corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "pv-1", CreationTimestamp: at(time.Hour)},
			Spec:   corev1.PersistentVolumeSpec{PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRecycle},
			Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeFailed, Message: "recycle failed"}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "coredns-1", Namespace: "kube-system"}},
		ev("orders", "worker.backoff", "Pod", benchPod, "orders", "Warning", "BackOff", "Back-off restarting failed container worker", 12, 9*time.Minute, 30*time.Second),
		// Two warnings seen in the same second, and one in another namespace
		// at that second: their order must not depend on map iteration.
		ev("orders", "worker.failed-a", "Pod", benchPod, "orders", "Warning", "Failed", "Error: a", 2, 8*time.Minute, 2*time.Minute),
		ev("orders", "worker.failed-b", "Pod", benchPod, "orders", "Warning", "Unhealthy", "Error: b", 2, 8*time.Minute, 2*time.Minute),
		ev("orders", "worker.scale", "Deployment", "worker", "orders", "Normal", "ScalingReplicaSet", "Scaled up replica set worker-5d8f7b6c9d from 0 to 1", 1, 10*time.Minute, 10*time.Minute),
		ev("default", "pv-1.recycle", "PersistentVolume", "pv-1", "", "Warning", "VolumeFailedRecycle", "Recycle failed: no recycler", 3, 50*time.Minute, 2*time.Minute),
		ev("default", "pv-1.recycle2", "PersistentVolume", "pv-1", "", "Warning", "VolumeFailedRecycle", "Recycle failed again", 1, 2*time.Minute, 2*time.Minute),
		ev("orders", "zz-pv-1.recycle", "PersistentVolume", "pv-1", "", "Warning", "VolumeFailedRecycle", "Recycle failed, seen from orders", 1, 2*time.Minute, 2*time.Minute),
		ev("kube-system", "coredns-1.x", "Pod", "coredns-1", "kube-system", "Warning", "BackOff", "kube-system noise", 1, time.Minute, time.Minute),
	}
}

// recordTestSnapshot records benchSourceObjects into dir/<name>.
func recordTestSnapshot(t *testing.T, dir, name string) string {
	t.Helper()
	objs := benchSourceObjects()
	cs := fake.NewSimpleClientset(objs...)
	dyn := dynamicfake.NewSimpleDynamicClient(clientgoscheme.Scheme, objs...)
	out := filepath.Join(dir, name)
	if _, err := recordSnapshot(context.Background(), cs, dyn, recordOpts{
		dir: out, context: "kind-test", at: benchRecordedAt, tail: 150, skip: benchSystemNamespaces,
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

// -namespaces records only the namespaces it lists, still minus the skipped ones.
func TestBenchRecordOnlyNamespaces(t *testing.T) {
	objs := benchSourceObjects()
	out := filepath.Join(t.TempDir(), "only")
	meta, err := recordSnapshot(context.Background(), fake.NewSimpleClientset(objs...),
		dynamicfake.NewSimpleDynamicClient(clientgoscheme.Scheme, objs...), recordOpts{
			dir: out, at: benchRecordedAt, tail: 150, skip: benchSystemNamespaces, only: []string{"orders", "kube-system"},
		})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(meta.Namespaces, []string{"orders"}) {
		t.Errorf("recorded namespaces %v, want [orders]", meta.Namespaces)
	}
	for _, ns := range []string{"default", "kube-system"} {
		if _, err := os.Stat(filepath.Join(out, "namespaces", ns)); !os.IsNotExist(err) {
			t.Errorf("%s was recorded", ns)
		}
	}
	if _, err := os.Stat(filepath.Join(out, "namespaces", "orders", "pods.yaml")); err != nil {
		t.Errorf("orders pods: %v", err)
	}
}

func TestBenchRecordReplayRoundTrip(t *testing.T) {
	dir := recordTestSnapshot(t, t.TempDir(), "test")

	// Nothing secret was written, nor what KubeHealth doesn't read.
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, _ := os.ReadFile(path)
		for _, s := range []string{benchSecret, base64.StdEncoding.EncodeToString([]byte(benchSecret)), benchEnvVal,
			"managedFields", lastAppliedAnnotation, "kube-system noise"} {
			if strings.Contains(string(b), s) {
				t.Errorf("%s contains %q", path, s)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "namespaces", "kube-system")); !os.IsNotExist(err) {
		t.Errorf("kube-system was recorded")
	}
	if b, err := os.ReadFile(logFile(dir, "orders", benchPod, "worker", false)); err != nil || string(b) != "fake logs" {
		t.Errorf("recorded log: %q, %v", b, err)
	}

	snap, err := loadSnapshot(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !snap.meta.RecordedAt.Equal(benchRecordedAt) || snap.meta.Context != "kind-test" {
		t.Errorf("meta: %+v", snap.meta)
	}
	// Replayed five hours later: every timestamp moves, every age stays.
	at := benchRecordedAt.Add(5*time.Hour + 17*time.Second)
	clock = func() time.Time { return at }
	t.Cleanup(func() { clock = time.Now })
	c, err := snap.cluster(at)
	if err != nil {
		t.Fatal(err)
	}
	evs, _, err := c.fetchEvents(context.Background(), "orders")
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 5 {
		t.Fatalf("orders events: %d, want 5", len(evs))
	}
	if e := evs[0]; e.Reason != "BackOff" || e.Age != "30s" || !e.LastTime.Equal(at.Add(-30*time.Second)) ||
		!e.FirstTime.Equal(at.Add(-9*time.Minute)) {
		t.Errorf("newest event: %+v", e)
	}
	if evs[1].Name != "worker.failed-a" || evs[2].Name != "worker.failed-b" {
		t.Errorf("same-second events out of name order: %s, %s", evs[1].Name, evs[2].Name)
	}
	pod := c.fetchObjectInfo("orders", "Pod", benchPod)
	if pod.Age != "10m" {
		t.Errorf("pod age %q, want 10m", pod.Age)
	}
	body := describeBody(pod)
	if !strings.Contains(body, "Env Refs: DB_PASSWORD=secret/db-creds:password") {
		t.Errorf("pod describe lacks the env ref:\n%s", body)
	}
	sec := c.fetchObjectInfo("orders", "Secret", "db-creds")
	if got := describeBody(sec); !strings.Contains(got, "Keys: password") || strings.Contains(got, benchSecret) {
		t.Errorf("secret describe:\n%s", got)
	}
	if got := describeBody(c.fetchObjectInfo("orders", "ConfigMap", "app")); !strings.Contains(got, "Keys: log-level") {
		t.Errorf("configmap describe:\n%s", got)
	}
	pv := c.fetchObjectInfo(clusterScopeNS, "PersistentVolume", "pv-1")
	if pv.NotFound || pv.Error != "" || pv.Age != "1h" {
		t.Errorf("pv: %+v", pv)
	}
	logs, err := fetchLogsForAI(c, "orders", benchPod)
	if err != nil {
		t.Fatal(err)
	}
	if want := "Container state: worker restarted 4 times, last run ended Error (exit code 1), now CrashLoopBackOff.\nfake logs\n"; logs != want {
		t.Errorf("logs:\n%q\nwant\n%q", logs, want)
	}
}

func TestBenchLogSeam(t *testing.T) {
	var pod map[string]any
	if err := yaml.Unmarshal([]byte(`
apiVersion: v1
kind: Pod
metadata: {name: api-0, namespace: shop, creationTimestamp: "2026-10-18T08:00:00Z"}
spec: {containers: [{name: api, image: acme/api:1}, {name: proxy, image: envoy:1}]}
`), &pod); err != nil {
		t.Fatal(err)
	}
	snap := &benchSnapshot{
		meta:    snapshotMeta{Name: "s", RecordedAt: benchRecordedAt},
		objects: []map[string]any{pod},
		logs: map[string]string{
			logKey("shop", "api-0", "api", false):   "",
			logKey("shop", "api-0", "api", true):    "one\ntwo\nthree\n",
			logKey("shop", "api-0", "proxy", false): "ready\n",
		},
	}
	if got, err := snap.podLogs("shop", "api-0", "api", 2, true); err != nil || got != "two\nthree\n" {
		t.Errorf("tail 2: %q, %v", got, err)
	}
	if _, err := snap.podLogs("shop", "api-0", "proxy", 10, true); err == nil {
		t.Error("previous logs that were never recorded: no error")
	}
	c, err := snap.cluster(benchRecordedAt.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	// The current run printed nothing, so KubeHealth reads the previous one.
	logs, err := fetchLogsForAI(c, "shop", "api-0")
	if err != nil {
		t.Fatal(err)
	}
	want := "=== container: api ===\n[previous container run]\none\ntwo\nthree\n=== container: proxy ===\nready\n"
	if logs != want {
		t.Errorf("logs:\n%q\nwant\n%q", logs, want)
	}
}

func TestBenchListLikeAPIServer(t *testing.T) {
	mk := func(name, typ string) *corev1.Event {
		return &corev1.Event{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "x"}, Type: typ}
	}
	cs := fake.NewSimpleClientset(mk("c", "Warning"), mk("a", "Warning"), mk("d", "Normal"), mk("b", "Warning"))
	cs.PrependReactor("list", "*", listLikeAPIServer(cs.Tracker()))
	names := func(l *corev1.EventList) (out []string) {
		for _, e := range l.Items {
			out = append(out, e.Name)
		}
		return out
	}
	ctx := context.Background()
	l, err := cs.CoreV1().Events("").List(ctx, metav1.ListOptions{FieldSelector: "type=Warning", Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if got := names(l); !reflect.DeepEqual(got, []string{"a", "b"}) || l.Continue == "" {
		t.Fatalf("page 1: %v, continue %q", got, l.Continue)
	}
	l, err = cs.CoreV1().Events("").List(ctx, metav1.ListOptions{FieldSelector: "type=Warning", Limit: 2, Continue: l.Continue})
	if err != nil {
		t.Fatal(err)
	}
	if got := names(l); !reflect.DeepEqual(got, []string{"c"}) || l.Continue != "" {
		t.Errorf("page 2: %v, continue %q", got, l.Continue)
	}
	l, _ = cs.CoreV1().Events("x").List(ctx, metav1.ListOptions{})
	if got := names(l); !reflect.DeepEqual(got, []string{"a", "b", "c", "d"}) {
		t.Errorf("all: %v", got)
	}
}

func TestBenchShiftTimes(t *testing.T) {
	obj := map[string]any{
		"metadata":      map[string]any{"creationTimestamp": "2026-10-18T09:00:00Z", "name": "2026-10-18T09:00:00Z"},
		"eventTime":     "2026-10-18T09:00:00.123456Z",
		"lastTimestamp": nil,
		"status":        map[string]any{"conditions": []any{map[string]any{"lastTransitionTime": "2026-10-18T08:00:00Z"}}},
	}
	shiftTimes(obj, 90*time.Minute)
	want := map[string]any{
		"metadata":      map[string]any{"creationTimestamp": "2026-10-18T10:30:00Z", "name": "2026-10-18T09:00:00Z"},
		"eventTime":     "2026-10-18T10:30:00.123456Z",
		"lastTimestamp": nil,
		"status":        map[string]any{"conditions": []any{map[string]any{"lastTransitionTime": "2026-10-18T09:30:00Z"}}},
	}
	if !reflect.DeepEqual(obj, want) {
		t.Errorf("got %v", obj)
	}
}

const goodRCA = "**Root Cause**: Pod worker exits with code 1.\n\n**Evidence**:\n1. BackOff (events)\n\n" +
	"**Resolution**: 1. Fix the worker.\n\n**Confidence**: High — events"

// Two runs of the same cases against the same answers send the same
// requests: the replay holds every age still and every list in one order.
func TestBenchRunRepeatable(t *testing.T) {
	root := t.TempDir()
	recordTestSnapshot(t, filepath.Join(root, "snapshots"), "test")
	cases := filepath.Join(root, "cases")
	os.MkdirAll(cases, 0o755)
	os.WriteFile(filepath.Join(cases, "worker.yaml"), []byte(`id: worker
snapshot: test
anchor: {ns: orders, kind: Deployment, name: worker}
truth: worker exits with code 1
cause: [["exit", "code 1"]]
fix: [["fix"]]
note_target: true
source: test
license: test
`), 0o644)
	os.WriteFile(filepath.Join(cases, "pv.yaml"), []byte(`id: pv
snapshot: test
anchor: {ns: _cluster, kind: PersistentVolume, name: pv-1}
truth: recycle failed
cause: [["recycle"]]
source: test
license: test
`), 0o644)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })
	stdout := os.Stdout
	devnull, _ := os.Open(os.DevNull)
	t.Cleanup(func() { os.Stdout = stdout; devnull.Close() })

	run := func(out, at string, extra ...string) *benchResults {
		answers := []string{"Check the worker's logs.", goodAnalysis, goodRCA, goodAnalysis, goodRCA}
		f := &fakeLLM{modelsCtx: 8192, answers: append(append([]string(nil), answers...), answers...)}
		f.start(t)
		os.Stdout = devnull
		err := benchRunCmd(append([]string{"-cases", cases, "-llm", aiConfig.baseURL, "-model", "fake", "-out", out, "-quiet", "-at", at}, extra...))
		os.Stdout = stdout
		if err != nil {
			t.Fatal(err)
		}
		r, err := readResults(out)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	// Replayed now, a second later and at the recording time: timestamps
	// move, ages don't (this cluster has no absolute times in describe).
	a := run(filepath.Join(root, "a.json"), "now")
	time.Sleep(1100 * time.Millisecond)
	b := run(filepath.Join(root, "b.json"), "now")
	c := run(filepath.Join(root, "c.json"), "recorded")
	if !c.Snapshots["test"].ReplayedAt.Equal(benchRecordedAt) || a.Snapshots["test"].ReplayedAt.Equal(b.Snapshots["test"].ReplayedAt) {
		t.Errorf("replayed at %v, %v, %v", a.Snapshots["test"].ReplayedAt, b.Snapshots["test"].ReplayedAt, c.Snapshots["test"].ReplayedAt)
	}

	type req struct {
		Messages []map[string]any `json:"messages"`
		Tools    any              `json:"tools"`
		Seed     int64            `json:"seed"`
	}
	requests := func(r *benchResults) (out []req) {
		add := func(exs []benchExchange) {
			for _, ex := range exs {
				var q req
				json.Unmarshal(ex.Request, &q)
				out = append(out, q)
			}
		}
		add(r.Notes["test"].Exchanges)
		for _, c := range r.Cases {
			add(c.Analysis.Exchanges)
			add(c.RCA.Exchanges)
		}
		return out
	}
	ra := requests(a)
	if len(ra) != 5 {
		t.Fatalf("%d requests, want 5 (note, 2 analyses, 2 RCAs)", len(ra))
	}
	for _, rb := range [][]req{requests(b), requests(c)} {
		if len(rb) != len(ra) {
			t.Fatalf("%d requests, then %d", len(ra), len(rb))
		}
		for i := range ra {
			if !reflect.DeepEqual(ra[i], rb[i]) {
				x, _ := json.MarshalIndent(ra[i], "", " ")
				y, _ := json.MarshalIndent(rb[i], "", " ")
				t.Fatalf("request %d differs between runs:\n%s\n---\n%s", i, x, y)
			}
		}
	}
	if ra[0].Seed != 1234 {
		t.Errorf("seed %d, want 1234", ra[0].Seed)
	}

	// The replay went through KubeHealth's own evidence gathering.
	n := a.Notes["test"]
	if n.Status != "ready" || n.Ref != "ns:orders/worker" || !n.OnTarget || n.Target != "worker" {
		t.Errorf("note: %+v", n)
	}
	byID := map[string]*benchCaseResult{}
	for _, c := range a.Cases {
		byID[c.Case.ID] = c
	}
	w := byID["worker"]
	prompt := func(ex benchExchange) string {
		var q req
		json.Unmarshal(ex.Request, &q)
		return q.Messages[len(q.Messages)-1]["content"].(string)
	}
	for _, want := range []string{
		"Warnings on objects this Deployment owns:\n- Pod " + benchPod + " BackOff: Back-off restarting failed container worker  (×12, 30s ago)",
		"Logs from pod " + benchPod + ", its newest failing Pod (last 150 lines):\nContainer state: worker restarted 4 times",
		"- [Normal] ScalingReplicaSet: Scaled up replica set worker-5d8f7b6c9d from 0 to 1  (×1, 10m ago)",
	} {
		if p := prompt(w.Analysis.Exchanges[0]); !strings.Contains(p, want) {
			t.Errorf("analysis prompt lacks %q:\n%s", want, p)
		}
	}
	if w.Analysis.Content != goodAnalysis || w.RCA.Status != "done" || w.RCA.Result != goodRCA {
		t.Errorf("worker: analysis %+v, rca %+v", w.Analysis.Content, w.RCA)
	}
	if p := prompt(byID["pv"].RCA.Exchanges[0]); !strings.Contains(p, "Events (3 total):\n- [Warning] VolumeFailedRecycle: Recycle failed: no recycler  (×3, 2m ago)\n- [Warning] VolumeFailedRecycle: Recycle failed again  (×1, 2m ago)\n- [Warning] VolumeFailedRecycle: Recycle failed, seen from orders") {
		t.Errorf("PV RCA prompt:\n%s", p)
	}
	s := scoreResults(a)
	if p, n, _ := s.pct("rca_cause"); p != 50 || n != 2 { // the PV answer does not say recycle
		t.Errorf("rca_cause %d%% of %d", p, n)
	}
	if a.Seed != 1234 || len(a.Runs) != 0 || len(a.Seeds) != 0 {
		t.Errorf("one seed: seed %d, %d runs, seeds %v", a.Seed, len(a.Runs), a.Seeds)
	}

	// -seeds runs every item once per seed, each run with its seed, and the
	// same prompts.
	m := run(filepath.Join(root, "m.json"), "recorded", "-seeds", "7,8")
	if !reflect.DeepEqual(m.Seeds, []int64{7, 8}) || len(m.Runs) != 2 || len(m.Cases) != 0 || m.Notes != nil {
		t.Fatalf("seeds %v, %d runs, %d top-level cases", m.Seeds, len(m.Runs), len(m.Cases))
	}
	for i, sd := range []int64{7, 8} {
		rn := m.Runs[i]
		if rn.Seed != sd || len(rn.Cases) != 2 || rn.Notes["test"] == nil || rn.Snapshots["test"].Problems == 0 {
			t.Fatalf("run %d: %+v", i, rn)
		}
		got := requests(&benchResults{benchRun: *rn})
		if len(got) != len(ra) {
			t.Fatalf("run %d: %d requests", i, len(got))
		}
		for j := range got {
			if got[j].Seed != sd {
				t.Errorf("run %d request %d: seed %d", i, j, got[j].Seed)
			}
			got[j].Seed = ra[j].Seed
			if !reflect.DeepEqual(got[j], ra[j]) {
				t.Errorf("run %d request %d differs from the one-seed run", i, j)
			}
		}
	}
	if ms := scoreResults(m); len(ms.Runs) != 2 || ms.Faults != 2 {
		t.Errorf("score: %d runs, %d faults", len(ms.Runs), ms.Faults)
	}
}

// Scoring reads what the model diagnosed from the whole answer, never its
// layout: plain-text answers score the same as sectioned ones.
func TestBenchScoreContent(t *testing.T) {
	c := func(id, snap, ns, kind, name string, healthy bool, cause, deep, fix [][]string) benchCase {
		return benchCase{ID: id, Snapshot: snap, Anchor: benchAnchor{ns, kind, name}, Healthy: healthy, Cause: cause, Deep: deep, Fix: fix}
	}
	g := func(gs ...[]string) [][]string { return gs }
	k := func(ks ...string) []string { return ks }
	item := func(bc benchCase, an, rcaStatus, rca string) *benchCaseResult {
		return &benchCaseResult{Case: bc, Analysis: benchAnalysisResult{Content: an}, RCA: benchRCAResult{Status: rcaStatus, Result: rca}}
	}
	steps := func(calls int) []RCAStep {
		st := []RCAStep{{Tool: "get_pod_logs", Initial: true}}
		for i := 0; i < calls; i++ {
			st = append(st, RCAStep{Iter: i + 1, Tool: "get_object_describe"})
		}
		return st
	}
	r := &benchResults{Model: "m", benchRun: benchRun{Notes: map[string]*benchNoteResult{
		"a": {Text: "Check the PVC data-db-0: StorageClass fast-ssd does not exist.", Ref: "ns:orders/data-db-0"},
		"b": {Text: "Okay, let me look at the scorer.", Ref: "ns:payments/scorer"},
	}, Cases: []*benchCaseResult{
		item(c("pvc", "a", "orders", "PersistentVolumeClaim", "data-db-0", false, g(k("fast-ssd", "storageclass", "storage class")), nil, g(k("storageclass", "storage class"))),
			"The PVC is Pending because StorageClass fast-ssd does not exist. Create the StorageClass or point the claim at an existing one.",
			"done", "The PVC asks for storage class fast-ssd, which does not exist.\nEvidence: ProvisioningFailed events.\nFix: create StorageClass fast-ssd."),
		item(c("worker", "a", "orders", "Deployment", "worker", false, g(k("orders-db"), k("no such host", "dns")), g(k("fast-ssd", "pvc")), g(k("orders-db", "volume"))),
			"worker crashes: lookup orders-db: no such host.",
			"done", "orders-db is unreachable (no such host) because its PVC is Pending. Fix: repair the orders-db volume."),
		item(c("oom", "a", "search", "Deployment", "indexer", false, g(k("oom", "137")), g(k("48mi")), g(k("limit", "memory"))),
			"The pod is OOMKilled with exit 137; raise the memory limit.", "error", ""),
		item(c("cm", "b", "payments", "Deployment", "scorer", false, g(k("rules-v7", "configmap")), nil, g(k("configmap"))),
			"scorer fails: missing ConfigMap rules-v7. Create the configmap.",
			"done", "CreateContainerConfigError. Recreate the pod."),
		item(c("ok1", "a", "mon", "DaemonSet", "agent", true, nil, nil, nil), "Nothing is wrong: all 3 pods are ready.", "done", "x"),
		item(c("ok2", "a", "mon", "CronJob", "rollup", true, nil, nil, nil), "The job is slow and may need more CPU.", "done", "x"),
	}}}
	// worker and cm: the cause is in another object. The worker's RCA called
	// two tools (the logs in the first prompt are not a call), cm's none.
	r.Cases[1].Case.NeedsTool, r.Cases[1].RCA.Steps = true, steps(2)
	r.Cases[3].Case.NeedsTool, r.Cases[3].Case.Deep, r.Cases[3].RCA.Steps = true, g(k("rules-v7")), steps(0)
	r.Cases[0].RCA.Steps = steps(0)
	s := scoreResults(r)
	want := map[string][2]int{
		"an_cause": {100, 4}, "an_fix": {75, 4}, "rca_done": {75, 4}, "rca_cause": {50, 4}, "rca_deep": {50, 2}, "rca_detail": {0, 1},
		"rca_fix": {50, 4}, "rca_tool": {50, 2}, "healthy_ok": {50, 2}, "note_cause": {50, 2}, "note_clean": {50, 2},
	}
	for check, w := range want {
		if p, n, _ := s.pct(check); p != w[0] || n != w[1] {
			t.Errorf("%s: %d%% (%d), want %d%% (%d)", check, p, n, w[0], w[1])
		}
	}
	rows := map[string]map[string]bool{
		"pvc":    {"an_cause": true, "an_fix": true, "rca_done": true, "rca_cause": true, "rca_fix": true},
		"worker": {"an_cause": true, "an_fix": false, "rca_done": true, "rca_cause": true, "rca_deep": true, "rca_fix": true, "rca_tool": true},
		"oom":    {"an_cause": true, "an_fix": true, "rca_done": false, "rca_cause": false, "rca_detail": false, "rca_fix": false},
		"cm":     {"an_cause": true, "an_fix": true, "rca_done": true, "rca_cause": false, "rca_deep": false, "rca_fix": false, "rca_tool": false},
		"ok1":    {"healthy_ok": true},
		"ok2":    {"healthy_ok": false},
	}
	for id, w := range rows {
		got := map[string]bool{}
		for k, v := range s.Runs[0].Cases[id] {
			if k != "note_cause" {
				got[k] = v
			}
		}
		if !reflect.DeepEqual(got, w) {
			t.Errorf("%s: %v, want %v", id, got, w)
		}
	}
	card := benchScorecard([]benchScore{s})
	for _, want := range []string{"| Check | m |", "| RCA: right root cause | 50% (4) |", "| RCA: finds the cause in another object | 50% (2) |",
		"| RCA: names the decisive detail | 0% (1) |", "| RCA: called a tool when the cause was elsewhere | 50% (2) |",
		"| RCA: tool calls per case (avg) | 0.5 (4) |", "| Note: an answer, not thinking | 50% (2) |"} {
		if !strings.Contains(card, want) {
			t.Errorf("scorecard lacks %q:\n%s", want, card)
		}
	}
	if strings.Contains(card, "±") || strings.Contains(card, "seed") {
		t.Errorf("one seed, yet the scorecard talks about seeds:\n%s", card)
	}
}

// Several seeds: every check is the mean over the seeds, ± half the range.
func TestBenchScoreSeeds(t *testing.T) {
	bc := benchCase{ID: "w", Snapshot: "a", Anchor: benchAnchor{"ns", "Deployment", "w"}, NeedsTool: true,
		Cause: [][]string{{"refused"}}, Deep: [][]string{{"secret"}}, Fix: [][]string{{"secret"}}}
	ok := benchCase{ID: "ok", Snapshot: "a", Anchor: benchAnchor{"ns", "Deployment", "ok"}, Healthy: true}
	run := func(seed int64, rca string, calls int, healthy string) *benchRun {
		st := []RCAStep{{Tool: "get_pod_logs", Initial: true}}
		for i := 0; i < calls; i++ {
			st = append(st, RCAStep{Iter: 1, Tool: "get_object_events"})
		}
		return &benchRun{Seed: seed, Cases: []*benchCaseResult{
			{Case: bc, RCA: benchRCAResult{Status: "done", Result: rca, Steps: st}},
			{Case: ok, Analysis: benchAnalysisResult{Content: healthy}},
		}}
	}
	r := &benchResults{Model: "m", Seeds: []int64{1, 2, 3}, Runs: []*benchRun{
		run(1, "Connection refused: the Secret is missing. Create the secret.", 3, "Nothing is wrong."),
		run(2, "Connection refused.", 0, "Nothing is wrong."),
		run(3, "Connection refused; restart it.", 1, "It may need more CPU."),
	}}
	s := scoreResults(r)
	if len(s.Runs) != 3 || s.Faults != 1 || s.Health != 1 {
		t.Fatalf("%d runs, %d faults, %d healthy", len(s.Runs), s.Faults, s.Health)
	}
	for check, w := range map[string][3]float64{ // mean, spread, n
		"rca_cause": {100, 0, 1}, "rca_deep": {100.0 / 3, 50, 1}, "rca_fix": {100.0 / 3, 50, 1},
		"rca_tool": {200.0 / 3, 50, 1}, "healthy_ok": {200.0 / 3, 50, 1},
	} {
		m, sp, n, ok := s.stat(check, false)
		if !ok || math.Abs(m-w[0]) > 1e-9 || sp != w[1] || n != int(w[2]) {
			t.Errorf("%s: %.2f ±%.2f (%d), want %.2f ±%.2f (%d)", check, m, sp, n, w[0], w[1], int(w[2]))
		}
	}
	if m, sp, n, _ := s.stat("rca_calls", true); m != 4.0/3 || sp != 1.5 || n != 1 {
		t.Errorf("rca_calls: %.2f ±%.2f (%d)", m, sp, n)
	}
	if p, _, _ := s.pct("rca_deep"); p != 33 {
		t.Errorf("rca_deep pct %d", p)
	}
	card := benchScorecard([]benchScore{s})
	for _, want := range []string{"mean over the seeds", "| RCA: right root cause | 100% ±0 (1) |",
		"| RCA: finds the cause in another object | 33% ±50 (1) |", "| RCA: tool calls per case (avg) | 1.3 ±1.5 (1) |",
		"| Healthy: no invented fault | 67% ±50 (1) |", "| Analysis: right issue | 0% ±0 (1) |"} {
		if !strings.Contains(card, want) {
			t.Errorf("scorecard lacks %q:\n%s", want, card)
		}
	}
}

func TestBenchCaseNeedsTool(t *testing.T) {
	dir := t.TempDir()
	write := func(body string) error {
		os.WriteFile(filepath.Join(dir, "c.yaml"), []byte(body), 0o644)
		_, err := loadCases(dir, nil)
		return err
	}
	base := "id: c\nsnapshot: s\nanchor: {ns: a, kind: Deployment, name: w}\ntruth: t\ncause: [[\"x\"]]\nsource: s\nlicense: l\n"
	if err := write(base + "needs_tool: true\ndeep: [[\"secret\"]]\n"); err != nil {
		t.Errorf("needs_tool with deep groups: %v", err)
	}
	if err := write(base + "needs_tool: true\n"); err == nil {
		t.Error("needs_tool without deep groups was accepted")
	}
	c, err := loadCases(dir, nil)
	if err == nil || c != nil {
		t.Errorf("loadCases: %v, %v", c, err)
	}
}

func TestBenchRecorderSeed(t *testing.T) {
	var got []byte
	rt := roundTripFunc(func(b []byte) { got = b })
	rec := &benchRecorder{base: rt, seed: 7, noCache: true}
	var exs []benchExchange
	rec.record(&exs)
	req, _ := httpNewPost("http://x/v1/chat/completions", `{"model":"m","stream":false}`)
	resp, err := rec.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(got) != `{"seed":7,"cache_prompt":false,"model":"m","stream":false}` {
		t.Errorf("sent %s", got)
	}
	if len(exs) != 1 || string(exs[0].Request) != string(got) || exs[0].Status != 200 {
		t.Errorf("recorded %+v", exs)
	}
	if b, _ := json.Marshal(exs[0].Response); string(b) != `{"ok":true}` {
		t.Errorf("response %s", b)
	}
}

// roundTripFunc is a transport that keeps the request body and answers
// {"ok":true}.
type roundTripFunc func(body []byte)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	b, _ := io.ReadAll(r.Body)
	f(b)
	return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"ok":true}`)), Header: http.Header{}}, nil
}

func httpNewPost(url, body string) (*http.Request, error) {
	return http.NewRequest(http.MethodPost, url, strings.NewReader(body))
}

// score -cases applies the current case definitions to an older results file.
func TestBenchRescoreWith(t *testing.T) {
	old := benchCase{ID: "j", Snapshot: "a", Anchor: benchAnchor{"ns", "Job", "j"}, Cause: [][]string{{"backoff"}}}
	r := &benchResults{Model: "m", benchRun: benchRun{Cases: []*benchCaseResult{
		{Case: old, RCA: benchRCAResult{Status: "done", Result: "The input file is missing: no such file."}}}}}
	if p, _, _ := scoreResults(r).pct("rca_cause"); p != 0 {
		t.Fatalf("before: %d%%", p)
	}
	cur := old
	cur.Cause, cur.Deep, cur.NeedsTool = [][]string{{"no such file"}}, [][]string{{"input file"}}, true
	if err := rescoreWith(r, map[string]benchCase{"j": cur}); err != nil {
		t.Fatal(err)
	}
	s := scoreResults(r)
	if p, _, _ := s.pct("rca_cause"); p != 100 {
		t.Errorf("after: rca_cause %d%%", p)
	}
	if p, n, _ := s.pct("rca_deep"); p != 100 || n != 1 {
		t.Errorf("after: rca_deep %d%% (%d)", p, n)
	}
	moved := cur
	moved.Anchor.Name = "other"
	if err := rescoreWith(r, map[string]benchCase{"j": moved}); err == nil {
		t.Error("a moved anchor was accepted")
	}
}
