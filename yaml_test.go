package main

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

// yamlTestCluster registers a cluster whose dynamic client holds objs, for
// the YAML panel's endpoint.
func yamlTestCluster(t *testing.T, objs ...runtime.Object) *clusterState {
	t.Helper()
	c := &clusterState{name: "yaml-test", dynamic: dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), objs...)}
	clusters.Lock()
	clusters.byName[c.name] = c
	clusters.Unlock()
	t.Cleanup(func() {
		clusters.Lock()
		delete(clusters.byName, c.name)
		clusters.Unlock()
	})
	return c
}

func getYAML(t *testing.T, path, ctx string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.Header.Set(contextHeader, ctx)
	rec := httptest.NewRecorder()
	handleAPIYAML(rec, req)
	var body map[string]any
	json.Unmarshal(rec.Body.Bytes(), &body)
	return rec.Code, body
}

// The endpoint: the object as YAML, 404 when it's gone, and the page's own
// cluster (409 when that cluster isn't loaded any more).
func TestAPIYAML(t *testing.T) {
	c := yamlTestCluster(t, &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "orders-worker", "namespace": "orders"},
		"spec":     map[string]any{"replicas": int64(2)},
	}})

	code, body := getYAML(t, "/api/yaml/orders/Deployment/orders-worker", c.name)
	y, _ := body["yaml"].(string)
	if code != http.StatusOK || !strings.Contains(y, "name: orders-worker") || !strings.Contains(y, "replicas: 2") {
		t.Fatalf("found: %d %q", code, y)
	}
	if code, body = getYAML(t, "/api/yaml/orders/Deployment/gone", c.name); code != http.StatusNotFound || body["error"] == nil {
		t.Errorf("missing object: %d %v", code, body)
	}
	if code, _ = getYAML(t, "/api/yaml/orders/Deployment/orders-worker", "not-loaded"); code != http.StatusConflict {
		t.Errorf("unloaded cluster: %d", code)
	}
	if code, _ = getYAML(t, "/api/yaml/orders/Deployment", c.name); code != http.StatusBadRequest {
		t.Errorf("short path: %d", code)
	}
}

// A Secret's values never reach the page unless one is asked for, and the
// last-applied annotation (a copy of them) is masked too.
func TestAPIYAMLSecret(t *testing.T) {
	b64 := func(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }
	c := yamlTestCluster(t, &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Secret",
		"metadata": map[string]any{"name": "orders-db", "namespace": "orders", "annotations": map[string]any{
			lastAppliedAnnotation: `{"data":{"password":"` + b64("hunter2") + `"}}`,
		}},
		"data": map[string]any{"password": b64("hunter2"), "cert": b64("line one\nline two\n"), "blob": b64("\xff\xfe")},
	}})

	code, body := getYAML(t, "/api/yaml/orders/Secret/orders-db", c.name)
	y, _ := body["yaml"].(string)
	if code != http.StatusOK || strings.Contains(y, b64("hunter2")) || strings.Contains(y, "hunter2") {
		t.Fatalf("secret value leaked: %q", y)
	}
	if !strings.Contains(y, "password: "+secretHidden) {
		t.Errorf("value not masked: %q", y)
	}
	keys, _ := body["secret_keys"].([]any)
	if len(keys) != 3 {
		t.Fatalf("secret keys: %v", body["secret_keys"])
	}
	if k := keys[2].(map[string]any); k["key"] != "password" || k["bytes"].(float64) != 7 {
		t.Errorf("password key: %v", k)
	}

	reveal := func(key string) map[string]any {
		_, b := getYAML(t, "/api/yaml/orders/Secret/orders-db?reveal="+key, c.name)
		return b
	}
	if v := reveal("password"); v["value"] != "hunter2" || v["binary"] != false {
		t.Errorf("reveal password: %v", v)
	}
	if v := reveal("cert"); v["value"] != "line one\nline two\n" {
		t.Errorf("reveal cert: %v", v)
	}
	if v := reveal("blob"); v["binary"] != true || v["value"] != b64("\xff\xfe") {
		t.Errorf("binary values come back base64: %v", v)
	}
	if code, _ := getYAML(t, "/api/yaml/orders/Secret/orders-db?reveal=nope", c.name); code != http.StatusNotFound {
		t.Errorf("missing key: %d", code)
	}
}

func TestMaskSecretLeavesOtherKindsAlone(t *testing.T) {
	cm := map[string]any{"kind": "ConfigMap", "data": map[string]any{"a": "b"}}
	if keys := maskSecret(cm); keys != nil || cm["data"].(map[string]any)["a"] != "b" {
		t.Errorf("ConfigMap changed: %v %v", keys, cm)
	}
}

// A Pod waiting on a missing ConfigMap: the panel is pointed at the line that
// names it.
func TestAPIYAMLIssues(t *testing.T) {
	pod := &corev1.Pod{}
	pod.APIVersion, pod.Kind = "v1", "Pod"
	pod.Name, pod.Namespace = "fraud-scorer-abc", "payments"
	pod.Spec.Containers = []corev1.Container{{Name: "scorer", Image: "busybox:1.36",
		EnvFrom: []corev1.EnvFromSource{{ConfigMapRef: &corev1.ConfigMapEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "fraud-rules-v7"}}}}}}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "scorer", State: corev1.ContainerState{
		Waiting: &corev1.ContainerStateWaiting{Reason: "CreateContainerConfigError", Message: `configmap "fraud-rules-v7" not found`}}}}
	u, err := runtime.DefaultUnstructuredConverter.ToUnstructured(pod)
	if err != nil {
		t.Fatal(err)
	}
	c := yamlTestCluster(t, &unstructured.Unstructured{Object: u})

	_, body := getYAML(t, "/api/yaml/payments/Pod/fraud-scorer-abc", c.name)
	issues, _ := body["issues"].([]any)
	if len(issues) != 1 {
		t.Fatalf("issues: %v", body["issues"])
	}
	is := issues[0].(map[string]any)
	if is["ref_kind"] != "ConfigMap" || is["ref_name"] != "fraud-rules-v7" || is["severity"] != "crit" {
		t.Errorf("issue: %v", is)
	}
}

// The event rules: which field each kind of failure points at.
func TestIssuesFromEvents(t *testing.T) {
	ev := func(reason, msg string) EventSummary {
		return EventSummary{Type: "Warning", Reason: reason, Message: msg}
	}
	cases := []struct {
		ev   EventSummary
		want string // suffix, or ref kind/name
	}{
		{ev("Unhealthy", "Readiness probe failed: HTTP probe failed with statuscode: 404"), "readinessProbe"},
		{ev("FailedScheduling", "0/3 nodes are available: 3 Insufficient cpu."), "resources.requests.cpu"},
		{ev("FailedScheduling", "0/3 nodes are available: pod has unbound immediate PersistentVolumeClaims."), "persistentVolumeClaim.claimName"},
		{ev("FailedScheduling", "0/2 nodes are available: 1 node(s) had untolerated taint {dedicated: reporting}, 1 node(s) didn't match Pod's node affinity/selector."), "nodeSelector"},
		{ev("FailedScheduling", "0/3 nodes are available: 2 node(s) didn't match pod anti-affinity rules."), "affinity.podAntiAffinity"},
		{ev("FailedMount", `MountVolume.SetUp failed for volume "cfg" : configmap "relay-conf" not found`), "ConfigMap/relay-conf"},
		{ev("Failed", `Error: couldn't find key password in Secret quillmoss-ops/pigment-db`), "Secret/pigment-db"},
		{ev("Failed", `Failed to pull image "nginx:1.27.9-alpine": not found`), "containers.[].image"},
		{ev("ProvisioningFailed", `storageclass.storage.k8s.io "fast-ssd" not found`), "storageClassName"},
		{ev("ProvisioningFailed", `NodeExpandVolume: volume only supports ReadWriteOnce access mode`), "accessModes"},
		{ev("FailedCreate", `pods "x" is forbidden: exceeded quota: sandbox-quota, requested: pods=1`), "replicas"},
		{ev("FailedCreate", `pods "x" is forbidden: maximum memory usage per Container is 256Mi, but limit is 1Gi`), "resources.limits.memory"},
		{ev("FailedCreate", `Error creating: pods "x" is forbidden: error looking up service account ops/glyph-indexer: serviceaccount "glyph-indexer" not found`), "ServiceAccount/glyph-indexer"},
		{ev("FailedToRetrieveImagePullSecret", `Unable to retrieve some image pull secrets (tidepool-ghcr); attempting to pull the image anyway`), "Secret/tidepool-ghcr"},
		{ev("BackoffLimitExceeded", "Job has reached the specified backoff limit"), "backoffLimit"},
		{ev("BackOff", "Back-off restarting failed container php in pod magento-cron-run-29872500-ldfx9_orbico-stg(dbf3)"), "containers.[].name"},
		{ev("BackOff", `Back-off pulling image "nginx:1.27.9-alpine"`), "containers.[].image"},
		{ev("DeadlineExceeded", "Job was active longer than specified deadline"), "activeDeadlineSeconds"},
		{ev("VolumeFailedDelete", `error getting deleter volume plugin for volume "legacy-reports-nfs"`), "persistentVolumeReclaimPolicy"},
	}
	if is := issuesFrom([]EventSummary{ev("FailedToRetrieveImagePullSecret", "Unable to retrieve some image pull secrets (regcred)")}, nil); len(is) != 1 ||
		len(is[0].Fallback) != 1 || is[0].Fallback[0][0] != "serviceAccountName" {
		t.Errorf("pull secret without a fallback to the ServiceAccount: %+v", is)
	}
	for _, tc := range cases {
		got := issuesFrom([]EventSummary{tc.ev}, nil)
		if len(got) != 1 {
			t.Errorf("%s %q: %d issues", tc.ev.Reason, tc.ev.Message, len(got))
			continue
		}
		have := strings.Join(got[0].Suffix, ".")
		if got[0].RefKind != "" {
			have = got[0].RefKind + "/" + got[0].RefName
		}
		if have != tc.want {
			t.Errorf("%s %q: points at %q, want %q", tc.ev.Reason, tc.ev.Message, have, tc.want)
		}
	}
}

// Container state: an OOM kill points at the memory limit, a crash loop at
// the container.
func TestIssuesFromContainers(t *testing.T) {
	pod := &corev1.Pod{}
	pod.Spec.Containers = []corev1.Container{{Name: "indexer", Resources: corev1.ResourceRequirements{
		Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("48Mi")}}}, {Name: "worker"}}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{
		{Name: "indexer", RestartCount: 9, LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "Error", ExitCode: 137}}},
		{Name: "worker", RestartCount: 3, LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "Error", ExitCode: 1}}},
	}
	got := issuesFrom(nil, pod)
	if len(got) != 2 {
		t.Fatalf("issues: %+v", got)
	}
	if strings.Join(got[0].Suffix, ".") != "containers.[].resources.limits.memory" || got[0].Container != "indexer" || !strings.Contains(got[0].Text, "48Mi") {
		t.Errorf("oom: %+v", got[0])
	}
	if strings.Join(got[1].Suffix, ".") != "containers.[].name" || got[1].Container != "worker" || got[1].Severity != "warn" {
		t.Errorf("crash loop: %+v", got[1])
	}
}
