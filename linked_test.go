package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
)

// ── A small cluster per test, replayed like a bench snapshot ─────────────────

var linkAt = time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)

func ago(d time.Duration) metav1.Time { return metav1.NewTime(linkAt.Add(-d)) }

// linkEvent is an event about an object, first seen 10 minutes ago.
func linkEvent(ns, kind, obj, typ, reason, msg string, count int32, last time.Duration) *corev1.Event {
	return &corev1.Event{
		ObjectMeta:     metav1.ObjectMeta{Name: fmt.Sprintf("%s.%s.%d", obj, strings.ToLower(reason), len(msg)), Namespace: ns, CreationTimestamp: ago(10 * time.Minute)},
		InvolvedObject: corev1.ObjectReference{Kind: kind, Name: obj, Namespace: ns},
		Type:           typ, Reason: reason, Message: msg, Count: count,
		FirstTimestamp: ago(10 * time.Minute), LastTimestamp: ago(last),
	}
}

func nsObj(name string) *corev1.Namespace {
	return &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name, CreationTimestamp: ago(2 * time.Hour)}}
}

// linkCluster replays objs (and container logs keyed ns/pod/container) as a
// warmed cluster at linkAt.
func linkCluster(t *testing.T, logs map[string]string, objs ...runtime.Object) *clusterState {
	t.Helper()
	saved := cfg
	t.Cleanup(func() { cfg, clock = saved, time.Now })
	cfg.AllNS, cfg.Namespaces = true, nil
	s := &benchSnapshot{meta: snapshotMeta{Name: "link", RecordedAt: linkAt}, logs: map[string]string{}}
	for k, v := range logs {
		parts := strings.SplitN(k, "/", 3)
		s.logs[logKey(parts[0], parts[1], parts[2], false)] = v
	}
	for _, o := range objs {
		gvks, _, err := clientgoscheme.Scheme.ObjectKinds(o)
		if err != nil {
			t.Fatal(err)
		}
		m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(o)
		if err != nil {
			t.Fatal(err)
		}
		m["apiVersion"], m["kind"] = gvks[0].GroupVersion().String(), gvks[0].Kind
		s.objects = append(s.objects, m)
	}
	clock = func() time.Time { return linkAt }
	c, err := s.cluster(linkAt)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.benchWarm(context.Background(), linkAt); err != nil {
		t.Fatal(err)
	}
	return c
}

// deployment is a Deployment with one ReplicaSet and its Pods (given),
// all labelled app=<name>.
func deployment(ns, name string, replicas int32, ports ...int32) []runtime.Object {
	ctrl := true
	var cports []corev1.ContainerPort
	for _, p := range ports {
		cports = append(cports, corev1.ContainerPort{ContainerPort: p})
	}
	lbl := map[string]string{"app": name}
	return []runtime.Object{
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, CreationTimestamp: ago(time.Hour)},
			Spec: appsv1.DeploymentSpec{Replicas: &replicas, Selector: &metav1.LabelSelector{MatchLabels: lbl},
				Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: lbl},
					Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: name, Image: "acme/" + name + ":1", Ports: cports}}}}}},
		&appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: name + "-7d9f6c", Namespace: ns, CreationTimestamp: ago(time.Hour), Labels: lbl,
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: name, Controller: &ctrl}}},
			Spec: appsv1.ReplicaSetSpec{Replicas: &replicas, Selector: &metav1.LabelSelector{MatchLabels: lbl}}},
	}
}

// pod is a Pod of a ReplicaSet (owner "" for a standalone Pod).
func pod(ns, name, owner string, labels map[string]string, phase corev1.PodPhase, ready bool, mod func(*corev1.Pod)) *corev1.Pod {
	ctrl := true
	p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Labels: labels, CreationTimestamp: ago(30 * time.Minute)},
		Spec:   corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "acme/main:1"}}},
		Status: corev1.PodStatus{Phase: phase}}
	if owner != "" {
		p.OwnerReferences = []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: owner, Controller: &ctrl}}
	}
	st := corev1.ConditionFalse
	if ready {
		st = corev1.ConditionTrue
	}
	p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: st}}
	if mod != nil {
		mod(p)
	}
	return p
}

func service(ns, name, ip string, sel map[string]string, port int32, target intstr.IntOrString) *corev1.Service {
	return &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, CreationTimestamp: ago(time.Hour)},
		Spec: corev1.ServiceSpec{ClusterIP: ip, Selector: sel, Ports: []corev1.ServicePort{{Port: port, TargetPort: target, Protocol: corev1.ProtocolTCP}}}}
}

func endpointSlice(ns, svc string, port int32, ips ...string) *discoveryv1.EndpointSlice {
	var eps []discoveryv1.Endpoint
	for _, ip := range ips {
		eps = append(eps, discoveryv1.Endpoint{Addresses: []string{ip}, Conditions: discoveryv1.EndpointConditions{Ready: ptr(true)}})
	}
	return &discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{Name: svc + "-abc", Namespace: ns,
		Labels: map[string]string{discoveryv1.LabelServiceName: svc}}, AddressType: discoveryv1.AddressTypeIPv4,
		Ports: []discoveryv1.EndpointPort{{Port: ptr(port)}}, Endpoints: eps}
}

func node(name string, labels map[string]string, taints ...corev1.Taint) *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels, CreationTimestamp: ago(2 * time.Hour)},
		Spec: corev1.NodeSpec{Taints: taints},
		Status: corev1.NodeStatus{
			Conditions:  []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
			Allocatable: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("4"), corev1.ResourceMemory: resource.MustParse("16273592Ki")}}}
}

func storageClass(name string, def bool) *storagev1.StorageClass {
	mode := storagev1.VolumeBindingWaitForFirstConsumer
	sc := &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: name}, Provisioner: "rancher.io/local-path", VolumeBindingMode: &mode}
	if def {
		sc.Annotations = map[string]string{"storageclass.kubernetes.io/is-default-class": "true"}
	}
	return sc
}

func pvc(ns, name, sc string, phase corev1.PersistentVolumeClaimPhase) *corev1.PersistentVolumeClaim {
	c := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, CreationTimestamp: ago(time.Hour)},
		Spec: corev1.PersistentVolumeClaimSpec{AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")}}},
		Status: corev1.PersistentVolumeClaimStatus{Phase: phase}}
	if sc != "" {
		c.Spec.StorageClassName = &sc
	}
	return c
}

func crashLooping(p *corev1.Pod) {
	p.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "main", RestartCount: 4,
		State:                corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}},
		LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, Reason: "Error"}}}}
}

func withClaim(claim string) func(*corev1.Pod) {
	return func(p *corev1.Pod) {
		p.Spec.Volumes = append(p.Spec.Volumes, corev1.Volume{Name: "data",
			VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claim}}})
	}
}

// links runs the rules on an anchor as the ✦ AI analysis gathers its
// evidence, without a context limit.
func links(c *clusterState, ns, kind, name string) []linkedItem {
	return analysisEvidence(c, ns, kind, name, 0).linkedItems
}

// analysisPrompt is the ✦ AI analysis prompt of an anchor, untrimmed.
func analysisPrompt(c *clusterState, ns, kind, name string) string {
	e := analysisEvidence(c, ns, kind, name, 0)
	return e.fit(0)
}

func linkTags(items []linkedItem) string {
	var out []string
	for _, it := range items {
		out = append(out, it.tag())
	}
	return strings.Join(out, " ")
}

// ── Scenarios shared by the tests and the dump ───────────────────────────────

// The worker can't reach redis (named by its cluster IP), whose only Pod is
// Pending on a taint.
func scenarioServicePending(t *testing.T) *clusterState {
	objs := append(deployment("shop", "worker", 1),
		nsObj("shop"),
		pod("shop", "worker-7d9f6c-x2k4p", "worker-7d9f6c", map[string]string{"app": "worker"}, corev1.PodRunning, false, crashLooping),
		pod("shop", "redis-0", "", map[string]string{"app": "redis"}, corev1.PodPending, false, nil),
		service("shop", "redis", "10.96.0.20", map[string]string{"app": "redis"}, 6379, intstr.FromInt32(6379)),
		linkEvent("shop", "Pod", "worker-7d9f6c-x2k4p", "Warning", "BackOff", "Back-off restarting failed container main in pod worker-7d9f6c-x2k4p_shop(1)", 12, 40*time.Second),
		linkEvent("shop", "Deployment", "worker", "Normal", "ScalingReplicaSet", "Scaled up replica set worker-7d9f6c from 0 to 1", 1, time.Hour),
		linkEvent("shop", "Pod", "redis-0", "Warning", "FailedScheduling", "0/3 nodes are available: 3 node(s) had untolerated taint {dedicated: db}. preemption: 0/3 nodes are available: 3 Preemption is not helpful for scheduling.", 4, time.Minute),
	)
	return linkCluster(t, map[string]string{"shop/worker-7d9f6c-x2k4p/main": "worker 2.1: connecting to redis:6379\n" +
		"wget: can't connect to remote host (10.96.0.20): Connection refused\nFATAL: cache unavailable\n"}, objs...)
}

// A standalone Pod Pending on an unbound claim whose StorageClass is
// missing, and on a nodeSelector no node satisfies.
func scenarioPodClaim(t *testing.T) *clusterState {
	return linkCluster(t, nil,
		nsObj("data"),
		pod("data", "loader", "", map[string]string{"app": "loader"}, corev1.PodPending, false, func(p *corev1.Pod) {
			withClaim("data-loader")(p)
			p.Spec.NodeSelector = map[string]string{"disktype": "nvme"}
		}),
		pvc("data", "data-loader", "fast", corev1.ClaimPending),
		storageClass("standard", true),
		node("node-a", map[string]string{"disktype": "ssd"}),
		node("node-b", nil, corev1.Taint{Key: "dedicated", Value: "gpu", Effect: corev1.TaintEffectNoSchedule}),
		linkEvent("data", "Pod", "loader", "Warning", "FailedScheduling", "0/2 nodes are available: pod has unbound immediate PersistentVolumeClaims. preemption: 0/2 nodes are available: 2 Preemption is not helpful for scheduling.", 6, 2*time.Minute),
		linkEvent("data", "Pod", "loader", "Warning", "FailedScheduling", "0/2 nodes are available: 1 node(s) didn't match Pod's node affinity/selector, 1 node(s) had untolerated taint {dedicated: gpu}.", 2, time.Minute),
		linkEvent("data", "PersistentVolumeClaim", "data-loader", "Warning", "ProvisioningFailed", `storageclass.storage.k8s.io "fast" not found`, 5, 2*time.Minute),
	)
}

// An api whose env reads a key its Secret lacks, and whose volume names a
// ConfigMap that doesn't exist.
func scenarioConfigRefs(t *testing.T) *clusterState {
	objs := append(deployment("billing", "api", 1),
		nsObj("billing"),
		pod("billing", "api-7d9f6c-q7w2x", "api-7d9f6c", map[string]string{"app": "api"}, corev1.PodPending, false, func(p *corev1.Pod) {
			p.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "main", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
				Reason: "CreateContainerConfigError", Message: "couldn't find key password in Secret billing/db-creds"}}}}
		}),
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "db-creds", Namespace: "billing"}, Type: corev1.SecretTypeOpaque,
			Data: map[string][]byte{"user": nil, "passwd": nil}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "api-settings", Namespace: "billing"}, Data: map[string]string{"mode": ""}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "kube-root-ca.crt", Namespace: "billing"}},
		linkEvent("billing", "Pod", "api-7d9f6c-q7w2x", "Warning", "Failed", "Error: couldn't find key password in Secret billing/db-creds", 7, 30*time.Second),
		linkEvent("billing", "Pod", "api-7d9f6c-q7w2x", "Warning", "FailedMount", `MountVolume.SetUp failed for volume "cfg" : configmap "api-config" not found`, 3, time.Minute),
	)
	return linkCluster(t, nil, objs...)
}

// A Pod whose image pull is refused: its own pull secret is missing, its
// ServiceAccount's exists.
func scenarioPullSecret(t *testing.T) *clusterState {
	return linkCluster(t, nil,
		nsObj("ci"),
		pod("ci", "scanner", "", map[string]string{"app": "scanner"}, corev1.PodPending, false, func(p *corev1.Pod) {
			p.Spec.ServiceAccountName = "builder"
			p.Spec.ImagePullSecrets = []corev1.LocalObjectReference{{Name: "ghcr-creds"}}
		}),
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "builder", Namespace: "ci"},
			ImagePullSecrets: []corev1.LocalObjectReference{{Name: "registry-a"}}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "registry-a", Namespace: "ci"}, Type: corev1.SecretTypeDockerConfigJson},
		linkEvent("ci", "Pod", "scanner", "Warning", "Failed", `Failed to pull image "ghcr.io/acme/scanner:1": failed to authorize: failed to fetch anonymous token: unexpected status: 401 Unauthorized`, 3, time.Minute),
		linkEvent("ci", "Pod", "scanner", "Normal", "BackOff", `Back-off pulling image "ghcr.io/acme/scanner:1"`, 9, 20*time.Second),
	)
}

// A healthy Deployment: nothing to follow.
func scenarioHealthy(t *testing.T) *clusterState {
	objs := append(deployment("web", "site", 1, 80),
		nsObj("web"),
		pod("web", "site-7d9f6c-b4c2d", "site-7d9f6c", map[string]string{"app": "site"}, corev1.PodRunning, true, nil),
		service("web", "site", "10.96.0.9", map[string]string{"app": "site"}, 80, intstr.FromInt32(80)),
		endpointSlice("web", "site", 80, "10.244.1.9"),
		linkEvent("web", "Deployment", "site", "Normal", "ScalingReplicaSet", "Scaled up replica set site-7d9f6c from 0 to 1", 1, time.Hour),
	)
	return linkCluster(t, map[string]string{"web/site-7d9f6c-b4c2d/main": "listening on :80\n"}, objs...)
}

// ── Rules ────────────────────────────────────────────────────────────────────

func TestLinkedService(t *testing.T) {
	c := scenarioServicePending(t)
	items := links(c, "shop", "Deployment", "worker")
	if len(items) != 1 {
		t.Fatalf("items: %s; want the redis Service only (redis-0's taint is two hops away)", linkTags(items))
	}
	it := items[0]
	if it.tag() != "service:Service/redis" || it.Facts != "selector app=redis, ports 6379/TCP, endpoints 0 ready" {
		t.Errorf("item: %+v", it)
	}
	if len(it.Lines) != 1 || !strings.HasPrefix(it.Lines[0], "Pod redis-0: Pending, not ready — FailedScheduling: 0/3 nodes are available: 3 node(s) had untolerated taint {dedicated: db}.") ||
		!strings.HasSuffix(it.Lines[0], "(×4, 1m ago)") {
		t.Errorf("lines: %q", it.Lines)
	}

	// A host by name and by DNS name, in an event or a log line.
	l := &linker{cl: c, ctx: context.Background(), ns: "shop", kind: "Deployment", name: "worker", seen: map[string]bool{}, pods: map[string]*corev1.Pod{}}
	for _, tok := range []string{"redis", "redis.shop", "redis.shop.svc", "redis.shop.svc.cluster.local", "10.96.0.20"} {
		if s := l.serviceFor(tok, map[string]*corev1.Service{}); s == nil || s.Name != "redis" {
			t.Errorf("%s: not resolved to Service redis", tok)
		}
	}
	for _, tok := range []string{"redis.cache", "redis.shop.example.com", "10.96.0.21", "cache", "6379"} {
		if s := l.serviceFor(tok, map[string]*corev1.Service{}); s != nil {
			t.Errorf("%s: resolved to %s", tok, s.Name)
		}
	}
}

func TestLinkedServiceNotFollowed(t *testing.T) {
	// No connection error in the logs: nothing to follow, even though redis
	// is named.
	objs := append(deployment("shop", "worker", 1),
		nsObj("shop"),
		pod("shop", "worker-7d9f6c-x2k4p", "worker-7d9f6c", map[string]string{"app": "worker"}, corev1.PodRunning, false, crashLooping),
		service("shop", "redis", "10.96.0.20", map[string]string{"app": "redis"}, 6379, intstr.FromInt32(6379)),
		service("shop", "worker", "10.96.0.21", map[string]string{"app": "worker"}, 80, intstr.FromInt32(80)),
		linkEvent("shop", "Pod", "worker-7d9f6c-x2k4p", "Warning", "BackOff", "Back-off restarting failed container main", 12, 40*time.Second),
	)
	c := linkCluster(t, map[string]string{"shop/worker-7d9f6c-x2k4p/main": "using cache redis:6379\npanic: nil map\n"}, objs...)
	if items := links(c, "shop", "Deployment", "worker"); len(items) != 0 {
		t.Errorf("no error line: %s", linkTags(items))
	}

	// The worker's own Service is not a dependency.
	c = linkCluster(t, map[string]string{"shop/worker-7d9f6c-x2k4p/main": "worker: dial tcp worker:80: connection refused\n"}, objs...)
	if items := links(c, "shop", "Deployment", "worker"); len(items) != 0 {
		t.Errorf("own Service followed: %s", linkTags(items))
	}
}

func TestLinkedServiceHints(t *testing.T) {
	objs := append(deployment("shop", "worker", 1),
		nsObj("shop"),
		pod("shop", "worker-7d9f6c-x2k4p", "worker-7d9f6c", map[string]string{"app": "worker"}, corev1.PodRunning, false, crashLooping),
		// A selector typo: pods are app=templates.
		service("shop", "templates", "10.96.0.30", map[string]string{"app": "template"}, 8080, intstr.FromInt32(80)),
		pod("shop", "templates-1", "", map[string]string{"app": "templates"}, corev1.PodRunning, true, nil),
		// A wrong targetPort: the Pods declare 80.
		service("shop", "api", "10.96.0.31", map[string]string{"app": "api"}, 80, intstr.FromInt32(8080)),
		pod("shop", "api-1", "", map[string]string{"app": "api"}, corev1.PodRunning, true, func(p *corev1.Pod) {
			p.Spec.Containers[0].Ports = []corev1.ContainerPort{{ContainerPort: 80}}
		}),
		endpointSlice("shop", "api", 8080, "10.244.0.5"),
		linkEvent("shop", "Pod", "worker-7d9f6c-x2k4p", "Warning", "BackOff", "Back-off restarting failed container main", 12, 40*time.Second),
	)
	c := linkCluster(t, map[string]string{"shop/worker-7d9f6c-x2k4p/main": "GET http://templates:8080/v1: connection refused\n" +
		"dial tcp api.shop.svc.cluster.local:80: connect: connection refused\n"}, objs...)
	items := links(c, "shop", "Deployment", "worker")
	if linkTags(items) != "service:Service/templates service:Service/api" {
		t.Fatalf("items: %s", linkTags(items))
	}
	if got := strings.Join(items[0].Lines, " | "); got != "selector matches no pods | pods in namespace shop with label app: app=templates (1 pod), app=api (1 pod), app=worker (1 pod)" {
		t.Errorf("templates: %s", got)
	}
	if items[1].Facts != "selector app=api, ports 80→8080/TCP, endpoints 1 ready" ||
		strings.Join(items[1].Lines, " | ") != "targetPort 8080 is not a port its pods declare (they declare 80/TCP)" {
		t.Errorf("api: %+v", items[1])
	}
}

func TestLinkedClaim(t *testing.T) {
	c := scenarioPodClaim(t)
	items := links(c, "data", "Pod", "loader")
	if got := linkTags(items); got != "claim:PersistentVolumeClaim/data-loader nodes:Node/node-a nodes:Node/node-b" {
		t.Fatalf("items: %s", got)
	}
	want := linkedItem{Rule: "claim", Kind: "PersistentVolumeClaim", NS: "data", Name: "data-loader",
		Facts: `Pending, StorageClass "fast" (does not exist), 10Gi ReadWriteOnce`,
		Lines: []string{`[Warning] ProvisioningFailed: storageclass.storage.k8s.io "fast" not found  (×5, 2m ago)`,
			"StorageClasses in the cluster: standard (default)"}}
	if b, w := mustJSON(items[0]), mustJSON(want); b != w {
		t.Errorf("claim:\n%s\nwant\n%s", b, w)
	}
	if items[1].Facts != "Ready, no taints, allocatable cpu=4 memory=15.5Gi, labels disktype=ssd" ||
		items[2].Facts != "Ready, taints dedicated=gpu:NoSchedule, allocatable cpu=4 memory=15.5Gi, labels disktype=<unset>" {
		t.Errorf("nodes: %q / %q", items[1].Facts, items[2].Facts)
	}

	// The claim as the anchor: its StorageClass.
	items = links(c, "data", "PersistentVolumeClaim", "data-loader")
	if len(items) != 1 || items[0].tag() != "storageclass:StorageClass/fast" || items[0].Facts != "does not exist" {
		t.Errorf("anchor claim: %+v", items)
	}
}

func TestLinkedClaimNotFollowed(t *testing.T) {
	c := linkCluster(t, nil,
		nsObj("data"),
		// Running on a bound claim: nothing about volumes.
		pod("data", "ok", "", nil, corev1.PodRunning, false, func(p *corev1.Pod) { withClaim("bound")(p); crashLooping(p) }),
		pvc("data", "bound", "standard", corev1.ClaimBound),
		// Pending on a claim that doesn't exist.
		pod("data", "orphan", "", nil, corev1.PodPending, false, withClaim("gone")),
		storageClass("standard", true),
		linkEvent("data", "Pod", "ok", "Warning", "BackOff", "Back-off restarting failed container main", 3, time.Minute),
		linkEvent("data", "Pod", "orphan", "Warning", "FailedScheduling", `0/1 nodes are available: persistentvolumeclaim "gone" not found.`, 3, time.Minute),
	)
	if items := links(c, "data", "Pod", "ok"); len(items) != 0 {
		t.Errorf("bound claim followed: %s", linkTags(items))
	}
	items := links(c, "data", "Pod", "orphan")
	if len(items) != 1 || items[0].Facts != "does not exist" || items[0].Name != "gone" {
		t.Errorf("missing claim: %+v", items)
	}
	// A bound anchor claim: its StorageClass is fine.
	if items := links(c, "data", "PersistentVolumeClaim", "bound"); len(items) != 0 {
		t.Errorf("bound anchor claim: %s", linkTags(items))
	}
}

func TestLinkedConfig(t *testing.T) {
	c := scenarioConfigRefs(t)
	items := links(c, "billing", "Deployment", "api")
	if got := linkTags(items); got != "config:Secret/db-creds config:ConfigMap/api-config" {
		t.Fatalf("items: %s", got)
	}
	if items[0].Facts != `exists, keys passwd, user (no key "password")` || len(items[0].Lines) != 0 {
		t.Errorf("secret: %+v", items[0])
	}
	if items[1].Facts != "does not exist" || strings.Join(items[1].Lines, "|") != "ConfigMaps in namespace billing: api-settings" {
		t.Errorf("configmap: %+v", items[1])
	}
	// No value is ever printed: the keys' values are empty here, and the
	// facts hold names only.
	for _, it := range items {
		if strings.Contains(mustJSON(it), "mode") {
			t.Errorf("listed another ConfigMap's keys: %+v", it)
		}
	}
}

func TestLinkedPullSecret(t *testing.T) {
	c := scenarioPullSecret(t)
	items := links(c, "ci", "Pod", "scanner")
	if got := linkTags(items); got != "pullsecret:Secret/ghcr-creds pullsecret:Secret/registry-a" {
		t.Fatalf("items: %s", got)
	}
	if items[0].Facts != "does not exist (in imagePullSecrets of Pod scanner)" ||
		items[1].Facts != "exists, type kubernetes.io/dockerconfigjson (in imagePullSecrets of ServiceAccount builder)" {
		t.Errorf("facts: %q / %q", items[0].Facts, items[1].Facts)
	}

	// A tag that doesn't exist is not an auth problem; no pull secrets at all.
	c = linkCluster(t, nil,
		nsObj("ci"),
		pod("ci", "typo", "", nil, corev1.PodPending, false, nil),
		pod("ci", "private", "", nil, corev1.PodPending, false, nil),
		linkEvent("ci", "Pod", "typo", "Warning", "Failed", `Failed to pull image "busybox:1.63": not found`, 3, time.Minute),
		linkEvent("ci", "Pod", "private", "Warning", "Failed", `Failed to pull image "ghcr.io/acme/x:1": pull access denied, repository does not exist or may require authorization`, 3, time.Minute),
	)
	if items := links(c, "ci", "Pod", "typo"); len(items) != 0 {
		t.Errorf("not found: %s", linkTags(items))
	}
	items = links(c, "ci", "Pod", "private")
	if len(items) != 1 || items[0].tag() != "pullsecret:ServiceAccount/default" ||
		items[0].Facts != "does not exist; Pod private sets no imagePullSecrets" {
		t.Errorf("no secrets: %+v", items)
	}
}

func TestLinkedNodesCapped(t *testing.T) {
	objs := []runtime.Object{nsObj("data"),
		pod("data", "big", "", nil, corev1.PodPending, false, nil),
		linkEvent("data", "Pod", "big", "Warning", "FailedScheduling", "0/8 nodes are available: 8 Insufficient cpu.", 3, time.Minute),
	}
	for _, n := range []string{"n1", "n2", "n3", "n4", "n5", "n6", "n7", "n8"} {
		objs = append(objs, node(n, nil))
	}
	items := links(linkCluster(t, nil, objs...), "data", "Pod", "big")
	if len(items) != linkedMaxNodes || strings.Join(items[5].Lines, "") != "(2 more nodes)" {
		t.Errorf("items: %s; last %+v", linkTags(items), items[len(items)-1])
	}
}

// One object named by two rules is listed once; the anchor never.
func TestLinkedDedup(t *testing.T) {
	c := linkCluster(t, nil,
		nsObj("ci"),
		pod("ci", "scanner", "", nil, corev1.PodPending, false, func(p *corev1.Pod) {
			p.Spec.ImagePullSecrets = []corev1.LocalObjectReference{{Name: "ghcr-creds"}}
		}),
		linkEvent("ci", "Pod", "scanner", "Warning", "Failed", `Failed to pull image "ghcr.io/a/b:1": 401 Unauthorized`, 3, time.Minute),
		linkEvent("ci", "Pod", "scanner", "Warning", "FailedToRetrieveImagePullSecret", `Unable to retrieve some image pull secrets (ghcr-creds); attempting to pull the image may not succeed.`, 3, time.Minute),
		linkEvent("ci", "Pod", "scanner", "Warning", "FailedMount", `MountVolume.SetUp failed for volume "x" : secret "ghcr-creds" not found`, 3, time.Minute),
	)
	if got := linkTags(links(c, "ci", "Pod", "scanner")); got != "config:Secret/ghcr-creds" {
		t.Errorf("items: %s", got)
	}
	c = linkCluster(t, nil, nsObj("ci"),
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "self", Namespace: "ci"}},
		linkEvent("ci", "Secret", "self", "Warning", "Odd", `secret "self" not found`, 1, time.Minute))
	if got := linkTags(links(c, "ci", "Secret", "self")); got != "" {
		t.Errorf("anchor followed: %s", got)
	}
}

func TestRenderLinkedBudget(t *testing.T) {
	items := []linkedItem{
		{Kind: "Secret", Name: "a", Facts: "does not exist"},
		{Kind: "Service", Name: "b", Facts: "selector app=b, ports 80/TCP, endpoints 0 ready", Lines: []string{"Pod b-0: Pending, not ready",
			"Pod b-1: Pending, not ready — FailedScheduling: 0/3 nodes are available: 3 node(s) had untolerated taint {dedicated: db}."}},
		{Kind: "Node", Name: "c", Facts: "Ready, taints dedicated=reporting:NoSchedule, allocatable cpu=8 memory=15.2Gi, labels node-pool=reporting"},
	}
	full, kept, trimmed := renderLinked(items, 0)
	want := "\nLinked evidence (followed from the evidence above):\n" +
		"- Secret a: does not exist\n" +
		"- Service b: selector app=b, ports 80/TCP, endpoints 0 ready\n" +
		"  - Pod b-0: Pending, not ready\n" +
		"  - Pod b-1: Pending, not ready — FailedScheduling: 0/3 nodes are available: 3 node(s) had untolerated taint {dedicated: db}.\n" +
		"- Node c: Ready, taints dedicated=reporting:NoSchedule, allocatable cpu=8 memory=15.2Gi, labels node-pool=reporting\n"
	if full != want || len(kept) != 3 || trimmed != 0 {
		t.Fatalf("no budget:\n%q", full)
	}
	if got, _, _ := renderLinked(items, estimateTokens(full)); got != full {
		t.Error("a block that fits must not change")
	}
	// The last object goes first (the note line counts too).
	head := "\nLinked evidence (followed from the evidence above):\n"
	two := head + "- Secret a: does not exist\n" +
		"- Service b: selector app=b, ports 80/TCP, endpoints 0 ready\n" +
		"  - Pod b-0: Pending, not ready\n" +
		"  - Pod b-1: Pending, not ready — FailedScheduling: 0/3 nodes are available: 3 node(s) had untolerated taint {dedicated: db}.\n" +
		"(1 linked evidence line trimmed to fit the model's context)\n"
	if got, kept, trimmed := renderLinked(items, estimateTokens(two)); got != two || len(kept) != 2 || trimmed != 1 {
		t.Errorf("one object trimmed (%d):\n%s", trimmed, got)
	}
	// Then whole objects, then the last one's lines.
	one := head + "- Secret a: does not exist\n(4 linked evidence lines trimmed to fit the model's context)\n"
	if got, kept, trimmed := renderLinked(items, estimateTokens(one)); got != one || len(kept) != 1 || trimmed != 4 {
		t.Errorf("down to one (%d):\n%s", trimmed, got)
	}
	b1 := head + "- Service b: selector app=b, ports 80/TCP, endpoints 0 ready\n" +
		"  - Pod b-0: Pending, not ready\n" +
		"(1 linked evidence line trimmed to fit the model's context)\n"
	got, kept, trimmed := renderLinked([]linkedItem{items[1]}, estimateTokens(b1))
	if got != b1 || trimmed != 1 || len(kept[0].Lines) != 1 || len(items[1].Lines) != 2 {
		t.Errorf("lines of the last object (%d; input %+v):\n%s", trimmed, items[1], got)
	}
	if got, kept, trimmed := renderLinked(items, 5); got != "" || kept != nil || trimmed != 5 {
		t.Errorf("nothing fits: %q %v %d", got, kept, trimmed)
	}
	if got := linkedBudget(8192); got != 8192*15/100 {
		t.Errorf("budget %d", got)
	}
	if linkedBudget(0) != 0 {
		t.Error("unknown context: no limit")
	}
}

// Off, the prompts are what they were without link-following; on, the block
// sits between the logs and the instruction.
func TestLinkFollowSwitch(t *testing.T) {
	c := scenarioServicePending(t)
	on := buildRCAInitialContext(c, "shop", "Deployment", "worker", 0, 0).text
	setLinkFollow(false)
	off := buildRCAInitialContext(c, "shop", "Deployment", "worker", 0, 0).text
	offAn := analysisPrompt(c, "shop", "Deployment", "worker")
	setLinkFollow(true)
	if strings.Contains(off, "Linked evidence") || strings.Contains(offAn, "Linked evidence") {
		t.Error("off: the block is there")
	}
	block := "\nLinked evidence (followed from the evidence above):\n- Service redis: "
	i := strings.Index(on, block)
	if i < 0 || !strings.Contains(on[:i], "FATAL: cache unavailable\n") ||
		!strings.HasSuffix(on, "\nUse the available tools to gather additional evidence as needed, then provide the Root Cause Analysis.") {
		t.Fatalf("on:\n%s", on)
	}
	if strings.Replace(on, on[i:strings.LastIndex(on, "\nUse the available tools")], "", 1) != off {
		t.Error("on minus the block is not the prompt without it")
	}
	an := analysisPrompt(c, "shop", "Deployment", "worker")
	if j := strings.Index(an, block); j < 0 || !strings.HasSuffix(an, "\nIn a few short lines: is anything wrong with this resource? If so, say what, why, and what to do. If nothing is wrong, say so.") {
		t.Errorf("analysis:\n%s", an)
	}

	// The settings API turns it on and off.
	t.Setenv("AI_LINK_FOLLOW", "0")
	initLinkFollow()
	if getLinkFollow() {
		t.Error("AI_LINK_FOLLOW=0 left it on")
	}
	t.Setenv("AI_LINK_FOLLOW", "")
	initLinkFollow()
	if !getLinkFollow() {
		t.Error("default is on")
	}
}

// The linked block has its own budget: the context is 15% for it, and the
// rest of the prompt is trimmed as before.
func TestLinkedBudgetInPrompt(t *testing.T) {
	c := scenarioPodClaim(t)
	full := links(c, "data", "Pod", "loader")
	text, _, _ := renderLinked(full, 0)
	ctxLen := (estimateTokens(text) - 1) * 100 / linkedShare
	small := analysisEvidence(c, "data", "Pod", "loader", ctxLen)
	if small.linkedTrimmed == 0 || len(small.linkedItems) >= len(full) || estimateTokens(small.linked) > linkedBudget(ctxLen) {
		t.Errorf("trimmed %d, %d of %d items, %d tokens:\n%s", small.linkedTrimmed, len(small.linkedItems), len(full), estimateTokens(small.linked), small.linked)
	}
	if !strings.Contains(small.fit(0), "trimmed to fit the model's context)\n\nIn a few short lines") {
		t.Error("the prompt doesn't say what was trimmed")
	}
}

func mustJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

// ── Prompt dump ───────────────────────────────────────────────────────────────

// TestLinkedDump writes each fixture's evidence as plain JSON (anchor, events,
// describe, member warnings, logs, linked …) with the RCA and analysis prompts
// KubeHealth builds from it, so external tools that reproduce KubeHealth's
// prompts can be checked against it byte for byte:
//
//	KH_LINKED_DUMP=/tmp/linked.jsonl go test -run TestLinkedDump
func TestLinkedDump(t *testing.T) {
	path := os.Getenv("KH_LINKED_DUMP")
	if path == "" {
		t.Skip("KH_LINKED_DUMP not set")
	}
	out, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	enc := json.NewEncoder(out)
	enc.SetEscapeHTML(false)
	type anchor struct{ ns, kind, name string }
	scenarios := []struct {
		id     string
		build  func(*testing.T) *clusterState
		a      anchor
		ctxLen int
	}{
		{"service-pending", scenarioServicePending, anchor{"shop", "Deployment", "worker"}, 0},
		{"service-pending-trimmed", scenarioServicePending, anchor{"shop", "Deployment", "worker"}, 500},
		{"pod-claim", scenarioPodClaim, anchor{"data", "Pod", "loader"}, 0},
		{"pod-claim-trimmed", scenarioPodClaim, anchor{"data", "Pod", "loader"}, 600},
		{"anchor-claim", scenarioPodClaim, anchor{"data", "PersistentVolumeClaim", "data-loader"}, 0},
		{"config-refs", scenarioConfigRefs, anchor{"billing", "Deployment", "api"}, 0},
		{"pull-secret", scenarioPullSecret, anchor{"ci", "Pod", "scanner"}, 0},
		{"healthy", scenarioHealthy, anchor{"web", "Deployment", "site"}, 0},
	}
	evJSON := func(evs []EventSummary) []map[string]any {
		out := []map[string]any{}
		for _, ev := range evs {
			out = append(out, map[string]any{"type": ev.Type, "reason": ev.Reason, "message": ev.Message, "count": ev.Count, "age": ev.Age})
		}
		return out
	}
	for _, sc := range scenarios {
		c := sc.build(t)
		a := sc.a
		rca := buildRCAInitialContext(c, a.ns, a.kind, a.name, 0, sc.ctxLen)
		ae := analysisEvidence(c, a.ns, a.kind, a.name, sc.ctxLen)
		info := c.fetchObjectInfo(a.ns, a.kind, a.name)
		d := map[string]any{"age": info.Age, "labels": info.Labels, "annotations": info.Annotations}
		var secs []map[string]any
		for _, s := range info.Sections {
			rows := [][]string{}
			for _, r := range s.Rows {
				rows = append(rows, []string{r.Key, r.Value})
			}
			secs = append(secs, map[string]any{"title": s.Title, "rows": rows})
		}
		d["sections"] = secs
		var conds []map[string]string
		for _, cd := range info.Conditions {
			conds = append(conds, map[string]string{"type": cd.Type, "status": cd.Status, "reason": cd.Reason, "message": cd.Message})
		}
		d["conditions"] = conds
		var members []map[string]any
		for _, ev := range ae.memberEvents {
			members = append(members, map[string]any{"kind": ev.Kind, "name": ev.ObjectName, "reason": ev.Reason,
				"message": ev.Message, "count": ev.Count, "age": ev.Age})
		}
		var linked []map[string]any
		for _, it := range ae.linkedItems {
			linked = append(linked, map[string]any{"kind": it.Kind, "name": it.Name, "facts": it.Facts, "lines": append([]string{}, it.Lines...)})
		}
		s := map[string]any{
			"id":              sc.id,
			"anchor":          map[string]string{"ns": a.ns, "kind": a.kind, "name": a.name},
			"anchor_events":   evJSON(ae.events),
			"describe":        d,
			"member_warnings": members,
			"logs":            ae.logs,
			"linked":          linked,
			"linked_trimmed":  ae.linkedTrimmed,
		}
		if !isPodKind(a.kind) {
			s["logs_pod"] = rca.logsPod
		}
		if err := enc.Encode(map[string]any{"scenario": s, "rca_initial": rca.text, "analysis_prompt": ae.fit(0)}); err != nil {
			t.Fatal(err)
		}
	}
}
