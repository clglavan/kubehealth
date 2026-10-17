package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// testCluster is a cluster with empty caches and no API clients; the tests
// only exercise code that reads caches and naming conventions.
func testCluster() *clusterState { return &clusterState{name: "test"} }

// setOwner records an API answer as if the resolver had fetched it.
func (c *clusterState) setOwner(ns, kind, name string, e ownerEntry) {
	c.owners.lockInit()
	c.owners.known[ownerKey(ns, kind, name)] = e
	c.owners.Unlock()
}

// warn builds a Warning event about a namespaced object.
func warn(ns, kind, name, reason string, count int32, first, last time.Time) EventSummary {
	return EventSummary{
		Namespace: ns, Name: name + "." + reason, Reason: reason, Type: "Warning", Count: count,
		Kind: kind, ObjectName: name, ObjectNamespace: ns, FirstTime: first, LastTime: last,
	}
}

func normal(ns, kind, name, reason string, at time.Time) EventSummary {
	e := warn(ns, kind, name, reason, 1, at, at)
	e.Type = "Normal"
	return e
}

func TestEnvOf(t *testing.T) {
	cases := map[string]string{
		"gke_acme-retail-nprd_europe-west1_gke-retail-nprd": "nonprod",
		"my-non-prod-cluster":                               "nonprod",
		"we-prod-cp-admin":                                  "prod",
		"WE-PROD-CP-admin":                                  "prod",
		"gke_acme-retail-prod1_europe-west1_x":              "prod",
		"kind-demo":                                         "local",
		"docker-desktop":                                    "local",
		"dev-cluster-admin":                                 "dev",
		"shop-gitops-qa1":                                   "stage",
		"gke_prj-acme-p-ai_europe-west1_gke-x":              "unknown",
		"delivery-cluster":                                  "unknown", // contains "live" but not as a word
	}
	for name, want := range cases {
		if got := envOf(name); got != want {
			t.Errorf("envOf(%q) = %q, want %q", name, got, want)
		}
	}
}

func TestInferParent(t *testing.T) {
	cases := []struct {
		kind, name string
		want       objRef
		ok         bool
	}{
		{"Pod", "web-7d4f9c8b6-xk2pq", objRef{"ReplicaSet", "web-7d4f9c8b6"}, true},
		{"ReplicaSet", "web-7d4f9c8b6", objRef{"Deployment", "web"}, true},
		{"Pod", "import-29345678-bx2pq", objRef{"Job", "import-29345678"}, true},
		{"Job", "import-29345678", objRef{"CronJob", "import"}, true},
		{"Pod", "postgres-0", objRef{"StatefulSet", "postgres"}, true},
		{"Pod", "fluent-bit-6zkt5", objRef{"", "fluent-bit"}, true},
		{"Pod", "catalog-import", objRef{}, false},        // "import" is a word, not a generated suffix
		{"ReplicaSet", "catalog-import", objRef{}, false}, // vowels never appear in a pod-template-hash
		{"Service", "web-7d4f9c8b6", objRef{}, false},
	}
	for _, c := range cases {
		got, ok := inferParent(c.kind, c.name)
		if ok != c.ok || got != c.want {
			t.Errorf("inferParent(%s, %s) = %v, %v; want %v, %v", c.kind, c.name, got, ok, c.want, c.ok)
		}
	}
}

func TestCronJobCascade(t *testing.T) {
	c := testCluster()
	now := time.Now()
	start := now.Add(-30 * time.Minute)
	evs := map[string][]EventSummary{"shop": {
		warn("shop", "Job", "import-29345678", "BackoffLimitExceeded", 1, start, start.Add(time.Minute)),
		warn("shop", "Job", "import-29345679", "BackoffLimitExceeded", 1, start.Add(time.Minute), start.Add(2*time.Minute)),
		warn("shop", "Pod", "import-29345678-bx2pq", "BackOff", 12, start, now.Add(-10*time.Second)),
		warn("shop", "Pod", "import-29345679-cd4fg", "BackOff", 9, start.Add(time.Minute), now),
		normal("shop", "Pod", "import-29345679-cd4fg", "Started", start.Add(time.Minute)),
	}}
	ps := c.buildProblems(evs, now)
	if len(ps) != 1 {
		t.Fatalf("got %d problems, want 1: %+v", len(ps), ps)
	}
	p := ps[0]
	if p.Kind != "CronJob" || p.Title != "import" {
		t.Errorf("root = %s/%s, want CronJob/import", p.Kind, p.Title)
	}
	if !p.Cascade || !p.Inferred || p.Tier != "new" || p.Idle {
		t.Errorf("cascade=%v inferred=%v tier=%s idle=%v; want true true new false", p.Cascade, p.Inferred, p.Tier, p.Idle)
	}
	if p.Members != "2 Jobs · 2 Pods" {
		t.Errorf("members = %q", p.Members)
	}
	if p.Occurrences != 23 || p.Reasons[0].Reason != "BackOff" {
		t.Errorf("occurrences=%d reasons=%v", p.Occurrences, p.Reasons)
	}
	if p.Focus != "CronJob/import" || p.OpenNS != "shop" {
		t.Errorf("focus=%s open=%s", p.Focus, p.OpenNS)
	}
}

func TestCronJobFailingSinceLastSuccess(t *testing.T) {
	c := testCluster()
	now := time.Now()
	lastOK := now.Add(-9 * 24 * time.Hour)
	c.setOwner("shop", "CronJob", "import", ownerEntry{at: now, lastSuccess: lastOK, created: lastOK.Add(-time.Hour)})
	evs := map[string][]EventSummary{"shop": {
		warn("shop", "Job", "import-29345678", "BackoffLimitExceeded", 1, now.Add(-50*time.Minute), now),
	}}
	p := c.buildProblems(evs, now)[0]
	if p.Tier != "chronic" || !strings.HasPrefix(p.SinceNote, "Last successful run") {
		t.Errorf("tier=%s note=%q; want chronic since the last successful run", p.Tier, p.SinceNote)
	}
}

// The top of a chain counts as unconfirmed until it has been fetched itself,
// otherwise a CronJob's last success (or an operator CR above a StatefulSet)
// would never be looked up once the lower links are confirmed.
func TestChainTopMustBeFetched(t *testing.T) {
	c := testCluster()
	now := time.Now()
	c.setOwner("shop", "Pod", "import-29345678-bx2pq", ownerEntry{at: now, parent: objRef{"Job", "import-29345678"}})
	c.setOwner("shop", "Job", "import-29345678", ownerEntry{at: now, parent: objRef{"CronJob", "import"}})

	chain, inferred := c.ownerChain("shop", objRef{"Pod", "import-29345678-bx2pq"})
	if len(chain) != 3 || !inferred {
		t.Fatalf("chain=%v inferred=%v; want 3 links, inferred until the CronJob is fetched", chain, inferred)
	}
	if !c.requestOwnerConfirmation("shop", chain) || len(c.owners.queue) != 1 || c.owners.queue[0].kind != "CronJob" {
		t.Fatalf("queue = %+v; want the CronJob queued", c.owners.queue)
	}

	c.setOwner("shop", "CronJob", "import", ownerEntry{at: now})
	if _, inferred := c.ownerChain("shop", objRef{"Pod", "import-29345678-bx2pq"}); inferred {
		t.Error("chain still inferred after every link was fetched")
	}
}

func TestPartialDeploymentFailure(t *testing.T) {
	c := testCluster()
	now := time.Now()
	evs := map[string][]EventSummary{"web": {
		warn("web", "Pod", "web-7d4f9c8b6-xk2pq", "BackOff", 40, now.Add(-3*time.Hour), now.Add(-20*time.Minute)),
		normal("web", "Pod", "web-7d4f9c8b6-bbbbb", "Started", now.Add(-3*time.Hour)),
		normal("web", "Pod", "web-7d4f9c8b6-ccccc", "Started", now.Add(-3*time.Hour)),
	}}
	p := c.buildProblems(evs, now)[0]
	if p.Kind != "Deployment" || p.Members != "1 of 3 Pods failing" || p.Cascade {
		t.Errorf("kind=%s members=%q cascade=%v", p.Kind, p.Members, p.Cascade)
	}
	if p.Tier != "ongoing" || !p.Idle {
		t.Errorf("tier=%s idle=%v; want ongoing and idle", p.Tier, p.Idle)
	}
}

func TestPatterns(t *testing.T) {
	c := testCluster()
	now := time.Now()
	t0 := now.Add(-5 * time.Hour)
	evs := map[string][]EventSummary{
		// Started within 3 s of each other: one pattern.
		"es-a": {warn("es-a", "Elasticsearch", "es-a", "Unexpected", 3, t0, now)},
		"es-b": {warn("es-b", "Elasticsearch", "es-b", "Unexpected", 4, t0.Add(3*time.Second), now)},
		// Infrastructure reason in three namespaces at different times: one pattern.
		"m1": {warn("m1", "Service", "a", "FailedMount", 1, t0.Add(time.Hour), now)},
		"m2": {warn("m2", "Service", "b", "FailedMount", 1, t0.Add(2*time.Hour), now)},
		"m3": {warn("m3", "Service", "c", "FailedMount", 1, t0.Add(3*time.Hour), now)},
		// Same reason as above but a different tier: stays on its own.
		"m4": {warn("m4", "Service", "d", "FailedMount", 1, now.Add(-10*time.Minute), now)},
		// The same workload failing the same way, an hour apart: one pattern named after it.
		"w1": {warn("w1", "Deployment", "log-shipper", "BackOff", 2, t0.Add(30*time.Minute), now)},
		"w2": {warn("w2", "Deployment", "log-shipper", "BackOff", 5, t0.Add(90*time.Minute), now)},
	}
	ps := c.buildProblems(evs, now)
	var patterns, singles int
	for _, p := range ps {
		switch {
		case p.Scope == "pattern" && strings.HasPrefix(p.Title, "Unexpected in 2"):
			patterns++
			if !strings.Contains(p.Pattern, "within 3s") {
				t.Errorf("time pattern explanation = %q", p.Pattern)
			}
		case p.Scope == "pattern" && strings.HasPrefix(p.Title, "FailedMount in 3"):
			patterns++
		case p.Scope == "pattern" && p.Title == "log-shipper in 2 namespaces":
			patterns++
			if !strings.Contains(p.Pattern, "The same workload, log-shipper") {
				t.Errorf("name pattern explanation = %q", p.Pattern)
			}
		case p.Scope == "namespace" && p.Namespaces[0] == "m4":
			singles++
			if p.Tier != "new" {
				t.Errorf("m4 tier = %s", p.Tier)
			}
		default:
			t.Errorf("unexpected problem %s %q", p.Scope, p.Title)
		}
	}
	if patterns != 3 || singles != 1 {
		t.Errorf("patterns=%d singles=%d; want 3 and 1", patterns, singles)
	}
	if ps[0].Tier != "new" {
		t.Errorf("first row tier = %s; new problems must sort first", ps[0].Tier)
	}
}

func TestClusterScopedEvents(t *testing.T) {
	c := testCluster()
	now := time.Now()
	node := func(name string) EventSummary {
		e := warn("default", "Node", name, "OOMKilling", 2, now.Add(-time.Hour), now)
		e.ObjectNamespace = ""
		return e
	}
	ps := c.buildProblems(map[string][]EventSummary{"default": {node("n1"), node("n2")}}, now)
	if len(ps) != 1 || ps[0].Scope != "cluster" || ps[0].Title != "OOMKilling on 2 Nodes" || ps[0].OpenNS != clusterScopeNS {
		t.Fatalf("got %+v", ps)
	}
}

func TestFormatAge(t *testing.T) {
	cases := map[time.Duration]string{
		12 * time.Second:              "12s",
		4 * time.Minute:               "4m",
		65 * time.Minute:              "1h 05m",
		58 * time.Hour:                "2d 10h",
		75*24*time.Hour + 3*time.Hour: "75d",
		-time.Second:                  "0s",
	}
	for d, want := range cases {
		if got := formatAge(d); got != want {
			t.Errorf("formatAge(%v) = %q, want %q", d, got, want)
		}
	}
}

func TestMergeEventsPrefersNewer(t *testing.T) {
	now := time.Now()
	old := warn("ns", "Pod", "p", "BackOff", 3, now.Add(-time.Hour), now.Add(-time.Minute))
	newer := old
	newer.Count, newer.LastTime = 5, now
	other := warn("ns", "Pod", "q", "BackOff", 1, now, now)
	got := mergeEvents([]EventSummary{old}, []EventSummary{newer, other})
	if len(got) != 2 || got[0].Count+got[1].Count != 6 {
		t.Errorf("merge = %+v", got)
	}
}

func TestCronJobRootLookedUpFirst(t *testing.T) {
	c := testCluster()
	now := time.Now()
	c.owners.lockInit()
	c.owners.queue = []ownerJob{{"other", "Pod", "x"}}
	c.owners.queued[ownerKey("other", "Pod", "x")] = true
	c.owners.Unlock()
	evs := map[string][]EventSummary{"shop": {
		warn("shop", "Pod", "import-29345678-bx2pq", "BackOff", 3, now.Add(-time.Minute), now),
	}}
	c.buildProblems(evs, now)
	if len(c.owners.queue) < 2 || c.owners.queue[0].kind != "CronJob" {
		t.Errorf("queue = %+v; want the CronJob first", c.owners.queue)
	}
}

func TestEnvOverride(t *testing.T) {
	prefs.Lock()
	saved := prefs.ContextEnv
	prefs.ContextEnv = map[string]string{"gke_prj-acme-p-ai_x": "prod"}
	prefs.Unlock()
	defer func() { prefs.Lock(); prefs.ContextEnv = saved; prefs.Unlock() }()

	if env, src := envFor("gke_prj-acme-p-ai_x"); env != "prod" || src != "settings" {
		t.Errorf("override: got %s/%s", env, src)
	}
	if env, src := envFor("dev-cluster"); env != "dev" || src != "name" {
		t.Errorf("from name: got %s/%s", env, src)
	}
	// Rejected before anything is written to disk.
	if err := setContextEnv("x", "production-ish"); err == nil {
		t.Error("invalid environment accepted")
	}
}

func TestAPIClusterPinning(t *testing.T) {
	a, b := &clusterState{name: "a"}, &clusterState{name: "b"}
	clusters.Lock()
	savedActive, savedByName := clusters.active, clusters.byName
	clusters.active, clusters.byName = a, map[string]*clusterState{"a": a, "b": b}
	clusters.Unlock()
	defer func() {
		clusters.Lock()
		clusters.active, clusters.byName = savedActive, savedByName
		clusters.Unlock()
	}()

	req := func(header string) *http.Request {
		r := httptest.NewRequest("GET", "/api/home", nil)
		if header != "" {
			r.Header.Set(contextHeader, header)
		}
		return r
	}
	if got := apiCluster(httptest.NewRecorder(), req("")); got != a {
		t.Errorf("no header: got %v, want the active cluster", got)
	}
	if got := apiCluster(httptest.NewRecorder(), req("b")); got != b {
		t.Errorf("pinned to a loaded, inactive cluster: got %v", got)
	}
	w := httptest.NewRecorder()
	if got := apiCluster(w, req("gone")); got != nil || w.Code != http.StatusConflict {
		t.Errorf("unloaded cluster: got %v, status %d; want nil and 409", got, w.Code)
	}
	// Pages fall back to the active cluster: they always say which one they show.
	if got := pageCluster(httptest.NewRequest("GET", "/?ctx=gone", nil)); got != a {
		t.Errorf("page with unknown ctx: got %v", got)
	}
}

func TestPrefsRoundTrip(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", home+"/.config")
	prefs.Lock()
	saved := prefs.ContextEnv
	prefs.ContextEnv = map[string]string{}
	prefs.Unlock()
	defer func() { prefs.Lock(); prefs.ContextEnv = saved; prefs.Unlock() }()

	if !strings.HasPrefix(prefsPath(), home) {
		t.Fatalf("prefsPath %q is outside the test HOME", prefsPath())
	}
	if err := setContextEnv("ctx-a", "prod"); err != nil {
		t.Fatal(err)
	}
	prefs.Lock()
	prefs.ContextEnv = map[string]string{}
	prefs.Unlock()
	loadPrefs()
	if env, src := envFor("ctx-a"); env != "prod" || src != "settings" {
		t.Errorf("after reload: %s/%s", env, src)
	}
	if err := setContextEnv("ctx-a", ""); err != nil {
		t.Fatal(err)
	}
	if env, src := envFor("ctx-a"); env != "unknown" || src != "name" {
		t.Errorf("after clearing: %s/%s", env, src)
	}
}

func TestScopeEvents(t *testing.T) {
	c := testCluster()
	now := time.Now()
	node := warn("default", "Node", "n1", "OOMKilling", 2, now, now)
	node.ObjectNamespace = ""
	pod := warn("default", "Pod", "web-1", "BackOff", 1, now, now)
	c.nsCache.Store("default", &nsEntry{events: []EventSummary{node, pod}})

	if evs := c.scopeEvents("default"); len(evs) != 1 || evs[0].Kind != "Pod" {
		t.Errorf("default page: %+v; want the Pod only", evs)
	}
	if evs := c.scopeEvents(clusterScopeNS); len(evs) != 1 || evs[0].Kind != "Node" {
		t.Errorf("cluster-wide page: %+v; want the Node only", evs)
	}
	if apiNamespace(clusterScopeNS) != "" || apiNamespace("default") != "default" {
		t.Error("apiNamespace mapping")
	}
}
