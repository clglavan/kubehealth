package main

import (
	"context"
	"embed"
	"encoding/json"
	"flag"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/util/homedir"
)

//go:embed templates/*.html
var templateFS embed.FS

// staticFS holds the UI's fonts, so the page looks the same offline.
//
//go:embed static
var staticFS embed.FS

// handleStatic serves /static/…, cached for a day: the files only change
// with the binary.
func handleStatic(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "public, max-age=86400")
	http.FileServer(http.FS(staticFS)).ServeHTTP(w, r)
}

type Config struct {
	Namespaces []string
	Port       string
	Kubeconfig string
	AllNS      bool
}

type EventSummary struct {
	Namespace  string
	Name       string
	Reason     string
	Message    string
	Type       string
	Count      int32
	Kind       string
	ObjectName string
	// ObjectNamespace is the involved object's namespace; empty for
	// cluster-scoped objects such as Nodes, whose events land in "default".
	ObjectNamespace string
	FirstTime       time.Time
	LastTime        time.Time
	Age             string
}

// ObjectEvents groups all events that reference the same k8s object.
type ObjectEvents struct {
	Kind         string
	ObjectName   string
	Events       []EventSummary // newest-first
	WarningCount int
	NormalCount  int
	HasWarning   bool
	LastTime     time.Time
	LastWarning  time.Time // newest warning, zero when there is none
	Idle         bool      // has warnings, but none for idleAfter
}

// KindGroup collects all ObjectEvents of the same Kubernetes Kind.
type KindGroup struct {
	Kind         string
	Objects      []ObjectEvents
	WarningCount int
	NormalCount  int
	HasWarning   bool
}

// FamilyMember is a node in an ownership tree.
// ObjectEvents is embedded so Kind, ObjectName, Events etc. are directly accessible.
type FamilyMember struct {
	ObjectEvents
	Depth    int
	Children []*FamilyMember
}

// ObjectInfo is the structured describe-style view of a single k8s object,
// returned by /api/object/ as JSON and rendered inline in the namespace page.
type ObjectInfo struct {
	Kind        string            `json:"kind"`
	Name        string            `json:"name"`
	Namespace   string            `json:"namespace"`
	NotFound    bool              `json:"notFound"`
	Error       string            `json:"error,omitempty"`
	Age         string            `json:"age,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
	Sections    []InfoSection     `json:"sections,omitempty"`
	Conditions  []InfoCondition   `json:"conditions,omitempty"`
}

type InfoSection struct {
	Title string    `json:"title"`
	Rows  []InfoRow `json:"rows"`
}

type InfoRow struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type InfoCondition struct {
	Type    string `json:"type"`
	Status  string `json:"status"`
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
}

// ObjectFamily is the root of a detected Kubernetes ownership chain
// (e.g. Deployment → ReplicaSet → Pod).
type ObjectFamily struct {
	Root           *FamilyMember
	HasWarning     bool
	Idle           bool // has warnings, but no member had a new one for idleAfter
	CascadeFailure bool // root AND all leaves have warnings
	WarningCount   int
	NormalCount    int
	Kinds          []string // distinct kinds present — used for filter data attribute
}

type NamespaceEvents struct {
	Namespace    string
	Events       []EventSummary // kept for /api/events JSON compat
	Objects      []ObjectEvents // flat grouped list
	KindGroups   []KindGroup    // objects bucketed by kind
	Families     []ObjectFamily // ownership-chain trees (namespace page)
	OrphanKinds  []KindGroup    // objects not part of any chain (namespace page)
	WarningCount int
	NormalCount  int
}

// ---- Ownership detection helpers ----

func isAlphanumLower(s string) bool {
	if len(s) == 0 {
		return false
	}
	for _, c := range s {
		if !((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')) {
			return false
		}
	}
	return true
}

func isAllDigits(s string) bool {
	if len(s) == 0 {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

func stripLast(name string) (prefix, suffix string, ok bool) {
	i := strings.LastIndex(name, "-")
	if i < 0 {
		return "", "", false
	}
	return name[:i], name[i+1:], true
}

// detectParent returns the probable Kubernetes parent Kind and Name for an object,
// using only naming conventions — no API calls required.
// Priority: ReplicaSet > StatefulSet > Job > DaemonSet.
func detectParent(kind, name string, has func(kind, name string) bool) (parentKind, parentName string) {
	prefix, suffix, ok := stripLast(name)
	if !ok {
		return "", ""
	}
	switch kind {
	case "Pod":
		// StatefulSet pods: ordinal suffix (digits only), e.g. postgres-0
		if isAllDigits(suffix) {
			if has("StatefulSet", prefix) {
				return "StatefulSet", prefix
			}
		}
		// Deployment/Job/DaemonSet pods: 5-char alphanum suffix, e.g. my-app-7f9d8-xk2pq
		if len(suffix) == 5 && isAlphanumLower(suffix) {
			if has("ReplicaSet", prefix) {
				return "ReplicaSet", prefix
			}
			if has("Job", prefix) {
				return "Job", prefix
			}
			if has("DaemonSet", prefix) {
				return "DaemonSet", prefix
			}
		}
	case "ReplicaSet":
		// RS names: deployment-name + "-" + 6-12 char hash, e.g. my-app-7d4f9c8b6
		if len(suffix) >= 6 && len(suffix) <= 12 && isAlphanumLower(suffix) {
			if has("Deployment", prefix) {
				return "Deployment", prefix
			}
		}
	case "Job":
		// CronJob-generated jobs: cronjob-name + "-" + 8-10 digit unix-minutes timestamp
		if len(suffix) >= 8 && len(suffix) <= 10 && isAllDigits(suffix) {
			if has("CronJob", prefix) {
				return "CronJob", prefix
			}
		}
	}
	return "", ""
}

// buildFamilies detects Kubernetes ownership chains among ObjectEvents and
// returns a slice of ObjectFamily trees plus any unclaimed objects as orphans.
func buildFamilies(objects []ObjectEvents) ([]ObjectFamily, []KindGroup) {
	type objKey struct{ Kind, Name string }

	// Build lookup: kind+name → *ObjectEvents
	byKey := make(map[objKey]*ObjectEvents, len(objects))
	for i := range objects {
		byKey[objKey{objects[i].Kind, objects[i].ObjectName}] = &objects[i]
	}
	has := func(kind, name string) bool {
		_, ok := byKey[objKey{kind, name}]
		return ok
	}

	// Resolve parent for every object
	parentOf := make(map[objKey]objKey, len(objects))
	for _, obj := range objects {
		pk, pn := detectParent(obj.Kind, obj.ObjectName, has)
		if pk != "" {
			parentOf[objKey{obj.Kind, obj.ObjectName}] = objKey{pk, pn}
		}
	}

	// Build children index
	childrenOf := make(map[objKey][]objKey)
	for child, parent := range parentOf {
		childrenOf[parent] = append(childrenOf[parent], child)
	}

	// Member pool — one FamilyMember per object
	memberPool := make(map[objKey]*FamilyMember, len(objects))
	getMember := func(key objKey) *FamilyMember {
		if m, ok := memberPool[key]; ok {
			return m
		}
		obj := byKey[key]
		if obj == nil {
			return nil
		}
		m := &FamilyMember{ObjectEvents: *obj}
		memberPool[key] = m
		return m
	}

	// Recursively assign depths and wire Children slices
	var wire func(key objKey, depth int)
	wire = func(key objKey, depth int) {
		m := getMember(key)
		if m == nil {
			return
		}
		m.Depth = depth
		kids := childrenOf[key]
		sort.Slice(kids, func(i, j int) bool {
			if kids[i].Kind != kids[j].Kind {
				return kids[i].Kind < kids[j].Kind
			}
			return kids[i].Name < kids[j].Name
		})
		for _, ck := range kids {
			cm := getMember(ck)
			if cm == nil {
				continue
			}
			m.Children = append(m.Children, cm)
			wire(ck, depth+1)
		}
	}

	// Identify roots: objects with children but no parent (= top of a chain)
	var roots []objKey
	for _, obj := range objects {
		key := objKey{obj.Kind, obj.ObjectName}
		_, hasParent := parentOf[key]
		_, hasChildren := childrenOf[key]
		if !hasParent && hasChildren {
			roots = append(roots, key)
		}
	}
	sort.Slice(roots, func(i, j int) bool {
		if roots[i].Kind != roots[j].Kind {
			return roots[i].Kind < roots[j].Kind
		}
		return roots[i].Name < roots[j].Name
	})

	// Wire trees and build ObjectFamily values
	claimed := make(map[objKey]bool)
	var markClaimed func(key objKey)
	markClaimed = func(key objKey) {
		claimed[key] = true
		for _, ck := range childrenOf[key] {
			markClaimed(ck)
		}
	}

	var families []ObjectFamily
	for _, rootKey := range roots {
		wire(rootKey, 0)
		rootMember := getMember(rootKey)
		if rootMember == nil {
			continue
		}
		fam := ObjectFamily{Root: rootMember}

		// Collect stats and distinct kinds
		kindSet := map[string]bool{}
		activeWarning := false
		var walk func(m *FamilyMember)
		walk = func(m *FamilyMember) {
			fam.WarningCount += m.WarningCount
			fam.NormalCount += m.NormalCount
			if m.HasWarning {
				fam.HasWarning = true
				activeWarning = activeWarning || !m.Idle
			}
			kindSet[m.Kind] = true
			for _, c := range m.Children {
				walk(c)
			}
		}
		walk(rootMember)
		fam.Idle = fam.HasWarning && !activeWarning

		for k := range kindSet {
			fam.Kinds = append(fam.Kinds, k)
		}
		sort.Strings(fam.Kinds)

		// CascadeFailure: root has warning AND every leaf has warning
		if rootMember.HasWarning {
			allLeavesWarn := true
			var checkLeaves func(m *FamilyMember)
			checkLeaves = func(m *FamilyMember) {
				if len(m.Children) == 0 {
					if !m.HasWarning {
						allLeavesWarn = false
					}
					return
				}
				for _, c := range m.Children {
					checkLeaves(c)
				}
			}
			checkLeaves(rootMember)
			fam.CascadeFailure = allLeavesWarn
		}

		families = append(families, fam)
		markClaimed(rootKey)
	}

	// Sort: cascade failures first, then warnings, then alphabetically
	sort.Slice(families, func(i, j int) bool {
		if families[i].CascadeFailure != families[j].CascadeFailure {
			return families[i].CascadeFailure
		}
		if families[i].HasWarning != families[j].HasWarning {
			return families[i].HasWarning
		}
		return families[i].Root.ObjectName < families[j].Root.ObjectName
	})

	// Collect unclaimed objects as orphans
	var orphans []ObjectEvents
	for _, obj := range objects {
		if !claimed[objKey{obj.Kind, obj.ObjectName}] {
			orphans = append(orphans, obj)
		}
	}

	return families, groupByKind(orphans)
}

// groupByKind arranges ObjectEvents into per-Kind columns.
// Columns are sorted: kinds with warnings first, then alphabetically.
func groupByKind(objects []ObjectEvents) []KindGroup {
	index := map[string]*KindGroup{}
	var order []string

	for _, obj := range objects {
		g, exists := index[obj.Kind]
		if !exists {
			g = &KindGroup{Kind: obj.Kind}
			index[obj.Kind] = g
			order = append(order, obj.Kind)
		}
		g.Objects = append(g.Objects, obj)
		g.WarningCount += obj.WarningCount
		g.NormalCount += obj.NormalCount
		if obj.HasWarning {
			g.HasWarning = true
		}
	}

	out := make([]KindGroup, 0, len(order))
	for _, k := range order {
		out = append(out, *index[k])
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].HasWarning != out[j].HasWarning {
			return out[i].HasWarning
		}
		return out[i].Kind < out[j].Kind
	})
	return out
}

// groupByObject buckets a flat event list into per-object groups,
// sorted warnings-first then by most recent activity.
func groupByObject(events []EventSummary) []ObjectEvents {
	type key struct{ kind, name string }
	index := map[key]*ObjectEvents{}
	var order []key

	for _, e := range events {
		k := key{e.Kind, e.ObjectName}
		obj, exists := index[k]
		if !exists {
			obj = &ObjectEvents{Kind: e.Kind, ObjectName: e.ObjectName}
			index[k] = obj
			order = append(order, k)
		}
		obj.Events = append(obj.Events, e)
		if e.Type == "Warning" {
			obj.WarningCount++
			obj.HasWarning = true
			if e.LastTime.After(obj.LastWarning) {
				obj.LastWarning = e.LastTime
			}
		} else {
			obj.NormalCount++
		}
		if e.LastTime.After(obj.LastTime) {
			obj.LastTime = e.LastTime
		}
	}

	out := make([]ObjectEvents, 0, len(order))
	for _, k := range order {
		o := *index[k]
		o.Idle = o.HasWarning && clock().Sub(o.LastWarning) >= idleAfter
		out = append(out, o)
	}

	// Warnings-first, then most recent activity
	sort.Slice(out, func(i, j int) bool {
		if out[i].HasWarning != out[j].HasWarning {
			return out[i].HasWarning
		}
		return out[i].LastTime.After(out[j].LastTime)
	})
	return out
}

type DashboardData struct {
	Namespaces  []NamespaceEvents
	LastUpdated string
	TotalWarn   int
	TotalNormal int
}

// nsEntry holds the per-namespace event cache.
type nsEntry struct {
	sync.RWMutex
	events    []EventSummary
	updatedAt time.Time
	truncated bool // the namespace has more events than fetchEvents loads
}

// cfg holds the command-line flags. Everything that belongs to a cluster
// (clients, caches, the worker) lives in clusterState, see cluster.go.
var cfg Config

// version is set at release time: go build -ldflags "-X main.version=v0.2.0".
var version = "dev"

func main() {
	// `kubehealth bench …` replays recorded incidents through the AI code
	// paths; see bench.go.
	if len(os.Args) > 1 && os.Args[1] == "bench" {
		os.Exit(benchMain(os.Args[2:]))
	}

	var namespaceList string
	var showVersion bool
	flag.StringVar(&namespaceList, "namespaces", "", "Comma-separated list of namespaces (empty = all)")
	flag.StringVar(&cfg.Port, "port", "8080", "HTTP server port")
	flag.StringVar(&cfg.Kubeconfig, "kubeconfig", defaultKubeconfig(), "Path to kubeconfig")
	flag.BoolVar(&showVersion, "version", false, "Print the version and exit")
	flag.Parse()
	if showVersion {
		fmt.Println("kubehealth", version)
		return
	}

	// Embedded build only: extract the bundled llama-server + model and start
	// it locally. Does nothing in the normal (non-embed) build, and is skipped
	// even in embedded builds when the user explicitly sets LMSTUDIO_URL.
	if _, err := startEmbeddedLLM(); err != nil {
		log.Fatalf("Embedded LLM failed to start: %v", err)
	}

	initAI()
	initSystemPrompt()

	if namespaceList != "" {
		for _, ns := range strings.Split(namespaceList, ",") {
			ns = strings.TrimSpace(ns)
			if ns != "" {
				cfg.Namespaces = append(cfg.Namespaces, ns)
			}
		}
	}
	cfg.AllNS = len(cfg.Namespaces) == 0

	// Start watching the kubeconfig's current context. Its worker refreshes one
	// namespace at a time; its owner resolver confirms workload owners.
	loadPrefs()
	if err := initClusters(); err != nil {
		log.Fatalf("Failed to start: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", handleDashboard)
	mux.HandleFunc("/namespace/", handleNamespace)
	mux.HandleFunc("/object/", handleObject)
	mux.HandleFunc("/static/", handleStatic)
	mux.HandleFunc("/api/events", handleAPIEvents)
	mux.HandleFunc("/api/home", handleAPIHome)
	mux.HandleFunc("/api/snapshot/", handleAPISnapshot)
	mux.HandleFunc("/fragment/namespace/", handleNamespaceFragment)
	mux.HandleFunc("/api/contexts", handleAPIContexts)
	mux.HandleFunc("/api/context", handleAPIContext)
	mux.HandleFunc("/api/context/env", handleAPIContextEnv)
	mux.HandleFunc("/api/object/", handleAPIObject)
	mux.HandleFunc("/api/yaml/", handleAPIYAML)
	mux.HandleFunc("/api/status", handleAPIStatus)
	mux.HandleFunc("/api/refresh", handleRefresh)
	mux.HandleFunc("/api/ai/insights/", handleAPIAIInsights)
	mux.HandleFunc("/api/ai/analyze/", handleAPIAIAnalyze)
	mux.HandleFunc("/api/ai/config", handleAPIAIConfig)
	mux.HandleFunc("/api/ai/brief", handleAPIBrief)
	mux.HandleFunc("/api/settings", handleAPISettings)
	mux.HandleFunc("/api/logs/", handleAPILogs)
	mux.HandleFunc("/api/correlate/", handleAPICorrelate)
	mux.HandleFunc("/api/rca/status/", handleAPIRCAStatus)
	mux.HandleFunc("/api/rca/", handleAPIRCAStart)

	nsInfo := "all namespaces"
	if !cfg.AllNS {
		nsInfo = strings.Join(cfg.Namespaces, ", ")
	}
	log.Printf("KubeHealth %s starting on :%s — watching %s", version, cfg.Port, nsInfo)
	log.Fatal(http.ListenAndServe(":"+cfg.Port, mux))
}

func defaultKubeconfig() string {
	if kc := os.Getenv("KUBECONFIG"); kc != "" {
		return kc
	}
	if home := homedir.HomeDir(); home != "" {
		return filepath.Join(home, ".kube", "config")
	}
	return ""
}

// resolveNamespaces returns the current namespace list from the API or config.
func (c *clusterState) resolveNamespaces(ctx context.Context) ([]string, error) {
	if !cfg.AllNS {
		return cfg.Namespaces, nil
	}
	nsList, err := c.client.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list namespaces: %w", err)
	}
	out := make([]string, 0, len(nsList.Items))
	for _, ns := range nsList.Items {
		out = append(out, ns.Name)
	}
	sort.Strings(out)
	return out, nil
}

// refreshNamespace fetches events for one namespace and stores them in nsCache.
func (c *clusterState) refreshNamespace(ctx context.Context, ns string) {
	events, truncated, err := c.fetchEvents(ctx, ns)
	if err != nil {
		if ctx.Err() == nil {
			log.Printf("Error fetching events for %s/%s: %v", c.name, ns, err)
		}
		return
	}
	v, _ := c.nsCache.LoadOrStore(ns, &nsEntry{})
	entry := v.(*nsEntry)
	entry.Lock()
	entry.events = events
	entry.updatedAt = time.Now()
	entry.truncated = truncated
	entry.Unlock()
	c.assembleDashboard()
}

// assembleDashboard rebuilds the cluster's dashboard data from its caches.
func (c *clusterState) assembleDashboard() {
	var (
		results []NamespaceEvents
		totalW  int
		totalN  int
	)
	for ns, events := range c.allNamespaceEvents() {
		objects := groupByObject(events)
		ne := NamespaceEvents{
			Namespace:  ns,
			Events:     events,
			Objects:    objects,
			KindGroups: groupByKind(objects),
		}
		for _, e := range events {
			if e.Type == "Warning" {
				ne.WarningCount++
			} else {
				ne.NormalCount++
			}
		}
		results = append(results, ne)
		totalW += ne.WarningCount
		totalN += ne.NormalCount
	}
	sort.Slice(results, func(i, j int) bool {
		if results[i].WarningCount != results[j].WarningCount {
			return results[i].WarningCount > results[j].WarningCount
		}
		return results[i].Namespace < results[j].Namespace
	})
	data := &DashboardData{
		Namespaces:  results,
		LastUpdated: time.Now().Format("2006-01-02 15:04:05 MST"),
		TotalWarn:   totalW,
		TotalNormal: totalN,
	}
	c.dashboard.Lock()
	c.dashboard.data = data
	c.dashboard.Unlock()
}

// runWorker cycles through namespaces one at a time, refreshing the stalest
// entry every 2 seconds. Every warnFeedInterval one tick goes to the
// cluster-wide warning list instead. This is the only place that calls the
// events API, so the kube-apiserver sees at most one list call per tick.
func (c *clusterState) runWorker(ctx context.Context) {
	// Re-resolve the namespace list every 5 minutes to pick up new namespaces.
	var (
		namespaces  []string
		nextNSFetch time.Time
	)
	// Run immediately, then tick every 2s until the cluster stops being active.
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		if time.Now().After(nextNSFetch) {
			ns, err := c.resolveNamespaces(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("Worker [%s]: could not list namespaces: %v", c.name, err)
				c.worker.Lock()
				c.worker.lastErr = err.Error()
				c.worker.Unlock()
			} else {
				namespaces = ns
				nextNSFetch = time.Now().Add(5 * time.Minute)
				c.worker.Lock()
				c.worker.total = len(namespaces)
				c.worker.namespaces = namespaces
				c.worker.lastErr = ""
				c.worker.Unlock()
			}
		}
		c.workerTick(ctx, namespaces)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// workerTick makes this tick's single list call: the warning feed when it
// is due, otherwise the stalest namespace.
func (c *clusterState) workerTick(ctx context.Context, namespaces []string) {
	if c.warnFeedDue() {
		c.refreshWarnings(ctx)
		return
	}
	if len(namespaces) == 0 {
		return
	}
	// Pick the namespace whose cache is oldest (or missing).
	var stalest string
	var stalestTime time.Time
	for _, ns := range namespaces {
		v, ok := c.nsCache.Load(ns)
		if !ok {
			stalest = ns
			break
		}
		// Locked read: after a quick switch away and back, the previous
		// worker may still be finishing its last refresh.
		entry := v.(*nsEntry)
		entry.RLock()
		t := entry.updatedAt
		entry.RUnlock()
		if stalest == "" || t.Before(stalestTime) {
			stalest = ns
			stalestTime = t
		}
	}
	if stalest != "" {
		c.refreshNamespace(ctx, stalest)
	}
	// Update ready flag: all namespaces have at least one cache entry.
	loaded := 0
	c.nsCache.Range(func(_, _ any) bool { loaded++; return true })
	c.worker.Lock()
	c.worker.ready = loaded >= len(namespaces) && len(namespaces) > 0
	c.worker.Unlock()
}

// fetchEvents lists up to nsEventLimit events in one namespace. truncated is
// true when the namespace has more; the API returns them in name order, so
// the missing ones are not necessarily the oldest. Warnings are covered by
// the cluster-wide feed regardless.
func (c *clusterState) fetchEvents(ctx context.Context, namespace string) (summaries []EventSummary, truncated bool, err error) {
	eventList, err := c.client.CoreV1().Events(namespace).List(ctx, metav1.ListOptions{
		Limit: nsEventLimit,
	})
	if err != nil {
		return nil, false, err
	}

	for _, e := range eventList.Items {
		summaries = append(summaries, toSummary(e))
	}
	sortNewestFirst(summaries)
	return summaries, eventList.Continue != "", nil
}

// nsEventLimit caps the per-namespace list call made by the sweep.
const nsEventLimit = 200

func toSummary(e corev1.Event) EventSummary {
	last := e.LastTimestamp.Time
	if e.Series != nil && e.Series.LastObservedTime.After(last) {
		last = e.Series.LastObservedTime.Time
	}
	if last.IsZero() {
		last = e.EventTime.Time
	}
	first := e.FirstTimestamp.Time
	if first.IsZero() {
		first = e.EventTime.Time
	}
	if first.IsZero() {
		first = last
	}
	// Events written through the events.k8s.io API keep their count in Series.
	count := e.Count
	if count == 0 && e.Series != nil {
		count = e.Series.Count
	}
	if count == 0 {
		count = 1
	}

	age := "unknown"
	if !last.IsZero() {
		d := clock().Sub(last)
		age = formatDuration(d)
	}

	return EventSummary{
		Namespace:  e.Namespace,
		Name:       e.Name,
		Reason:     e.Reason,
		Message:    e.Message,
		Type:       e.Type,
		Count:      count,
		Kind:       e.InvolvedObject.Kind,
		ObjectName: e.InvolvedObject.Name,

		ObjectNamespace: e.InvolvedObject.Namespace,
		FirstTime:       first,
		LastTime:        last,
		Age:             age,
	}
}

// clock is the time that event and object ages are measured against. It is
// time.Now except in `kubehealth bench run`, which holds it still so that a
// recorded snapshot shows the same ages for the whole run.
var clock = time.Now

func formatDuration(d time.Duration) string {
	d = d.Round(time.Second)
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm", int(d.Minutes()))
	}
	if d < 24*time.Hour {
		return fmt.Sprintf("%dh", int(d.Hours()))
	}
	return fmt.Sprintf("%dd", int(d.Hours()/24))
}

// ---- HTTP Handlers ----

// appPage is the one page KubeHealth serves: the shared header, the
// namespace rail, and a main area showing the overview or a namespace. After
// the first load the page swaps namespaces in itself (/fragment/namespace/).
type appPage struct {
	Home      HomePayload
	Header    headerData
	View      string      // "overview" or "namespace"
	CurrentNS string      // the open namespace, "" on the overview
	NS        *nsPageData // nil on the overview
}

// nsPageData feeds the namespace fragment: its objects, panels and status.
type nsPageData struct {
	NamespaceEvents
	Cluster  clusterInfo
	Title    string         // the namespace, or "Cluster-wide objects"
	Warnings []EventSummary // newest warnings, for the status panel
	LoadedAt string         // when this view's events were read
}

const statusWarnings = 8

func handleDashboard(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	// Rendered straight away, even before the first fetch: the page shows its
	// own connecting state, with the header and cluster switcher available.
	c := pageCluster(r)
	home := c.buildHome(time.Now())
	renderApp(w, appPage{Home: home, Header: headerData{Cluster: home.Cluster}, View: "overview"})
}

func handleNamespace(w http.ResponseWriter, r *http.Request) {
	ns := strings.Trim(strings.TrimPrefix(r.URL.Path, "/namespace/"), "/")
	if ns == "" {
		http.Redirect(w, r, "/", http.StatusFound)
		return
	}
	c := pageCluster(r)
	home := c.buildHome(time.Now())
	data := c.namespacePage(ns)
	renderApp(w, appPage{
		Home:      home,
		Header:    namespaceHeader(home, ns, data.Title),
		View:      "namespace",
		CurrentNS: ns,
		NS:        &data,
	})
}

// handleNamespaceFragment serves the namespace view alone, for the page to
// swap in: GET /fragment/namespace/<ns>.
func handleNamespaceFragment(w http.ResponseWriter, r *http.Request) {
	ns := strings.Trim(strings.TrimPrefix(r.URL.Path, "/fragment/namespace/"), "/")
	if ns == "" {
		http.Error(w, "expected /fragment/namespace/<namespace>", http.StatusBadRequest)
		return
	}
	c := apiCluster(w, r)
	if c == nil {
		return
	}
	data := c.namespacePage(ns)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	if err := appTmpl.ExecuteTemplate(w, "ns_fragment", data); err != nil {
		log.Printf("template error: %v", err)
	}
}

func renderApp(w http.ResponseWriter, page appPage) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := appTmpl.Execute(w, page); err != nil {
		log.Printf("template error: %v", err)
	}
}

// namespacePage reads a namespace's cached events into its objects, ownership
// chains and recent warnings. No API call: the live snapshot comes separately.
func (c *clusterState) namespacePage(ns string) nsPageData {
	events := c.scopeEvents(ns)
	objects := groupByObject(events)
	families, orphans := buildFamilies(objects)
	d := nsPageData{
		NamespaceEvents: NamespaceEvents{
			Namespace:   ns,
			Events:      events,
			Objects:     objects,
			KindGroups:  groupByKind(objects),
			Families:    families,
			OrphanKinds: orphans,
		},
		Cluster:  c.identity(),
		Title:    ns,
		LoadedAt: time.Now().Format("15:04:05"),
	}
	if ns == clusterScopeNS {
		d.Title = "Cluster-wide objects"
	}
	for _, e := range events {
		if e.Type == "Warning" && len(d.Warnings) < statusWarnings {
			d.Warnings = append(d.Warnings, e)
		}
	}
	return d
}

// namespaceHeader is the breadcrumb for a namespace: its name and how many
// problems the home page counts in it (one per failing workload).
func namespaceHeader(home HomePayload, ns, title string) headerData {
	failing, cascade := 0, false
	for _, p := range home.Problems {
		mine := p.Scope == "cluster" && ns == clusterScopeNS
		for _, pns := range p.Namespaces {
			mine = mine || (pns == ns && ns != clusterScopeNS)
		}
		if mine {
			failing++
			cascade = cascade || p.Cascade
		}
	}
	hdr := headerData{Cluster: home.Cluster, Crumb: title, CrumbTag: "No warnings", CrumbTagClass: "ok"}
	if failing > 0 {
		word := "problems"
		if failing == 1 {
			word = "problem"
		}
		hdr.CrumbTag, hdr.CrumbTagClass = fmt.Sprintf("● %d %s", failing, word), "warn"
		if cascade {
			hdr.CrumbTag, hdr.CrumbTagClass = fmt.Sprintf("▲ %d %s", failing, word), "cascade"
		}
	}
	return hdr
}

func handleObject(w http.ResponseWriter, r *http.Request) {
	// /object/{namespace}/{kind}/{name}
	parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/object/"), "/", 3)
	if len(parts) < 3 {
		http.Error(w, "invalid path", http.StatusBadRequest)
		return
	}
	ns, kind, name := parts[0], parts[1], parts[2]

	c := pageCluster(r)
	yamlStr, err := c.fetchObjectYAML(ns, kind, name)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error fetching object from %s: %v", c.name, err), http.StatusInternalServerError)
		return
	}

	data := struct {
		Namespace string
		Kind      string
		Name      string
		YAML      string
		Cluster   clusterInfo
	}{ns, kind, name, yamlStr, c.identity()}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := objectTmpl.Execute(w, data); err != nil {
		log.Printf("template error: %v", err)
	}
}

// staticGVR is a fast-path lookup for common built-in resource types.
var staticGVR = map[string]schema.GroupVersionResource{
	"pod":                   {Group: "", Version: "v1", Resource: "pods"},
	"pods":                  {Group: "", Version: "v1", Resource: "pods"},
	"deployment":            {Group: "apps", Version: "v1", Resource: "deployments"},
	"deployments":           {Group: "apps", Version: "v1", Resource: "deployments"},
	"replicaset":            {Group: "apps", Version: "v1", Resource: "replicasets"},
	"replicasets":           {Group: "apps", Version: "v1", Resource: "replicasets"},
	"statefulset":           {Group: "apps", Version: "v1", Resource: "statefulsets"},
	"statefulsets":          {Group: "apps", Version: "v1", Resource: "statefulsets"},
	"daemonset":             {Group: "apps", Version: "v1", Resource: "daemonsets"},
	"daemonsets":            {Group: "apps", Version: "v1", Resource: "daemonsets"},
	"service":               {Group: "", Version: "v1", Resource: "services"},
	"services":              {Group: "", Version: "v1", Resource: "services"},
	"configmap":             {Group: "", Version: "v1", Resource: "configmaps"},
	"configmaps":            {Group: "", Version: "v1", Resource: "configmaps"},
	"persistentvolumeclaim": {Group: "", Version: "v1", Resource: "persistentvolumeclaims"},
	"job":                   {Group: "batch", Version: "v1", Resource: "jobs"},
	"jobs":                  {Group: "batch", Version: "v1", Resource: "jobs"},
	"cronjob":               {Group: "batch", Version: "v1", Resource: "cronjobs"},
	"ingress":               {Group: "networking.k8s.io", Version: "v1", Resource: "ingresses"},
	"node":                  {Group: "", Version: "v1", Resource: "nodes"},
	"nodes":                 {Group: "", Version: "v1", Resource: "nodes"},
	"secret":                {Group: "", Version: "v1", Resource: "secrets"},
	"serviceaccount":        {Group: "", Version: "v1", Resource: "serviceaccounts"},
	"persistentvolume":      {Group: "", Version: "v1", Resource: "persistentvolumes"},
	// v2: v1 has no metrics list.
	"horizontalpodautoscaler": {Group: "autoscaling", Version: "v2", Resource: "horizontalpodautoscalers"},
}

// kindToGVR resolves a Kubernetes kind to its GroupVersionResource.
// It first checks the static built-in map, then falls back to live API discovery
// so that CRDs (e.g. Elasticsearch, Certificate, VirtualService) work automatically.
// Discovered GVRs are cached per cluster, since CRDs differ between clusters.
func (c *clusterState) kindToGVR(kind string) (schema.GroupVersionResource, error) {
	lower := strings.ToLower(kind)
	if gvr, ok := staticGVR[lower]; ok {
		return gvr, nil
	}
	if v, ok := c.gvrCache.Load(lower); ok {
		return v.(schema.GroupVersionResource), nil
	}
	return c.discoverGVR(lower)
}

// discoverGVR queries the API server's discovery endpoint to find the GVR for
// any kind not in the static map, then caches all results for future lookups.
func (c *clusterState) discoverGVR(kindLower string) (schema.GroupVersionResource, error) {
	c.gvrDiscoMu.Lock()
	defer c.gvrDiscoMu.Unlock()

	// Re-check after acquiring the lock — another goroutine may have just populated it.
	if v, ok := c.gvrCache.Load(kindLower); ok {
		return v.(schema.GroupVersionResource), nil
	}

	_, lists, err := c.client.Discovery().ServerGroupsAndResources()
	if err != nil && lists == nil {
		return schema.GroupVersionResource{}, fmt.Errorf("API discovery: %w", err)
	}
	// Partial errors (some groups unavailable) are acceptable — use what we got.

	var found schema.GroupVersionResource
	var foundOK bool

	for _, list := range lists {
		gv, parseErr := schema.ParseGroupVersion(list.GroupVersion)
		if parseErr != nil {
			continue
		}
		for _, r := range list.APIResources {
			if strings.Contains(r.Name, "/") {
				continue // skip sub-resources (pods/log, pods/status, etc.)
			}
			key := strings.ToLower(r.Kind)
			gvr := schema.GroupVersionResource{Group: gv.Group, Version: gv.Version, Resource: r.Name}
			c.gvrCache.Store(key, gvr)
			if key == kindLower {
				found = gvr
				foundOK = true
			}
		}
	}

	if !foundOK {
		return schema.GroupVersionResource{}, fmt.Errorf("unknown kind: %s", kindLower)
	}
	return found, nil
}

// ---- Object describe helpers ----

func nmStr(m map[string]interface{}, key string) string {
	if m == nil {
		return ""
	}
	v := m[key]
	if v == nil {
		return ""
	}
	return fmt.Sprintf("%v", v)
}

func nmMap(m map[string]interface{}, key string) map[string]interface{} {
	if m == nil {
		return nil
	}
	v, _ := m[key].(map[string]interface{})
	return v
}

func nmSlice(m map[string]interface{}, key string) []interface{} {
	if m == nil {
		return nil
	}
	v, _ := m[key].([]interface{})
	return v
}

func nmInt(m map[string]interface{}, key string) int64 {
	if m == nil {
		return 0
	}
	switch v := m[key].(type) {
	case float64:
		return int64(v)
	case int64:
		return v
	case int:
		return int64(v)
	}
	return 0
}

func toStrMap(v interface{}) map[string]string {
	raw, ok := v.(map[string]interface{})
	if !ok || len(raw) == 0 {
		return nil
	}
	out := make(map[string]string, len(raw))
	for k, val := range raw {
		out[k] = fmt.Sprintf("%v", val)
	}
	return out
}

func rowIf(rows []InfoRow, key, value string) []InfoRow {
	if value == "" || value == "<nil>" {
		return rows
	}
	return append(rows, InfoRow{key, value})
}

func extractConditions(status map[string]interface{}) []InfoCondition {
	var out []InfoCondition
	for _, raw := range nmSlice(status, "conditions") {
		c, _ := raw.(map[string]interface{})
		if c == nil {
			continue
		}
		out = append(out, InfoCondition{
			Type:    nmStr(c, "type"),
			Status:  nmStr(c, "status"),
			Reason:  nmStr(c, "reason"),
			Message: nmStr(c, "message"),
		})
	}
	return out
}

func (c *clusterState) fetchObjectInfo(namespace, kind, name string) ObjectInfo {
	info := ObjectInfo{Kind: kind, Name: name, Namespace: namespace}
	namespace = apiNamespace(namespace)
	gvr, err := c.kindToGVR(kind)
	if err != nil {
		info.Error = err.Error()
		return info
	}
	ctx := context.Background()
	var raw map[string]interface{}
	if namespace == "" {
		o, err := c.dynamic.Resource(gvr).Get(ctx, name, metav1.GetOptions{})
		if k8serrors.IsNotFound(err) {
			info.NotFound = true
			return info
		}
		if err != nil {
			info.Error = err.Error()
			return info
		}
		raw = o.Object
	} else {
		o, err := c.dynamic.Resource(gvr).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
		if k8serrors.IsNotFound(err) {
			info.NotFound = true
			return info
		}
		if err != nil {
			info.Error = err.Error()
			return info
		}
		raw = o.Object
	}

	endpoints := ""
	if strings.EqualFold(kind, "Service") {
		endpoints = c.serviceEndpoints(namespace, name)
	}
	return describeObject(info, raw, endpoints, clock())
}

func handleAPIObject(w http.ResponseWriter, r *http.Request) {
	// /api/object/{namespace}/{kind}/{name}
	parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/api/object/"), "/", 3)
	if len(parts) < 3 {
		http.Error(w, "invalid path", http.StatusBadRequest)
		return
	}
	ns, kind, name := parts[0], parts[1], parts[2]
	c := apiCluster(w, r)
	if c == nil {
		return
	}
	info := c.fetchObjectInfo(ns, kind, name)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(info)
}

func handleAPIEvents(w http.ResponseWriter, r *http.Request) {
	c := apiCluster(w, r)
	if c == nil {
		return
	}
	c.dashboard.RLock()
	data := c.dashboard.data
	c.dashboard.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

// handleRefresh just returns the current cached state immediately.
// The background worker will continue refreshing namespaces on its own schedule.
func handleRefresh(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/", http.StatusFound)
}

// ---- Templates ----

// headerData feeds the shared page header (templates/header.html).
type headerData struct {
	Cluster       clusterInfo
	Crumb         string // namespace shown after the cluster, if any
	CrumbTag      string // e.g. "▲ 1 failing"
	CrumbTagClass string // cascade, warn or ok
}

var tmplFuncs = template.FuncMap{
	"lower": strings.ToLower,
	// firstAge returns the Age of the first (most recent) event in a slice.
	"firstAge": func(events []EventSummary) string {
		if len(events) == 0 {
			return ""
		}
		return events[0].Age
	},
	// joinStrings joins a string slice with a separator.
	"joinStrings": strings.Join,
	// clock shows a time of day, with the date when it isn't today.
	"clock": func(t time.Time) string {
		if t.IsZero() {
			return ""
		}
		t, now := t.Local(), time.Now()
		if t.YearDay() == now.YearDay() && t.Year() == now.Year() {
			return t.Format("15:04")
		}
		return t.Format("2 Jan 15:04")
	},
	// envLabel is the tag shown for a cluster's environment.
	"envLabel": func(env string) string {
		switch env {
		case "prod":
			return "PROD"
		case "nonprod":
			return "NON-PROD"
		case "stage":
			return "STAGE"
		case "dev":
			return "DEV"
		case "local":
			return "LOCAL"
		}
		return "?"
	},
}

func mustParseTemplate(name, file string, extras ...string) *template.Template {
	files := append([]string{file}, extras...)
	return template.Must(
		template.New(name).Funcs(tmplFuncs).ParseFS(templateFS, files...),
	)
}

var (
	appTmpl    = mustParseTemplate("app.html", "templates/app.html", "templates/ns_view.html", "templates/header.html", "templates/ai_modal.html", "templates/settings_modal.html", "templates/docs_modal.html", "templates/yaml_drawer.html")
	objectTmpl = mustParseTemplate("object.html", "templates/object.html")
)
