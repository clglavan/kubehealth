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
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	"sigs.k8s.io/yaml"
)

//go:embed templates/*.html
var templateFS embed.FS

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
	FirstTime  time.Time
	LastTime   time.Time
	Age        string
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
	CascadeFailure bool   // root AND all leaves have warnings
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
		var walk func(m *FamilyMember)
		walk = func(m *FamilyMember) {
			fam.WarningCount += m.WarningCount
			fam.NormalCount += m.NormalCount
			if m.HasWarning {
				fam.HasWarning = true
			}
			kindSet[m.Kind] = true
			for _, c := range m.Children {
				walk(c)
			}
		}
		walk(rootMember)

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
		} else {
			obj.NormalCount++
		}
		if e.LastTime.After(obj.LastTime) {
			obj.LastTime = e.LastTime
		}
	}

	out := make([]ObjectEvents, 0, len(order))
	for _, k := range order {
		out = append(out, *index[k])
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
}

var (
	client        *kubernetes.Clientset
	dynamicClient dynamic.Interface
	cfg           Config

	// nsCache is the per-namespace event store. Never re-fetched all at once.
	nsCache sync.Map // map[string]*nsEntry

	// dashboard is the assembled view served to the browser — rebuilt after
	// each individual namespace refresh.
	dashboard struct {
		sync.RWMutex
		data *DashboardData
	}

	// workerState tracks how many namespaces are known vs. already cached,
	// so the browser can show accurate loading progress.
	workerState struct {
		sync.RWMutex
		total int
		ready bool // true once every known namespace has been fetched at least once
	}
)

func main() {
	var namespaceList string
	flag.StringVar(&namespaceList, "namespaces", "", "Comma-separated list of namespaces (empty = all)")
	flag.StringVar(&cfg.Port, "port", "8080", "HTTP server port")
	flag.StringVar(&cfg.Kubeconfig, "kubeconfig", defaultKubeconfig(), "Path to kubeconfig")
	flag.Parse()

	if namespaceList != "" {
		for _, ns := range strings.Split(namespaceList, ",") {
			ns = strings.TrimSpace(ns)
			if ns != "" {
				cfg.Namespaces = append(cfg.Namespaces, ns)
			}
		}
	}
	cfg.AllNS = len(cfg.Namespaces) == 0

	restConfig, err := clientcmd.BuildConfigFromFlags("", cfg.Kubeconfig)
	if err != nil {
		log.Fatalf("Failed to build kubeconfig: %v", err)
	}
	// Conservative rate limit — the worker fetches one namespace at a time anyway,
	// but this caps any incidental bursts (e.g. YAML drill-down requests).
	restConfig.QPS = 5
	restConfig.Burst = 10

	client, err = kubernetes.NewForConfig(restConfig)
	if err != nil {
		log.Fatalf("Failed to create k8s client: %v", err)
	}

	dynamicClient, err = dynamic.NewForConfig(restConfig)
	if err != nil {
		log.Fatalf("Failed to create dynamic client: %v", err)
	}

	// Start the single background worker that refreshes one namespace at a time.
	go runWorker()

	mux := http.NewServeMux()
	mux.HandleFunc("/", handleDashboard)
	mux.HandleFunc("/namespace/", handleNamespace)
	mux.HandleFunc("/object/", handleObject)
	mux.HandleFunc("/api/events", handleAPIEvents)
	mux.HandleFunc("/api/object/", handleAPIObject)
	mux.HandleFunc("/api/status", handleAPIStatus)
	mux.HandleFunc("/api/refresh", handleRefresh)

	nsInfo := "all namespaces"
	if !cfg.AllNS {
		nsInfo = strings.Join(cfg.Namespaces, ", ")
	}
	log.Printf("KubeHealth starting on :%s — watching %s", cfg.Port, nsInfo)
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
func resolveNamespaces() ([]string, error) {
	if !cfg.AllNS {
		return cfg.Namespaces, nil
	}
	nsList, err := client.CoreV1().Namespaces().List(context.Background(), metav1.ListOptions{})
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
func refreshNamespace(ns string) {
	events, err := fetchEvents(ns)
	if err != nil {
		log.Printf("Error fetching events for %s: %v", ns, err)
		return
	}
	v, _ := nsCache.LoadOrStore(ns, &nsEntry{})
	entry := v.(*nsEntry)
	entry.Lock()
	entry.events = events
	entry.updatedAt = time.Now()
	entry.Unlock()
	assembleDashboard()
}

// assembleDashboard rebuilds the global dashboard from all cached ns entries.
func assembleDashboard() {
	var (
		results []NamespaceEvents
		totalW  int
		totalN  int
	)
	nsCache.Range(func(key, val any) bool {
		entry := val.(*nsEntry)
		entry.RLock()
		events := entry.events
		entry.RUnlock()
		objects := groupByObject(events)
		ne := NamespaceEvents{
			Namespace:  key.(string),
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
		return true
	})
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
	dashboard.Lock()
	dashboard.data = data
	dashboard.Unlock()
}

// runWorker cycles through namespaces one at a time, refreshing the stalest
// entry every 2 seconds. This is the only place that calls the events API,
// so the kube-apiserver never sees a burst regardless of namespace count.
func runWorker() {
	// Re-resolve the namespace list every 5 minutes to pick up new namespaces.
	var (
		namespaces  []string
		nextNSFetch time.Time
	)
	// Run immediately on startup, then tick every 2s.
	ticker := time.NewTicker(2 * time.Second)
	for ; ; <-ticker.C {
		if time.Now().After(nextNSFetch) {
			ns, err := resolveNamespaces()
			if err != nil {
				log.Printf("Worker: could not list namespaces: %v", err)
			} else {
				namespaces = ns
				nextNSFetch = time.Now().Add(5 * time.Minute)
				workerState.Lock()
				workerState.total = len(namespaces)
				workerState.Unlock()
			}
		}
		if len(namespaces) == 0 {
			continue
		}
		// Pick the namespace whose cache is oldest (or missing).
		var stalest string
		var stalestTime time.Time
		for _, ns := range namespaces {
			v, ok := nsCache.Load(ns)
			if !ok {
				stalest = ns
				break
			}
			t := v.(*nsEntry).updatedAt
			if stalest == "" || t.Before(stalestTime) {
				stalest = ns
				stalestTime = t
			}
		}
		if stalest != "" {
			refreshNamespace(stalest)
		}
		// Update ready flag: all namespaces have at least one cache entry.
		loaded := 0
		nsCache.Range(func(_, _ any) bool { loaded++; return true })
		workerState.Lock()
		workerState.ready = loaded >= len(namespaces) && len(namespaces) > 0
		workerState.Unlock()
	}
}

func fetchEvents(namespace string) ([]EventSummary, error) {
	// Field selector: only last 1h events to reduce load
	eventList, err := client.CoreV1().Events(namespace).List(context.Background(), metav1.ListOptions{
		Limit: 200,
	})
	if err != nil {
		return nil, err
	}

	// Sort by last timestamp desc
	events := eventList.Items
	sort.Slice(events, func(i, j int) bool {
		ti := events[i].LastTimestamp.Time
		tj := events[j].LastTimestamp.Time
		return ti.After(tj)
	})

	var summaries []EventSummary
	for _, e := range events {
		summaries = append(summaries, toSummary(e))
	}
	return summaries, nil
}

func toSummary(e corev1.Event) EventSummary {
	last := e.LastTimestamp.Time
	if last.IsZero() {
		last = e.EventTime.Time
	}
	first := e.FirstTimestamp.Time

	age := "unknown"
	if !last.IsZero() {
		d := time.Since(last)
		age = formatDuration(d)
	}

	return EventSummary{
		Namespace:  e.Namespace,
		Name:       e.Name,
		Reason:     e.Reason,
		Message:    e.Message,
		Type:       e.Type,
		Count:      e.Count,
		Kind:       e.InvolvedObject.Kind,
		ObjectName: e.InvolvedObject.Name,
		FirstTime:  first,
		LastTime:   last,
		Age:        age,
	}
}

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

func handleDashboard(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	dashboard.RLock()
	data := dashboard.data
	dashboard.RUnlock()
	if data == nil {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusServiceUnavailable)
		loadingTmpl.Execute(w, nil)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := dashboardTmpl.Execute(w, data); err != nil {
		log.Printf("template error: %v", err)
	}
}

func handleNamespace(w http.ResponseWriter, r *http.Request) {
	ns := strings.TrimPrefix(r.URL.Path, "/namespace/")
	ns = strings.Trim(ns, "/")
	if ns == "" {
		http.Redirect(w, r, "/", http.StatusFound)
		return
	}

	// Serve from cache — no live API call.
	var events []EventSummary
	if v, ok := nsCache.Load(ns); ok {
		entry := v.(*nsEntry)
		entry.RLock()
		events = entry.events
		entry.RUnlock()
	}

	objects := groupByObject(events)
	families, orphans := buildFamilies(objects)
	data := NamespaceEvents{
		Namespace:   ns,
		Events:      events,
		Objects:     objects,
		KindGroups:  groupByKind(objects),
		Families:    families,
		OrphanKinds: orphans,
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := namespaceTmpl.Execute(w, data); err != nil {
		log.Printf("template error: %v", err)
	}
}

func handleObject(w http.ResponseWriter, r *http.Request) {
	// /object/{namespace}/{kind}/{name}
	parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/object/"), "/", 3)
	if len(parts) < 3 {
		http.Error(w, "invalid path", http.StatusBadRequest)
		return
	}
	ns, kind, name := parts[0], parts[1], parts[2]

	yamlStr, err := fetchObjectYAML(ns, kind, name)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error fetching object: %v", err), http.StatusInternalServerError)
		return
	}

	data := struct {
		Namespace string
		Kind      string
		Name      string
		YAML      string
	}{ns, kind, name, yamlStr}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := objectTmpl.Execute(w, data); err != nil {
		log.Printf("template error: %v", err)
	}
}

func fetchObjectYAML(namespace, kind, name string) (string, error) {
	gvr, err := kindToGVR(kind)
	if err != nil {
		return "", err
	}

	var obj interface{}
	if namespace == "" || namespace == "_" {
		o, err := dynamicClient.Resource(gvr).Get(context.Background(), name, metav1.GetOptions{})
		if err != nil {
			return "", err
		}
		obj = o.Object
	} else {
		o, err := dynamicClient.Resource(gvr).Namespace(namespace).Get(context.Background(), name, metav1.GetOptions{})
		if err != nil {
			return "", err
		}
		obj = o.Object
	}

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

func kindToGVR(kind string) (schema.GroupVersionResource, error) {
	mapping := map[string]schema.GroupVersionResource{
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
	}
	gvr, ok := mapping[strings.ToLower(kind)]
	if !ok {
		return schema.GroupVersionResource{}, fmt.Errorf("unknown kind: %s", kind)
	}
	return gvr, nil
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

func fetchObjectInfo(namespace, kind, name string) ObjectInfo {
	info := ObjectInfo{Kind: kind, Name: name, Namespace: namespace}
	gvr, err := kindToGVR(kind)
	if err != nil {
		info.Error = err.Error()
		return info
	}
	ctx := context.Background()
	var raw map[string]interface{}
	if namespace == "" {
		o, err := dynamicClient.Resource(gvr).Get(ctx, name, metav1.GetOptions{})
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
		o, err := dynamicClient.Resource(gvr).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
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

	meta := nmMap(raw, "metadata")
	info.Labels = toStrMap(meta["labels"])
	if ct := nmStr(meta, "creationTimestamp"); ct != "" {
		if t, err := time.Parse(time.RFC3339, ct); err == nil {
			info.Age = formatDuration(time.Since(t))
		}
	}
	if ann := toStrMap(meta["annotations"]); ann != nil {
		filtered := make(map[string]string)
		for k, v := range ann {
			if strings.Contains(k, "last-applied-configuration") || len(v) > 200 {
				continue
			}
			filtered[k] = v
		}
		if len(filtered) > 0 {
			info.Annotations = filtered
		}
	}

	spec := nmMap(raw, "spec")
	status := nmMap(raw, "status")
	switch strings.ToLower(kind) {
	case "pod":
		info.Sections, info.Conditions = infoForPod(spec, status)
	case "deployment":
		info.Sections, info.Conditions = infoForDeployment(spec, status)
	case "replicaset":
		info.Sections, info.Conditions = infoForReplicaSet(spec, status)
	case "statefulset":
		info.Sections, info.Conditions = infoForStatefulSet(spec, status)
	case "daemonset":
		info.Sections, info.Conditions = infoForDaemonSet(spec, status)
	case "job":
		info.Sections, info.Conditions = infoForJob(spec, status)
	case "cronjob":
		info.Sections, _ = infoForCronJob(spec, status)
	case "service":
		info.Sections, _ = infoForService(spec, status)
	}
	return info
}

func infoForPod(spec, status map[string]interface{}) ([]InfoSection, []InfoCondition) {
	var sections []InfoSection
	var rows []InfoRow
	rows = rowIf(rows, "Phase", nmStr(status, "phase"))
	rows = rowIf(rows, "Pod IP", nmStr(status, "podIP"))
	rows = rowIf(rows, "Node", nmStr(spec, "nodeName"))
	rows = rowIf(rows, "Host IP", nmStr(status, "hostIP"))
	rows = rowIf(rows, "QoS Class", nmStr(status, "qosClass"))
	rows = rowIf(rows, "Service Account", nmStr(spec, "serviceAccountName"))
	if len(rows) > 0 {
		sections = append(sections, InfoSection{"Runtime", rows})
	}
	for _, raw := range nmSlice(spec, "containers") {
		c, _ := raw.(map[string]interface{})
		if c == nil {
			continue
		}
		var cr []InfoRow
		cr = rowIf(cr, "Image", nmStr(c, "image"))
		if res := nmMap(c, "resources"); res != nil {
			if req := nmMap(res, "requests"); req != nil {
				cr = rowIf(cr, "CPU Request", nmStr(req, "cpu"))
				cr = rowIf(cr, "Memory Request", nmStr(req, "memory"))
			}
			if lim := nmMap(res, "limits"); lim != nil {
				cr = rowIf(cr, "CPU Limit", nmStr(lim, "cpu"))
				cr = rowIf(cr, "Memory Limit", nmStr(lim, "memory"))
			}
		}
		sections = append(sections, InfoSection{"Container: " + nmStr(c, "name"), cr})
	}
	var csRows []InfoRow
	for _, raw := range nmSlice(status, "containerStatuses") {
		cs, _ := raw.(map[string]interface{})
		if cs == nil {
			continue
		}
		csRows = append(csRows, InfoRow{
			nmStr(cs, "name"),
			fmt.Sprintf("ready=%v  restarts=%d", nmStr(cs, "ready"), nmInt(cs, "restartCount")),
		})
	}
	if len(csRows) > 0 {
		sections = append(sections, InfoSection{"Container Status", csRows})
	}
	return sections, extractConditions(status)
}

func infoForDeployment(spec, status map[string]interface{}) ([]InfoSection, []InfoCondition) {
	var rows []InfoRow
	rows = rowIf(rows, "Replicas (desired)", fmt.Sprintf("%d", nmInt(spec, "replicas")))
	rows = rowIf(rows, "Ready", fmt.Sprintf("%d", nmInt(status, "readyReplicas")))
	rows = rowIf(rows, "Available", fmt.Sprintf("%d", nmInt(status, "availableReplicas")))
	rows = rowIf(rows, "Unavailable", fmt.Sprintf("%d", nmInt(status, "unavailableReplicas")))
	rows = rowIf(rows, "Updated", fmt.Sprintf("%d", nmInt(status, "updatedReplicas")))
	if strat := nmMap(spec, "strategy"); strat != nil {
		rows = rowIf(rows, "Strategy", nmStr(strat, "type"))
	}
	return []InfoSection{{"Replicas", rows}}, extractConditions(status)
}

func infoForReplicaSet(spec, status map[string]interface{}) ([]InfoSection, []InfoCondition) {
	var rows []InfoRow
	rows = rowIf(rows, "Replicas (desired)", fmt.Sprintf("%d", nmInt(spec, "replicas")))
	rows = rowIf(rows, "Ready", fmt.Sprintf("%d", nmInt(status, "readyReplicas")))
	rows = rowIf(rows, "Available", fmt.Sprintf("%d", nmInt(status, "availableReplicas")))
	return []InfoSection{{"Replicas", rows}}, extractConditions(status)
}

func infoForStatefulSet(spec, status map[string]interface{}) ([]InfoSection, []InfoCondition) {
	var rows []InfoRow
	rows = rowIf(rows, "Replicas (desired)", fmt.Sprintf("%d", nmInt(spec, "replicas")))
	rows = rowIf(rows, "Ready", fmt.Sprintf("%d", nmInt(status, "readyReplicas")))
	rows = rowIf(rows, "Current", fmt.Sprintf("%d", nmInt(status, "currentReplicas")))
	rows = rowIf(rows, "Updated", fmt.Sprintf("%d", nmInt(status, "updatedReplicas")))
	rows = rowIf(rows, "Service Name", nmStr(spec, "serviceName"))
	return []InfoSection{{"Replicas", rows}}, extractConditions(status)
}

func infoForDaemonSet(spec, status map[string]interface{}) ([]InfoSection, []InfoCondition) {
	var rows []InfoRow
	rows = rowIf(rows, "Desired", fmt.Sprintf("%d", nmInt(status, "desiredNumberScheduled")))
	rows = rowIf(rows, "Current", fmt.Sprintf("%d", nmInt(status, "currentNumberScheduled")))
	rows = rowIf(rows, "Ready", fmt.Sprintf("%d", nmInt(status, "numberReady")))
	rows = rowIf(rows, "Available", fmt.Sprintf("%d", nmInt(status, "numberAvailable")))
	rows = rowIf(rows, "Unavailable", fmt.Sprintf("%d", nmInt(status, "numberUnavailable")))
	return []InfoSection{{"Node Schedule", rows}}, extractConditions(status)
}

func infoForJob(spec, status map[string]interface{}) ([]InfoSection, []InfoCondition) {
	var rows []InfoRow
	if comp := spec["completions"]; comp != nil {
		rows = rowIf(rows, "Completions", fmt.Sprintf("%v", comp))
	}
	rows = rowIf(rows, "Active", fmt.Sprintf("%d", nmInt(status, "active")))
	rows = rowIf(rows, "Succeeded", fmt.Sprintf("%d", nmInt(status, "succeeded")))
	rows = rowIf(rows, "Failed", fmt.Sprintf("%d", nmInt(status, "failed")))
	rows = rowIf(rows, "Start Time", nmStr(status, "startTime"))
	rows = rowIf(rows, "Completion Time", nmStr(status, "completionTime"))
	return []InfoSection{{"Status", rows}}, extractConditions(status)
}

func infoForCronJob(spec, status map[string]interface{}) ([]InfoSection, []InfoCondition) {
	var rows []InfoRow
	rows = rowIf(rows, "Schedule", nmStr(spec, "schedule"))
	rows = rowIf(rows, "Suspend", nmStr(spec, "suspend"))
	rows = rowIf(rows, "Last Schedule", nmStr(status, "lastScheduleTime"))
	rows = rowIf(rows, "Last Successful", nmStr(status, "lastSuccessfulTime"))
	rows = rowIf(rows, "Active Jobs", fmt.Sprintf("%d", len(nmSlice(status, "active"))))
	return []InfoSection{{"CronJob", rows}}, nil
}

func infoForService(spec, status map[string]interface{}) ([]InfoSection, []InfoCondition) {
	var rows []InfoRow
	rows = rowIf(rows, "Type", nmStr(spec, "type"))
	rows = rowIf(rows, "Cluster IP", nmStr(spec, "clusterIP"))
	if eips := nmSlice(spec, "externalIPs"); len(eips) > 0 {
		var ips []string
		for _, ip := range eips {
			ips = append(ips, fmt.Sprintf("%v", ip))
		}
		rows = rowIf(rows, "External IPs", strings.Join(ips, ", "))
	}
	var portStrs []string
	for _, raw := range nmSlice(spec, "ports") {
		p, _ := raw.(map[string]interface{})
		if p == nil {
			continue
		}
		portStrs = append(portStrs, fmt.Sprintf("%s:%v→%v/%s",
			nmStr(p, "name"), nmStr(p, "port"), nmStr(p, "targetPort"), nmStr(p, "protocol")))
	}
	if len(portStrs) > 0 {
		rows = rowIf(rows, "Ports", strings.Join(portStrs, "  "))
	}
	rows = rowIf(rows, "Session Affinity", nmStr(spec, "sessionAffinity"))
	return []InfoSection{{"Service", rows}}, nil
}

func handleAPIObject(w http.ResponseWriter, r *http.Request) {
	// /api/object/{namespace}/{kind}/{name}
	parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/api/object/"), "/", 3)
	if len(parts) < 3 {
		http.Error(w, "invalid path", http.StatusBadRequest)
		return
	}
	ns, kind, name := parts[0], parts[1], parts[2]
	info := fetchObjectInfo(ns, kind, name)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(info)
}

func handleAPIStatus(w http.ResponseWriter, r *http.Request) {
	workerState.RLock()
	total := workerState.total
	ready := workerState.ready
	workerState.RUnlock()

	loaded := 0
	nsCache.Range(func(_, _ any) bool { loaded++; return true })

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"loaded": loaded,
		"total":  total,
		"ready":  ready,
	})
}

func handleAPIEvents(w http.ResponseWriter, r *http.Request) {
	dashboard.RLock()
	data := dashboard.data
	dashboard.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

// handleRefresh just returns the current cached state immediately.
// The background worker will continue refreshing namespaces on its own schedule.
func handleRefresh(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/", http.StatusFound)
}

// ---- Templates ----

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
}

func mustParseTemplate(name, file string) *template.Template {
	return template.Must(
		template.New(name).Funcs(tmplFuncs).ParseFS(templateFS, file),
	)
}

var (
	dashboardTmpl = mustParseTemplate("dashboard.html", "templates/dashboard.html")
	namespaceTmpl = mustParseTemplate("namespace.html", "templates/namespace.html")
	objectTmpl    = mustParseTemplate("object.html", "templates/object.html")
	loadingTmpl   = mustParseTemplate("loading.html", "templates/loading.html")
)
