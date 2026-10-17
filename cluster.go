package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

// A clusterState holds everything KubeHealth knows about one kubeconfig
// context: API clients, event caches, the warning feed, owner lookups and
// discovered resource types. One cluster is active at a time and only its
// worker polls. Clusters you switch away from keep their caches, so switching
// back is instant, and pages already open for them keep working.
type clusterState struct {
	name    string // kubeconfig context
	server  string // API server URL
	client  kubernetes.Interface
	dynamic dynamic.Interface

	nsCache sync.Map // namespace → *nsEntry

	dashboard struct {
		sync.RWMutex
		data *DashboardData
	}

	// worker tracks namespace discovery and the sweep's progress.
	worker struct {
		sync.RWMutex
		total      int
		namespaces []string // last resolved namespace list, sorted
		ready      bool     // every known namespace fetched at least once
		lastErr    string   // last namespace-list error, cleared on success
	}

	feed      warnFeedState
	owners    ownerState
	brief     briefState
	snapshots snapshotCache // live namespace snapshots, cached 30 s

	gvrCache   sync.Map // lower-case kind → schema.GroupVersionResource
	gvrDiscoMu sync.Mutex

	runMu     sync.Mutex
	stopRun   context.CancelFunc // nil while the cluster is not active
	stoppedAt time.Time
}

// contextHeader carries the cluster a page was rendered for, so the page's
// API calls keep going to that cluster after KubeHealth switches.
const contextHeader = "X-KubeHealth-Context"

var clusters = struct {
	sync.RWMutex
	active *clusterState
	byName map[string]*clusterState
}{byName: map[string]*clusterState{}}

func activeCluster() *clusterState {
	clusters.RLock()
	defer clusters.RUnlock()
	return clusters.active
}

func loadedCluster(name string) *clusterState {
	clusters.RLock()
	defer clusters.RUnlock()
	return clusters.byName[name]
}

// pageCluster is the cluster a page shows: the one named by ?ctx= when it is
// loaded, otherwise the active one. Every page says which cluster it shows.
func pageCluster(r *http.Request) *clusterState {
	if name := r.URL.Query().Get("ctx"); name != "" {
		if c := loadedCluster(name); c != nil {
			return c
		}
	}
	return activeCluster()
}

// apiCluster is the cluster an API call is about. Pages send the context they
// were rendered for; if KubeHealth no longer has it loaded (it restarted), the
// call fails with 409 rather than silently reading another cluster.
func apiCluster(w http.ResponseWriter, r *http.Request) *clusterState {
	name := r.Header.Get(contextHeader)
	if name == "" {
		name = r.URL.Query().Get("ctx")
	}
	if name == "" {
		return activeCluster()
	}
	if c := loadedCluster(name); c != nil {
		return c
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusConflict)
	json.NewEncoder(w).Encode(map[string]string{
		"error": fmt.Sprintf("This page belongs to context %s, which KubeHealth no longer has loaded. Reload the page.", name),
	})
	return nil
}

// ---- Kubeconfig ----------------------------------------------------------

// kubeconfigRules honours -kubeconfig and KUBECONFIG, including a
// colon-separated list of files.
func kubeconfigRules() *clientcmd.ClientConfigLoadingRules {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if paths := filepath.SplitList(cfg.Kubeconfig); len(paths) == 1 {
		rules.ExplicitPath = paths[0]
	} else if len(paths) > 1 {
		rules.Precedence = paths
	}
	return rules
}

func newClusterState(name string) (*clusterState, error) {
	cc := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(kubeconfigRules(), &clientcmd.ConfigOverrides{CurrentContext: name})
	rc, err := cc.ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("context %s: %w", name, err)
	}
	// Conservative rate limit: the worker makes one list call per tick anyway,
	// but this caps incidental bursts (owner lookups, YAML drill-downs).
	rc.QPS = 5
	rc.Burst = 10
	cs, err := kubernetes.NewForConfig(rc)
	if err != nil {
		return nil, fmt.Errorf("context %s: %w", name, err)
	}
	dyn, err := dynamic.NewForConfig(rc)
	if err != nil {
		return nil, fmt.Errorf("context %s: %w", name, err)
	}
	return &clusterState{name: name, server: rc.Host, client: cs, dynamic: dyn}, nil
}

// initClusters loads the kubeconfig's current context and makes it active.
// KubeHealth never changes the kubeconfig itself: switching only changes
// which context this process watches.
func initClusters() error {
	raw, err := kubeconfigRules().Load()
	if err != nil {
		return fmt.Errorf("load kubeconfig: %w", err)
	}
	if raw.CurrentContext == "" {
		return errors.New("kubeconfig has no current-context")
	}
	_, err = switchCluster(raw.CurrentContext)
	return err
}

// switchCluster makes the named context active: its worker starts (or
// resumes) and the previous cluster's worker stops, keeping its caches.
func switchCluster(name string) (*clusterState, error) {
	clusters.Lock()
	defer clusters.Unlock()
	if a := clusters.active; a != nil && a.name == name {
		return a, nil
	}
	c := clusters.byName[name]
	if c == nil {
		var err error
		if c, err = newClusterState(name); err != nil {
			return nil, err
		}
		clusters.byName[name] = c
	}
	if old := clusters.active; old != nil {
		old.stop()
	}
	c.start()
	clusters.active = c
	log.Printf("Watching context %s (%s)", name, c.server)
	return c, nil
}

func (c *clusterState) start() {
	c.runMu.Lock()
	defer c.runMu.Unlock()
	if c.stopRun != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	c.stopRun = cancel
	go c.runWorker(ctx)
	go c.runOwnerResolver(ctx)
}

func (c *clusterState) stop() {
	c.runMu.Lock()
	defer c.runMu.Unlock()
	if c.stopRun != nil {
		c.stopRun()
		c.stopRun = nil
		c.stoppedAt = time.Now()
	}
}

func (c *clusterState) isActive() bool {
	c.runMu.Lock()
	defer c.runMu.Unlock()
	return c.stopRun != nil
}

// ---- Identity and environment --------------------------------------------

type clusterInfo struct {
	Context   string `json:"context"`
	Env       string `json:"env"`        // prod, nonprod, stage, dev, local or unknown
	EnvSource string `json:"env_source"` // "name" (classified) or "settings" (chosen by the user)
	Server    string `json:"server"`
	Active    bool   `json:"active"` // this cluster is the one being polled
	Error     string `json:"error,omitempty"`
}

func (c *clusterState) identity() clusterInfo {
	env, src := envFor(c.name)
	c.worker.RLock()
	errMsg := c.worker.lastErr
	c.worker.RUnlock()
	return clusterInfo{Context: c.name, Env: env, EnvSource: src, Server: c.server, Active: c.isActive(), Error: errMsg}
}

var validEnvs = map[string]bool{"prod": true, "nonprod": true, "stage": true, "dev": true, "local": true}

// envOf classifies a kubeconfig context name. It splits the name into tokens
// and checks non-production words first: "nprd" contains "prd", and a plain
// substring match would call it production.
func envOf(name string) string {
	lower := strings.ToLower(name)
	toks := strings.FieldsFunc(lower, func(r rune) bool { return strings.ContainsRune("-_.:/@", r) })
	has := func(words ...string) bool {
		for _, t := range toks {
			base := strings.TrimRight(t, "0123456789")
			for _, w := range words {
				if base == w {
					return true
				}
			}
		}
		return false
	}
	switch {
	case has("nprd", "nonprod", "preprod") || strings.Contains(lower, "non-prod") || strings.Contains(lower, "pre-prod"):
		return "nonprod"
	case has("prod", "prd", "production", "live"):
		return "prod"
	case len(toks) > 0 && (toks[0] == "kind" || toks[0] == "minikube" || toks[0] == "k3d") || lower == "docker-desktop":
		return "local"
	case has("stg", "stage", "staging", "uat", "qa"):
		return "stage"
	case has("dev", "test", "sandbox"):
		return "dev"
	}
	return "unknown"
}

// envFor is the environment shown for a context: the one chosen in
// Settings, else the one read from its name.
func envFor(name string) (env, source string) {
	prefs.Lock()
	override := prefs.ContextEnv[name]
	prefs.Unlock()
	if override != "" {
		return override, "settings"
	}
	return envOf(name), "name"
}

// ---- Saved preferences ---------------------------------------------------

// prefs are the few choices KubeHealth remembers across restarts. Runtime
// settings (log lines, timeouts) are deliberately not saved.
var prefs = struct {
	sync.Mutex
	ContextEnv map[string]string `json:"context_env"` // context → env chosen in Settings
}{ContextEnv: map[string]string{}}

func prefsPath() string {
	dir, err := os.UserConfigDir()
	if err != nil {
		return ""
	}
	return filepath.Join(dir, "kubehealth", "prefs.json")
}

func loadPrefs() {
	path := prefsPath()
	if path == "" {
		return
	}
	b, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("Preferences: %v", err)
		}
		return
	}
	prefs.Lock()
	defer prefs.Unlock()
	if err := json.Unmarshal(b, &prefs); err != nil {
		log.Printf("Preferences: %s is not valid JSON, ignoring it: %v", path, err)
	}
	if prefs.ContextEnv == nil {
		prefs.ContextEnv = map[string]string{}
	}
}

// setContextEnv saves an environment override; env "" goes back to reading
// the context name.
func setContextEnv(name, env string) error {
	if env != "" && !validEnvs[env] {
		return fmt.Errorf("unknown environment %q", env)
	}
	prefs.Lock()
	defer prefs.Unlock()
	if env == "" {
		delete(prefs.ContextEnv, name)
	} else {
		prefs.ContextEnv[name] = env
	}
	path := prefsPath()
	if path == "" {
		return errors.New("no user config directory to save preferences in")
	}
	b, err := json.MarshalIndent(&prefs, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// ---- HTTP ----------------------------------------------------------------

type contextEntry struct {
	Name      string `json:"name"`
	Env       string `json:"env"`
	EnvSource string `json:"env_source"`
	Server    string `json:"server,omitempty"`
	Active    bool   `json:"active"`
	Loaded    bool   `json:"loaded"`           // KubeHealth has data for it in memory
	IdleSec   int    `json:"idle_s,omitempty"` // seconds since it stopped being polled
	Detected  string `json:"detected_env"`     // what the name alone says
}

// handleAPIContexts lists the kubeconfig's contexts. The kubeconfig is read
// again on every call, so new credentials show up without a restart.
func handleAPIContexts(w http.ResponseWriter, r *http.Request) {
	raw, err := kubeconfigRules().Load()
	if err != nil {
		http.Error(w, "load kubeconfig: "+err.Error(), http.StatusInternalServerError)
		return
	}
	var out []contextEntry
	for name, kc := range raw.Contexts {
		e := contextEntry{Name: name, Detected: envOf(name)}
		e.Env, e.EnvSource = envFor(name)
		if cl := raw.Clusters[kc.Cluster]; cl != nil {
			e.Server = cl.Server
		}
		if c := loadedCluster(name); c != nil {
			e.Loaded = true
			e.Active = c.isActive()
			if !e.Active {
				c.runMu.Lock()
				e.IdleSec = int(time.Since(c.stoppedAt).Seconds())
				c.runMu.Unlock()
			}
		}
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"contexts": out, "active": activeCluster().name, "prefs_path": prefsPath()})
}

// handleAPIContext switches the watched cluster: POST {"name": "..."}.
func handleAPIContext(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Name == "" {
		http.Error(w, "expected {\"name\": \"<context>\"}", http.StatusBadRequest)
		return
	}
	raw, err := kubeconfigRules().Load()
	if err != nil {
		http.Error(w, "load kubeconfig: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if _, ok := raw.Contexts[body.Name]; !ok {
		http.Error(w, "no context "+body.Name+" in the kubeconfig", http.StatusNotFound)
		return
	}
	c, err := switchCluster(body.Name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(c.identity())
}

// handleAPIContextEnv sets or clears a context's environment:
// POST {"name": "...", "env": "prod"}; env "" reads it from the name again.
func handleAPIContextEnv(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		Name string `json:"name"`
		Env  string `json:"env"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Name == "" {
		http.Error(w, "expected {\"name\": \"<context>\", \"env\": \"prod|nonprod|stage|dev|local|\"}", http.StatusBadRequest)
		return
	}
	if err := setContextEnv(body.Name, body.Env); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	env, src := envFor(body.Name)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"name": body.Name, "env": env, "env_source": src})
}
