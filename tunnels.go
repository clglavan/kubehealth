package main

// Tunnels. Some clusters answer only through a tunnel the user opens first,
// e.g. gcloud compute ssh <bastion> --tunnel-through-iap -- -D 1080 -N, an
// ssh -L forward or an SSM session. A tunnel is the user's full command plus
// how KubeHealth reaches the API through it: a proxy URL (socks5:// or
// http://), the API server's local address (an -L forward), or nothing (the
// command routes traffic itself). Tunnels are named, serve one or more
// contexts, and are saved in prefs.json; the kubeconfig is never changed.
//
// KubeHealth starts a context's tunnel when the context is picked, waits for
// its local port, restarts it if it drops while that cluster is watched, and
// stops everything it started when it exits. The commands are the user's own;
// the endpoints that define, start or show them answer only this machine's
// browser (localRequest).

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/rest"
)

const (
	tunnelProxy   = "proxy"   // HTTP or SOCKS5 proxy URL: ssh -D, gcloud … -- -D
	tunnelAddress = "address" // the API server's local address: ssh -L
	tunnelNone    = "none"    // the command routes traffic itself (sshuttle, VPN-style)

	tunnelReadyTimeout = 60 * time.Second
	tunnelOutputLines  = 200
)

// Tunnel states. "external": its port was already open, so KubeHealth uses
// it without having started it.
const (
	tsStopped  = "stopped"
	tsStarting = "starting"
	tsRunning  = "running"
	tsExternal = "external"
	tsExited   = "exited"
	tsFailed   = "failed"
)

// TunnelDef is a saved tunnel.
type TunnelDef struct {
	Name     string   `json:"name"`
	Command  string   `json:"command"`
	Mode     string   `json:"mode"`
	Proxy    string   `json:"proxy,omitempty"`
	Address  string   `json:"address,omitempty"`
	Contexts []string `json:"contexts"`
}

var tunnelNameRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

// endpoint is the host:port the tunnel opens locally, read from its proxy or
// address; "" when it has none (mode none).
func (d TunnelDef) endpoint() string {
	var raw string
	switch d.Mode {
	case tunnelProxy:
		raw = d.Proxy
	case tunnelAddress:
		raw = d.Address
	default:
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" {
		return ""
	}
	port := u.Port()
	if port == "" {
		port = map[string]string{"http": "80", "https": "443", "socks5": "1080", "socks5h": "1080"}[u.Scheme]
	}
	return net.JoinHostPort(u.Hostname(), port)
}

func (d TunnelDef) validate() error {
	if !tunnelNameRe.MatchString(d.Name) {
		return errors.New("name: letters, digits, '.', '_' or '-', up to 64")
	}
	if strings.TrimSpace(d.Command) == "" || len(d.Command) > 4096 {
		return errors.New("command: required, up to 4096 characters")
	}
	switch d.Mode {
	case tunnelProxy:
		u, err := url.Parse(d.Proxy)
		if err != nil || u.Hostname() == "" || !slices.Contains([]string{"http", "https", "socks5", "socks5h"}, u.Scheme) {
			return errors.New("proxy: a URL like socks5://127.0.0.1:1080 or http://127.0.0.1:8888")
		}
	case tunnelAddress:
		u, err := url.Parse(d.Address)
		if err != nil || u.Hostname() == "" || (u.Scheme != "https" && u.Scheme != "http") {
			return errors.New("address: a URL like https://127.0.0.1:16443")
		}
	case tunnelNone:
	default:
		return errors.New("mode: proxy, address or none")
	}
	if len(d.Contexts) == 0 {
		return errors.New("pick at least one cluster for this tunnel")
	}
	return nil
}

// ── Saved tunnels ───────────────────────────────────────────────────────────

func tunnelDefs() []TunnelDef {
	prefs.Lock()
	defer prefs.Unlock()
	return slices.Clone(prefs.Tunnels)
}

func tunnelByName(name string) (TunnelDef, bool) {
	for _, d := range tunnelDefs() {
		if d.Name == name {
			return d, true
		}
	}
	return TunnelDef{}, false
}

func tunnelForContext(ctx string) (TunnelDef, bool) {
	for _, d := range tunnelDefs() {
		if slices.Contains(d.Contexts, ctx) {
			return d, true
		}
	}
	return TunnelDef{}, false
}

// saveTunnel adds or replaces a tunnel (renamed from previous, if set) and
// returns the contexts whose connection changes.
func saveTunnel(d TunnelDef, previous string) ([]string, error) {
	if err := d.validate(); err != nil {
		return nil, err
	}
	if previous == "" {
		previous = d.Name
	}
	prefs.Lock()
	defer prefs.Unlock()
	var changed []string
	out := prefs.Tunnels[:0:0]
	for _, t := range prefs.Tunnels {
		if t.Name == previous {
			changed = append(changed, t.Contexts...)
			continue
		}
		if t.Name == d.Name {
			return nil, fmt.Errorf("a tunnel named %s already exists", d.Name)
		}
		for _, c := range d.Contexts {
			if slices.Contains(t.Contexts, c) {
				return nil, fmt.Errorf("%s already uses tunnel %s", c, t.Name)
			}
		}
		if ep := d.endpoint(); ep != "" && ep == t.endpoint() {
			return nil, fmt.Errorf("tunnel %s already uses %s: give this one another port", t.Name, ep)
		}
		out = append(out, t)
	}
	prefs.Tunnels = append(out, d)
	changed = append(changed, d.Contexts...)
	if err := savePrefsLocked(); err != nil {
		return nil, err
	}
	slices.Sort(changed)
	return slices.Compact(changed), nil
}

func deleteTunnel(name string) ([]string, error) {
	prefs.Lock()
	defer prefs.Unlock()
	var changed []string
	out := prefs.Tunnels[:0:0]
	for _, t := range prefs.Tunnels {
		if t.Name == name {
			changed = t.Contexts
			continue
		}
		out = append(out, t)
	}
	if changed == nil {
		return nil, fmt.Errorf("no tunnel %s", name)
	}
	prefs.Tunnels = out
	return changed, savePrefsLocked()
}

// applyTunnel makes a cluster's client go through its tunnel: through the
// proxy, or to the API server's local address, still checking the server's
// certificate against its real name.
func applyTunnel(rc *rest.Config, d TunnelDef) {
	switch d.Mode {
	case tunnelProxy:
		if u, err := url.Parse(d.Proxy); err == nil {
			rc.Proxy = http.ProxyURL(u)
		}
	case tunnelAddress:
		orig, err1 := url.Parse(rc.Host)
		addr, err2 := url.Parse(d.Address)
		if err1 != nil || err2 != nil {
			return
		}
		if rc.TLSClientConfig.ServerName == "" {
			rc.TLSClientConfig.ServerName = orig.Hostname()
		}
		orig.Scheme, orig.Host = addr.Scheme, addr.Host
		rc.Host = orig.String()
	}
}

// ── Running tunnels ─────────────────────────────────────────────────────────

type tunnelRun struct {
	mu        sync.Mutex
	state     string
	err       string
	pid       int
	startedAt time.Time
	readyAt   time.Time
	exitCode  int
	out       []string
	cmd       *exec.Cmd
	done      chan struct{} // closed when cmd has exited
	keep      bool          // restart if it drops while a cluster that uses it is watched
	restarts  int
	gen       int // bumps on every start, so an old process can't touch a new one
}

var tunnelRuns = struct {
	sync.Mutex
	m map[string]*tunnelRun
}{m: map[string]*tunnelRun{}}

func runFor(name string) *tunnelRun {
	tunnelRuns.Lock()
	defer tunnelRuns.Unlock()
	r := tunnelRuns.m[name]
	if r == nil {
		r = &tunnelRun{state: tsStopped}
		tunnelRuns.m[name] = r
	}
	return r
}

func tunnelState(name string) string {
	r := runFor(name)
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state
}

func (r *tunnelRun) logLocked(line string) {
	r.out = append(r.out, line)
	if n := len(r.out) - tunnelOutputLines; n > 0 {
		r.out = slices.Delete(r.out, 0, n)
	}
}

func (r *tunnelRun) log(line string) {
	r.mu.Lock()
	r.logLocked(line)
	r.mu.Unlock()
}

func portOpen(addr string) bool {
	c, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
	if err != nil {
		return false
	}
	c.Close()
	return true
}

// startTunnel starts a tunnel unless it is up already, and returns at once.
// keep marks it to be restarted if it drops while it is in use.
func startTunnel(name string, keep bool) error {
	def, ok := tunnelByName(name)
	if !ok {
		return fmt.Errorf("no tunnel %s", name)
	}
	r := runFor(name)
	r.mu.Lock()
	defer r.mu.Unlock()
	if keep {
		r.keep = true
	}
	switch r.state {
	case tsStarting, tsRunning, tsExternal:
		return nil
	}
	r.restarts = 0
	if ep := def.endpoint(); ep != "" && portOpen(ep) {
		r.state, r.err, r.pid = tsExternal, "", 0
		r.logLocked(ep + " is already open: using it (KubeHealth did not start it)")
		return nil
	}
	return r.spawnLocked(def)
}

func (r *tunnelRun) spawnLocked(def TunnelDef) error {
	cmd := shellCommand(def.Command)
	// No terminal to answer prompts: gcloud takes its defaults instead of waiting.
	cmd.Env = append(os.Environ(), "CLOUDSDK_CORE_DISABLE_PROMPTS=1")
	newProcessGroup(cmd)
	pr, pw, err := os.Pipe()
	if err != nil {
		return err
	}
	cmd.Stdout, cmd.Stderr = pw, pw
	r.gen++
	gen := r.gen
	r.logLocked("$ " + strings.Join(strings.Fields(strings.ReplaceAll(def.Command, "\\\n", " ")), " "))
	if err := cmd.Start(); err != nil {
		pw.Close()
		pr.Close()
		r.state, r.err = tsFailed, err.Error()
		r.logLocked("could not start: " + err.Error())
		return err
	}
	pw.Close()
	done := make(chan struct{})
	r.cmd, r.done, r.pid = cmd, done, cmd.Process.Pid
	r.state, r.err, r.exitCode = tsStarting, "", 0
	r.startedAt, r.readyAt = time.Now(), time.Time{}
	log.Printf("Tunnel %s: started (pid %d)", def.Name, r.pid)

	go func() { // its output, a line at a time
		buf := make([]byte, 4096)
		var partial string
		for {
			n, err := pr.Read(buf)
			partial += string(buf[:n])
			for {
				i := strings.IndexAny(partial, "\r\n")
				if i < 0 {
					break
				}
				if line := strings.TrimRight(partial[:i], " \t"); line != "" {
					r.log(line)
				}
				partial = partial[i+1:]
			}
			if err != nil {
				if strings.TrimSpace(partial) != "" {
					r.log(partial)
				}
				pr.Close()
				return
			}
		}
	}()
	go func() {
		err := cmd.Wait()
		close(done)
		r.exited(def.Name, gen, err)
	}()
	go r.watchReady(def, gen)
	return nil
}

// watchReady marks the tunnel running once its local port accepts
// connections (after a moment, for a tunnel without one).
func (r *tunnelRun) watchReady(def TunnelDef, gen int) {
	ep := def.endpoint()
	start := time.Now()
	for time.Since(start) < tunnelReadyTimeout {
		r.mu.Lock()
		if r.gen != gen || r.state != tsStarting {
			r.mu.Unlock()
			return
		}
		r.mu.Unlock()
		ready := ep == "" && time.Since(start) >= 3*time.Second
		if ep != "" {
			ready = portOpen(ep)
		}
		if ready {
			r.mu.Lock()
			if r.gen == gen && r.state == tsStarting {
				r.state, r.readyAt = tsRunning, time.Now()
				if ep != "" {
					r.logLocked(fmt.Sprintf("%s is open after %.1f s", ep, time.Since(start).Seconds()))
				}
			}
			r.mu.Unlock()
			return
		}
		time.Sleep(300 * time.Millisecond)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.gen == gen && r.state == tsStarting {
		r.state = tsFailed
		r.err = fmt.Sprintf("%s did not open within %s", ep, tunnelReadyTimeout)
		r.logLocked(r.err)
		if r.cmd != nil {
			go terminate(r.cmd, r.done, time.Second)
		}
	}
}

// exited records how a tunnel's process ended. A tunnel that was up and
// drops is restarted while a watched cluster uses it; one that fails before
// it is up says why and waits for the user (a login to renew, a typo).
func (r *tunnelRun) exited(name string, gen int, err error) {
	r.mu.Lock()
	if r.gen != gen {
		r.mu.Unlock()
		return
	}
	code, how := 0, "exited with code 0"
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		code, how = ee.ExitCode(), "exited with code "+fmt.Sprint(ee.ExitCode())
		if code == -1 { // killed by a signal: "signal: terminated"
			how = "stopped by " + ee.Error() + ", from outside KubeHealth"
		}
	}
	r.cmd, r.pid = nil, 0
	if r.state == tsStopped {
		r.mu.Unlock()
		return
	}
	if r.state != tsFailed {
		r.state = tsExited
		r.err = tunnelHint(r.out, how)
	}
	r.exitCode = code
	r.logLocked(how)
	wasUp := !r.readyAt.IsZero()
	if wasUp && time.Since(r.readyAt) > 2*time.Minute {
		r.restarts = 0
	}
	keep, n := r.keep, r.restarts
	r.mu.Unlock()
	log.Printf("Tunnel %s: %s", name, how)

	if !keep || !wasUp || !tunnelInUse(name) {
		return
	}
	delay := tunnelRestartDelays[min(n, len(tunnelRestartDelays)-1)]
	r.log(fmt.Sprintf("restarting in %s", delay))
	time.AfterFunc(delay, func() {
		def, ok := tunnelByName(name)
		r.mu.Lock()
		defer r.mu.Unlock()
		if !ok || r.gen != gen || r.state != tsExited || !r.keep || !tunnelInUse(name) {
			return
		}
		r.restarts++
		r.spawnLocked(def)
	})
}

// tunnelRestartDelays: how long a dropped tunnel waits before each restart.
var tunnelRestartDelays = []time.Duration{5 * time.Second, 15 * time.Second, time.Minute}

// tunnelInUse: the cluster KubeHealth watches goes through this tunnel.
var tunnelInUse = func(name string) bool {
	a := activeCluster()
	if a == nil {
		return false
	}
	d, ok := tunnelForContext(a.name)
	return ok && d.Name == name
}

// tunnelHint says why a tunnel probably failed, from its last output, or
// else how it ended.
func tunnelHint(out []string, how string) string {
	text := strings.ToLower(strings.Join(out[max(0, len(out)-30):], "\n"))
	switch {
	case strings.Contains(text, "reauthentication") || strings.Contains(text, "gcloud auth login"):
		return "gcloud needs you to sign in again: run `gcloud auth login` in a terminal, then start the tunnel."
	case strings.Contains(text, "command not found") || strings.Contains(text, "not found") && strings.Contains(text, "sh:"):
		return "Command not found: is it on the PATH KubeHealth was started with?"
	case strings.Contains(text, "address already in use"):
		return "Its local port is already in use by another program."
	case strings.Contains(text, "permission denied (publickey"):
		return "SSH refused the key."
	case strings.Contains(text, "could not resolve hostname") || strings.Contains(text, "name or service not known"):
		return "The host in the command could not be resolved."
	}
	return strings.ToUpper(how[:1]) + how[1:] + "."
}

// stopTunnel stops a tunnel KubeHealth started.
func stopTunnel(name string) error {
	r := runFor(name)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.keep = false
	switch {
	case r.state == tsExternal:
		r.state = tsStopped
		r.logLocked("no longer used (KubeHealth did not start it, so it keeps running)")
		return nil
	case r.cmd == nil:
		r.state = tsStopped
		return nil
	}
	r.state, r.err = tsStopped, ""
	r.logLocked("stopped")
	go terminate(r.cmd, r.done, 3*time.Second)
	return nil
}

// terminate asks a process group to stop, and kills it after grace.
func terminate(cmd *exec.Cmd, done chan struct{}, grace time.Duration) {
	stopProcessGroup(cmd, false)
	select {
	case <-done:
	case <-time.After(grace):
		stopProcessGroup(cmd, true)
	}
}

// stopAllTunnels stops every tunnel KubeHealth started; at exit.
func stopAllTunnels() {
	tunnelRuns.Lock()
	var wg sync.WaitGroup
	for name, r := range tunnelRuns.m {
		r.mu.Lock()
		if r.cmd != nil {
			r.state, r.keep = tsStopped, false
			log.Printf("Tunnel %s: stopping", name)
			wg.Add(1)
			go func(cmd *exec.Cmd, done chan struct{}) {
				defer wg.Done()
				terminate(cmd, done, 2*time.Second)
			}(r.cmd, r.done)
		}
		r.mu.Unlock()
	}
	tunnelRuns.Unlock()
	wg.Wait()
}

// startContextTunnel starts the tunnel a context uses, if any, and reports
// whether it is ready to use.
func startContextTunnel(ctx string) (name string, ready bool, err error) {
	d, ok := tunnelForContext(ctx)
	if !ok {
		return "", true, nil
	}
	if err := startTunnel(d.Name, true); err != nil {
		return d.Name, false, err
	}
	st := tunnelState(d.Name)
	return d.Name, st == tsRunning || st == tsExternal, nil
}

// reloadClusters rebuilds the clients of loaded contexts, after their tunnel
// changed. The watched one starts again right away.
func reloadClusters(names []string) {
	clusters.Lock()
	defer clusters.Unlock()
	for _, n := range names {
		c := clusters.byName[n]
		if c == nil {
			continue
		}
		active := clusters.active == c
		c.stop()
		nc, err := newClusterState(n)
		if err != nil {
			log.Printf("Tunnel: reload %s: %v", n, err)
			delete(clusters.byName, n)
			continue
		}
		clusters.byName[n] = nc
		if active {
			nc.start()
			clusters.active = nc
		}
	}
}

// ── API ─────────────────────────────────────────────────────────────────────

// localRequest admits only this machine's browser, on KubeHealth's own page:
// a loopback client, a localhost Host (no DNS rebinding) and, for changes, a
// same-origin JSON request (no other site's form or fetch). Tunnels run
// commands, so their endpoints must not answer anyone else.
func localRequest(w http.ResponseWriter, r *http.Request) bool {
	client, _, _ := net.SplitHostPort(r.RemoteAddr)
	ip := net.ParseIP(client)
	host := r.Host
	if h, _, err := net.SplitHostPort(r.Host); err == nil {
		host = h
	}
	host = strings.Trim(host, "[]")
	if ip == nil || !ip.IsLoopback() || (host != "localhost" && host != "127.0.0.1" && host != "::1") {
		http.Error(w, "tunnels can be managed only from this machine, at http://localhost:"+cfg.Port, http.StatusForbidden)
		return false
	}
	if r.Method != http.MethodGet {
		if r.Header.Get("Origin") != "http://"+r.Host || !strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") {
			http.Error(w, "refused: not a request from KubeHealth's own page", http.StatusForbidden)
			return false
		}
	}
	return true
}

type tunnelView struct {
	TunnelDef
	State    string   `json:"state"`
	Error    string   `json:"error,omitempty"`
	PID      int      `json:"pid,omitempty"`
	UpSec    int      `json:"up_s,omitempty"`
	ExitCode int      `json:"exit_code,omitempty"`
	Restarts int      `json:"restarts,omitempty"`
	Endpoint string   `json:"endpoint,omitempty"`
	Output   []string `json:"output"`
}

func viewTunnel(d TunnelDef) tunnelView {
	r := runFor(d.Name)
	r.mu.Lock()
	defer r.mu.Unlock()
	v := tunnelView{TunnelDef: d, State: r.state, Error: r.err, PID: r.pid, ExitCode: r.exitCode,
		Restarts: r.restarts, Endpoint: d.endpoint(), Output: slices.Clone(r.out)}
	if v.Output == nil {
		v.Output = []string{}
	}
	if (r.state == tsRunning || r.state == tsStarting) && !r.startedAt.IsZero() {
		v.UpSec = int(time.Since(r.startedAt).Seconds())
	}
	return v
}

// handleAPITunnels serves
//
//	GET    /api/tunnels[?context=X]   every tunnel and its state; with a
//	                                  context, what the dialog needs about it
//	POST   /api/tunnels               create or edit {…, "previous": old name}
//	DELETE /api/tunnels/{name}
//	POST   /api/tunnels/{name}/start | /stop
func handleAPITunnels(w http.ResponseWriter, r *http.Request) {
	if !localRequest(w, r) {
		return
	}
	rest := strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/tunnels"), "/")
	name, action, _ := strings.Cut(rest, "/")
	w.Header().Set("Content-Type", "application/json")
	fail := func(code int, err error) {
		w.WriteHeader(code)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
	}

	switch {
	case r.Method == http.MethodGet && name == "":
		out := map[string]any{"tunnels": tunnelList()}
		if c := r.URL.Query().Get("context"); c != "" {
			out["context"] = contextForTunnel(c)
		}
		json.NewEncoder(w).Encode(out)

	case r.Method == http.MethodPost && name == "":
		var body struct {
			TunnelDef
			Previous string `json:"previous"`
			Start    bool   `json:"start"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&body); err != nil {
			fail(http.StatusBadRequest, err)
			return
		}
		body.Command = strings.TrimSpace(body.Command)
		if err := checkContexts(body.Contexts); err != nil {
			fail(http.StatusBadRequest, err)
			return
		}
		old, existed := tunnelByName(body.Previous)
		if existed && (old.Command != body.Command || old.Mode != body.Mode || old.endpoint() != body.endpoint()) {
			stopTunnel(old.Name) // its definition changed: the next start uses the new one
		}
		changed, err := saveTunnel(body.TunnelDef, body.Previous)
		if err != nil {
			fail(http.StatusBadRequest, err)
			return
		}
		if body.Previous != "" && body.Previous != body.Name {
			tunnelRuns.Lock()
			delete(tunnelRuns.m, body.Previous)
			tunnelRuns.Unlock()
		}
		reloadClusters(changed)
		if body.Start {
			if err := startTunnel(body.Name, true); err != nil {
				fail(http.StatusBadGateway, err)
				return
			}
		}
		d, _ := tunnelByName(body.Name)
		json.NewEncoder(w).Encode(viewTunnel(d))

	case r.Method == http.MethodDelete && name != "" && action == "":
		stopTunnel(name)
		changed, err := deleteTunnel(name)
		if err != nil {
			fail(http.StatusNotFound, err)
			return
		}
		tunnelRuns.Lock()
		delete(tunnelRuns.m, name)
		tunnelRuns.Unlock()
		reloadClusters(changed)
		json.NewEncoder(w).Encode(map[string]string{"status": "deleted"})

	case r.Method == http.MethodPost && name != "" && (action == "start" || action == "stop"):
		d, ok := tunnelByName(name)
		if !ok {
			fail(http.StatusNotFound, fmt.Errorf("no tunnel %s", name))
			return
		}
		var err error
		if action == "start" {
			err = startTunnel(name, true)
		} else {
			err = stopTunnel(name)
		}
		if err != nil {
			fail(http.StatusBadGateway, err)
			return
		}
		json.NewEncoder(w).Encode(viewTunnel(d))

	default:
		fail(http.StatusMethodNotAllowed, errors.New("unknown tunnels request"))
	}
}

func tunnelList() []tunnelView {
	out := []tunnelView{}
	for _, d := range tunnelDefs() {
		out = append(out, viewTunnel(d))
	}
	return out
}

// checkContexts: every context a tunnel serves is in the kubeconfig.
func checkContexts(names []string) error {
	raw, err := kubeconfigRules().Load()
	if err != nil {
		return fmt.Errorf("load kubeconfig: %w", err)
	}
	for _, n := range names {
		if _, ok := raw.Contexts[n]; !ok {
			return fmt.Errorf("no context %s in the kubeconfig", n)
		}
	}
	return nil
}

// tunnelContext is what the tunnel dialog shows about a context it opened for.
type tunnelContext struct {
	Name            string `json:"name"`
	Server          string `json:"server,omitempty"`
	KubeconfigProxy string `json:"kubeconfig_proxy,omitempty"` // the cluster's proxy-url, if set
	Error           string `json:"error,omitempty"`
	ErrorKind       string `json:"error_kind,omitempty"`
	Tunnel          string `json:"tunnel,omitempty"`
}

func contextForTunnel(name string) tunnelContext {
	tc := tunnelContext{Name: name}
	if raw, err := kubeconfigRules().Load(); err == nil {
		if kc := raw.Contexts[name]; kc != nil {
			if cl := raw.Clusters[kc.Cluster]; cl != nil {
				tc.Server, tc.KubeconfigProxy = cl.Server, cl.ProxyURL
			}
		}
	}
	if c := loadedCluster(name); c != nil {
		id := c.identity()
		tc.Error, tc.ErrorKind = id.Error, id.ErrorKind
	}
	if d, ok := tunnelForContext(name); ok {
		tc.Tunnel = d.Name
	}
	return tc
}

// tunnelBrief is a context's tunnel, in its identity (header, /api/home).
type tunnelBrief struct {
	Name  string `json:"name"`
	State string `json:"state"`
	Error string `json:"error,omitempty"`
}

func contextTunnelBrief(ctx string) *tunnelBrief {
	d, ok := tunnelForContext(ctx)
	if !ok {
		return nil
	}
	r := runFor(d.Name)
	r.mu.Lock()
	defer r.mu.Unlock()
	return &tunnelBrief{Name: d.Name, State: r.state, Error: r.err}
}

// ── Why a cluster doesn't answer ────────────────────────────────────────────

// errKind sorts an API error: "unreachable" (network: no route, refused,
// timeout, DNS, proxy), "auth", "tls" or "other". Only an unreachable cluster
// is offered a tunnel.
func errKind(err error) string {
	if err == nil {
		return ""
	}
	msg := strings.ToLower(err.Error())
	switch {
	case k8serrors.IsUnauthorized(err) || k8serrors.IsForbidden(err):
		return "auth"
	case strings.Contains(msg, "x509:") || strings.Contains(msg, "certificate"):
		return "tls"
	}
	var dnsErr *net.DNSError
	var opErr *net.OpError
	var netErr net.Error
	if errors.As(err, &dnsErr) || errors.As(err, &opErr) || (errors.As(err, &netErr) && netErr.Timeout()) ||
		errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.ErrUnexpectedEOF) {
		return "unreachable"
	}
	for _, s := range []string{"connection refused", "i/o timeout", "no route to host", "network is unreachable",
		"no such host", "proxyconnect", "socks connect", "tls handshake timeout", "connection reset", "eof"} {
		if strings.Contains(msg, s) {
			return "unreachable"
		}
	}
	return "other"
}
