package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
)

// fakeTunnel acts as a tunnel command: it prints a line, waits
// FAKE_TUNNEL_DELAY, then listens on FAKE_TUNNEL_ADDR until killed. With
// FAKE_TUNNEL_MODE=exit it fails the way gcloud does when its login expired;
// with child, a grandchild holds the port (only killing the group frees it).
func fakeTunnel() int {
	addr := os.Getenv("FAKE_TUNNEL_ADDR")
	fmt.Println("External IP address was not found; defaulting to using IAP tunneling.")
	if d, err := time.ParseDuration(os.Getenv("FAKE_TUNNEL_DELAY")); err == nil {
		time.Sleep(d)
	}
	switch os.Getenv("FAKE_TUNNEL_MODE") {
	case "drop": // up for a moment, then the connection drops
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			return 1
		}
		fmt.Println("Listening on " + addr)
		time.Sleep(300 * time.Millisecond)
		ln.Close()
		fmt.Fprintln(os.Stderr, "Connection to compute.7262871136 closed by remote host.")
		return 255
	case "exit":
		fmt.Fprintln(os.Stderr, "ERROR: (gcloud.compute.ssh) There was a problem refreshing your current auth tokens: Reauthentication required.")
		fmt.Fprintln(os.Stderr, "Please run:\n\n  $ gcloud auth login")
		return 1
	case "child":
		child := exec.Command(os.Args[0])
		child.Env = append(os.Environ(), "FAKE_TUNNEL_MODE=", "FAKE_TUNNEL_DELAY=0s")
		child.Stdout, child.Stderr = os.Stdout, os.Stderr
		if err := child.Start(); err != nil {
			fmt.Println(err)
			return 2
		}
		child.Wait()
		return 0
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "bind: "+err.Error())
		return 1
	}
	fmt.Println("Listening on " + addr)
	for {
		c, err := ln.Accept()
		if err != nil {
			return 0
		}
		c.Close()
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

// withTunnels replaces the saved tunnels and the running ones for one test,
// with prefs.json in a temporary home.
func withTunnels(t *testing.T, defs ...TunnelDef) {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", home)
	prefs.Lock()
	saved := prefs.Tunnels
	prefs.Tunnels = defs
	prefs.Unlock()
	t.Cleanup(func() {
		stopAllTunnels()
		prefs.Lock()
		prefs.Tunnels = saved
		prefs.Unlock()
		tunnelRuns.Lock()
		tunnelRuns.m = map[string]*tunnelRun{}
		tunnelRuns.Unlock()
	})
}

// fakeTunnelDef is a tunnel whose command is the test binary as fakeTunnel.
func fakeTunnelDef(t *testing.T, mode, delay string) TunnelDef {
	t.Helper()
	addr := freeAddr(t)
	t.Setenv("GO_WANT_FAKE_TUNNEL", "1")
	t.Setenv("FAKE_TUNNEL_ADDR", addr)
	t.Setenv("FAKE_TUNNEL_MODE", mode)
	t.Setenv("FAKE_TUNNEL_DELAY", delay)
	return TunnelDef{Name: "bastion", Command: "'" + os.Args[0] + "' \\\n  --tunnel-through-iap -- -D " + addr, Mode: tunnelProxy,
		Proxy: "socks5://" + addr, Contexts: []string{"private-gke"}}
}

func waitState(t *testing.T, name string, want ...string) string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if st := tunnelState(name); slices.Contains(want, st) {
			return st
		}
		time.Sleep(50 * time.Millisecond)
	}
	v := viewTunnel(TunnelDef{Name: name})
	t.Fatalf("tunnel %s: state %s, want %v; output %q", name, v.State, want, v.Output)
	return ""
}

func TestTunnelDefs(t *testing.T) {
	d := TunnelDef{Name: "b", Command: "x", Mode: tunnelProxy, Proxy: "socks5://127.0.0.1", Contexts: []string{"c"}}
	if d.endpoint() != "127.0.0.1:1080" || d.validate() != nil {
		t.Errorf("socks without a port: %q %v", d.endpoint(), d.validate())
	}
	d.Mode, d.Address = tunnelAddress, "https://127.0.0.1:16443"
	if d.endpoint() != "127.0.0.1:16443" || d.validate() != nil {
		t.Errorf("address: %q %v", d.endpoint(), d.validate())
	}
	d.Mode = tunnelNone
	if d.endpoint() != "" || d.validate() != nil {
		t.Errorf("none: %q %v", d.endpoint(), d.validate())
	}
	for _, bad := range []TunnelDef{
		{Name: "has space", Command: "x", Mode: tunnelNone, Contexts: []string{"c"}},
		{Name: "b", Command: " ", Mode: tunnelNone, Contexts: []string{"c"}},
		{Name: "b", Command: "x", Mode: tunnelProxy, Proxy: "ftp://h:1", Contexts: []string{"c"}},
		{Name: "b", Command: "x", Mode: tunnelAddress, Address: "127.0.0.1:6443", Contexts: []string{"c"}},
		{Name: "b", Command: "x", Mode: tunnelNone},
	} {
		if bad.validate() == nil {
			t.Errorf("accepted %+v", bad)
		}
	}
}

func TestApplyTunnel(t *testing.T) {
	rc := &rest.Config{Host: "https://172.16.0.2"}
	applyTunnel(rc, TunnelDef{Mode: tunnelProxy, Proxy: "socks5://127.0.0.1:1080"})
	u, _ := rc.Proxy(&http.Request{URL: &url.URL{Scheme: "https", Host: "172.16.0.2"}})
	if u == nil || u.String() != "socks5://127.0.0.1:1080" || rc.Host != "https://172.16.0.2" {
		t.Errorf("proxy: %v %s", u, rc.Host)
	}

	rc = &rest.Config{Host: "https://api.private.example:443/k8s/clusters/c-1"}
	applyTunnel(rc, TunnelDef{Mode: tunnelAddress, Address: "https://127.0.0.1:16443"})
	if rc.Host != "https://127.0.0.1:16443/k8s/clusters/c-1" || rc.TLSClientConfig.ServerName != "api.private.example" {
		t.Errorf("address: %s, server name %q", rc.Host, rc.TLSClientConfig.ServerName)
	}
}

func TestErrKind(t *testing.T) {
	gr := schema.GroupResource{Resource: "namespaces"}
	for err, want := range map[error]string{
		&url.Error{Op: "Get", URL: "https://10.0.0.1", Err: &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connect: connection refused")}}: "unreachable",
		&url.Error{Op: "Get", URL: "https://x", Err: &net.DNSError{Err: "no such host", Name: "x"}}:                                               "unreachable",
		fmt.Errorf("list: %w", context.DeadlineExceeded):                                                                                          "unreachable",
		errors.New(`Get "https://172.16.0.2/api": proxyconnect tcp: dial tcp 127.0.0.1:1080: connect: connection refused`):                        "unreachable",
		k8serrors.NewUnauthorized("token expired"):                                                                                                "auth",
		k8serrors.NewForbidden(gr, "", errors.New("no list")):                                                                                     "auth",
		errors.New("tls: failed to verify certificate: x509: certificate signed by unknown authority"):                                            "tls",
		errors.New("something else"): "other",
	} {
		if got := errKind(err); got != want {
			t.Errorf("%v: %s, want %s", err, got, want)
		}
	}
}

// Started, ready once its port opens, output kept, stopped.
func TestTunnelLifecycle(t *testing.T) {
	d := fakeTunnelDef(t, "", "300ms")
	withTunnels(t, d)
	if err := startTunnel(d.Name, false); err != nil {
		t.Fatal(err)
	}
	if st := tunnelState(d.Name); st != tsStarting {
		t.Errorf("state right after start: %s", st)
	}
	waitState(t, d.Name, tsRunning)
	v := viewTunnel(d)
	out := strings.Join(v.Output, "\n")
	if v.PID == 0 || !strings.Contains(out, "IAP tunneling") || !strings.Contains(out, "Listening on") || !strings.Contains(out, " is open after ") {
		t.Errorf("view: %+v", v)
	}
	if !strings.HasPrefix(v.Output[0], "$ '") || strings.Contains(v.Output[0], "\\") {
		t.Errorf("command line in the output: %q", v.Output[0])
	}
	stopTunnel(d.Name)
	waitPortClosed(t, d.endpoint())
	if st := tunnelState(d.Name); st != tsStopped {
		t.Errorf("after stop: %s", st)
	}
}

func waitPortClosed(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for portOpen(addr) {
		if time.Now().After(deadline) {
			t.Fatalf("%s still open", addr)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// What the command starts goes with it: gcloud runs ssh, which holds the port.
func TestTunnelStopsItsChildren(t *testing.T) {
	d := fakeTunnelDef(t, "child", "0s")
	withTunnels(t, d)
	startTunnel(d.Name, false)
	waitState(t, d.Name, tsRunning)
	stopTunnel(d.Name)
	waitPortClosed(t, d.endpoint())
}

// A tunnel that fails before it is up says why, and waits for the user.
func TestTunnelFailsToStart(t *testing.T) {
	d := fakeTunnelDef(t, "exit", "0s")
	withTunnels(t, d)
	oldDelays, oldInUse := tunnelRestartDelays, tunnelInUse
	tunnelRestartDelays = []time.Duration{50 * time.Millisecond}
	tunnelInUse = func(string) bool { return true }
	defer func() { tunnelRestartDelays, tunnelInUse = oldDelays, oldInUse }()

	startTunnel(d.Name, true)
	waitState(t, d.Name, tsExited)
	time.Sleep(300 * time.Millisecond)
	v := viewTunnel(d)
	if !strings.Contains(v.Error, "gcloud auth login") || v.ExitCode != 1 || v.Restarts != 0 || v.State != tsExited {
		t.Errorf("exit: %+v", v)
	}
}

// A tunnel that was up and drops comes back while its cluster is watched.
func TestTunnelDropAndRestart(t *testing.T) {
	d := fakeTunnelDef(t, "drop", "0s")
	withTunnels(t, d)
	oldDelays, oldInUse := tunnelRestartDelays, tunnelInUse
	tunnelRestartDelays = []time.Duration{100 * time.Millisecond}
	tunnelInUse = func(string) bool { return true }
	defer func() { tunnelRestartDelays, tunnelInUse = oldDelays, oldInUse }()

	startTunnel(d.Name, true)
	waitState(t, d.Name, tsRunning)
	waitState(t, d.Name, tsExited, tsStarting)
	if v := viewTunnel(d); v.ExitCode != 255 && v.State == tsExited {
		t.Errorf("drop: %+v", v)
	}
	deadline := time.Now().Add(5 * time.Second)
	for viewTunnel(d).Restarts < 2 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if viewTunnel(d).Restarts < 2 {
		t.Errorf("not restarted: %+v", viewTunnel(d))
	}
	stopTunnel(d.Name)
	n := viewTunnel(d).Restarts
	time.Sleep(400 * time.Millisecond)
	if viewTunnel(d).Restarts != n {
		t.Error("restarted after being stopped")
	}
}

// A port already open (a tunnel opened in a terminal) is used, not doubled.
func TestTunnelExternal(t *testing.T) {
	d := fakeTunnelDef(t, "", "0s")
	withTunnels(t, d)
	ln, err := net.Listen("tcp", d.endpoint())
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	startTunnel(d.Name, true)
	if v := viewTunnel(d); v.State != tsExternal || v.PID != 0 {
		t.Errorf("external: %+v", v)
	}
	stopTunnel(d.Name)
	if !portOpen(d.endpoint()) {
		t.Error("stopped a tunnel KubeHealth did not start")
	}
}

func testKubeconfig(t *testing.T, server string, contexts ...string) string {
	t.Helper()
	var b strings.Builder
	b.WriteString("apiVersion: v1\nkind: Config\nclusters:\n- name: c\n  cluster: {server: \"" + server + "\"}\nusers:\n- name: u\n  user: {token: x}\ncontexts:\n")
	for _, c := range contexts {
		b.WriteString("- name: " + c + "\n  context: {cluster: c, user: u}\n")
	}
	b.WriteString("current-context: " + contexts[0] + "\n")
	p := t.TempDir() + "/kubeconfig"
	os.WriteFile(p, []byte(b.String()), 0o600)
	return p
}

func tunnelReq(method, path, body string, mut func(*http.Request)) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.RemoteAddr = "127.0.0.1:50000"
	req.Host = "localhost:8080"
	if method != http.MethodGet {
		req.Header.Set("Origin", "http://localhost:8080")
		req.Header.Set("Content-Type", "application/json")
	}
	if mut != nil {
		mut(req)
	}
	rec := httptest.NewRecorder()
	handleAPITunnels(rec, req)
	return rec
}

// Only this machine's browser, on KubeHealth's own page, manages tunnels.
func TestTunnelAPI(t *testing.T) {
	withTunnels(t)
	oldKC := cfg.Kubeconfig
	cfg.Kubeconfig = testKubeconfig(t, "https://10.255.255.1:443", "private-gke", "private-gke-2", "other")
	defer func() { cfg.Kubeconfig = oldKC }()
	body := `{"name":"bastion","command":"gcloud compute ssh bastion --tunnel-through-iap -- -D 1080 -N","mode":"proxy","proxy":"socks5://127.0.0.1:1080","contexts":["private-gke"]}`

	for what, mut := range map[string]func(*http.Request){
		"another machine": func(r *http.Request) { r.RemoteAddr = "192.0.2.10:5000" },
		"DNS rebinding":   func(r *http.Request) { r.Host = "evil.example:8080" },
		"another site":    func(r *http.Request) { r.Header.Set("Origin", "https://evil.example") },
		"no origin":       func(r *http.Request) { r.Header.Del("Origin") },
		"a plain form":    func(r *http.Request) { r.Header.Set("Content-Type", "application/x-www-form-urlencoded") },
	} {
		if rec := tunnelReq(http.MethodPost, "/api/tunnels", body, mut); rec.Code != http.StatusForbidden {
			t.Errorf("%s: %d", what, rec.Code)
		}
	}
	if rec := tunnelReq(http.MethodGet, "/api/tunnels", "", func(r *http.Request) { r.Host = "evil.example" }); rec.Code != http.StatusForbidden {
		t.Errorf("reading the commands from a rebound name: %d", rec.Code)
	}

	if rec := tunnelReq(http.MethodPost, "/api/tunnels", body, nil); rec.Code != http.StatusOK {
		t.Fatalf("create: %d %s", rec.Code, rec.Body)
	}
	if d, ok := tunnelForContext("private-gke"); !ok || d.Name != "bastion" {
		t.Errorf("saved: %+v", d)
	}
	path := os.Getenv("HOME") + "/Library/Application Support/kubehealth/prefs.json"
	if _, err := os.Stat(path); err != nil {
		path = os.Getenv("XDG_CONFIG_HOME") + "/kubehealth/prefs.json"
	}
	if fi, err := os.Stat(path); err != nil || fi.Mode().Perm() != 0o600 {
		t.Errorf("prefs.json: %v %v", fi, err)
	} else if b, _ := os.ReadFile(path); !bytes.Contains(b, []byte(`"tunnel-through-iap`)) && !bytes.Contains(b, []byte(`--tunnel-through-iap`)) {
		t.Errorf("prefs.json: %s", b)
	}

	for what, b := range map[string]string{
		"same context":    `{"name":"b2","command":"x","mode":"none","contexts":["private-gke"]}`,
		"same port":       `{"name":"b2","command":"x","mode":"proxy","proxy":"socks5://127.0.0.1:1080","contexts":["other"]}`,
		"unknown cluster": `{"name":"b2","command":"x","mode":"none","contexts":["nope"]}`,
	} {
		if rec := tunnelReq(http.MethodPost, "/api/tunnels", b, nil); rec.Code != http.StatusBadRequest {
			t.Errorf("%s: %d %s", what, rec.Code, rec.Body)
		}
	}

	// Renamed, and serving a second cluster behind the same bastion.
	edit := strings.Replace(body, `"name":"bastion"`, `"name":"iap-prod","previous":"bastion"`, 1)
	edit = strings.Replace(edit, `["private-gke"]`, `["private-gke","private-gke-2"]`, 1)
	if rec := tunnelReq(http.MethodPost, "/api/tunnels", edit, nil); rec.Code != http.StatusOK {
		t.Fatalf("edit: %d %s", rec.Code, rec.Body)
	}
	var list struct {
		Tunnels []tunnelView  `json:"tunnels"`
		Context tunnelContext `json:"context"`
	}
	rec := tunnelReq(http.MethodGet, "/api/tunnels?context=private-gke-2", "", nil)
	json.Unmarshal(rec.Body.Bytes(), &list)
	if len(list.Tunnels) != 1 || list.Tunnels[0].Name != "iap-prod" || list.Tunnels[0].State != tsStopped || list.Context.Tunnel != "iap-prod" ||
		list.Context.Server != "https://10.255.255.1:443" {
		t.Errorf("list: %s", rec.Body)
	}

	if rec := tunnelReq(http.MethodDelete, "/api/tunnels/iap-prod", "", nil); rec.Code != http.StatusOK {
		t.Errorf("delete: %d %s", rec.Code, rec.Body)
	}
	if _, ok := tunnelForContext("private-gke"); ok {
		t.Error("still there after delete")
	}
}

// Switching to a cluster behind a tunnel starts the tunnel first.
func TestSwitchStartsTunnel(t *testing.T) {
	d := fakeTunnelDef(t, "", "2s")
	withTunnels(t, d)
	oldKC := cfg.Kubeconfig
	cfg.Kubeconfig = testKubeconfig(t, "https://10.255.255.1:443", "private-gke")
	defer func() { cfg.Kubeconfig = oldKC }()

	req := httptest.NewRequest(http.MethodPost, "/api/context", strings.NewReader(`{"name":"private-gke"}`))
	rec := httptest.NewRecorder()
	handleAPIContext(rec, req)
	if rec.Code != http.StatusAccepted || !strings.Contains(rec.Body.String(), `"tunnel":"bastion"`) {
		t.Fatalf("switch: %d %s", rec.Code, rec.Body)
	}
	if st := tunnelState(d.Name); st != tsStarting {
		t.Errorf("tunnel: %s", st)
	}
	req = httptest.NewRequest(http.MethodPost, "/api/context", strings.NewReader(`{"name":"private-gke"}`))
	req.Header.Set("Origin", "https://evil.example")
	rec = httptest.NewRecorder()
	handleAPIContext(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Errorf("another site starting a tunnel: %d", rec.Code)
	}
}

// socks5Proxy is a minimal SOCKS5 server that connects every request to
// target and reports the address each one asked for.
func socks5Proxy(t *testing.T, target string) (addr string, asked func() []string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	var mu sync.Mutex
	var seen []string
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, 262)
				if _, err := io.ReadFull(c, buf[:2]); err != nil {
					return
				}
				io.ReadFull(c, buf[:buf[1]])
				c.Write([]byte{5, 0})
				if _, err := io.ReadFull(c, buf[:4]); err != nil {
					return
				}
				var host string
				switch buf[3] {
				case 1:
					io.ReadFull(c, buf[:4])
					host = net.IP(buf[:4]).String()
				case 3:
					io.ReadFull(c, buf[:1])
					n := int(buf[0])
					io.ReadFull(c, buf[:n])
					host = string(buf[:n])
				default:
					return
				}
				io.ReadFull(c, buf[:2])
				dest := net.JoinHostPort(host, fmt.Sprint(binary.BigEndian.Uint16(buf[:2])))
				mu.Lock()
				seen = append(seen, dest)
				mu.Unlock()
				up, err := net.Dial("tcp", target)
				if err != nil {
					c.Write([]byte{5, 5, 0, 1, 0, 0, 0, 0, 0, 0})
					return
				}
				defer up.Close()
				c.Write([]byte{5, 0, 0, 1, 0, 0, 0, 0, 0, 0})
				go io.Copy(up, c)
				io.Copy(c, up)
			}(c)
		}
	}()
	return ln.Addr().String(), func() []string { mu.Lock(); defer mu.Unlock(); return slices.Clone(seen) }
}

// A private API server, reachable only through a SOCKS tunnel: KubeHealth's
// client goes through it, asks for the private name, and checks the cert.
func TestTunnelSOCKSClient(t *testing.T) {
	api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"kind":"NamespaceList","apiVersion":"v1","metadata":{},"items":[{"metadata":{"name":"payments"}}]}`)
	}))
	defer api.Close()
	proxy, asked := socks5Proxy(t, api.Listener.Addr().String())
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: api.Certificate().Raw})

	withTunnels(t, TunnelDef{Name: "bastion", Command: "true", Mode: tunnelProxy, Proxy: "socks5://" + proxy, Contexts: []string{"private-gke"}})
	kc := t.TempDir() + "/kubeconfig"
	os.WriteFile(kc, []byte(fmt.Sprintf(`apiVersion: v1
kind: Config
clusters:
- name: c
  cluster:
    server: https://private-api.internal:443
    tls-server-name: example.com
    certificate-authority-data: %s
users: [{name: u, user: {token: x}}]
contexts: [{name: private-gke, context: {cluster: c, user: u}}]
current-context: private-gke
`, base64.StdEncoding.EncodeToString(ca))), 0o600)
	oldKC := cfg.Kubeconfig
	cfg.Kubeconfig = kc
	defer func() { cfg.Kubeconfig = oldKC }()

	c, err := newClusterState("private-gke")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ns, err := c.client.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil || len(ns.Items) != 1 || ns.Items[0].Name != "payments" {
		t.Fatalf("list through the tunnel: %v %+v", err, ns)
	}
	if a := asked(); len(a) == 0 || a[0] != "private-api.internal:443" {
		t.Errorf("the proxy was asked for %v", a)
	}

	// Without the tunnel the private name doesn't resolve: unreachable.
	prefs.Lock()
	prefs.Tunnels = nil
	prefs.Unlock()
	c, _ = newClusterState("private-gke")
	_, err = c.client.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if errKind(err) != "unreachable" {
		t.Errorf("without the tunnel: %v (%s)", err, errKind(err))
	}
}

// At launch only the current context's tunnel starts; another starts only
// when its cluster is picked.
func TestStartupStartsOnlyCurrentTunnel(t *testing.T) {
	a := fakeTunnelDef(t, "", "0s")
	addrB := freeAddr(t)
	b := TunnelDef{Name: "bastion-2", Command: "FAKE_TUNNEL_ADDR=" + addrB + " '" + os.Args[0] + "' -- -D " + addrB,
		Mode: tunnelProxy, Proxy: "socks5://" + addrB, Contexts: []string{"private-gke-2"}}
	withTunnels(t, a, b)
	oldKC := cfg.Kubeconfig
	cfg.Kubeconfig = testKubeconfig(t, "https://10.255.255.1:443", "private-gke", "private-gke-2") // current: private-gke
	clusters.Lock()
	oldActive, oldByName := clusters.active, clusters.byName
	clusters.active, clusters.byName = nil, map[string]*clusterState{}
	clusters.Unlock()
	t.Cleanup(func() {
		clusters.Lock()
		for _, c := range clusters.byName {
			c.stop()
		}
		clusters.active, clusters.byName = oldActive, oldByName
		clusters.Unlock()
		cfg.Kubeconfig = oldKC
	})

	if err := initClusters(); err != nil {
		t.Fatal(err)
	}
	waitState(t, a.Name, tsRunning)
	time.Sleep(300 * time.Millisecond)
	if st := tunnelState(b.Name); st != tsStopped || portOpen(addrB) {
		t.Fatalf("a tunnel no cluster in use needs was started at launch: %s", st)
	}

	rec := httptest.NewRecorder()
	handleAPIContext(rec, httptest.NewRequest(http.MethodPost, "/api/context", strings.NewReader(`{"name":"private-gke-2"}`)))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("picking private-gke-2: %d %s", rec.Code, rec.Body)
	}
	waitState(t, b.Name, tsRunning)
}
