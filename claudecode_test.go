package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"testing"
	"time"
)

// TestMain doubles as a fake claude CLI: tests point claudeBin at the test
// binary and set GO_WANT_FAKE_CLAUDE, and the process acts as claude -p.
func TestMain(m *testing.M) {
	if os.Getenv("GO_WANT_FAKE_CLAUDE") == "1" {
		os.Exit(fakeClaude(os.Args[1:]))
	}
	os.Exit(m.Run())
}

// fakeClaude behaves like claude -p --output-format stream-json. With
// --mcp-config it calls KubeHealth's tools the way Claude Code does; it
// records its arguments and stdin in FAKE_CLAUDE_LOG.
func fakeClaude(args []string) int {
	if slices.Contains(args, "--version") {
		fmt.Println("9.9.9 (Claude Code)")
		return 0
	}
	if len(args) >= 2 && args[0] == "auth" && args[1] == "status" {
		fmt.Println(`{"loggedIn": true, "authMethod": "claude.ai", "email": "dev@example.com"}`)
		return 0
	}
	stdin, _ := io.ReadAll(os.Stdin)
	if p := os.Getenv("FAKE_CLAUDE_LOG"); p != "" {
		b, _ := json.Marshal(map[string]any{"args": args, "stdin": string(stdin)})
		os.WriteFile(p, b, 0o600)
	}
	out := json.NewEncoder(os.Stdout)
	emit := func(v map[string]any) { out.Encode(v) }
	delta := func(kind, field, s string) {
		emit(map[string]any{"type": "stream_event", "event": map[string]any{"type": "content_block_delta",
			"delta": map[string]any{"type": kind, field: s}}})
	}
	start := func() { emit(map[string]any{"type": "stream_event", "event": map[string]any{"type": "message_start"}}) }
	if os.Getenv("FAKE_CLAUDE_MODE") == "signedout" {
		emit(map[string]any{"type": "result", "subtype": "success", "is_error": true, "result": "Invalid API key · Please run /login"})
		return 1
	}
	emit(map[string]any{"type": "system", "subtype": "init"})

	i := slices.Index(args, "--mcp-config")
	if i < 0 {
		start()
		delta("thinking_delta", "thinking", "Reading the prompt.")
		delta("text_delta", "text", "Answer: ")
		first := strings.SplitN(string(stdin), "\n", 2)[0]
		delta("text_delta", "text", first)
		emit(map[string]any{"type": "result", "subtype": "success", "result": "Answer: " + first, "num_turns": 1})
		return 0
	}

	var cfg struct {
		MCPServers map[string]struct{ URL string } `json:"mcpServers"`
	}
	json.Unmarshal([]byte(args[i+1]), &cfg)
	url := cfg.MCPServers["kubehealth"].URL
	rpc := func(id int, method string, params any) string {
		m := map[string]any{"jsonrpc": "2.0", "method": method, "params": params}
		if id > 0 {
			m["id"] = id
		}
		b, _ := json.Marshal(m)
		resp, err := http.Post(url, "application/json", bytes.NewReader(b))
		if err != nil {
			return "error: " + err.Error()
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return string(body)
	}
	rpc(1, "initialize", map[string]any{"protocolVersion": "2025-11-25"})
	rpc(0, "notifications/initialized", nil)
	rpc(2, "tools/list", nil)
	start()
	delta("text_delta", "text", "Checking the events.")
	calls := 1
	if os.Getenv("FAKE_CLAUDE_MODE") == "runaway" {
		calls = 20
	}
	var got string
	for c := 0; c < calls; c++ {
		raw := rpc(10+c, "tools/call", map[string]any{"name": "get_object_events",
			"arguments": map[string]any{"namespace": "shop", "kind": "Pod", "name": "web-1"}})
		var r struct {
			Result struct {
				Content []struct{ Text string } `json:"content"`
			} `json:"result"`
		}
		json.Unmarshal([]byte(raw), &r)
		if len(r.Result.Content) > 0 && got == "" {
			got = r.Result.Content[0].Text
		}
		if os.Getenv("FAKE_CLAUDE_MODE") == "runaway" {
			time.Sleep(20 * time.Millisecond)
		}
	}
	start()
	delta("text_delta", "text", "Cause: "+got)
	emit(map[string]any{"type": "result", "subtype": "success", "result": "Cause: " + got, "num_turns": 2, "total_cost_usd": 0.01})
	return 0
}

// useFakeClaude routes AI calls to the fake CLI for one test.
func useFakeClaude(t *testing.T, mode string) (logPath string) {
	t.Helper()
	logPath = t.TempDir() + "/claude.json"
	t.Setenv("GO_WANT_FAKE_CLAUDE", "1")
	t.Setenv("FAKE_CLAUDE_MODE", mode)
	t.Setenv("FAKE_CLAUDE_LOG", logPath)
	claudeBin = os.Args[0]
	aiBackend.Lock()
	aiBackend.name, aiBackend.model, aiBackend.thinking = backendClaude, "haiku", ""
	aiBackend.Unlock()
	claudeStatusCache.Lock()
	claudeStatusCache.at = time.Time{}
	claudeStatusCache.Unlock()
	t.Cleanup(func() {
		claudeBin = ""
		aiBackend.Lock()
		aiBackend.name, aiBackend.model, aiBackend.thinking = backendLocal, defaultClaudeModel, ""
		aiBackend.Unlock()
		claudeStatusCache.Lock()
		claudeStatusCache.at = time.Time{}
		claudeStatusCache.Unlock()
	})
	return logPath
}

func readFakeLog(t *testing.T, path string) (args []string, stdin string) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var l struct {
		Args  []string `json:"args"`
		Stdin string   `json:"stdin"`
	}
	json.Unmarshal(b, &l)
	return l.Args, l.Stdin
}

// The command line keeps Claude Code to KubeHealth's prompt and tools.
func TestCCArgs(t *testing.T) {
	a := ccArgs(ccRequest{System: "be brief", Prompt: "SECRET PROMPT"}, "sonnet")
	flag := func(name string) string {
		i := slices.Index(a, name)
		if i < 0 || i+1 >= len(a) {
			t.Fatalf("missing %s in %q", name, a)
		}
		return a[i+1]
	}
	for _, f := range []string{"-p", "--strict-mcp-config", "--disable-slash-commands", "--no-session-persistence"} {
		if !slices.Contains(a, f) {
			t.Errorf("missing %s", f)
		}
	}
	if flag("--tools") != "" || flag("--setting-sources") != "" {
		t.Errorf("built-in tools and settings must be off: %q", a)
	}
	if flag("--system-prompt") != "be brief" || flag("--model") != "sonnet" || flag("--output-format") != "stream-json" {
		t.Errorf("args: %q", a)
	}
	if slices.Contains(a, "SECRET PROMPT") || slices.Contains(a, "--mcp-config") || slices.Contains(a, "--effort") || slices.Contains(a, "--settings") {
		t.Errorf("prompt, tools or thinking where they don't belong: %q", a)
	}

	a = ccArgs(ccRequest{System: "x", MCPURL: "http://127.0.0.1:1/mcp/rca/t", Tools: []string{"mcp__kubehealth__a", "mcp__kubehealth__b"}, Thinking: "high"}, "")
	if slices.Contains(a, "--model") || flag("--effort") != "high" || flag("--allowedTools") != "mcp__kubehealth__a,mcp__kubehealth__b" {
		t.Errorf("args: %q", a)
	}
	if !strings.Contains(flag("--mcp-config"), `"url":"http://127.0.0.1:1/mcp/rca/t"`) {
		t.Errorf("mcp config: %s", flag("--mcp-config"))
	}

	// Thinking off is a setting, not an effort level.
	a = ccArgs(ccRequest{System: "x", Thinking: "off"}, "")
	if flag("--settings") != `{"alwaysThinkingEnabled":false}` || slices.Contains(a, "--effort") {
		t.Errorf("thinking off: %q", a)
	}
}

// The answer is the last turn's text; errors in the result line are errors.
func TestCCReadStream(t *testing.T) {
	lines := strings.Join([]string{
		`{"type":"system","subtype":"init","mcp_servers":[{"name":"kubehealth","status":"connected"}]}`,
		`{"type":"stream_event","event":{"type":"message_start"}}`,
		`{"type":"stream_event","event":{"type":"content_block_delta","delta":{"type":"thinking_delta","thinking":"Let me look."}}}`,
		`{"type":"stream_event","event":{"type":"content_block_delta","delta":{"type":"text_delta","text":"I'll read the logs."}}}`,
		`{"type":"assistant","message":{"content":[{"type":"tool_use","name":"mcp__kubehealth__get_pod_logs"}]}}`,
		`not json`,
		`{"type":"stream_event","event":{"type":"message_start"}}`,
		`{"type":"stream_event","event":{"type":"content_block_delta","delta":{"type":"text_delta","text":"The DB "}}}`,
		`{"type":"stream_event","event":{"type":"content_block_delta","delta":{"type":"text_delta","text":"is down."}}}`,
		`{"type":"result","subtype":"success","is_error":false,"num_turns":2,"total_cost_usd":0.02}`,
	}, "\n")
	var partials []string
	res, ok, err := ccReadStream(strings.NewReader(lines), func(text, _ string) { partials = append(partials, text) })
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	if res.Text != "The DB is down." || res.Reasoning != "Let me look." || res.Turns != 2 || res.CostUSD != 0.02 {
		t.Errorf("result: %+v", res)
	}
	if len(partials) == 0 {
		t.Error("no partial text")
	}

	// Thinking streamed without its text still shows as thinking, and is not kept.
	var r string
	res, _, _ = ccReadStream(strings.NewReader(`{"type":"stream_event","event":{"type":"content_block_delta","delta":{"type":"thinking_delta","thinking":""}}}
{"type":"result","subtype":"success","result":"ok"}`), func(_, reasoning string) { r = reasoning })
	if r != ccThinking || res.Reasoning != "" {
		t.Errorf("hidden thinking: partial %q, kept %q", r, res.Reasoning)
	}

	_, ok, err = ccReadStream(strings.NewReader(`{"type":"result","subtype":"error_during_execution","is_error":true,"errors":["rate limited"]}`), nil)
	if !ok || err == nil || !strings.Contains(err.Error(), "rate limited") {
		t.Errorf("error result: %v %v", ok, err)
	}
	_, ok, _ = ccReadStream(strings.NewReader(`{"type":"system","subtype":"init"}`), nil)
	if ok {
		t.Error("no result line, but ok")
	}
}

func mcpPost(t *testing.T, path, remote string, body any) *httptest.ResponseRecorder {
	t.Helper()
	b, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(b))
	req.RemoteAddr = remote
	rec := httptest.NewRecorder()
	handleMCPRCA(rec, req)
	return rec
}

// The MCP endpoint: local callers with a live token only; its tools are the
// RCA tools, each call a step; past the budget it tells the model to answer.
func TestMCPRCAEndpoint(t *testing.T) {
	state := &RCAState{Status: "running", MaxIter: 1}
	cancelled := false
	run := &rcaToolRun{cl: &clusterState{name: "t"}, anchorNS: "shop", state: state, maxCalls: 1,
		upd: func(f func()) { state.Lock(); f(); state.Unlock() }, cancel: func() { cancelled = true }}
	rcaToolRuns.Store("tok", run)
	defer rcaToolRuns.Delete("tok")
	const local = "127.0.0.1:5555"

	if rec := mcpPost(t, "/mcp/rca/tok", "192.0.2.7:5555", map[string]any{"jsonrpc": "2.0", "id": 1, "method": "tools/list"}); rec.Code != http.StatusForbidden {
		t.Errorf("remote caller: %d", rec.Code)
	}
	if rec := mcpPost(t, "/mcp/rca/nope", local, map[string]any{"jsonrpc": "2.0", "id": 1, "method": "tools/list"}); rec.Code != http.StatusNotFound {
		t.Errorf("unknown token: %d", rec.Code)
	}
	if rec := mcpPost(t, "/mcp/rca/tok", local, map[string]any{"jsonrpc": "2.0", "method": "notifications/initialized"}); rec.Code != http.StatusAccepted {
		t.Errorf("notification: %d", rec.Code)
	}

	var resp struct {
		Result struct {
			ProtocolVersion string `json:"protocolVersion"`
			Tools           []struct {
				Name        string         `json:"name"`
				InputSchema map[string]any `json:"inputSchema"`
			} `json:"tools"`
			Content []struct{ Text string } `json:"content"`
		} `json:"result"`
		Error *mcpError `json:"error"`
	}
	decode := func(rec *httptest.ResponseRecorder) {
		resp.Result.Tools, resp.Result.Content, resp.Error = nil, nil, nil
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("%d %s", rec.Code, rec.Body)
		}
	}
	decode(mcpPost(t, "/mcp/rca/tok", local, map[string]any{"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": map[string]any{"protocolVersion": "2025-11-25"}}))
	if resp.Result.ProtocolVersion != "2025-11-25" {
		t.Errorf("initialize: %+v", resp)
	}
	decode(mcpPost(t, "/mcp/rca/tok", local, map[string]any{"jsonrpc": "2.0", "id": 2, "method": "tools/list"}))
	if len(resp.Result.Tools) != len(rcaToolDefs()) || resp.Result.Tools[0].InputSchema["type"] != "object" {
		t.Errorf("tools/list: %+v", resp.Result.Tools)
	}
	decode(mcpPost(t, "/mcp/rca/tok", local, map[string]any{"jsonrpc": "2.0", "id": 3, "method": "server/discover"}))
	if resp.Error == nil || resp.Error.Code != -32601 {
		t.Errorf("unknown method: %+v", resp.Error)
	}

	call := map[string]any{"jsonrpc": "2.0", "id": 4, "method": "tools/call",
		"params": map[string]any{"name": "get_object_events", "arguments": map[string]any{"namespace": "shop", "kind": "Pod", "name": "web-1"}}}
	decode(mcpPost(t, "/mcp/rca/tok", local, call))
	if len(resp.Result.Content) != 1 || !strings.Contains(resp.Result.Content[0].Text, "no cached events for shop/Pod/web-1") {
		t.Errorf("tools/call: %+v", resp.Result.Content)
	}
	if len(state.Steps) != 1 || state.Steps[0].Status != "done" || state.Steps[0].Label != "shop / Pod / web-1" || state.Iteration != 1 {
		t.Errorf("steps: %+v", state.Steps)
	}
	decode(mcpPost(t, "/mcp/rca/tok", local, call))
	if !strings.Contains(resp.Result.Content[0].Text, "used all 1 investigation steps") || len(state.Steps) != 1 {
		t.Errorf("over budget: %+v, %d steps", resp.Result.Content, len(state.Steps))
	}
	for range mcpRunawayCalls {
		mcpPost(t, "/mcp/rca/tok", local, call)
	}
	if !cancelled || !run.runaway() {
		t.Error("a run that keeps calling tools is not stopped")
	}
}

// AI analysis and the note through Claude Code: the system prompt as a flag,
// the prompt on stdin, the answer streamed back.
func TestCCChatFakeCLI(t *testing.T) {
	logPath := useFakeClaude(t, "")
	var partial string
	out, reasoning, err := streamChatMsgs(context.Background(), chatMessages("Why is web-1 failing?\nevidence…", chatOpts{System: "You are terse."}),
		chatOpts{}, func(p, r string) { partial = p + r })
	if err != nil {
		t.Fatal(err)
	}
	if out != "Answer: Why is web-1 failing?" || reasoning != "Reading the prompt." || partial == "" {
		t.Errorf("out %q reasoning %q partial %q", out, reasoning, partial)
	}
	args, stdin := readFakeLog(t, logPath)
	if !strings.HasPrefix(stdin, "Why is web-1 failing?") || slices.Contains(args, stdin) {
		t.Errorf("prompt must go on stdin only: %q", stdin)
	}
	if i := slices.Index(args, "--system-prompt"); i < 0 || args[i+1] != "You are terse." {
		t.Errorf("system prompt: %q", args)
	}
	if i := slices.Index(args, "--model"); i < 0 || args[i+1] != "haiku" {
		t.Errorf("model: %q", args)
	}

	if aiModelName() != "Claude Code · haiku" {
		t.Errorf("model name: %q", aiModelName())
	}

	// Thinking as set in the dialog…
	aiBackend.Lock()
	aiBackend.thinking = "max"
	aiBackend.Unlock()
	streamChatMsgs(context.Background(), chatMessages("why", chatOpts{}), chatOpts{}, nil)
	args, _ = readFakeLog(t, logPath)
	if i := slices.Index(args, "--effort"); i < 0 || args[i+1] != "max" {
		t.Errorf("effort: %q", args)
	}
	if aiModelName() != "Claude Code · haiku · thinking max" {
		t.Errorf("model name: %q", aiModelName())
	}
	// …except the overview note, which never thinks and keeps no reasoning.
	_, reasoning, err = streamChatMsgs(context.Background(), chatMessages("note", chatOpts{}), chatOpts{NoThink: true}, nil)
	args, _ = readFakeLog(t, logPath)
	if err != nil || reasoning != "" || !slices.Contains(args, `{"alwaysThinkingEnabled":false}`) || slices.Contains(args, "--effort") {
		t.Errorf("note: %v %q %q", err, reasoning, args)
	}
}

func TestCCSignedOut(t *testing.T) {
	useFakeClaude(t, "signedout")
	_, _, err := streamChatMsgs(context.Background(), chatMessages("x", chatOpts{}), chatOpts{}, nil)
	if err == nil || !strings.Contains(err.Error(), "claude auth login") {
		t.Errorf("err: %v", err)
	}
}

// An RCA through Claude Code: it calls KubeHealth's tools over MCP, each call
// a step, and its last turn is the result.
func TestRCAClaudeFakeCLI(t *testing.T) {
	logPath := useFakeClaude(t, "")
	srv := httptest.NewServer(http.HandlerFunc(handleMCPRCA))
	defer srv.Close()
	_, port, _ := net.SplitHostPort(strings.TrimPrefix(srv.URL, "http://"))
	oldPort := cfg.Port
	cfg.Port = port
	defer func() { cfg.Port = oldPort }()

	state := &RCAState{Status: "running", MaxIter: 3}
	upd := func(f func()) { state.Lock(); f(); state.Unlock() }
	runRCAClaude(&clusterState{name: "t"}, "shop", "RCA system prompt", "Perform Root Cause Analysis for Pod web-1", state, upd)
	if state.Status != "done" || state.Result != "Cause: no cached events for shop/Pod/web-1" {
		t.Fatalf("state: %s %q %q", state.Status, state.Result, state.Error)
	}
	if len(state.Steps) != 1 || state.Steps[0].Tool != "get_object_events" || state.Steps[0].Status != "done" {
		t.Errorf("steps: %+v", state.Steps)
	}
	args, _ := readFakeLog(t, logPath)
	if i := slices.Index(args, "--allowedTools"); i < 0 || !strings.Contains(args[i+1], "mcp__kubehealth__get_pod_logs") {
		t.Errorf("allowed tools: %q", args)
	}
	n := 0
	rcaToolRuns.Range(func(_, _ any) bool { n++; return true })
	if n != 0 {
		t.Errorf("%d tool runs left registered", n)
	}
}

func TestRCAClaudeRunaway(t *testing.T) {
	useFakeClaude(t, "runaway")
	srv := httptest.NewServer(http.HandlerFunc(handleMCPRCA))
	defer srv.Close()
	_, port, _ := net.SplitHostPort(strings.TrimPrefix(srv.URL, "http://"))
	oldPort := cfg.Port
	cfg.Port = port
	defer func() { cfg.Port = oldPort }()

	state := &RCAState{Status: "running", MaxIter: 2}
	runRCAClaude(&clusterState{name: "t"}, "shop", "s", "p", state, func(f func()) { state.Lock(); f(); state.Unlock() })
	if state.Status != "error" || !strings.Contains(state.Error, "kept calling tools") || len(state.Steps) != 2 {
		t.Errorf("state: %s %q, %d steps", state.Status, state.Error, len(state.Steps))
	}
}
