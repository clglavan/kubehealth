package main

// The RCA tools as an MCP server, for Claude Code (claudecode.go). Each RCA
// run registers a random token; /mcp/rca/<token> serves that run's tools over
// MCP's streamable HTTP transport, answering with plain JSON (no SSE). Only
// requests from this machine are served: KubeHealth listens on every
// interface. Tool calls go through executeRCATool and become steps of the
// run's evidence trail, as with a local model.

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
)

// mcpToolPrefix is how Claude Code names the tools of the "kubehealth" server.
const mcpToolPrefix = "mcp__kubehealth__"

// mcpRunawayCalls is how many calls past its budget a run may make before
// KubeHealth stops it: each one is told to answer, and should.
const mcpRunawayCalls = 3

type rcaToolRun struct {
	cl       *clusterState
	anchorNS string
	state    *RCAState
	upd      func(func())
	maxCalls int
	cancel   func()

	mu    sync.Mutex
	calls int
}

func (r *rcaToolRun) runaway() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls > r.maxCalls+mcpRunawayCalls
}

var rcaToolRuns sync.Map // token → *rcaToolRun

type mcpRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type mcpError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func handleMCPRCA(w http.ResponseWriter, r *http.Request) {
	host, _, _ := net.SplitHostPort(r.RemoteAddr)
	if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	v, ok := rcaToolRuns.Load(strings.TrimPrefix(r.URL.Path, "/mcp/rca/"))
	if !ok {
		http.Error(w, "no such RCA run", http.StatusNotFound)
		return
	}
	run := v.(*rcaToolRun)
	switch r.Method {
	case http.MethodPost:
	case http.MethodDelete:
		w.WriteHeader(http.StatusOK)
		return
	default: // no server-initiated stream (GET)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req mcpRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		http.Error(w, "expected one JSON-RPC message", http.StatusBadRequest)
		return
	}
	if len(req.ID) == 0 { // a notification
		w.WriteHeader(http.StatusAccepted)
		return
	}
	result, merr := run.handle(req)
	out := map[string]any{"jsonrpc": "2.0", "id": req.ID}
	if merr != nil {
		out["error"] = merr
	} else {
		out["result"] = result
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(out)
}

func (run *rcaToolRun) handle(req mcpRequest) (any, *mcpError) {
	switch req.Method {
	case "initialize":
		var p struct {
			ProtocolVersion string `json:"protocolVersion"`
		}
		json.Unmarshal(req.Params, &p)
		if p.ProtocolVersion == "" {
			p.ProtocolVersion = "2025-06-18"
		}
		return map[string]any{
			"protocolVersion": p.ProtocolVersion,
			"capabilities":    map[string]any{"tools": map[string]any{}},
			"serverInfo":      map[string]string{"name": "kubehealth", "version": version},
		}, nil
	case "ping":
		return map[string]any{}, nil
	case "tools/list":
		var tools []map[string]any
		for _, t := range rcaToolDefs() {
			tools = append(tools, map[string]any{
				"name":        t.Function.Name,
				"description": t.Function.Description,
				"inputSchema": t.Function.Parameters,
			})
		}
		return map[string]any{"tools": tools}, nil
	case "tools/call":
		var p struct {
			Name      string         `json:"name"`
			Arguments map[string]any `json:"arguments"`
		}
		if err := json.Unmarshal(req.Params, &p); err != nil || p.Name == "" {
			return nil, &mcpError{Code: -32602, Message: "expected a tool name and arguments"}
		}
		return mcpText(run.call(p.Name, p.Arguments)), nil
	}
	return nil, &mcpError{Code: -32601, Message: "method not found: " + req.Method}
}

// call runs one tool as a step of the trail. Past the run's budget it tells
// the model to answer instead, and past that it stops the run.
func (run *rcaToolRun) call(name string, raw map[string]any) string {
	run.mu.Lock()
	run.calls++
	n := run.calls
	run.mu.Unlock()
	if n > run.maxCalls {
		if n > run.maxCalls+mcpRunawayCalls {
			run.cancel()
		}
		return fmt.Sprintf("You have used all %d investigation steps. Do not call more tools: give the final Root Cause Analysis now, from the evidence you have.", run.maxCalls)
	}
	args := map[string]string{}
	for k, v := range raw {
		if s, ok := v.(string); ok {
			args[k] = s
		} else {
			args[k] = fmt.Sprint(v)
		}
	}
	argJSON, _ := json.Marshal(args)
	stepIdx := 0
	run.upd(func() {
		run.state.Iteration = n
		run.state.Steps = append(run.state.Steps, RCAStep{Iter: n, Tool: name, Label: rcaStepLabel(name, args), Status: "running"})
		stepIdx = len(run.state.Steps) - 1
	})
	out := executeRCATool(run.cl, rcaToolCall{Type: "function", Function: rcaToolCallFn{Name: name, Arguments: string(argJSON)}}, run.anchorNS)
	run.upd(func() { run.state.Steps[stepIdx].Status = "done" })
	return out
}

func mcpText(s string) map[string]any {
	return map[string]any{"content": []map[string]string{{"type": "text", "text": s}}}
}
