package main

// Claude Code as the model. With "Send to Claude Code" on (AI Agent dialog),
// the overview note, AI analysis and RCA run the local claude CLI (claude -p)
// instead of LM Studio or the embedded llama-server. Each call is one process,
// started in an empty directory, with KubeHealth's system prompt, no built-in
// tools, none of the user's settings, plugins, skills or slash commands, and
// no saved session. The prompt goes in on stdin, so it never shows in ps. RCA
// gets KubeHealth's read-only tools through an MCP endpoint on this server
// (mcp.go).

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	backendLocal  = "local"
	backendClaude = "claude-code"

	defaultClaudeModel = "sonnet"
	// claudeCtxLen is the context KubeHealth budgets prompts against with
	// Claude Code. Prompts stay far below it: the log-lines and events
	// settings bound the evidence.
	claudeCtxLen = 200000
)

// claudeModels are the choices in the dialog; "" is Claude Code's default.
var claudeModels = map[string]bool{"": true, "haiku": true, "sonnet": true, "opus": true}

// claudeThinking are the thinking choices: "" leaves it to Claude Code, "off"
// turns thinking off, the rest are claude --effort levels (they steer how much
// Sonnet and Opus think; Haiku thinks about the same at every level).
var claudeThinking = map[string]bool{"": true, "off": true, "low": true, "medium": true, "high": true, "xhigh": true, "max": true}

var aiBackend = struct {
	sync.RWMutex
	name     string
	model    string
	thinking string
}{name: backendLocal, model: defaultClaudeModel}

// initAIBackend reads the saved choice; AI_BACKEND (local or claude-code)
// overrides it at startup. Call after loadPrefs.
func initAIBackend() {
	prefs.Lock()
	name, model, thinking := prefs.AIBackend, prefs.ClaudeModel, prefs.ClaudeThinking
	prefs.Unlock()
	if v := os.Getenv("AI_BACKEND"); v != "" {
		name = v
	}
	aiBackend.Lock()
	defer aiBackend.Unlock()
	if n, err := normalizeBackend(name); err == nil {
		aiBackend.name = n
	} else {
		log.Printf("AI backend: %v, using LM Studio / embedded", err)
	}
	if model != nil && claudeModels[*model] {
		aiBackend.model = *model
	}
	if claudeThinking[thinking] {
		aiBackend.thinking = thinking
	}
	if aiBackend.name == backendClaude {
		log.Printf("AI: sending to Claude Code (model %s)", orDefault(aiBackend.model))
	}
}

func normalizeBackend(name string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "", "local", "lmstudio", "lm-studio", "embedded":
		return backendLocal, nil
	case "claude-code", "claude", "claudecode":
		return backendClaude, nil
	}
	return "", fmt.Errorf("unknown AI backend %q (want local or claude-code)", name)
}

func useClaudeCode() bool {
	aiBackend.RLock()
	defer aiBackend.RUnlock()
	return aiBackend.name == backendClaude
}

func claudeModel() string {
	aiBackend.RLock()
	defer aiBackend.RUnlock()
	return aiBackend.model
}

func claudeThinkingLevel() string {
	aiBackend.RLock()
	defer aiBackend.RUnlock()
	return aiBackend.thinking
}

func orDefault(model string) string {
	if model == "" {
		return "default"
	}
	return model
}

// setAIBackend switches where AI calls go and saves the choice.
func setAIBackend(name string) error {
	n, err := normalizeBackend(name)
	if err != nil {
		return err
	}
	if n == backendClaude {
		if st := claudeStatus(true); !st.Available {
			return errors.New(st.Error)
		}
	}
	aiBackend.Lock()
	aiBackend.name = n
	aiBackend.Unlock()
	prefs.Lock()
	defer prefs.Unlock()
	prefs.AIBackend = n
	return savePrefsLocked()
}

func setClaudeModel(model string) error {
	if !claudeModels[model] {
		return fmt.Errorf("unknown Claude model %q (want haiku, sonnet, opus or empty for the default)", model)
	}
	aiBackend.Lock()
	aiBackend.model = model
	aiBackend.Unlock()
	prefs.Lock()
	defer prefs.Unlock()
	prefs.ClaudeModel = &model
	return savePrefsLocked()
}

func setClaudeThinking(level string) error {
	if !claudeThinking[level] {
		return fmt.Errorf("unknown thinking setting %q (want off, low, medium, high, xhigh, max or empty for the default)", level)
	}
	aiBackend.Lock()
	aiBackend.thinking = level
	aiBackend.Unlock()
	prefs.Lock()
	defer prefs.Unlock()
	prefs.ClaudeThinking = level
	return savePrefsLocked()
}

// aiModelName names the model in the UI (the brief's note, analysis results).
func aiModelName() string {
	if !useClaudeCode() {
		return aiConfig.model
	}
	name := "Claude Code"
	if m := claudeModel(); m != "" {
		name += " · " + m
	}
	switch t := claudeThinkingLevel(); t {
	case "":
	case "off":
		name += " · no thinking"
	default:
		name += " · thinking " + t
	}
	return name
}

// ── The CLI and its sign-in ─────────────────────────────────────────────────

// ClaudeStatus is what the AI Agent dialog shows about the claude CLI.
type ClaudeStatus struct {
	Available  bool   `json:"available"`
	Path       string `json:"path,omitempty"`
	Version    string `json:"version,omitempty"`
	LoggedIn   bool   `json:"logged_in"`
	AuthMethod string `json:"auth_method,omitempty"`
	Email      string `json:"email,omitempty"`
	Error      string `json:"error,omitempty"`
}

// claudeBin, when set, is used instead of looking claude up (tests).
var claudeBin string

var claudeStatusCache = struct {
	sync.Mutex
	at time.Time
	st ClaudeStatus
}{}

const claudeStatusTTL = time.Minute

// claudePath finds the claude CLI: on PATH, or where its installers put it
// (KubeHealth may run with a shorter PATH than the user's shell).
func claudePath() (string, error) {
	if claudeBin != "" {
		return claudeBin, nil
	}
	if p, err := exec.LookPath("claude"); err == nil {
		return p, nil
	}
	var dirs []string
	if home, err := os.UserHomeDir(); err == nil {
		dirs = append(dirs, filepath.Join(home, ".local", "bin"), filepath.Join(home, ".claude", "local"))
	}
	dirs = append(dirs, "/opt/homebrew/bin", "/usr/local/bin")
	for _, d := range dirs {
		p := filepath.Join(d, "claude")
		if fi, err := os.Stat(p); err == nil && !fi.IsDir() {
			return p, nil
		}
	}
	return "", errors.New("the claude CLI was not found on PATH or in ~/.local/bin; install Claude Code first")
}

// claudeStatus reports the CLI's version and sign-in, cached for a minute:
// the header asks for it every 30 s.
func claudeStatus(fresh bool) ClaudeStatus {
	claudeStatusCache.Lock()
	defer claudeStatusCache.Unlock()
	if !fresh && !claudeStatusCache.at.IsZero() && time.Since(claudeStatusCache.at) < claudeStatusTTL {
		return claudeStatusCache.st
	}
	st := probeClaude()
	claudeStatusCache.st, claudeStatusCache.at = st, time.Now()
	return st
}

func probeClaude() ClaudeStatus {
	path, err := claudePath()
	if err != nil {
		return ClaudeStatus{Error: err.Error()}
	}
	st := ClaudeStatus{Available: true, Path: path}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if out, err := exec.CommandContext(ctx, path, "--version").Output(); err == nil {
		st.Version = strings.Fields(string(out) + " ")[0]
	}
	out, err := exec.CommandContext(ctx, path, "auth", "status", "--json").Output()
	var auth struct {
		LoggedIn   bool   `json:"loggedIn"`
		AuthMethod string `json:"authMethod"`
		Email      string `json:"email"`
	}
	if jerr := json.Unmarshal(out, &auth); jerr != nil {
		st.Error = "could not read `claude auth status`"
		if err != nil {
			st.Error += ": " + err.Error()
		}
		return st
	}
	st.LoggedIn, st.AuthMethod, st.Email = auth.LoggedIn, auth.AuthMethod, auth.Email
	if !st.LoggedIn {
		st.Error = "Claude Code is not signed in: run `claude auth login` in a terminal."
	}
	return st
}

// ── One call ────────────────────────────────────────────────────────────────

type ccRequest struct {
	System   string
	Prompt   string
	Thinking string   // "", "off" or a claude --effort level
	MCPURL   string   // RCA: KubeHealth's tools; empty for no tools at all
	Tools    []string // RCA: the tool names Claude may call (mcp__kubehealth__…)
}

type ccResult struct {
	Text      string // the final turn's answer
	Reasoning string
	Turns     int
	CostUSD   float64
}

// ccArgs is the locked-down command line: KubeHealth's system prompt, no
// built-in tools, no user/project/local settings, only the MCP servers given
// here, no slash commands or skills, nothing saved. The prompt is not in it.
func ccArgs(req ccRequest, model string) []string {
	sys := req.System
	if strings.TrimSpace(sys) == "" {
		sys = "You are a Kubernetes SRE expert."
	}
	a := []string{"-p",
		"--output-format", "stream-json", "--verbose", "--include-partial-messages",
		"--system-prompt", sys,
		"--tools", "",
		"--setting-sources", "",
		"--strict-mcp-config",
		"--disable-slash-commands",
		"--no-session-persistence",
	}
	if model != "" {
		a = append(a, "--model", model)
	}
	switch req.Thinking {
	case "":
	case "off":
		a = append(a, "--settings", `{"alwaysThinkingEnabled":false}`)
	default:
		a = append(a, "--effort", req.Thinking)
	}
	if req.MCPURL != "" {
		cfg, _ := json.Marshal(map[string]any{"mcpServers": map[string]any{
			"kubehealth": map[string]string{"type": "http", "url": req.MCPURL},
		}})
		a = append(a, "--mcp-config", string(cfg), "--allowedTools", strings.Join(req.Tools, ","))
	}
	return a
}

// claudeWorkDir is an empty directory to run claude in, so no project's
// CLAUDE.md or settings apply.
func claudeWorkDir() string {
	base, err := os.UserCacheDir()
	if err != nil {
		base = os.TempDir()
	}
	dir := filepath.Join(base, "kubehealth", "claude-code")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return os.TempDir()
	}
	return dir
}

// ccRun runs one claude -p call. onText gets the current turn's answer and
// reasoning as they stream (at most every 300 ms). Cancelling ctx kills the
// process and anything it started.
func ccRun(ctx context.Context, req ccRequest, onText func(text, reasoning string)) (ccResult, error) {
	path, err := claudePath()
	if err != nil {
		return ccResult{}, err
	}
	cmd := exec.CommandContext(ctx, path, ccArgs(req, claudeModel())...)
	cmd.Dir = claudeWorkDir()
	cmd.Stdin = strings.NewReader(req.Prompt)
	var stderr bytes.Buffer
	cmd.Stderr = &capped{buf: &stderr, max: 8 << 10}
	ownProcessGroup(cmd)
	cmd.WaitDelay = 5 * time.Second
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return ccResult{}, err
	}
	if err := cmd.Start(); err != nil {
		return ccResult{}, fmt.Errorf("start claude: %w", err)
	}
	res, gotResult, perr := ccReadStream(stdout, onText)
	io.Copy(io.Discard, stdout)
	werr := cmd.Wait()

	switch {
	case ctx.Err() == context.DeadlineExceeded:
		return res, fmt.Errorf("Claude Code did not answer within %d min (Settings → AI Request Timeout)", getLLMTimeout())
	case ctx.Err() != nil:
		return res, ctx.Err()
	case perr != nil:
		if notSignedIn(perr.Error()) {
			claudeStatus(true)
			return res, errors.New("Claude Code is not signed in: run `claude auth login` in a terminal, then try again")
		}
		return res, perr
	case !gotResult:
		msg := strings.TrimSpace(stderr.String())
		if werr != nil && msg == "" {
			msg = werr.Error()
		}
		if msg == "" {
			msg = "no answer"
		}
		return res, fmt.Errorf("claude: %s", msg)
	}
	return res, nil
}

func notSignedIn(msg string) bool {
	m := strings.ToLower(msg)
	return strings.Contains(m, "/login") || strings.Contains(m, "not logged in") ||
		strings.Contains(m, "invalid api key") || strings.Contains(m, "not signed in")
}

// capped keeps the first max bytes written to it and drops the rest.
type capped struct {
	buf *bytes.Buffer
	max int
}

func (c *capped) Write(p []byte) (int, error) {
	if room := c.max - c.buf.Len(); room > 0 {
		c.buf.Write(p[:min(room, len(p))])
	}
	return len(p), nil
}

// ccLine is the part of a stream-json line KubeHealth reads.
type ccLine struct {
	Type    string `json:"type"`
	Subtype string `json:"subtype"`
	Event   *struct {
		Type  string `json:"type"`
		Delta *struct {
			Type     string `json:"type"`
			Text     string `json:"text"`
			Thinking string `json:"thinking"`
		} `json:"delta"`
	} `json:"event"`
	MCPServers []struct {
		Name   string `json:"name"`
		Status string `json:"status"`
	} `json:"mcp_servers"`
	IsError  bool     `json:"is_error"`
	Result   string   `json:"result"`
	Errors   []string `json:"errors"`
	NumTurns int      `json:"num_turns"`
	CostUSD  float64  `json:"total_cost_usd"`
}

// ccThinking stands in for the reasoning while Claude thinks: claude -p
// streams thinking without its text, and the panels show "Thinking…" when
// there is reasoning but no answer yet. It is never kept.
const ccThinking = "…"

// ccReadStream reads claude's stream-json output. The answer is the final
// turn's text: earlier turns of an RCA ("I'll read the logs") are dropped as
// each message starts. It reports whether a result line arrived; a result
// marked as an error comes back as err.
func ccReadStream(r io.Reader, onText func(text, reasoning string)) (res ccResult, gotResult bool, err error) {
	br := bufio.NewReaderSize(r, 64<<10)
	var text, reasoning strings.Builder
	var last time.Time
	for {
		line, rerr := br.ReadBytes('\n')
		if len(bytes.TrimSpace(line)) > 0 {
			var l ccLine
			if json.Unmarshal(line, &l) == nil {
				switch l.Type {
				case "system":
					if l.Subtype == "init" {
						for _, s := range l.MCPServers {
							if s.Status != "connected" {
								log.Printf("Claude Code: MCP server %s is %s", s.Name, s.Status)
							}
						}
					}
				case "stream_event":
					if l.Event == nil {
						break
					}
					if l.Event.Type == "message_start" {
						text.Reset()
					}
					if d := l.Event.Delta; l.Event.Type == "content_block_delta" && d != nil {
						text.WriteString(d.Text)
						reasoning.WriteString(d.Thinking)
						thinking := d.Type == "thinking_delta"
						if onText != nil && (d.Text != "" || thinking) && time.Since(last) >= 300*time.Millisecond {
							r := reasoning.String()
							if r == "" && thinking {
								r = ccThinking
							}
							onText(text.String(), r)
							last = time.Now()
						}
					}
				case "result":
					gotResult = true
					res.Turns, res.CostUSD = l.NumTurns, l.CostUSD
					res.Text = strings.TrimSpace(l.Result)
					if res.Text == "" {
						res.Text = strings.TrimSpace(text.String())
					}
					res.Reasoning = strings.TrimSpace(reasoning.String())
					if l.IsError || strings.HasPrefix(l.Subtype, "error") {
						msg := strings.TrimSpace(l.Result)
						if msg == "" {
							msg = strings.Join(l.Errors, "; ")
						}
						if msg == "" {
							msg = l.Subtype
						}
						err = fmt.Errorf("Claude Code: %s", msg)
					}
				}
			}
		}
		if rerr != nil {
			if rerr != io.EOF && err == nil && !gotResult {
				err = fmt.Errorf("read claude output: %w", rerr)
			}
			return res, gotResult, err
		}
	}
}

// ── The places KubeHealth calls a model ────────────────────────────────────

// ccChat is streamChatMsgs for Claude Code: the system message becomes the
// system prompt, the rest the prompt. It thinks as set in the dialog, except a
// note asked without thinking (the overview note), which doesn't think.
func ccChat(ctx context.Context, msgs []lmMsg, opts chatOpts, writeFn func(partial, reasoning string)) (string, string, error) {
	var sys string
	var prompt []string
	for _, m := range msgs {
		if m.Role == "system" {
			sys = m.Content
		} else {
			prompt = append(prompt, m.Content)
		}
	}
	req := ccRequest{System: sys, Prompt: strings.Join(prompt, "\n\n"), Thinking: claudeThinkingLevel()}
	if opts.NoThink {
		req.Thinking = "off"
	}
	res, err := ccRun(ctx, req, func(text, reasoning string) {
		if writeFn != nil {
			if opts.NoThink {
				reasoning = ""
			}
			writeFn(text, reasoning)
		}
	})
	if opts.NoThink {
		res.Reasoning = ""
	}
	return res.Text, res.Reasoning, err
}

// runRCAClaude runs an RCA as one Claude Code session: Claude calls
// KubeHealth's read-only tools through this server's MCP endpoint, and each
// call is a step of the evidence trail, as with a local model.
func runRCAClaude(cl *clusterState, anchorNS, system, prompt string, state *RCAState, upd func(func())) {
	if !acquireLLMSlotForUser() {
		rcaFail(state, upd, fmt.Errorf("too many concurrent AI requests (%d/%d active) — try again shortly",
			getLLMActiveCalls(), getLLMMaxCalls()))
		return
	}
	defer releaseLLMSlot()
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(getLLMTimeout())*time.Minute)
	defer cancel()

	run := &rcaToolRun{cl: cl, anchorNS: anchorNS, state: state, upd: upd, maxCalls: state.MaxIter, cancel: cancel}
	token := randomToken()
	rcaToolRuns.Store(token, run)
	defer rcaToolRuns.Delete(token)

	var tools []string
	for _, t := range rcaToolDefs() {
		tools = append(tools, mcpToolPrefix+t.Function.Name)
	}
	res, err := ccRun(ctx, ccRequest{
		System:   system,
		Prompt:   prompt,
		Thinking: claudeThinkingLevel(),
		MCPURL:   fmt.Sprintf("http://127.0.0.1:%s/mcp/rca/%s", cfg.Port, token),
		Tools:    tools,
	}, nil)
	if err != nil {
		if run.runaway() {
			err = fmt.Errorf("stopped: the model kept calling tools after its %d steps were used up", run.maxCalls)
		}
		rcaFail(state, upd, err)
		return
	}
	log.Printf("RCA via Claude Code: %d turns, $%.4f", res.Turns, res.CostUSD)
	upd(func() { state.Status = "done"; state.Result = cleanLMResponse(res.Text) })
}

func rcaFail(state *RCAState, upd func(func()), err error) {
	log.Printf("RCA: %v", err)
	upd(func() { state.Status = "error"; state.Error = err.Error() })
}

func randomToken() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}
