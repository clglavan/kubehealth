package main

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

const defaultSystemPrompt = "You are a Kubernetes SRE expert. Analyze the provided Kubernetes events and logs. Identify issues, explain root causes, and provide specific actionable recommendations. Be concise and technical."

var (
	aiSysPromptMu   sync.RWMutex
	aiSysPrompt     string
	aiDefaultPrompt string // remembered so the UI can offer "reset to default"
)

// LMModelInfo is the relevant subset of LM Studio's /api/v0/models entry.
// That endpoint (not /v1/models) is where LM Studio exposes context length.
type LMModelInfo struct {
	ID                  string `json:"id"`
	State               string `json:"state,omitempty"`
	Arch                string `json:"arch,omitempty"`
	Quantization        string `json:"quantization,omitempty"`
	MaxContextLength    int    `json:"max_context_length,omitempty"`
	LoadedContextLength int    `json:"loaded_context_length,omitempty"` // actual RAM-allocated ctx; only set when model is loaded
}

// ctxLen returns the usable context length: loaded (actual) takes priority over max (theoretical).
func (m *LMModelInfo) ctxLen() int {
	if m.LoadedContextLength > 0 {
		return m.LoadedContextLength
	}
	return m.MaxContextLength
}

// lmStudioRoot derives the bare server root from the OpenAI-compat base URL.
// e.g. "http://localhost:1234/v1" → "http://localhost:1234"
func lmStudioRoot() string {
	return strings.TrimSuffix(strings.TrimSuffix(aiConfig.baseURL, "/v1"), "/")
}

// AIConfigPayload is the JSON payload for GET /api/ai/config.
type AIConfigPayload struct {
	Enabled             bool         `json:"enabled"`
	BaseURL             string       `json:"base_url"`
	Model               string       `json:"model"`
	SystemPrompt        string       `json:"system_prompt"`
	DefaultSystemPrompt string       `json:"default_system_prompt"`
	ModelInfo           *LMModelInfo `json:"model_info,omitempty"`
	LogTailLines        int64        `json:"log_tail_lines"`
	// Embedded mode (only set in the kubehealth-embed build flavor).
	Embedded bool `json:"embedded"`
	// Backend is where AI calls go: "local" (LM Studio or embedded) or
	// "claude-code"; ClaudeModel and Claude describe Claude Code.
	Backend        string        `json:"backend"`
	ClaudeModel    string        `json:"claude_model"`
	ClaudeThinking string        `json:"claude_thinking"`
	Claude         *ClaudeStatus `json:"claude,omitempty"`
}

func initSystemPrompt() {
	sp := os.Getenv("AI_SYSTEM_PROMPT")
	if sp == "" {
		sp = defaultSystemPrompt
	}
	aiDefaultPrompt = sp
	aiSysPromptMu.Lock()
	aiSysPrompt = sp
	aiSysPromptMu.Unlock()
}

func getSystemPrompt() string {
	aiSysPromptMu.RLock()
	defer aiSysPromptMu.RUnlock()
	return aiSysPrompt
}

func setSystemPrompt(s string) {
	aiSysPromptMu.Lock()
	aiSysPrompt = s
	aiSysPromptMu.Unlock()
}

// fetchLMModelInfo returns info about the active model. In embedded builds it
// reports the bundled model directly. Otherwise it queries LM Studio's native
// /api/v0/models endpoint (the OpenAI-compat /v1/models omits context length).
func fetchLMModelInfo() *LMModelInfo {
	if !aiConfig.enabled {
		return nil
	}
	if useClaudeCode() {
		st := claudeStatus(false)
		state := "loaded"
		switch {
		case !st.Available:
			state = "not found"
		case !st.LoggedIn:
			state = "signed out"
		}
		return &LMModelInfo{ID: orDefault(claudeModel()), State: state, MaxContextLength: claudeCtxLen, LoadedContextLength: claudeCtxLen}
	}
	if isEmbedded() {
		ctx, running := embeddedRuntime()
		state := "loaded"
		if !running {
			state = "stopped"
		}
		return &LMModelInfo{
			ID:                  aiConfig.model,
			State:               state,
			Arch:                embeddedModelArch(),
			Quantization:        embeddedModelQuantization(),
			MaxContextLength:    ctx,
			LoadedContextLength: ctx,
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	var result struct {
		Data []LMModelInfo `json:"data"`
	}
	if getModelsJSON(ctx, lmStudioRoot()+"/api/v0/models", &result) {
		return pickModel(result.Data)
	}
	return fetchLlamaServerModelInfo(ctx)
}

// fetchLlamaServerModelInfo reads the context length llama-server reports for
// each slot in its /v1/models entries (meta.n_ctx). It returns nil for servers
// that don't report one.
func fetchLlamaServerModelInfo(ctx context.Context) *LMModelInfo {
	var result struct {
		Data []struct {
			ID   string `json:"id"`
			Meta struct {
				NCtx int `json:"n_ctx"`
			} `json:"meta"`
		} `json:"data"`
	}
	if !getModelsJSON(ctx, aiConfig.baseURL+"/models", &result) {
		return nil
	}
	var models []LMModelInfo
	for _, d := range result.Data {
		models = append(models, LMModelInfo{ID: d.ID, State: "loaded", LoadedContextLength: d.Meta.NCtx})
	}
	m := pickModel(models)
	if m == nil || m.LoadedContextLength <= 0 {
		return nil
	}
	return m
}

// getModelsJSON GETs url and decodes a 200 answer into out.
func getModelsJSON(ctx context.Context, url string, out any) bool {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return false
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK && json.NewDecoder(resp.Body).Decode(out) == nil
}

// pickModel returns the configured model's entry, or the only one.
func pickModel(models []LMModelInfo) *LMModelInfo {
	for i := range models {
		if models[i].ID == aiConfig.model {
			return &models[i]
		}
	}
	if len(models) == 1 {
		return &models[0]
	}
	return nil
}

func handleAPIAIConfig(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		payload := AIConfigPayload{
			Enabled:             aiConfig.enabled,
			BaseURL:             aiConfig.baseURL,
			Model:               aiConfig.model,
			SystemPrompt:        getSystemPrompt(),
			DefaultSystemPrompt: aiDefaultPrompt,
			ModelInfo:           fetchLMModelInfo(),
			LogTailLines:        getLogTailLines(),
			Embedded:            isEmbedded(),
			Backend:             backendLocal,
			ClaudeModel:         claudeModel(),
			ClaudeThinking:      claudeThinkingLevel(),
		}
		if useClaudeCode() {
			payload.Backend = backendClaude
		}
		if r.URL.Query().Get("claude") == "1" || useClaudeCode() {
			st := claudeStatus(r.URL.Query().Get("fresh") == "1")
			payload.Claude = &st
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(payload)

	case http.MethodPost:
		var body struct {
			SystemPrompt    *string `json:"system_prompt,omitempty"`
			EmbeddedCtxSize *int    `json:"embedded_ctx_size,omitempty"`
			Backend         *string `json:"backend,omitempty"`
			ClaudeModel     *string `json:"claude_model,omitempty"`
			ClaudeThinking  *string `json:"claude_thinking,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		if body.SystemPrompt != nil {
			setSystemPrompt(*body.SystemPrompt)
		}
		if body.ClaudeModel != nil {
			if err := setClaudeModel(*body.ClaudeModel); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
		}
		if body.ClaudeThinking != nil {
			if err := setClaudeThinking(*body.ClaudeThinking); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
		}
		if body.Backend != nil {
			if err := setAIBackend(*body.Backend); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
		}
		if body.EmbeddedCtxSize != nil {
			if !isEmbedded() {
				http.Error(w, "context size is only configurable in the embedded build", http.StatusBadRequest)
				return
			}
			if err := restartEmbedded(*body.EmbeddedCtxSize); err != nil {
				http.Error(w, "restart failed: "+err.Error(), http.StatusInternalServerError)
				return
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}
