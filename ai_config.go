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

// fetchLMModelInfo calls LM Studio's /api/v0/models (the native API, not the OpenAI-compat
// /v1/models which omits context length) and returns info for the configured model.
func fetchLMModelInfo() *LMModelInfo {
	if !aiConfig.enabled {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "GET", lmStudioRoot()+"/api/v0/models", nil)
	if err != nil {
		return nil
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		return nil
	}
	defer resp.Body.Close()
	var result struct {
		Data []LMModelInfo `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil
	}
	for i := range result.Data {
		if result.Data[i].ID == aiConfig.model {
			return &result.Data[i]
		}
	}
	if len(result.Data) == 1 {
		return &result.Data[0]
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
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(payload)

	case http.MethodPost:
		var body struct {
			SystemPrompt string `json:"system_prompt"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		setSystemPrompt(body.SystemPrompt)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}
