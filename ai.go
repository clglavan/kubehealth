package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

type aiCfgT struct {
	baseURL string
	model   string
	enabled bool
}

// AIInsight is the JSON payload for /api/ai/insights/… responses.
type AIInsight struct {
	Content   string `json:"content"`
	Model     string `json:"model"`
	UpdatedAt string `json:"updated_at"`
	Enabled   bool   `json:"enabled"`
}

var (
	aiConfig       aiCfgT
	aiInsightCache sync.Map // cacheKey → *AIInsight
	aiRunning      sync.Map // cacheKey → struct{} (dedup guard)
)

func initAI() {
	url := os.Getenv("LMSTUDIO_URL")
	if url == "" {
		url = "http://localhost:1234/v1"
	}
	model := os.Getenv("LMSTUDIO_MODEL")
	if model == "" {
		model = "qwen/qwen3-4b-2507"
	}
	v := os.Getenv("AI_ENABLED")
	enabled := v != "false" && v != "0"
	aiConfig = aiCfgT{baseURL: url, model: model, enabled: enabled}
	if enabled {
		log.Printf("AI monitoring enabled — model: %s  url: %s", model, url)
	}
}

// parseCacheKey converts a URL suffix into an internal cache key.
// Supported forms: "ns/{namespace}" or "object/{namespace}/{kind}/{name}"
func parseCacheKey(suffix string) (string, bool) {
	suffix = strings.Trim(suffix, "/")
	if ns, ok := strings.CutPrefix(suffix, "ns/"); ok && ns != "" {
		return "ns:" + ns, true
	}
	if obj, ok := strings.CutPrefix(suffix, "object/"); ok {
		parts := strings.SplitN(obj, "/", 3)
		if len(parts) == 3 && parts[0] != "" && parts[1] != "" && parts[2] != "" {
			return "obj:" + obj, true
		}
	}
	return "", false
}

func handleAPIAIInsights(w http.ResponseWriter, r *http.Request) {
	suffix := strings.TrimPrefix(r.URL.Path, "/api/ai/insights/")
	key, ok := parseCacheKey(suffix)
	w.Header().Set("Content-Type", "application/json")
	if !ok {
		json.NewEncoder(w).Encode(AIInsight{Enabled: aiConfig.enabled, Model: aiConfig.model})
		return
	}
	if v, hit := aiInsightCache.Load(key); hit {
		json.NewEncoder(w).Encode(v.(*AIInsight))
		return
	}
	json.NewEncoder(w).Encode(AIInsight{Enabled: aiConfig.enabled, Model: aiConfig.model})
}

func handleAPIAIAnalyze(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !aiConfig.enabled {
		http.Error(w, "AI not enabled", http.StatusServiceUnavailable)
		return
	}
	suffix := strings.TrimPrefix(r.URL.Path, "/api/ai/analyze/")
	key, ok := parseCacheKey(suffix)
	if !ok {
		http.Error(w, "invalid path", http.StatusBadRequest)
		return
	}
	triggerScopedAnalysis(key)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "triggered"})
}

func triggerScopedAnalysis(key string) {
	if _, loaded := aiRunning.LoadOrStore(key, struct{}{}); loaded {
		return
	}
	go func() {
		defer aiRunning.Delete(key)
		var err error
		if ns, ok := strings.CutPrefix(key, "ns:"); ok {
			err = analyzeNSKey(key, ns)
		} else if obj, ok := strings.CutPrefix(key, "obj:"); ok {
			parts := strings.SplitN(obj, "/", 3)
			if len(parts) == 3 {
				err = analyzeObjKey(key, parts[0], parts[1], parts[2])
			}
		}
		if err != nil {
			log.Printf("AI [%s]: %v", key, err)
		}
	}()
}

func analyzeNSKey(key, ns string) error {
	var events []EventSummary
	if v, ok := nsCache.Load(ns); ok {
		e := v.(*nsEntry)
		e.RLock()
		events = append(events, e.events...)
		e.RUnlock()
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "Analyze all Kubernetes events in namespace %q.\n\n", ns)
	if len(events) == 0 {
		sb.WriteString("No events recorded in this namespace.\n")
	} else {
		var nw int
		for _, ev := range events {
			if ev.Type == "Warning" {
				nw++
			}
		}
		sb.WriteString(fmt.Sprintf("Events (%d total, %d warnings):\n", len(events), nw))
		for i, ev := range events {
			if i >= 60 {
				sb.WriteString(fmt.Sprintf("... and %d more\n", len(events)-60))
				break
			}
			sb.WriteString(fmt.Sprintf("- [%s] %s/%s  %s: %s  (×%d, %s ago)\n",
				ev.Type, ev.Kind, ev.ObjectName, ev.Reason, ev.Message, ev.Count, ev.Age))
		}
	}
	sb.WriteString("\nRespond exactly:\n**Summary**: <one sentence about overall namespace health>\n**Issues**:\n- <issue or 'None'>\n**Actions**:\n- <action or 'None'>\n\nBe concise and actionable.")
	return storeAnalysis(key, sb.String())
}

func analyzeObjKey(key, ns, kind, name string) error {
	var events []EventSummary
	if v, ok := nsCache.Load(ns); ok {
		e := v.(*nsEntry)
		e.RLock()
		for _, ev := range e.events {
			if strings.EqualFold(ev.Kind, kind) && ev.ObjectName == name {
				events = append(events, ev)
			}
		}
		e.RUnlock()
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "Analyze events for %s %q in namespace %q.\n\n", kind, name, ns)
	if len(events) == 0 {
		sb.WriteString("No events recorded for this object.\n")
	} else {
		fmt.Fprintf(&sb, "Events (%d total):\n", len(events))
		for _, ev := range events {
			fmt.Fprintf(&sb, "- [%s] %s: %s  (×%d, %s ago)\n",
				ev.Type, ev.Reason, ev.Message, ev.Count, ev.Age)
		}
	}

	// Object describe — always included; gives the AI current spec/status/conditions
	info := fetchObjectInfo(ns, kind, name)
	switch {
	case info.NotFound:
		sb.WriteString("\nNote: object no longer exists in the cluster.\n")
	case info.Error == "":
		sb.WriteString("\nObject describe:\n")
		if info.Age != "" {
			fmt.Fprintf(&sb, "Age: %s\n", info.Age)
		}
		if len(info.Labels) > 0 {
			sb.WriteString("Labels:")
			for k, v := range info.Labels {
				fmt.Fprintf(&sb, " %s=%s", k, v)
			}
			sb.WriteString("\n")
		}
		for _, sec := range info.Sections {
			fmt.Fprintf(&sb, "%s:\n", sec.Title)
			for _, row := range sec.Rows {
				fmt.Fprintf(&sb, "  %s: %s\n", row.Key, row.Value)
			}
		}
		if len(info.Conditions) > 0 {
			sb.WriteString("Conditions:\n")
			for _, c := range info.Conditions {
				if c.Reason != "" || c.Message != "" {
					fmt.Fprintf(&sb, "  %s: %s (%s: %s)\n", c.Type, c.Status, c.Reason, c.Message)
				} else {
					fmt.Fprintf(&sb, "  %s: %s\n", c.Type, c.Status)
				}
			}
		}
	}

	// Pod logs — actual container output, most useful for crash diagnosis
	if strings.EqualFold(kind, "Pod") {
		logs, err := fetchLogsForAI(ns, name)
		if err == nil && strings.TrimSpace(logs) != "" {
			fmt.Fprintf(&sb, "\nPod logs (last %d lines):\n", getLogTailLines())
			sb.WriteString(logs)
		} else if err != nil {
			fmt.Fprintf(&sb, "\nNote: could not fetch pod logs: %v\n", err)
		}
	}

	sb.WriteString("\nRespond exactly:\n**Summary**: <one sentence about this specific resource>\n**Issues**:\n- <issue or 'None'>\n**Actions**:\n- <action or 'None'>\n\nBe concise and actionable.")
	return storeAnalysis(key, sb.String())
}

func storeAnalysis(key, prompt string) error {
	content, err := callLMStudio(prompt)
	if err != nil {
		return err
	}
	aiInsightCache.Store(key, &AIInsight{
		Content:   cleanLMResponse(content),
		Model:     aiConfig.model,
		UpdatedAt: time.Now().Format("2006-01-02 15:04:05 MST"),
		Enabled:   true,
	})
	return nil
}

type lmMsg struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type lmReq struct {
	Model    string  `json:"model"`
	Messages []lmMsg `json:"messages"`
	Stream   bool    `json:"stream"`
}

type lmResp struct {
	Choices []struct {
		Message lmMsg `json:"message"`
	} `json:"choices"`
}

func callLMStudio(prompt string) (string, error) {
	msgs := []lmMsg{{Role: "user", Content: prompt}}
	if sp := getSystemPrompt(); sp != "" {
		msgs = append([]lmMsg{{Role: "system", Content: sp}}, msgs...)
	}
	body, _ := json.Marshal(lmReq{
		Model:    aiConfig.model,
		Messages: msgs,
		Stream:   false,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "POST", aiConfig.baseURL+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("LM Studio: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("LM Studio returned %d", resp.StatusCode)
	}
	var out lmResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", fmt.Errorf("decode: %w", err)
	}
	if len(out.Choices) == 0 {
		return "", fmt.Errorf("empty response from LM Studio")
	}
	return out.Choices[0].Message.Content, nil
}

// cleanLMResponse strips <think>…</think> blocks emitted by chain-of-thought models (e.g. Qwen3).
func cleanLMResponse(s string) string {
	if i := strings.Index(s, "</think>"); i >= 0 {
		s = strings.TrimSpace(s[i+8:])
	}
	return s
}
