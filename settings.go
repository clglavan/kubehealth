package main

import (
	"encoding/json"
	"net/http"
	"sync"
)

// ── Log tail lines ────────────────────────────────────────────────────────────

var (
	logTailLinesMu  sync.RWMutex
	logTailLinesVal int64 = 150
)

func getLogTailLines() int64 {
	logTailLinesMu.RLock()
	defer logTailLinesMu.RUnlock()
	return logTailLinesVal
}

func setLogTailLines(n int64) {
	if n < 10 {
		n = 10
	}
	if n > 5000 {
		n = 5000
	}
	logTailLinesMu.Lock()
	logTailLinesVal = n
	logTailLinesMu.Unlock()
}

// ── RCA max iterations ────────────────────────────────────────────────────────

var (
	rcaMaxIterMu  sync.RWMutex
	rcaMaxIterVal = 5
)

func getRCAMaxIter() int {
	rcaMaxIterMu.RLock()
	defer rcaMaxIterMu.RUnlock()
	return rcaMaxIterVal
}

func setRCAMaxIter(n int) {
	if n < 1 {
		n = 1
	}
	if n > 20 {
		n = 20
	}
	rcaMaxIterMu.Lock()
	rcaMaxIterVal = n
	rcaMaxIterMu.Unlock()
}

// ── HTTP handler ──────────────────────────────────────────────────────────────

// ── LLM concurrency ───────────────────────────────────────────────────────────

var (
	llmMaxCallsMu  sync.RWMutex
	llmMaxCallsVal = 3

	llmActiveCallsMu sync.Mutex
	llmActiveCalls   int
)

func getLLMMaxCalls() int {
	llmMaxCallsMu.RLock()
	defer llmMaxCallsMu.RUnlock()
	return llmMaxCallsVal
}

func setLLMMaxCalls(n int) {
	if n < 1 {
		n = 1
	}
	if n > 10 {
		n = 10
	}
	llmMaxCallsMu.Lock()
	llmMaxCallsVal = n
	llmMaxCallsMu.Unlock()
}

// acquireLLMSlot reserves a concurrency slot. Returns false when at capacity.
func acquireLLMSlot() bool {
	llmMaxCallsMu.RLock()
	max := llmMaxCallsVal
	llmMaxCallsMu.RUnlock()

	llmActiveCallsMu.Lock()
	defer llmActiveCallsMu.Unlock()
	if llmActiveCalls >= max {
		return false
	}
	llmActiveCalls++
	return true
}

func releaseLLMSlot() {
	llmActiveCallsMu.Lock()
	llmActiveCalls--
	llmActiveCallsMu.Unlock()
}

func getLLMActiveCalls() int {
	llmActiveCallsMu.Lock()
	defer llmActiveCallsMu.Unlock()
	return llmActiveCalls
}

// ── LLM request timeout ───────────────────────────────────────────────────────

var (
	llmTimeoutMu  sync.RWMutex
	llmTimeoutVal = 10 // minutes
)

func getLLMTimeout() int {
	llmTimeoutMu.RLock()
	defer llmTimeoutMu.RUnlock()
	return llmTimeoutVal
}

func setLLMTimeout(minutes int) {
	if minutes < 1 {
		minutes = 1
	}
	if minutes > 60 {
		minutes = 60
	}
	llmTimeoutMu.Lock()
	llmTimeoutVal = minutes
	llmTimeoutMu.Unlock()
}

type SettingsPayload struct {
	LogTailLines int64 `json:"log_tail_lines"`
	RCAMaxIter   int   `json:"rca_max_iter"`
	LLMMaxCalls  int   `json:"llm_max_calls"`
	LLMTimeout   int   `json:"llm_timeout_min"`
}

func handleAPISettings(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(SettingsPayload{
			LogTailLines: getLogTailLines(),
			RCAMaxIter:   getRCAMaxIter(),
			LLMMaxCalls:  getLLMMaxCalls(),
			LLMTimeout:   getLLMTimeout(),
		})

	case http.MethodPost:
		var body SettingsPayload
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		if body.LogTailLines > 0 {
			setLogTailLines(body.LogTailLines)
		}
		if body.RCAMaxIter > 0 {
			setRCAMaxIter(body.RCAMaxIter)
		}
		if body.LLMMaxCalls > 0 {
			setLLMMaxCalls(body.LLMMaxCalls)
		}
		if body.LLMTimeout > 0 {
			setLLMTimeout(body.LLMTimeout)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}
