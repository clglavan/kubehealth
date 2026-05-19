package main

import (
	"encoding/json"
	"net/http"
	"sync"
)

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

type SettingsPayload struct {
	LogTailLines int64 `json:"log_tail_lines"`
}

func handleAPISettings(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(SettingsPayload{LogTailLines: getLogTailLines()})

	case http.MethodPost:
		var body SettingsPayload
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		if body.LogTailLines > 0 {
			setLogTailLines(body.LogTailLines)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}
