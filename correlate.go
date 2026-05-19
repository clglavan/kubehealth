package main

import (
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"time"
)

const correlateBufferMin = 5

type CorrelatedEvent struct {
	Namespace  string    `json:"namespace"`
	Kind       string    `json:"kind"`
	ObjectName string    `json:"object_name"`
	Reason     string    `json:"reason"`
	Message    string    `json:"message"`
	Type       string    `json:"type"`
	Age        string    `json:"age"`
	LastTime   time.Time `json:"last_time"`
	Count      int32     `json:"count"`
}

type CorrelateResponse struct {
	Namespace  string            `json:"namespace"`
	Kind       string            `json:"kind"`
	Name       string            `json:"name"`
	TimeFrom   time.Time         `json:"time_from"`
	TimeTo     time.Time         `json:"time_to"`
	WindowFrom time.Time         `json:"window_from"`
	WindowTo   time.Time         `json:"window_to"`
	BufferMin  int               `json:"buffer_min"`
	Correlated []CorrelatedEvent `json:"correlated"`
}

func handleAPICorrelate(w http.ResponseWriter, r *http.Request) {
	// /api/correlate/{namespace}/{kind}/{name}
	path := strings.TrimPrefix(r.URL.Path, "/api/correlate/")
	parts := strings.SplitN(path, "/", 3)
	if len(parts) < 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		http.Error(w, "expected /api/correlate/{namespace}/{kind}/{name}", http.StatusBadRequest)
		return
	}
	ns, kind, name := parts[0], parts[1], parts[2]

	resp := CorrelateResponse{
		Namespace: ns,
		Kind:      kind,
		Name:      name,
		BufferMin: correlateBufferMin,
	}

	// Collect the anchor object's events to establish the time window.
	var anchorEvents []EventSummary
	if v, ok := nsCache.Load(ns); ok {
		e := v.(*nsEntry)
		e.RLock()
		for _, ev := range e.events {
			if strings.EqualFold(ev.Kind, kind) && ev.ObjectName == name {
				anchorEvents = append(anchorEvents, ev)
			}
		}
		e.RUnlock()
	}

	if len(anchorEvents) == 0 {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
		return
	}

	// Find the earliest first-time and latest last-time across all anchor events.
	var earliest, latest time.Time
	for _, ev := range anchorEvents {
		if !ev.FirstTime.IsZero() && (earliest.IsZero() || ev.FirstTime.Before(earliest)) {
			earliest = ev.FirstTime
		}
		if ev.LastTime.After(latest) {
			latest = ev.LastTime
		}
	}
	if earliest.IsZero() {
		earliest = latest
	}

	windowStart := earliest.Add(-time.Duration(correlateBufferMin) * time.Minute)
	windowEnd := latest.Add(time.Duration(correlateBufferMin) * time.Minute)

	resp.TimeFrom = earliest
	resp.TimeTo = latest
	resp.WindowFrom = windowStart
	resp.WindowTo = windowEnd

	// Scan every namespace cache for events whose LastTime falls inside the window,
	// excluding the anchor object itself.
	nsCache.Range(func(key, val any) bool {
		otherNS := key.(string)
		e := val.(*nsEntry)
		e.RLock()
		for _, ev := range e.events {
			if otherNS == ns && strings.EqualFold(ev.Kind, kind) && ev.ObjectName == name {
				continue // skip the anchor itself
			}
			if ev.LastTime.After(windowStart) && ev.LastTime.Before(windowEnd) {
				resp.Correlated = append(resp.Correlated, CorrelatedEvent{
					Namespace:  otherNS,
					Kind:       ev.Kind,
					ObjectName: ev.ObjectName,
					Reason:     ev.Reason,
					Message:    ev.Message,
					Type:       ev.Type,
					Age:        ev.Age,
					LastTime:   ev.LastTime,
					Count:      ev.Count,
				})
			}
		}
		e.RUnlock()
		return true
	})

	// Most recent events first.
	sort.Slice(resp.Correlated, func(i, j int) bool {
		return resp.Correlated[i].LastTime.After(resp.Correlated[j].LastTime)
	})

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
