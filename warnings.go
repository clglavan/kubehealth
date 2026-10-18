package main

import (
	"context"
	"log"
	"sort"
	"sync"
	"time"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// The warning feed lists every Warning event in the cluster with one call
// (fieldSelector type=Warning, filtered by the API server), so the home page
// sees new problems within warnFeedInterval instead of waiting for the
// per-namespace sweep to come round. The sweep still loads Normal events.
const (
	warnFeedInterval = 10 * time.Second
	warnFeedPageSize = 500
	warnFeedMaxPages = 20
)

type warnFeedState struct {
	sync.RWMutex
	byNS        map[string][]EventSummary // event namespace → warnings, newest first
	fetchedAt   time.Time                 // last successful fetch
	attemptedAt time.Time
	truncated   bool   // more than warnFeedMaxPages pages of warnings
	disabled    bool   // cluster-wide list not possible; fall back to the sweep
	lastErr     string // last fetch error, cleared on success
}

// warnFeedDue reports whether the worker should spend this tick on the feed.
// The feed only runs when watching all namespaces: with -namespaces the
// user may not be allowed to list events cluster-wide.
func (c *clusterState) warnFeedDue() bool {
	if !cfg.AllNS {
		return false
	}
	c.feed.RLock()
	defer c.feed.RUnlock()
	return !c.feed.disabled && time.Since(c.feed.attemptedAt) >= warnFeedInterval
}

// refreshWarnings fetches all Warning events cluster-wide and stores them.
func (c *clusterState) refreshWarnings(ctx context.Context) {
	c.feed.Lock()
	c.feed.attemptedAt = time.Now()
	c.feed.Unlock()

	byNS := map[string][]EventSummary{}
	opts := metav1.ListOptions{FieldSelector: "type=Warning", Limit: warnFeedPageSize}
	truncated := false
	for page := 1; ; page++ {
		list, err := c.client.CoreV1().Events("").List(ctx, opts)
		if err != nil {
			if ctx.Err() != nil {
				return // switched away mid-fetch
			}
			c.feed.Lock()
			c.feed.lastErr = err.Error()
			if k8serrors.IsForbidden(err) {
				c.feed.disabled = true
				log.Printf("Warning feed [%s]: cluster-wide event list forbidden, using the per-namespace sweep only: %v", c.name, err)
			}
			c.feed.Unlock()
			return
		}
		for _, e := range list.Items {
			byNS[e.Namespace] = append(byNS[e.Namespace], toSummary(e))
		}
		if list.Continue == "" {
			break
		}
		if page >= warnFeedMaxPages {
			truncated = true
			break
		}
		opts.Continue = list.Continue
	}
	for _, evs := range byNS {
		sortNewestFirst(evs)
	}

	c.feed.Lock()
	c.feed.byNS = byNS
	c.feed.fetchedAt = time.Now()
	c.feed.truncated = truncated
	c.feed.lastErr = ""
	c.feed.Unlock()
	c.assembleDashboard()
}

// warnFeedStatus is what the home page shows about warning freshness.
type warnFeedStatus struct {
	Mode      string `json:"mode"` // "cluster" (one cluster-wide call) or "sweep"
	CheckedAt string `json:"checked_at,omitempty"`
	AgeSec    int    `json:"age_s"`
	Truncated bool   `json:"truncated"`
	Error     string `json:"error,omitempty"`
}

func (c *clusterState) warnFeedStatus() warnFeedStatus {
	c.feed.RLock()
	defer c.feed.RUnlock()
	st := warnFeedStatus{Mode: "cluster", Truncated: c.feed.truncated, Error: c.feed.lastErr}
	if !cfg.AllNS || c.feed.disabled {
		st.Mode = "sweep"
	}
	if !c.feed.fetchedAt.IsZero() {
		st.CheckedAt = c.feed.fetchedAt.Format(time.RFC3339)
		st.AgeSec = int(time.Since(c.feed.fetchedAt).Seconds())
	}
	return st
}

// namespaceEvents returns the sweep's cached events for ns merged with the
// warning feed, newest first. Use it instead of reading nsCache directly so
// warnings show up everywhere as soon as the feed sees them.
func (c *clusterState) namespaceEvents(ns string) []EventSummary {
	var base []EventSummary
	if v, ok := c.nsCache.Load(ns); ok {
		entry := v.(*nsEntry)
		entry.RLock()
		base = append(base, entry.events...)
		entry.RUnlock()
	}
	c.feed.RLock()
	warns := c.feed.byNS[ns]
	c.feed.RUnlock()
	return mergeEvents(base, warns)
}

// clusterScopeNS is the pseudo-namespace for cluster-scoped objects (Nodes,
// PersistentVolumes, …). Kubernetes records their events in "default"; the
// pages show them on their own instead.
const clusterScopeNS = "_cluster"

// scopeEvents is what a namespace page, and the AI, RCA, logs and correlate
// features working from it, see: the namespace's own objects, or for
// clusterScopeNS, every cluster-scoped object's events.
func (c *clusterState) scopeEvents(ns string) []EventSummary {
	var out []EventSummary
	if ns == clusterScopeNS {
		for _, evs := range c.allNamespaceEvents() {
			for _, e := range evs {
				if e.ObjectNamespace == "" && e.Kind != "" {
					out = append(out, e)
				}
			}
		}
		sortNewestFirst(out)
		return out
	}
	for _, e := range c.namespaceEvents(ns) {
		if e.ObjectNamespace != "" || e.Kind == "" {
			out = append(out, e)
		}
	}
	return out
}

// apiNamespace maps a page namespace to the one used in API calls.
func apiNamespace(ns string) string {
	if ns == clusterScopeNS {
		return ""
	}
	return ns
}

// allNamespaceEvents returns namespaceEvents for every namespace known to
// either the sweep or the warning feed.
func (c *clusterState) allNamespaceEvents() map[string][]EventSummary {
	names := map[string]bool{}
	c.nsCache.Range(func(k, _ any) bool { names[k.(string)] = true; return true })
	c.feed.RLock()
	for ns := range c.feed.byNS {
		names[ns] = true
	}
	c.feed.RUnlock()
	out := make(map[string][]EventSummary, len(names))
	for ns := range names {
		out[ns] = c.namespaceEvents(ns)
	}
	return out
}

// mergeEvents combines two event lists from the same namespace. Event names
// are unique per namespace; when both lists have an event, the copy that was
// observed more recently wins.
func mergeEvents(base, extra []EventSummary) []EventSummary {
	if len(extra) == 0 {
		return base
	}
	out := make([]EventSummary, 0, len(base)+len(extra))
	out = append(out, base...)
	idx := make(map[string]int, len(out))
	for i, e := range out {
		idx[e.Name] = i
	}
	for _, e := range extra {
		if i, ok := idx[e.Name]; ok {
			if e.LastTime.After(out[i].LastTime) || e.Count > out[i].Count {
				out[i] = e
			}
			continue
		}
		idx[e.Name] = len(out)
		out = append(out, e)
	}
	sortNewestFirst(out)
	return out
}

// sortNewestFirst orders events newest first. Events seen in the same second
// go by namespace and name, so lists merged from maps always come out in one
// order (prompts built from them don't change between runs).
func sortNewestFirst(evs []EventSummary) {
	sort.SliceStable(evs, func(i, j int) bool {
		a, b := evs[i], evs[j]
		if !a.LastTime.Equal(b.LastTime) {
			return a.LastTime.After(b.LastTime)
		}
		if a.Namespace != b.Namespace {
			return a.Namespace < b.Namespace
		}
		return a.Name < b.Name
	})
}
