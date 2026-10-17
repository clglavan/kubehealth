package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"
)

// Problems are the rows on the home page: one per failing workload, built
// from Warning events. Every event already carries firstTimestamp,
// lastTimestamp and count, so tiers work from the moment KubeHealth starts,
// without keeping any history.

const (
	tierNewMax         = 2 * time.Hour    // first seen less than this ago → "new"
	tierOngoingMax     = 24 * time.Hour   // ... less than this → "ongoing", else "chronic"
	idleAfter          = 15 * time.Minute // no occurrence for this long → idle
	patternStartWindow = 10 * time.Second // problems first seen this close together are linked
	messageMaxLen      = 240
)

var tierOrder = map[string]int{"new": 0, "ongoing": 1, "chronic": 2}

// infraReasons are reasons that usually point at shared infrastructure (a
// registry secret, the scheduler, storage, the network). The same one in
// three or more namespaces becomes a pattern.
var infraReasons = map[string]bool{
	"FailedToRetrieveImagePullSecret": true,
	"FailedScheduling":                true,
	"FailedMount":                     true,
	"FailedAttachVolume":              true,
	"FailedCreatePodSandBox":          true,
	"NetworkNotReady":                 true,
	"DNSConfigForming":                true,
	"Evicted":                         true,
}

type ReasonCount struct {
	Reason string `json:"reason"`
	Count  int64  `json:"count"`
}

// PatternItem is one member problem of a pattern row.
type PatternItem struct {
	Namespace   string `json:"namespace"`
	Kind        string `json:"kind"`
	Name        string `json:"name"`
	Reason      string `json:"reason"`
	Occurrences int64  `json:"occurrences"`
	Last        string `json:"last"`
	Idle        bool   `json:"idle"`
	Focus       string `json:"focus"`
}

type Problem struct {
	Key         string        `json:"key"`
	Scope       string        `json:"scope"` // "namespace", "cluster" or "pattern"
	Kind        string        `json:"kind"`
	Title       string        `json:"title"`
	Namespaces  []string      `json:"namespaces"`
	OpenNS      string        `json:"open_ns,omitempty"` // namespace page to open
	Focus       string        `json:"focus,omitempty"`   // Kind/Name to select there
	Tier        string        `json:"tier"`
	Cascade     bool          `json:"cascade"`
	Idle        bool          `json:"idle"`
	Inferred    bool          `json:"inferred"`
	Pattern     string        `json:"pattern,omitempty"` // why the members were grouped
	Reasons     []ReasonCount `json:"reasons"`
	Occurrences int64         `json:"occurrences"`
	Members     string        `json:"members,omitempty"` // e.g. "61 Jobs · 66 Pods"
	FirstSeen   time.Time     `json:"first_seen"`
	LastSeen    time.Time     `json:"last_seen"`
	Since       string        `json:"since"`
	SinceNote   string        `json:"since_note,omitempty"` // when "since" isn't the oldest event
	Last        string        `json:"last"`
	Message     string        `json:"message,omitempty"`
	Pod         string        `json:"pod,omitempty"` // newest failing Pod, for logs and RCA
	Items       []PatternItem `json:"items,omitempty"`
}

// ---- Building problems ---------------------------------------------------

type groupMember struct {
	depth int
	warn  bool
	last  time.Time
}

type problemGroup struct {
	ns   string
	root objRef
	// The owner is "inferred" until at least one member's chain to the root
	// has been confirmed through the API; siblings follow the same names.
	hasLinks, confirmedLink bool
	inferred                bool
	members                 map[objRef]*groupMember
	chains                  map[objRef][]objRef
	warnings                []EventSummary
}

// buildProblems turns the merged event caches into home-page problems,
// newest tier first.
func (c *clusterState) buildProblems(events map[string][]EventSummary, now time.Time) []Problem {
	var problems []Problem
	var clusterWarnings []EventSummary

	for ns, evs := range events {
		type resolved struct {
			chain    []objRef
			inferred bool
		}
		groups := map[objRef]*problemGroup{}
		chainOf := map[objRef]resolved{}
		for _, e := range evs {
			if e.Kind == "" || e.ObjectName == "" {
				continue
			}
			if e.ObjectNamespace == "" {
				if e.Type == "Warning" {
					clusterWarnings = append(clusterWarnings, e)
				}
				continue
			}
			obj := objRef{e.Kind, e.ObjectName}
			res, ok := chainOf[obj]
			if !ok {
				res.chain, res.inferred = c.ownerChain(ns, obj)
				chainOf[obj] = res
			}
			chain, inferred := res.chain, res.inferred
			root := chain[len(chain)-1]
			g := groups[root]
			if g == nil {
				g = &problemGroup{ns: ns, root: root, members: map[objRef]*groupMember{}, chains: map[objRef][]objRef{}}
				groups[root] = g
			}
			if len(chain) > 1 {
				g.hasLinks = true
				g.confirmedLink = g.confirmedLink || !inferred
			}
			m := g.members[obj]
			if m == nil {
				m = &groupMember{depth: len(chain) - 1}
				g.members[obj] = m
				g.chains[obj] = chain
			}
			if e.Type == "Warning" {
				m.warn = true
				if e.LastTime.After(m.last) {
					m.last = e.LastTime
				}
				g.warnings = append(g.warnings, e)
			}
		}
		for _, g := range groups {
			if len(g.warnings) == 0 {
				continue
			}
			g.inferred = g.hasLinks && !g.confirmedLink
			c.confirmOwners(g)
			problems = append(problems, c.groupProblem(g, now))
		}
	}

	problems = append(problems, clusterProblems(clusterWarnings, now)...)
	problems = groupPatterns(problems, now)
	sortProblems(problems)
	return problems
}

// confirmOwners asks the resolver to check this group's chain through its
// newest failing members, skipping members that no longer exist.
func (c *clusterState) confirmOwners(g *problemGroup) {
	if !g.inferred {
		return
	}
	if g.root.Kind == "CronJob" {
		c.requestRootLookup(g.ns, g.root)
	}
	type cand struct {
		ref  objRef
		last time.Time
	}
	var cands []cand
	for ref, m := range g.members {
		if m.warn {
			cands = append(cands, cand{ref, m.last})
		}
	}
	sort.Slice(cands, func(i, j int) bool { return cands[i].last.After(cands[j].last) })
	for i, cd := range cands {
		if i >= 3 {
			break
		}
		if c.requestOwnerConfirmation(g.ns, g.chains[cd.ref]) {
			return
		}
	}
}

func (c *clusterState) groupProblem(g *problemGroup, now time.Time) Problem {
	kind := g.root.Kind
	if kind == "" {
		kind = "Workload"
	}
	p := Problem{
		Key:        "ns:" + g.ns + "/" + g.root.Name,
		Scope:      "namespace",
		Kind:       kind,
		Title:      g.root.Name,
		Namespaces: []string{g.ns},
		OpenNS:     g.ns,
		Focus:      kind + "/" + g.root.Name,
		Inferred:   g.inferred,
	}
	fillFromWarnings(&p, g.warnings, now)
	if g.root.Kind == "CronJob" {
		if since, never, ok := c.cronJobFailingSince(g.ns, g.root.Name); ok && since.Before(p.FirstSeen) {
			p.FirstSeen = since
			if never {
				p.SinceNote = "Never succeeded; created " + since.Local().Format("2 Jan 15:04")
			} else {
				p.SinceNote = "Last successful run " + since.Local().Format("2 Jan 15:04")
			}
			setTiming(&p, now)
		}
	}

	// Member counts per kind, below the root, ordered by depth.
	type kc struct {
		kind           string
		depth          int
		total, failing int
	}
	byKind := map[string]*kc{}
	warnDepths := map[int]bool{}
	maxDepth := 0
	for ref, m := range g.members {
		if m.depth > maxDepth {
			maxDepth = m.depth
		}
		if m.warn {
			warnDepths[m.depth] = true
		}
		if m.depth == 0 {
			continue
		}
		k := byKind[ref.Kind]
		if k == nil {
			k = &kc{kind: ref.Kind, depth: m.depth}
			byKind[ref.Kind] = k
		}
		k.total++
		if m.warn {
			k.failing++
		}
	}
	var kinds []*kc
	for _, k := range byKind {
		kinds = append(kinds, k)
	}
	sort.Slice(kinds, func(i, j int) bool {
		if kinds[i].depth != kinds[j].depth {
			return kinds[i].depth < kinds[j].depth
		}
		return kinds[i].kind < kinds[j].kind
	})
	var parts []string
	for _, k := range kinds {
		if k.failing == k.total {
			parts = append(parts, fmt.Sprintf("%d %s", k.total, plural(k.kind, k.total)))
		} else {
			parts = append(parts, fmt.Sprintf("%d of %d %s failing", k.failing, k.total, plural(k.kind, k.total)))
		}
	}
	p.Members = strings.Join(parts, " · ")

	var newestPod time.Time
	for ref, m := range g.members {
		if ref.Kind == "Pod" && m.warn && m.last.After(newestPod) {
			p.Pod, newestPod = ref.Name, m.last
		}
	}

	// Cascade: warnings at two or more levels of the chain, and every member
	// at the deepest level failing (e.g. every Job and every Pod of a CronJob).
	if len(warnDepths) >= 2 {
		allDeepestFail := true
		for _, m := range g.members {
			if m.depth == maxDepth && !m.warn {
				allDeepestFail = false
				break
			}
		}
		p.Cascade = allDeepestFail
	}
	return p
}

// clusterProblems groups warnings about cluster-scoped objects (Nodes,
// PersistentVolumes, ...) by kind and reason. Their events are recorded in
// "default", which is where the namespace page shows them.
func clusterProblems(warnings []EventSummary, now time.Time) []Problem {
	type key struct{ kind, reason string }
	groups := map[key][]EventSummary{}
	for _, e := range warnings {
		k := key{e.Kind, e.Reason}
		groups[k] = append(groups[k], e)
	}
	var out []Problem
	for k, evs := range groups {
		sortNewestFirst(evs)
		var names []string
		seen := map[string]bool{}
		for _, e := range evs {
			if !seen[e.ObjectName] {
				seen[e.ObjectName] = true
				names = append(names, e.ObjectName)
			}
		}
		p := Problem{
			Key:        "cluster:" + k.kind + "/" + k.reason,
			Scope:      "cluster",
			Kind:       k.kind,
			Namespaces: []string{},
			OpenNS:     clusterScopeNS,
			Focus:      k.kind + "/" + evs[0].ObjectName,
		}
		if len(names) == 1 {
			p.Title = names[0]
		} else {
			p.Title = fmt.Sprintf("%s on %d %s", k.reason, len(names), plural(k.kind, len(names)))
			shown := names
			if len(shown) > 3 {
				shown = shown[:3]
			}
			p.Members = strings.Join(shown, ", ")
			if len(names) > 3 {
				p.Members += fmt.Sprintf(" +%d more", len(names)-3)
			}
		}
		fillFromWarnings(&p, evs, now)
		out = append(out, p)
	}
	return out
}

// fillFromWarnings sets reasons, occurrences, timing, tier and idle state.
func fillFromWarnings(p *Problem, warnings []EventSummary, now time.Time) {
	counts := map[string]int64{}
	var newest EventSummary
	for i, e := range warnings {
		counts[e.Reason] += int64(e.Count)
		p.Occurrences += int64(e.Count)
		first := e.FirstTime
		if first.IsZero() {
			first = e.LastTime
		}
		if p.FirstSeen.IsZero() || first.Before(p.FirstSeen) {
			p.FirstSeen = first
		}
		if i == 0 || e.LastTime.After(newest.LastTime) {
			newest = e
		}
	}
	p.LastSeen = newest.LastTime
	p.Message = truncate(newest.Message, messageMaxLen)
	p.Reasons = sortedReasons(counts)
	setTiming(p, now)
}

func setTiming(p *Problem, now time.Time) {
	age := now.Sub(p.FirstSeen)
	switch {
	case age < tierNewMax:
		p.Tier = "new"
	case age < tierOngoingMax:
		p.Tier = "ongoing"
	default:
		p.Tier = "chronic"
	}
	p.Idle = now.Sub(p.LastSeen) >= idleAfter
	p.Since = formatAge(age)
	p.Last = formatAge(now.Sub(p.LastSeen))
}

// ---- Patterns ------------------------------------------------------------

// groupPatterns merges namespace problems that look like one cause into
// pattern rows. Problems are linked when, within the same tier and in
// different namespaces, they
//   - were first seen within patternStartWindow of each other,
//   - share the workload name and primary reason (one app, several envs), or
//   - share an infrastructure reason across three or more namespaces.
//
// Different tiers are never merged, so a new problem is never hidden inside
// an old pattern.
func groupPatterns(problems []Problem, now time.Time) []Problem {
	var idx []int // indexes of namespace-scoped problems
	for i, p := range problems {
		if p.Scope == "namespace" {
			idx = append(idx, i)
		}
	}
	parent := make([]int, len(problems))
	for i := range parent {
		parent[i] = i
	}
	var find func(int) int
	find = func(x int) int {
		for parent[x] != x {
			parent[x] = parent[parent[x]]
			x = parent[x]
		}
		return x
	}
	union := func(a, b int) { parent[find(a)] = find(b) }

	primary := func(p Problem) string {
		if len(p.Reasons) == 0 {
			return ""
		}
		return p.Reasons[0].Reason
	}
	for ai, a := range idx {
		for _, b := range idx[ai+1:] {
			pa, pb := problems[a], problems[b]
			if pa.Namespaces[0] == pb.Namespaces[0] || pa.Tier != pb.Tier {
				continue
			}
			d := pa.FirstSeen.Sub(pb.FirstSeen)
			if d < 0 {
				d = -d
			}
			if d <= patternStartWindow || (pa.Title == pb.Title && primary(pa) == primary(pb)) {
				union(a, b)
			}
		}
	}
	// Infrastructure reasons in 3+ namespaces, per tier.
	type rt struct{ reason, tier string }
	byReason := map[rt][]int{}
	for _, i := range idx {
		if r := primary(problems[i]); infraReasons[r] {
			k := rt{r, problems[i].Tier}
			byReason[k] = append(byReason[k], i)
		}
	}
	for _, members := range byReason {
		nss := map[string]bool{}
		for _, i := range members {
			nss[problems[i].Namespaces[0]] = true
		}
		if len(nss) >= 3 {
			for _, i := range members[1:] {
				union(members[0], i)
			}
		}
	}

	comps := map[int][]int{}
	for _, i := range idx {
		comps[find(i)] = append(comps[find(i)], i)
	}
	merged := map[int]bool{}
	var out []Problem
	for _, members := range comps {
		nss := map[string]bool{}
		for _, i := range members {
			nss[problems[i].Namespaces[0]] = true
		}
		if len(nss) < 2 {
			continue
		}
		var ps []Problem
		for _, i := range members {
			ps = append(ps, problems[i])
			merged[i] = true
		}
		out = append(out, makePattern(ps, now))
	}
	for i, p := range problems {
		if !merged[i] {
			out = append(out, p)
		}
	}
	return out
}

func makePattern(ps []Problem, now time.Time) Problem {
	sort.Slice(ps, func(i, j int) bool { return ps[i].Occurrences > ps[j].Occurrences })
	p := Problem{Scope: "pattern"}
	counts := map[string]int64{}
	nsSet := map[string]bool{}
	primaries := map[string]int64{}
	var keys []string
	sameKind, sameName := true, true
	for i, m := range ps {
		keys = append(keys, m.Key)
		nsSet[m.Namespaces[0]] = true
		for _, r := range m.Reasons {
			counts[r.Reason] += r.Count
		}
		primaries[m.Reasons[0].Reason] += m.Occurrences
		p.Occurrences += m.Occurrences
		if p.FirstSeen.IsZero() || m.FirstSeen.Before(p.FirstSeen) {
			p.FirstSeen = m.FirstSeen
		}
		if m.LastSeen.After(p.LastSeen) {
			p.LastSeen = m.LastSeen
			p.Message = m.Message
		}
		p.Cascade = p.Cascade || m.Cascade
		if i > 0 && m.Kind != ps[0].Kind {
			sameKind = false
		}
		if i > 0 && m.Title != ps[0].Title {
			sameName = false
		}
		p.Items = append(p.Items, PatternItem{
			Namespace: m.Namespaces[0], Kind: m.Kind, Name: m.Title, Reason: m.Reasons[0].Reason,
			Occurrences: m.Occurrences, Last: m.Last, Idle: m.Idle, Focus: m.Focus,
		})
	}
	sort.Strings(keys)
	p.Key = "pattern:" + strings.Join(keys, ",")
	for ns := range nsSet {
		p.Namespaces = append(p.Namespaces, ns)
	}
	sort.Strings(p.Namespaces)
	if sameKind {
		p.Kind = ps[0].Kind
	}
	p.Reasons = sortedReasons(counts)
	setTiming(&p, now)
	// A pattern is only as new as its newest member: all members share a tier.
	p.Tier = ps[0].Tier

	prim := sortedReasons(primaries)
	n := len(p.Namespaces)
	switch {
	case sameName:
		// One workload deployed to several namespaces: its name says more than
		// the reason, which the row shows anyway.
		p.Title = fmt.Sprintf("%s in %d namespaces", ps[0].Title, n)
	case len(prim) == 1:
		p.Title = fmt.Sprintf("%s in %d namespaces", prim[0].Reason, n)
	case len(prim) == 2:
		p.Title = fmt.Sprintf("%s + %s in %d namespaces", prim[0].Reason, prim[1].Reason, n)
	default:
		p.Title = fmt.Sprintf("%s + %d more in %d namespaces", prim[0].Reason, len(prim)-1, n)
	}

	var minFirst, maxFirst time.Time
	for _, m := range ps {
		if minFirst.IsZero() || m.FirstSeen.Before(minFirst) {
			minFirst = m.FirstSeen
		}
		if m.FirstSeen.After(maxFirst) {
			maxFirst = m.FirstSeen
		}
	}
	spread := maxFirst.Sub(minFirst)
	switch {
	case spread < time.Second:
		p.Pattern = fmt.Sprintf("All started in the same second, %s.", minFirst.Local().Format("2 Jan 15:04:05"))
	case spread <= patternStartWindow:
		p.Pattern = fmt.Sprintf("All first seen within %ds of each other, %s–%s.", int(spread.Seconds()+0.5),
			minFirst.Local().Format("2 Jan 15:04:05"), maxFirst.Local().Format("15:04:05"))
	case sameName:
		p.Pattern = fmt.Sprintf("The same workload, %s, failing the same way in %d namespaces.", ps[0].Title, n)
	default:
		p.Pattern = fmt.Sprintf("The same reason in %d namespaces.", n)
	}
	return p
}

func sortProblems(ps []Problem) {
	sort.SliceStable(ps, func(i, j int) bool {
		a, b := ps[i], ps[j]
		if tierOrder[a.Tier] != tierOrder[b.Tier] {
			return tierOrder[a.Tier] < tierOrder[b.Tier]
		}
		if a.Cascade != b.Cascade {
			return a.Cascade
		}
		if a.Idle != b.Idle {
			return !a.Idle
		}
		if !a.LastSeen.Equal(b.LastSeen) {
			return a.LastSeen.After(b.LastSeen)
		}
		return a.Key < b.Key
	})
}

// ---- /api/home -----------------------------------------------------------

type railNS struct {
	Name      string `json:"name"`
	Problems  int    `json:"problems,omitempty"`
	State     string `json:"state"` // cascade, warn, idle, normal or quiet
	Truncated bool   `json:"truncated,omitempty"`
	rank      int
}

type homeRail struct {
	Cluster *railNS  `json:"cluster,omitempty"`
	Active  []railNS `json:"active"`
	Normal  []railNS `json:"normal"`
	Quiet   []railNS `json:"quiet"`
}

type sweepStatus struct {
	Loaded       int  `json:"loaded"`
	Total        int  `json:"total"`
	Ready        bool `json:"ready"`
	OldestAgeSec int  `json:"oldest_age_s"`
}

type homeSummary struct {
	Problems   int  `json:"problems"`
	Namespaces int  `json:"namespaces"`
	Cluster    bool `json:"cluster"`
	New        int  `json:"new"`
	Ongoing    int  `json:"ongoing"`
	Chronic    int  `json:"chronic"`
}

type HomePayload struct {
	Cluster       clusterInfo    `json:"cluster"`        // the cluster this payload describes
	ActiveContext string         `json:"active_context"` // the cluster being polled now
	Warnings      warnFeedStatus `json:"warnings"`
	Sweep         sweepStatus    `json:"sweep"`
	LLMActive     int            `json:"llm_active"`
	Summary       homeSummary    `json:"summary"`
	Problems      []Problem      `json:"problems"`
	Brief         BriefPayload   `json:"brief"`
	Rail          homeRail       `json:"rail"`
}

func (c *clusterState) buildHome(now time.Time) HomePayload {
	events := c.allNamespaceEvents()
	problems := c.buildProblems(events, now)
	if problems == nil {
		problems = []Problem{}
	}

	st := c.status(now)
	h := HomePayload{
		Cluster:       st.Cluster,
		ActiveContext: st.ActiveContext,
		Warnings:      st.Warnings,
		Sweep:         st.Sweep,
		LLMActive:     st.LLMActive,
		Problems:      problems,
	}
	// Only the watched cluster writes new briefs; tabs left on another
	// cluster keep showing that cluster's last one.
	if c.isActive() {
		c.maybeStartBrief(problems, now)
	}
	h.Brief = c.briefPayload(problems, now)
	allNS, truncated := st.namespaces, st.truncated

	// Rail: namespaces with problems, then normal-only, then quiet.
	stateRank := func(p Problem) (string, int) {
		r := tierOrder[p.Tier] * 3
		switch {
		case p.Cascade:
			return "cascade", r
		case p.Idle:
			return "idle", r + 2
		default:
			return "warn", r + 1
		}
	}
	active := map[string]*railNS{}
	clusterKinds := map[string]bool{}
	for _, p := range problems {
		h.Summary.Problems++
		switch p.Tier {
		case "new":
			h.Summary.New++
		case "ongoing":
			h.Summary.Ongoing++
		default:
			h.Summary.Chronic++
		}
		state, rank := stateRank(p)
		if p.Scope == "cluster" {
			h.Summary.Cluster = true
			clusterKinds[plural(p.Kind, 2)] = true
			if h.Rail.Cluster == nil {
				h.Rail.Cluster = &railNS{State: state, rank: rank}
			}
			c := h.Rail.Cluster
			c.Problems++
			if rank < c.rank {
				c.State, c.rank = state, rank
			}
			continue
		}
		for _, ns := range p.Namespaces {
			a := active[ns]
			if a == nil {
				a = &railNS{Name: ns, State: state, rank: rank, Truncated: truncated[ns]}
				active[ns] = a
			}
			a.Problems++
			if rank < a.rank {
				a.State, a.rank = state, rank
			}
		}
	}
	if h.Rail.Cluster != nil {
		var names []string
		for k := range clusterKinds {
			names = append(names, k)
		}
		sort.Strings(names)
		h.Rail.Cluster.Name = strings.Join(names, ", ")
	}
	h.Summary.Namespaces = len(active)
	h.Rail.Active = []railNS{}
	for _, a := range active {
		h.Rail.Active = append(h.Rail.Active, *a)
	}
	sort.Slice(h.Rail.Active, func(i, j int) bool {
		a, b := h.Rail.Active[i], h.Rail.Active[j]
		if a.rank != b.rank {
			return a.rank < b.rank
		}
		return a.Name < b.Name
	})

	hasEvents := map[string]bool{}
	for ns, evs := range events {
		for _, e := range evs {
			if e.ObjectNamespace != "" {
				hasEvents[ns] = true
				break
			}
		}
	}
	h.Rail.Normal, h.Rail.Quiet = []railNS{}, []railNS{}
	seen := map[string]bool{}
	for _, ns := range append(allNS, keysOf(events)...) {
		if seen[ns] || active[ns] != nil {
			continue
		}
		seen[ns] = true
		if hasEvents[ns] {
			h.Rail.Normal = append(h.Rail.Normal, railNS{Name: ns, State: "normal", Truncated: truncated[ns]})
		} else {
			h.Rail.Quiet = append(h.Rail.Quiet, railNS{Name: ns, State: "quiet"})
		}
	}
	sort.Slice(h.Rail.Normal, func(i, j int) bool { return h.Rail.Normal[i].Name < h.Rail.Normal[j].Name })
	sort.Slice(h.Rail.Quiet, func(i, j int) bool { return h.Rail.Quiet[i].Name < h.Rail.Quiet[j].Name })
	return h
}

// StatusPayload is what the shared page header shows: which cluster, how
// fresh its data is, and whether the model is busy. GET /api/status[?ns=].
type StatusPayload struct {
	Cluster       clusterInfo    `json:"cluster"`
	ActiveContext string         `json:"active_context"`
	Warnings      warnFeedStatus `json:"warnings"`
	Sweep         sweepStatus    `json:"sweep"`
	LLMActive     int            `json:"llm_active"`
	Namespace     *nsStatus      `json:"namespace,omitempty"`

	namespaces []string        // every known namespace
	truncated  map[string]bool // namespaces with more events than loaded
}

type nsStatus struct {
	Name       string `json:"name"`
	UpdatedSec int    `json:"updated_s"`
	Truncated  bool   `json:"truncated"`
}

func (c *clusterState) status(now time.Time) StatusPayload {
	st := StatusPayload{
		Cluster:   c.identity(),
		Warnings:  c.warnFeedStatus(),
		LLMActive: getLLMActiveCalls(),
		truncated: map[string]bool{},
	}
	if a := activeCluster(); a != nil {
		st.ActiveContext = a.name
	}
	c.worker.RLock()
	st.Sweep.Total = c.worker.total
	st.Sweep.Ready = c.worker.ready
	st.namespaces = append([]string(nil), c.worker.namespaces...)
	c.worker.RUnlock()
	// Sweep progress and the age of its oldest namespace.
	c.nsCache.Range(func(k, v any) bool {
		e := v.(*nsEntry)
		e.RLock()
		age := int(now.Sub(e.updatedAt).Seconds())
		st.truncated[k.(string)] = e.truncated
		e.RUnlock()
		st.Sweep.Loaded++
		if age > st.Sweep.OldestAgeSec {
			st.Sweep.OldestAgeSec = age
		}
		return true
	})
	return st
}

func handleAPIStatus(w http.ResponseWriter, r *http.Request) {
	c := apiCluster(w, r)
	if c == nil {
		return
	}
	now := time.Now()
	st := c.status(now)
	if ns := r.URL.Query().Get("ns"); ns != "" && ns != clusterScopeNS {
		n := &nsStatus{Name: ns, Truncated: st.truncated[ns]}
		if v, ok := c.nsCache.Load(ns); ok {
			e := v.(*nsEntry)
			e.RLock()
			n.UpdatedSec = int(now.Sub(e.updatedAt).Seconds())
			e.RUnlock()
		}
		st.Namespace = n
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	json.NewEncoder(w).Encode(st)
}

func handleAPIHome(w http.ResponseWriter, r *http.Request) {
	c := apiCluster(w, r)
	if c == nil {
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	json.NewEncoder(w).Encode(c.buildHome(time.Now()))
}

// ---- Small helpers -------------------------------------------------------

func sortedReasons(counts map[string]int64) []ReasonCount {
	out := make([]ReasonCount, 0, len(counts))
	for r, c := range counts {
		out = append(out, ReasonCount{r, c})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}
		return out[i].Reason < out[j].Reason
	})
	return out
}

func plural(kind string, n int) string {
	if n == 1 {
		return kind
	}
	if strings.HasSuffix(kind, "s") {
		return kind + "es"
	}
	return kind + "s"
}

func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return strings.TrimSpace(string(r[:n])) + "…"
}

// formatAge is a compact duration for problem timing: 12s, 4m, 1h 05m,
// 2d 10h, 75d.
func formatAge(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh %02dm", int(d.Hours()), int(d.Minutes())%60)
	case d < 7*24*time.Hour:
		return fmt.Sprintf("%dd %dh", int(d.Hours())/24, int(d.Hours())%24)
	default:
		return fmt.Sprintf("%dd", int(d.Hours())/24)
	}
}

func keysOf(m map[string][]EventSummary) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
