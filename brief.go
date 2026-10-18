package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode"
)

// The cluster brief sits at the top of the home page. It has two parts:
//
//   - Facts: a few sentences computed from the problem list (where to start,
//     what's new, what's linked, what's old). No model is involved, so they
//     are always right, appear instantly and work with AI turned off.
//   - A note from the model on the most urgent problem only, written from
//     that problem's events and the error lines in its newest failing pod's
//     logs, and labelled with the model and the evidence it saw.
//
// Tested on the bundled 0.6B model, asking it to summarise several problems
// at once produced invented links between them every time; one problem with
// its own evidence is the task it does reliably.

const (
	briefDebounce    = 30 * time.Second // wait for the top problem to settle before rewriting
	briefBudgetShare = 4                // the prompt may use 1/4 of the loaded context
	briefDefaultCtx  = 8192             // context length when the model doesn't report one
	briefSlotRetry   = 5 * time.Second  // after waiting for a slot or being preempted
	briefErrorRetry  = 60 * time.Second // after a failed call
	briefMaxOutput   = 220              // tokens; the note is two sentences
	briefTemperature = 0.2              // reading evidence, not a creative task
	briefNoteLen     = 360              // characters shown
	briefLogLines    = 30               // log lines offered to the model at most
	briefMaxChips    = 5
	// A top problem whose owner is still a guess may be about to change rank
	// (a CronJob turns out to have failed for days), so the first note waits
	// this long for the resolver.
	briefSettle = 20 * time.Second
)

const briefSystemPrompt = `You suggest the first thing an engineer should check for one failing Kubernetes workload.
Use only the facts and log lines given. Do not guess causes that are not stated in them.`

// ---- Facts ----------------------------------------------------------------

type briefPart struct {
	Text  string `json:"text,omitempty"`
	Ref   string `json:"ref,omitempty"`   // problem key
	Label string `json:"label,omitempty"` // chip text for that problem
}

type briefBullet struct {
	Label string      `json:"label"`
	Parts []briefPart `json:"parts"`
}

func chip(p Problem) briefPart { return briefPart{Ref: p.Key, Label: p.Title} }

// briefFacts writes the brief's sentences from the ranked problem list.
func briefFacts(problems []Problem) []briefBullet {
	if len(problems) == 0 {
		return nil
	}
	mentioned := map[string]bool{}
	var out []briefBullet

	top := problems[0]
	mentioned[top.Key] = true
	out = append(out, briefBullet{Label: "Start here", Parts: []briefPart{chip(top), {Text: describeProblem(top)}}})

	var fresh []Problem
	newest := problems[0]
	for _, p := range problems {
		if p.Tier == "new" {
			fresh = append(fresh, p)
		}
		if p.FirstSeen.After(newest.FirstSeen) {
			newest = p
		}
	}
	if len(fresh) > 0 {
		parts := []briefPart{{Text: fmt.Sprintf("%d %s in the last 2 h:", len(fresh), pluralWord("problem started", "problems started", len(fresh)))}}
		parts = append(parts, chips(fresh, mentioned)...)
		out = append(out, briefBullet{Label: "New", Parts: parts})
	} else {
		mentioned[newest.Key] = true
		out = append(out, briefBullet{Label: "New", Parts: []briefPart{
			{Text: fmt.Sprintf("Nothing started in the last 2 h. The newest problem was first seen %s ago:", newest.Since)}, chip(newest),
		}})
	}

	// Patterns get one line: each row below already says why its members
	// were grouped.
	var linked []Problem
	for _, p := range problems {
		if p.Scope == "pattern" && !mentioned[p.Key] {
			linked = append(linked, p)
		}
	}
	switch len(linked) {
	case 0:
	case 1:
		mentioned[linked[0].Key] = true
		out = append(out, briefBullet{Label: "Linked", Parts: []briefPart{chip(linked[0]), {Text: lowerFirst(linked[0].Pattern)}}})
	default:
		parts := []briefPart{{Text: fmt.Sprintf("%d groups of problems look like one cause each, across namespaces:", len(linked))}}
		out = append(out, briefBullet{Label: "Linked", Parts: append(parts, chips(linked, mentioned)...)})
	}
	for _, p := range problems {
		if p.Scope != "cluster" || mentioned[p.Key] {
			continue
		}
		mentioned[p.Key] = true
		out = append(out, briefBullet{Label: "Cluster-wide", Parts: []briefPart{chip(p), {Text: fmt.Sprintf("affects cluster-scoped objects, ongoing for %s.", p.Since)}}})
	}

	var old []Problem
	for _, p := range problems {
		if p.Tier == "chronic" && !mentioned[p.Key] {
			old = append(old, p)
		}
	}
	if len(old) > 0 {
		parts := chips(old, mentioned)
		parts = append(parts, briefPart{Text: "have been failing for more than a day, so they are unlikely to explain a new alert."})
		out = append(out, briefBullet{Label: "Older, still open", Parts: parts})
	}
	return out
}

func chips(ps []Problem, mentioned map[string]bool) []briefPart {
	var parts []briefPart
	for i, p := range ps {
		if i == briefMaxChips {
			parts = append(parts, briefPart{Text: fmt.Sprintf("and %d more", len(ps)-briefMaxChips)})
			break
		}
		mentioned[p.Key] = true
		parts = append(parts, chip(p))
	}
	return parts
}

// describeProblem is the "Start here" sentence after the problem's chip.
func describeProblem(p Problem) string {
	var b strings.Builder
	switch p.Scope {
	case "cluster":
		b.WriteString("(cluster-wide)")
	case "pattern":
		fmt.Fprintf(&b, "in %d namespaces", len(p.Namespaces))
	default:
		fmt.Fprintf(&b, "(%s in %s)", p.Kind, strings.Join(p.Namespaces, ", "))
	}
	var rs []string
	for i, r := range p.Reasons {
		if i == 2 {
			break
		}
		rs = append(rs, fmt.Sprintf("%s ×%s", r.Reason, commas(r.Count)))
	}
	fmt.Fprintf(&b, ": %s", strings.Join(rs, " and "))
	switch {
	case p.Cascade && p.Members != "":
		fmt.Fprintf(&b, ", every one of %s failing", p.Members)
	case p.Scope == "cluster" && p.Members != "":
		fmt.Fprintf(&b, " on %s", p.Members)
	case p.Members != "":
		fmt.Fprintf(&b, ", %s", p.Members)
	}
	b.WriteString(". ")
	if p.SinceNote != "" {
		fmt.Fprintf(&b, "Failing for %s (%s)", p.Since, lowerFirst(p.SinceNote))
	} else {
		fmt.Fprintf(&b, "First seen %s ago", p.Since)
	}
	if p.Idle {
		fmt.Fprintf(&b, ", quiet for %s.", p.Last)
	} else {
		fmt.Fprintf(&b, ", last %s ago.", p.Last)
	}
	return b.String()
}

func commas(n int64) string {
	s := fmt.Sprintf("%d", n)
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	return s
}

func lowerFirst(s string) string {
	if s == "" {
		return s
	}
	return strings.ToLower(s[:1]) + s[1:]
}

func pluralWord(one, many string, n int) string {
	if n == 1 {
		return one
	}
	return many
}

// ---- The model's note -----------------------------------------------------

// Running notes, so an analysis you start can take a note's model slot. A
// note may still be finishing on a cluster you just switched away from, so
// there can be more than one.
var briefRuns struct {
	sync.Mutex
	stops map[*clusterState]func()
}

func setBriefRun(c *clusterState, stop func()) {
	briefRuns.Lock()
	defer briefRuns.Unlock()
	if briefRuns.stops == nil {
		briefRuns.stops = map[*clusterState]func(){}
	}
	if stop == nil {
		delete(briefRuns.stops, c)
	} else {
		briefRuns.stops[c] = stop
	}
}

// preemptBrief stops one running note, reporting whether there was one. The
// note gives its slot back and retries a few seconds later.
func preemptBrief() bool {
	briefRuns.Lock()
	var stop func()
	for c, s := range briefRuns.stops {
		stop = s
		delete(briefRuns.stops, c)
		break
	}
	briefRuns.Unlock()
	if stop == nil {
		return false
	}
	stop()
	return true
}

type briefState struct {
	sync.Mutex
	status string // idle, waiting, running, ready, error
	reason string // why it is waiting, or the error

	// The last finished note.
	noteID   string // target key + tier it was written for
	target   Problem
	text     string
	evidence string
	tokens   int
	budget   int
	doneAt   time.Time

	// The run in progress.
	runID       string
	runTarget   Problem
	runEvidence string
	partial     string
	preempted   bool

	seenID  string    // top problem the page showed last
	seenAt  time.Time // when it became the top problem
	retryAt time.Time
	force   bool // Regenerate was pressed
}

func noteID(p Problem) string { return p.Key + "|" + p.Tier }

// maybeStartBrief starts a note on the most urgent problem when one is due:
// on first view, after Regenerate, or once a new top problem has stayed on
// top for briefDebounce.
func (c *clusterState) maybeStartBrief(problems []Problem, now time.Time) {
	if !aiConfig.enabled || len(problems) == 0 {
		return
	}
	top := problems[0]
	id := noteID(top)
	b := &c.brief
	b.Lock()
	defer b.Unlock()
	if id != b.seenID {
		b.seenID, b.seenAt = id, now
	}
	if b.status == "running" {
		return
	}
	settled := !top.Inferred || now.Sub(b.seenAt) >= briefSettle
	due := b.force || (b.noteID == "" && settled) || (id != b.noteID && now.Sub(b.seenAt) >= briefDebounce)
	if !due || (!b.force && now.Before(b.retryAt)) {
		return
	}
	if !acquireLLMSlot() {
		b.status = "waiting"
		b.reason = "Waiting for the model: an AI analysis or RCA is using it."
		b.retryAt = now.Add(briefSlotRetry)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(getLLMTimeout())*time.Minute)
	b.status, b.reason, b.partial, b.force, b.preempted = "running", "", "", false, false
	b.runID, b.runTarget, b.runEvidence = id, top, ""
	setBriefRun(c, func() {
		b.Lock()
		b.preempted = true
		b.Unlock()
		cancel()
	})
	go c.runBrief(ctx, cancel, top)
}

func (c *clusterState) runBrief(ctx context.Context, cancel context.CancelFunc, p Problem) {
	defer cancel()
	defer releaseLLMSlot()
	defer setBriefRun(c, nil)

	budget := briefDefaultCtx / briefBudgetShare
	if info := fetchLMModelInfo(); info != nil && info.ctxLen() > 0 {
		budget = info.ctxLen() / briefBudgetShare
	}
	var logs string
	if p.Pod != "" {
		if raw, err := fetchLogsForAI(c, p.OpenNS, p.Pod); err == nil {
			logs = raw
		}
	}
	prompt, evidence := buildNotePrompt(p, logs, budget)
	tokens := estimateTokens(prompt)

	b := &c.brief
	b.Lock()
	b.runEvidence = evidence
	b.Unlock()

	// A thinking model would spend briefMaxOutput inside <think> and show
	// its reasoning as the note, so the note is asked for without it.
	temp := briefTemperature
	opts := chatOpts{System: briefSystemPrompt, Temperature: &temp, MaxTokens: briefMaxOutput, NoThink: true}
	out, err := streamChat(ctx, prompt, opts, func(partial, _ string) {
		b.Lock()
		b.partial = partial
		b.Unlock()
	})

	b.Lock()
	defer b.Unlock()
	now := time.Now()
	b.partial = ""
	note := extractNote(out)
	switch {
	case b.preempted:
		// Resume once the model is free, even if the top problem is the one
		// the current note is about (a preempted Regenerate).
		b.status = "waiting"
		b.reason = "Paused while an analysis you started uses the model."
		b.retryAt = now.Add(briefSlotRetry)
		b.force = true
	case err != nil:
		b.status = "error"
		b.reason = err.Error()
		b.retryAt = now.Add(briefErrorRetry)
		log.Printf("Cluster brief [%s]: %v", c.name, err)
	case note == "":
		b.status = "error"
		b.reason = "The model's answer was empty."
		b.retryAt = now.Add(briefErrorRetry)
	default:
		b.status, b.reason = "ready", ""
		b.noteID, b.target, b.text, b.evidence = b.runID, p, note, evidence
		b.tokens, b.budget, b.doneAt = tokens, budget, now
	}
}

var (
	stackFrameRe = regexp.MustCompile(`^\s*(#\d+\s|at\s+[\w.$]+\(|File ".*", line \d+)`)
	logErrorRe   = regexp.MustCompile(`(?i)error|exception|fatal|fail|denied|refused|no space|out of memory|oom|panic|timeout|cannot|unable|not found|invalid`)
)

// buildNotePrompt describes one problem and adds the last log lines of its
// newest failing pod, error lines first, until the token budget is used.
func buildNotePrompt(p Problem, logs string, budget int) (prompt, evidence string) {
	var facts strings.Builder
	switch p.Scope {
	case "pattern":
		fmt.Fprintf(&facts, "%s across namespaces %s. %s\n", p.Title, strings.Join(p.Namespaces, ", "), p.Pattern)
	case "cluster":
		fmt.Fprintf(&facts, "Cluster-wide: %s %s.\n", p.Kind, p.Title)
	default:
		fmt.Fprintf(&facts, "%s/%s in namespace %s.", p.Kind, p.Title, strings.Join(p.Namespaces, ", "))
		if p.Members != "" {
			fmt.Fprintf(&facts, " %s.", p.Members)
		}
		facts.WriteString("\n")
	}
	var rs []string
	for _, r := range p.Reasons {
		rs = append(rs, fmt.Sprintf("%s ×%d", r.Reason, r.Count))
	}
	fmt.Fprintf(&facts, "Reasons: %s. First seen %s ago, last %s ago.", strings.Join(rs, ", "), p.Since, p.Last)
	if p.SinceNote != "" {
		fmt.Fprintf(&facts, " %s.", p.SinceNote)
	}
	facts.WriteString("\n")
	if p.Message != "" {
		// Not %q: escaped quotes come back verbatim in the model's answer.
		fmt.Fprintf(&facts, "Latest event: %s\n", strings.Join(strings.Fields(p.Message), " "))
	}
	evidence = "its events"

	// The containers' state is a fact, not a log line: it always goes in.
	if strings.HasPrefix(logs, containerStatePrefix) {
		state, rest, _ := strings.Cut(logs, "\n")
		facts.WriteString(state + "\n")
		logs = rest
		evidence = "its events and the pod's container state"
	}

	question := "\nIn one or two sentences, what should the engineer check first?"
	lines := interestingLogLines(logs)
	head := facts.String()
	logBlock := func(ls []string) string {
		return fmt.Sprintf("Last log lines from pod %s:\n%s\n", p.Pod, strings.Join(ls, "\n"))
	}
	// Measure the exact prompt, header included, so the budget holds.
	for len(lines) > 0 && estimateTokens(head+logBlock(lines)+question) > budget {
		lines = lines[1:]
	}
	if len(lines) > 0 {
		head += logBlock(lines)
		n := fmt.Sprintf("%d log lines", len(lines))
		if len(lines) == 1 {
			n = "1 log line"
		}
		if strings.Contains(evidence, "container state") {
			evidence = fmt.Sprintf("its events, the container state and %s from pod %s", n, p.Pod)
		} else {
			evidence = fmt.Sprintf("its events and %s from pod %s", n, p.Pod)
		}
	}
	return head + question, evidence
}

// interestingLogLines keeps the last log lines worth reading: stack frames and
// blank lines are dropped, and when the tail is long, lines that look like
// errors are kept ahead of the rest.
func interestingLogLines(logs string) []string {
	return pickLogLines(readableLogLines(logs), briefLogLines)
}

// readableLogLines drops blank lines, stack frames and the markers
// fetchLogsForAI adds, and shortens very long lines.
func readableLogLines(logs string) []string {
	var kept []string
	for _, l := range strings.Split(logs, "\n") {
		if strings.TrimSpace(l) == "" || stackFrameRe.MatchString(l) || strings.HasPrefix(l, "===") || strings.HasPrefix(l, "[previous container run]") {
			continue
		}
		kept = append(kept, truncate(strings.TrimRight(l, " \t"), 200))
	}
	return kept
}

// pickLogLines keeps at most limit lines, in their order: error-looking lines
// first (up to two thirds of limit), then the most recent others.
func pickLogLines(kept []string, limit int) []string {
	if len(kept) <= limit {
		return kept
	}
	var errs []int
	for i, l := range kept {
		if logErrorRe.MatchString(l) {
			errs = append(errs, i)
		}
	}
	pick := map[int]bool{}
	for i := len(errs) - 1; i >= 0 && len(pick) < limit*2/3; i-- {
		pick[errs[i]] = true
	}
	for i := len(kept) - 1; i >= 0 && len(pick) < limit; i-- {
		pick[i] = true
	}
	var out []string
	for i, l := range kept {
		if pick[i] {
			out = append(out, l)
		}
	}
	return out
}

var (
	noteLabelRe    = regexp.MustCompile(`^\s*(?:[-*•]\s*)?([A-Z][A-Za-z/ ]{1,24}?)\s*:\s*(.+)$`)
	noteSentenceRe = regexp.MustCompile(`[^.!?]+[.!?]+(\s|$)`)
)

// extractNote trims the model's answer to what's worth showing. Models tuned
// for incident write-ups answer in labelled lines ("Observed:", "Correlate:",
// "Verdict:"); the observation and the suggested check are kept, the verdict
// (the most speculative part) is left out. Plain answers keep their first two
// sentences.
func extractNote(out string) string {
	out = strings.NewReplacer("**", "", "`", "").Replace(stripThink(out))
	labelled := map[string]string{}
	var order []string
	for _, line := range strings.Split(out, "\n") {
		if m := noteLabelRe.FindStringSubmatch(line); m != nil {
			k := strings.ToLower(strings.TrimSpace(m[1]))
			if _, ok := labelled[k]; !ok {
				order = append(order, k)
			}
			labelled[k] = strings.TrimSpace(m[2])
		}
	}
	var note string
	if len(labelled) >= 2 {
		var picks []string
		for _, k := range []string{"observed", "fix/next", "fix", "next", "check", "action", "correlate"} {
			if v, ok := labelled[k]; ok && len(picks) < 2 {
				picks = append(picks, v)
			}
		}
		if len(picks) == 0 {
			picks = append(picks, labelled[order[0]])
		}
		note = strings.Join(picks, " ")
	} else {
		text := strings.Join(strings.Fields(out), " ")
		if sentences := noteSentenceRe.FindAllString(text, 2); len(sentences) > 0 {
			note = strings.Join(sentences, "")
		} else {
			note = text
		}
	}
	return truncate(strings.Join(strings.Fields(note), " "), briefNoteLen)
}

// stripThink removes a reasoning block, including an unfinished one while
// the answer is still streaming.
func stripThink(s string) string {
	for {
		i := strings.Index(s, "<think>")
		if i < 0 {
			break
		}
		j := strings.Index(s[i:], "</think>")
		if j < 0 {
			return s[:i]
		}
		s = s[:i] + s[i+j+len("</think>"):]
	}
	if i := strings.Index(s, "</think>"); i >= 0 {
		s = s[i+len("</think>"):]
	}
	return s
}

// estimateTokens is a rough count: about 3.5 characters per token for this
// mix of English, identifiers and log text.
// estimateTokens is a cheap, slightly high token count for Qwen-family
// tokenizers: digits and punctuation cost far more than letters, so a flat
// characters-per-token ratio undercounts logs, IPs and hashes by up to 2.4×.
// Weights fitted on 501 real kubehealth prompts and tool results (never below
// 0.96 of the real count, median 1.13).
func estimateTokens(s string) int {
	var letters, digits, spaces, other int
	for _, r := range s {
		switch {
		case unicode.IsLetter(r):
			letters++
		case unicode.IsDigit(r):
			digits++
		case unicode.IsSpace(r):
			spaces++
		default:
			other++
		}
	}
	return int(0.23*float64(letters)+1.36*float64(digits)+0.52*float64(other)+0.55*float64(spaces)) + 1
}

// ---- Payload --------------------------------------------------------------

// BriefNote is the model's note as the home page shows it.
type BriefNote struct {
	Status   string `json:"status"` // idle, waiting, running, ready, error
	Reason   string `json:"reason,omitempty"`
	Text     string `json:"text,omitempty"`
	Ref      string `json:"ref,omitempty"`   // problem the note is about
	Label    string `json:"label,omitempty"` // its title
	OpenNS   string `json:"open_ns,omitempty"`
	Focus    string `json:"focus,omitempty"`
	Pod      string `json:"pod,omitempty"` // newest failing pod, for the full analysis link
	Model    string `json:"model,omitempty"`
	Age      string `json:"age,omitempty"`
	Evidence string `json:"evidence,omitempty"`
	Tokens   int    `json:"tokens,omitempty"`
	Budget   int    `json:"budget,omitempty"`
	Stale    bool   `json:"stale"`               // the top problem changed since
	NextIn   int    `json:"next_in_s,omitempty"` // seconds until the automatic update
}

// BriefPayload is the whole brief: computed facts plus the model's note.
type BriefPayload struct {
	Facts []briefBullet `json:"facts,omitempty"`
	Note  *BriefNote    `json:"note,omitempty"` // nil when AI is off or there are no problems
}

func (c *clusterState) briefPayload(problems []Problem, now time.Time) BriefPayload {
	p := BriefPayload{Facts: briefFacts(problems)}
	if !aiConfig.enabled || len(problems) == 0 {
		return p
	}
	b := &c.brief
	b.Lock()
	defer b.Unlock()
	n := &BriefNote{Status: b.status, Reason: b.reason, Model: aiConfig.model}
	if n.Status == "" {
		n.Status = "idle"
	}
	t := b.target
	switch {
	case b.status == "running":
		t = b.runTarget
		n.Text = extractNote(b.partial)
		n.Evidence = b.runEvidence
	case b.text != "":
		n.Text, n.Evidence = b.text, b.evidence
		n.Tokens, n.Budget = b.tokens, b.budget
		n.Age = formatAge(now.Sub(b.doneAt))
	default:
		t = problems[0]
	}
	n.Ref, n.Label, n.OpenNS, n.Focus, n.Pod = t.Key, t.Title, t.OpenNS, t.Focus, t.Pod
	if b.noteID != "" && b.status != "running" && noteID(problems[0]) != b.noteID {
		n.Stale = true
		wait := briefDebounce - now.Sub(b.seenAt)
		if r := b.retryAt.Sub(now); r > wait {
			wait = r
		}
		if wait > 0 {
			n.NextIn = int(wait.Seconds()) + 1
		}
	}
	p.Note = n
	return p
}

// handleAPIBrief rewrites the model's note now: POST /api/ai/brief.
func handleAPIBrief(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	c := apiCluster(w, r)
	if c == nil {
		return
	}
	if !aiConfig.enabled {
		http.Error(w, "AI not enabled", http.StatusServiceUnavailable)
		return
	}
	now := time.Now()
	problems := c.regenerateBrief(now)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(c.briefPayload(problems, now))
}

// regenerateBrief starts a new note on the most urgent problem now, as the
// Regenerate button does, and returns the problem list it ranked.
func (c *clusterState) regenerateBrief(now time.Time) []Problem {
	problems := c.buildProblems(c.allNamespaceEvents(), now)
	c.brief.Lock()
	c.brief.force = true
	c.brief.Unlock()
	c.maybeStartBrief(problems, now)
	return problems
}
