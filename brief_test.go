package main

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func sampleProblems() []Problem {
	return []Problem{
		{Key: "a", Scope: "namespace", Kind: "CronJob", Title: "catalog-sync", Namespaces: []string{"shop-stg"}, OpenNS: "shop-stg",
			Tier: "chronic", Cascade: true, Reasons: []ReasonCount{{"BackOff", 1408}, {"BackoffLimitExceeded", 60}},
			Members: "60 Jobs · 66 Pods", Since: "8d", Last: "14s", SinceNote: "Last successful run 8 Oct 16:25",
			FirstSeen: time.Now().Add(-8 * 24 * time.Hour)},
		{Key: "b", Scope: "cluster", Kind: "Node", Title: "OOMKilling on 3 Nodes", Tier: "ongoing", Since: "8h 57m", Last: "3m",
			FirstSeen: time.Now().Add(-9 * time.Hour)},
		{Key: "c", Scope: "pattern", Title: "Unexpected in 3 namespaces", Namespaces: []string{"x", "y", "z"}, Tier: "ongoing",
			Pattern: "All first seen within 6s of each other, 16 Oct 14:05:10–14:05:16.", Since: "20h", Last: "51m", Idle: true,
			FirstSeen: time.Now().Add(-20 * time.Hour)},
		{Key: "d", Scope: "namespace", Kind: "DaemonSet", Title: "fluent-bit", Namespaces: []string{"fluent"}, Tier: "chronic", Since: "7d", Last: "1m",
			FirstSeen: time.Now().Add(-7 * 24 * time.Hour)},
	}
}

func TestBriefFacts(t *testing.T) {
	facts := briefFacts(sampleProblems())
	var labels []string
	for _, f := range facts {
		labels = append(labels, f.Label)
		for _, p := range f.Parts {
			if p.Ref != "" && p.Label == "" {
				t.Errorf("chip without a label in %q", f.Label)
			}
		}
	}
	if strings.Join(labels, "|") != "Start here|New|Linked|Older, still open" {
		t.Errorf("labels = %v", labels)
	}
	start := facts[0].Parts[1].Text
	for _, want := range []string{"(CronJob in shop-stg)", "BackOff ×1,408", "every one of 60 Jobs · 66 Pods failing", "last successful run 8 Oct 16:25"} {
		if !strings.Contains(start, want) {
			t.Errorf("start-here sentence %q lacks %q", start, want)
		}
	}
	// The node problem (9 h) is the newest; the pattern is 20 h old.
	if !strings.Contains(facts[1].Parts[0].Text, "Nothing started in the last 2 h") || facts[1].Parts[1].Ref != "b" {
		t.Errorf("new: %+v", facts[1])
	}
	// The top problem is already mentioned, so "Older" only lists fluent-bit.
	old := facts[3]
	if old.Parts[0].Ref != "d" || len(old.Parts) != 2 {
		t.Errorf("older: %+v", old)
	}
	if briefFacts(nil) != nil {
		t.Error("facts for no problems")
	}

	// Several patterns fold into one line of chips.
	ps := sampleProblems()
	ps = append(ps, Problem{Key: "e", Scope: "pattern", Title: "FailedMount in 3 namespaces", Tier: "chronic", Since: "3d", Last: "1m"})
	var linked []briefBullet
	for _, f := range briefFacts(ps) {
		if f.Label == "Linked" {
			linked = append(linked, f)
		}
	}
	if len(linked) != 1 || !strings.HasPrefix(linked[0].Parts[0].Text, "2 groups of problems") {
		t.Errorf("linked = %+v", linked)
	}
}

func TestExtractNote(t *testing.T) {
	magos := "Events first: BackOff restarting failed container and a write error in StreamHandler.php.\n" +
		"Observed: Write operation (log writing) is failing at 9477 bytes due to no space available.\n" +
		"Correlate: this maps to `spec.containers[].volumeMounts` — the volume has no space left.\n" +
		"Verdict: BROKEN — the root cause is probably a misconfigured command."
	got := extractNote(magos)
	if !strings.HasPrefix(got, "Write operation") || !strings.Contains(got, "volumeMounts") || strings.Contains(got, "Verdict") || strings.Contains(got, "root cause") {
		t.Errorf("labelled answer: %q", got)
	}
	plain := "<think>hmm</think>Check whether the gitlab-docker-registry secret exists in the namespace. It may have expired. Then redeploy."
	if got := extractNote(plain); got != "Check whether the gitlab-docker-registry secret exists in the namespace. It may have expired." {
		t.Errorf("plain answer: %q", got)
	}
	if got := extractNote(strings.Repeat("word ", 200)); len([]rune(got)) > briefNoteLen+1 {
		t.Errorf("not truncated: %d chars", len(got))
	}
}

func TestInterestingLogLines(t *testing.T) {
	var b strings.Builder
	for i := 0; i < 50; i++ {
		fmt.Fprintf(&b, "#%d /var/www/vendor/x.php(%d): call()\n", i, i)
		fmt.Fprintf(&b, "info: processed batch %d\n", i)
	}
	b.WriteString("Writing to the log file failed: errno=28 No space left on device\n\n")
	lines := interestingLogLines(b.String())
	if len(lines) != briefLogLines {
		t.Fatalf("got %d lines", len(lines))
	}
	for _, l := range lines {
		if strings.HasPrefix(l, "#") {
			t.Errorf("stack frame kept: %q", l)
		}
	}
	if lines[len(lines)-1] != "Writing to the log file failed: errno=28 No space left on device" {
		t.Errorf("error line missing or out of order: %q", lines[len(lines)-1])
	}
}

func TestNotePromptBudget(t *testing.T) {
	p := sampleProblems()[0]
	p.Pod, p.Message = "catalog-sync-29870395-lcfdd", "Back-off restarting failed container php"
	logs := strings.Repeat("ERROR something failed while writing a fairly long log line here\n", 40)
	big, ev := buildNotePrompt(p, logs, 4000)
	if !strings.Contains(big, "Last log lines from pod catalog-sync-29870395-lcfdd") || !strings.Contains(ev, "30 log lines") {
		t.Errorf("large budget: evidence %q", ev)
	}
	small, ev := buildNotePrompt(p, logs, 250)
	if estimateTokens(small) > 260 || !strings.Contains(small, "Back-off restarting failed container php") {
		t.Errorf("small budget: %d tokens, evidence %q", estimateTokens(small), ev)
	}
	if _, ev := buildNotePrompt(p, "", 4000); ev != "its events" {
		t.Errorf("no logs: evidence %q", ev)
	}
}

func TestBriefNoteDueAndStale(t *testing.T) {
	saved := aiConfig
	aiConfig.enabled = true
	defer func() { aiConfig = saved }()
	// Fill every model slot so a due note reports "waiting" instead of calling a model.
	llmActiveCallsMu.Lock()
	savedActive := llmActiveCalls
	llmActiveCalls = getLLMMaxCalls()
	llmActiveCallsMu.Unlock()
	defer func() { llmActiveCallsMu.Lock(); llmActiveCalls = savedActive; llmActiveCallsMu.Unlock() }()

	c := testCluster()
	now := time.Now()
	ps := sampleProblems()

	// An unconfirmed owner waits briefly, so the note isn't written for a
	// problem about to change rank.
	ps[0].Inferred = true
	c.maybeStartBrief(ps, now)
	if c.brief.status != "" {
		t.Fatalf("unsettled top problem: status %q; want no note yet", c.brief.status)
	}
	c.maybeStartBrief(ps, now.Add(briefSettle))
	if c.brief.status != "waiting" {
		t.Fatalf("first view: status %q; want a note to be due", c.brief.status)
	}
	ps[0].Inferred = false

	// Pretend a note was written for the current top problem.
	c.brief.status, c.brief.noteID, c.brief.text, c.brief.target, c.brief.retryAt = "ready", noteID(ps[0]), "Check the volume.", ps[0], time.Time{}
	c.maybeStartBrief(ps, now)
	if c.brief.status != "ready" {
		t.Errorf("same top problem: status %q", c.brief.status)
	}
	// Lower-ranked problems changing doesn't matter.
	c.maybeStartBrief(ps[:2], now.Add(time.Hour))
	if c.brief.status != "ready" {
		t.Errorf("only lower problems changed: status %q", c.brief.status)
	}

	// A new top problem: stale at once, rewritten after the debounce.
	top := append([]Problem{{Key: "z", Tier: "new", Title: "checkout-api"}}, ps...)
	c.maybeStartBrief(top, now)
	if p := c.briefPayload(top, now.Add(10*time.Second)); !p.Note.Stale || p.Note.NextIn < 15 || p.Note.NextIn > 21 || p.Note.Ref != "a" {
		t.Errorf("payload: %+v", p.Note)
	}
	c.maybeStartBrief(top, now.Add(briefDebounce+time.Second))
	if c.brief.status != "waiting" {
		t.Errorf("after the debounce: status %q; want a note to be due", c.brief.status)
	}

	if p := c.briefPayload(nil, now); p.Note != nil || p.Facts != nil {
		t.Errorf("no problems: %+v", p)
	}
	aiConfig.enabled = false
	if p := c.briefPayload(ps, now); p.Note != nil || len(p.Facts) == 0 {
		t.Errorf("AI off: facts=%d note=%v; want facts only", len(p.Facts), p.Note)
	}
}

func TestStripThinkWhileStreaming(t *testing.T) {
	if got := stripThink("<think>still reasoning"); got != "" {
		t.Errorf("unfinished think block shown: %q", got)
	}
}

// A note stopped for your analysis resumes afterwards, even when the top
// problem hasn't changed (a preempted Regenerate).
func TestPreemptedNoteResumes(t *testing.T) {
	saved := aiConfig
	aiConfig.enabled = true
	defer func() { aiConfig = saved }()
	llmActiveCallsMu.Lock()
	savedActive := llmActiveCalls
	llmActiveCalls = getLLMMaxCalls()
	llmActiveCallsMu.Unlock()
	defer func() { llmActiveCallsMu.Lock(); llmActiveCalls = savedActive; llmActiveCallsMu.Unlock() }()

	c := testCluster()
	now := time.Now()
	ps := sampleProblems()
	c.brief.status, c.brief.noteID, c.brief.text = "ready", noteID(ps[0]), "old note"
	// What runBrief records when it is preempted:
	c.brief.status, c.brief.force, c.brief.retryAt = "waiting", true, now.Add(briefSlotRetry)

	c.maybeStartBrief(ps, now.Add(briefSlotRetry+time.Second))
	if c.brief.status != "waiting" || c.brief.reason != "Waiting for the model: an AI analysis or RCA is using it." {
		t.Errorf("after the retry delay: status %q (%q); want it to try again", c.brief.status, c.brief.reason)
	}
}

// The containers' state goes into the facts, ahead of the log lines, so an
// OOMKilled container is named even when its logs say nothing about memory.
func TestNotePromptContainerState(t *testing.T) {
	p := Problem{Scope: "namespace", Kind: "Deployment", Title: "search-indexer", Namespaces: []string{"search"},
		OpenNS: "search", Pod: "search-indexer-7f7b79844f-cxgl4", Reasons: []ReasonCount{{Reason: "BackOff", Count: 9}}, Since: "8m", Last: "14s"}
	logs := containerStatePrefix + "indexer restarted 9 times, last run ended OOMKilled (exit code 137), now CrashLoopBackOff.\n" +
		"2026-10-17T09:10:50Z INFO  search-indexer: loading 4 index shards into memory\n"
	prompt, ev := buildNotePrompt(p, logs, 4000)
	state := strings.Index(prompt, "OOMKilled")
	logLines := strings.Index(prompt, "Last log lines")
	if state < 0 || logLines < 0 || state > logLines {
		t.Errorf("container state missing or after the logs:\n%s", prompt)
	}
	if strings.Count(prompt, "OOMKilled") != 1 {
		t.Errorf("container state repeated as a log line:\n%s", prompt)
	}
	if ev != "its events, the container state and 1 log line from pod search-indexer-7f7b79844f-cxgl4" {
		t.Errorf("evidence = %q", ev)
	}
}
