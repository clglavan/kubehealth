package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// Scoring: what the model diagnosed, not how it laid the answer out. Every
// check reads the WHOLE final answer (kubehealth asks a plain question, with
// no required sections): a keyword check passes when every group has at least
// one of its keywords in the text (case-insensitive substring). A fix also
// needs an action in the text (create, set, roll back, kubectl …), so naming
// the broken object alone doesn't count as fixing it.

var (
	benchThinky = regexp.MustCompile(`(?i)^\s*(okay|ok,|let'?s|let me|the user|hmm|alright|so,|first,)`)
	benchNoneRe = regexp.MustCompile(`(?i)\bnone\b|no (issues|problems|errors|faults)|nothing (is )?(wrong|broken)|healthy|working (as expected|normally|correctly)|nothing to fix|no action`)
	benchFixRe  = regexp.MustCompile(`(?i)\b(create|set|change|update|add|remove|delete|increase|raise|lower|reduce|roll ?back|rollout undo|undo|restart|patch|apply|kubectl|edit|rename|correct|replace|grant|bind|rotate|renew|refresh|fix|point|use)`)
)

// groupsOK reports whether every group has a keyword in text; ok is false
// when there are no groups (nothing to check).
func groupsOK(text string, groups [][]string) (pass, ok bool) {
	if len(groups) == 0 {
		return false, false
	}
	t := strings.ToLower(text)
	for _, g := range groups {
		hit := false
		for _, k := range g {
			if strings.Contains(t, strings.ToLower(k)) {
				hit = true
				break
			}
		}
		if !hit {
			return false, true
		}
	}
	return true, true
}

// benchChecks, in scorecard order, with their labels. A check is the share
// of passing items, except avg checks, which are an average count per item.
//
// The deeper cause is split by where it is: rca_deep scores needs_tool cases
// (the cause is in another object, outside KubeHealth's first prompt), and
// rca_detail scores the other cases with deep groups (the decisive detail of
// a cause the anchor's own evidence shows: the tag, the limit).
var benchChecks = []struct {
	key, label string
	avg        bool
}{
	{"rca_cause", "RCA: right root cause", false},
	{"rca_deep", "RCA: finds the cause in another object", false},
	{"rca_detail", "RCA: names the decisive detail", false},
	{"rca_fix", "RCA: right fix", false},
	{"rca_done", "RCA: finished", false},
	{"rca_tool", "RCA: called a tool when the cause was elsewhere", false},
	{"rca_calls", "RCA: tool calls per case (avg)", true},
	{"an_cause", "Analysis: right issue", false},
	{"an_fix", "Analysis: right action", false},
	{"healthy_ok", "Healthy: no invented fault", false},
	{"note_cause", "Note: names the right first check", false},
	{"note_clean", "Note: an answer, not thinking", false},
}

// benchScore is one results file's scores: one benchRunScore per seed.
type benchScore struct {
	Model  string          `json:"model"`
	Faults int             `json:"faults"`
	Health int             `json:"healthy"`
	Runs   []benchRunScore `json:"runs"`
}

// benchRunScore is one seed's scores: per check, the passing items out of
// the scored ones (for an avg check, the summed count out of the items), and
// per case its checks.
type benchRunScore struct {
	Seed  int64                      `json:"seed,omitempty"`
	Pass  map[string]int             `json:"pass"`
	Total map[string]int             `json:"total"`
	Cases map[string]map[string]bool `json:"cases"`
}

func (s *benchRunScore) add(caseID, check string, pass bool) {
	s.Total[check]++
	if pass {
		s.Pass[check]++
	}
	if caseID != "" {
		if s.Cases[caseID] == nil {
			s.Cases[caseID] = map[string]bool{}
		}
		s.Cases[caseID][check] = pass
	}
}

// count adds one item with n to an avg check.
func (s *benchRunScore) count(check string, n int) {
	s.Total[check]++
	s.Pass[check] += n
}

// value is a check's score in one run: the percentage of passing items, or
// for an avg check the average count; ok is false when nothing was scored.
func (s benchRunScore) value(check string, avg bool) (v float64, n int, ok bool) {
	n = s.Total[check]
	if n == 0 {
		return 0, 0, false
	}
	v = float64(s.Pass[check]) / float64(n)
	if !avg {
		v *= 100
	}
	return v, n, true
}

// stat is a check's mean over the seeds that scored it, half the range of
// its per-seed values (the ± of the scorecard) and the most items one seed
// scored.
func (s benchScore) stat(check string, avg bool) (mean, spread float64, n int, ok bool) {
	lo, hi, k := math.Inf(1), math.Inf(-1), 0
	for _, r := range s.Runs {
		v, rn, rok := r.value(check, avg)
		if !rok {
			continue
		}
		mean += v
		lo, hi, n, k = min(lo, v), max(hi, v), max(n, rn), k+1
	}
	if k == 0 {
		return 0, 0, 0, false
	}
	return mean / float64(k), (hi - lo) / 2, n, true
}

// pct is the check's mean share of passing items, rounded half to even like
// Python's round(), and the number of items; ok is false when none were scored.
func (s benchScore) pct(check string) (pct, n int, ok bool) {
	m, _, n, ok := s.stat(check, false)
	return int(math.RoundToEven(m)), n, ok
}

// toolCalls is how many tools the RCA agent called itself; the logs in the
// first prompt are listed as a step too, marked Initial.
func toolCalls(steps []RCAStep) int {
	n := 0
	for _, st := range steps {
		if !st.Initial {
			n++
		}
	}
	return n
}

func scoreResults(r *benchResults) benchScore {
	s := benchScore{Model: r.Model}
	if r.LinkFollow != nil {
		s.Model += map[bool]string{true: " (links on)", false: " (links off)"}[*r.LinkFollow]
	}
	for i, run := range r.runs() {
		rs := scoreRun(run)
		if i == 0 {
			for _, cr := range run.Cases {
				if cr.Case.Healthy {
					s.Health++
				} else {
					s.Faults++
				}
			}
		}
		s.Runs = append(s.Runs, rs)
	}
	return s
}

func scoreRun(r *benchRun) benchRunScore {
	s := benchRunScore{Seed: r.Seed, Pass: map[string]int{}, Total: map[string]int{}, Cases: map[string]map[string]bool{}}
	for _, cr := range r.Cases {
		c := cr.Case
		an, rca := cr.Analysis.Content, cr.RCA.Result
		if c.Healthy {
			s.add(c.ID, "healthy_ok", an != "" && benchNoneRe.MatchString(an))
			continue
		}
		group := func(check, text string, groups [][]string) {
			if pass, ok := groupsOK(text, groups); ok {
				s.add(c.ID, check, pass)
			}
		}
		fix := func(check, text string) {
			if pass, ok := groupsOK(text, c.Fix); ok {
				s.add(c.ID, check, pass && benchFixRe.MatchString(text))
			}
		}
		group("an_cause", an, c.Cause)
		fix("an_fix", an)
		s.add(c.ID, "rca_done", cr.RCA.Status == "done" && strings.TrimSpace(rca) != "")
		group("rca_cause", rca, c.Cause)
		if c.NeedsTool {
			group("rca_deep", rca, c.Deep)
		} else {
			group("rca_detail", rca, c.Deep)
		}
		fix("rca_fix", rca)
		calls := toolCalls(cr.RCA.Steps)
		if c.NeedsTool {
			s.add(c.ID, "rca_tool", calls > 0)
		}
		s.count("rca_calls", calls)
	}
	// The note is about one problem; it is scored against the case of that
	// snapshot whose anchor the problem is about, if any (see noteAbout).
	names := make([]string, 0, len(r.Notes))
	for name := range r.Notes {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		n := r.Notes[name]
		s.add("", "note_clean", n.Text != "" && !benchThinky.MatchString(n.Text))
		for _, cr := range r.Cases {
			if c := cr.Case; c.Snapshot == name && noteAbout(c.Anchor, n.Ref) {
				if !c.Healthy {
					pass, _ := groupsOK(n.Text, c.Cause)
					s.add(c.ID, "note_cause", pass)
				}
				break
			}
		}
	}
	return s
}

// benchScorecard is the markdown scorecard of one or more results.
func benchScorecard(scores []benchScore) string {
	faults, healthy, seeds := 0, 0, 1
	for _, s := range scores {
		faults, healthy, seeds = max(faults, s.Faults), max(healthy, s.Health), max(seeds, len(s.Runs))
	}
	var b strings.Builder
	fmt.Fprintf(&b, "# KubeHealth-Bench (replay: recorded incidents, %d faults + %d healthy)\n\n", faults, healthy)
	b.WriteString("Replayed by `kubehealth bench run` through KubeHealth's own prompts, context budget and tools (no cluster). " +
		"Scores what the model diagnosed, read from the whole answer — never its layout. Higher is better. n = scored items.")
	if seeds > 1 {
		b.WriteString(" Every item ran once per seed; a score is the mean over the seeds, ± half the range of the per-seed scores, n per seed.")
	}
	b.WriteString("\n\n| Check |")
	for _, s := range scores {
		b.WriteString(" " + s.Model + " |")
	}
	b.WriteString("\n" + strings.Repeat("|---", len(scores)+1) + "|\n")
	for _, ch := range benchChecks {
		b.WriteString("| " + ch.label + " |")
		for _, s := range scores {
			m, sp, n, ok := s.stat(ch.key, ch.avg)
			switch {
			case !ok:
				b.WriteString(" — |")
			case ch.avg && len(s.Runs) > 1:
				fmt.Fprintf(&b, " %.1f ±%.1f (%d) |", m, sp, n)
			case ch.avg:
				fmt.Fprintf(&b, " %.1f (%d) |", m, n)
			case len(s.Runs) > 1:
				fmt.Fprintf(&b, " %d%% ±%d (%d) |", int(math.RoundToEven(m)), int(math.RoundToEven(sp)), n)
			default:
				fmt.Fprintf(&b, " %d%% (%d) |", int(math.RoundToEven(m)), n)
			}
		}
		b.WriteString("\n")
	}
	return b.String()
}

// rescoreWith replaces the cases a results file kept with the current
// definitions of the same ids, so keywords and flags fixed after a run apply
// to it. A case whose snapshot or anchor changed was a different question:
// that is an error. Ids no longer defined keep what the results file kept.
func rescoreWith(r *benchResults, current map[string]benchCase) error {
	for _, run := range r.runs() {
		for _, cr := range run.Cases {
			c, ok := current[cr.Case.ID]
			if !ok {
				continue
			}
			if c.Snapshot != cr.Case.Snapshot || c.Anchor != cr.Case.Anchor {
				return fmt.Errorf("case %s: snapshot or anchor changed since the run", c.ID)
			}
			cr.Case = c
		}
	}
	return nil
}

func readResults(path string) (*benchResults, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var r benchResults
	if err := json.Unmarshal(b, &r); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return &r, nil
}

func benchScoreCmd(args []string) error {
	fs := flag.NewFlagSet("bench score", flag.ExitOnError)
	results := fs.String("results", "", "comma-separated results files from bench run")
	out := fs.String("out", "", "scorecard to write (markdown; default: print only)")
	casesDir := fs.String("cases", "", "score with these case files' keywords and flags instead of the ones the results file kept (same ids and anchors)")
	fs.Parse(args)
	files := splitList(*results)
	if len(files) == 0 {
		return errors.New("score: -results is required")
	}
	var current map[string]benchCase
	if *casesDir != "" {
		cases, err := loadCases(*casesDir, nil)
		if err != nil {
			return err
		}
		current = map[string]benchCase{}
		for _, c := range cases {
			current[c.ID] = c
		}
	}
	var scores []benchScore
	seen := map[string]bool{}
	for _, f := range files {
		r, err := readResults(f)
		if err != nil {
			return err
		}
		if current != nil {
			if err := rescoreWith(r, current); err != nil {
				return fmt.Errorf("%s: %w", f, err)
			}
		}
		s := scoreResults(r)
		// Two runs of one model get their file names as columns.
		if seen[s.Model] || s.Model == "" {
			s.Model = strings.TrimSuffix(filepath.Base(f), filepath.Ext(f))
		}
		seen[s.Model] = true
		scores = append(scores, s)
	}
	card := benchScorecard(scores)
	fmt.Print(card)
	if *out != "" {
		return os.WriteFile(*out, []byte(card), 0o644)
	}
	return nil
}
