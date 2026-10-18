package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The site's data: every case, with the prompts KubeHealth would send, and
// the results table from the README.
func TestBenchExport(t *testing.T) {
	out := filepath.Join(t.TempDir(), "bench.json")
	if err := benchExportCmd([]string{"-cases", "bench/cases", "-out", out}); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	var exp benchExport
	if err := json.Unmarshal(b, &exp); err != nil {
		t.Fatal(err)
	}
	cases, err := loadCases("bench/cases", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(exp.Cases) != len(cases) {
		t.Fatalf("%d cases exported, %d in bench/cases", len(exp.Cases), len(cases))
	}
	for _, c := range exp.Cases {
		a := c.Anchor
		if !strings.Contains(c.RCAPrompt, a.Name) || !strings.Contains(c.AnalysisPrompt, a.Name) {
			t.Errorf("%s: prompts don't name the anchor", c.ID)
		}
		if c.Category == "" || (c.Healthy != (c.Category == "healthy")) {
			t.Errorf("%s: category %q", c.ID, c.Category)
		}
	}
	if len(exp.Snapshots) == 0 || exp.Snapshots[0].NotePrompt == "" {
		t.Error("snapshots or their note prompts are missing")
	}
	if exp.Results == nil || len(exp.Results.Rows) == 0 || len(exp.Results.Models) == 0 {
		t.Fatal("results table not parsed from bench/README.md")
	}
}

func TestParseResultsTable(t *testing.T) {
	md := `# Bench

## Results so far

Mean of 3 seeds.
Same server.

| Check | base | **mine + links** | *reference:* Big |
|---|---|---|---|
| **RCA: right root cause**<br><sub>Does it name the cause?</sub> | 67% | **87%** | 99% |
| **RCA: tool calls per incident**<br><sub>How many lookups.</sub> | 0.0 | – | 1.5 |

Link-following is on
by default.

## Adding a case
| not | this |
`
	r := parseResultsTable(strings.NewReader(md))
	if r == nil || len(r.Models) != 3 {
		t.Fatalf("models: %+v", r)
	}
	if m := r.Models; m[0].Featured || m[0].Reference || m[1].Name != "mine + links" || !m[1].Featured || m[2].Name != "Big" || !m[2].Reference {
		t.Errorf("models: %+v", m)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("rows: %+v", r.Rows)
	}
	a, b := r.Rows[0], r.Rows[1]
	if a.Check != "RCA: right root cause" || a.Description != "Does it name the cause?" || a.Unit != "%" || *a.Values[1] != 87 {
		t.Errorf("percent row: %+v", a)
	}
	if b.Unit != "" || *b.Values[0] != 0 || b.Values[1] != nil || *b.Values[2] != 1.5 {
		t.Errorf("count row with a gap: %+v", b)
	}
	if len(r.Intro) != 1 || r.Intro[0] != "Mean of 3 seeds. Same server." {
		t.Errorf("intro: %q", r.Intro)
	}
	if len(r.Notes) != 1 || r.Notes[0] != "Link-following is on by default." {
		t.Errorf("notes: %q", r.Notes)
	}
}
