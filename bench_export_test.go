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

| Check | base | *reference:* Big |
|---|---|---|
| RCA: right root cause | 67% | 99% |
| Note: on target | n/a | 90% |

Link-following is on
by default.

## Adding a case
| not | this |
`
	r := parseResultsTable(strings.NewReader(md))
	if r == nil || len(r.Models) != 2 || r.Models[1].Name != "Big" || !r.Models[1].Reference || r.Models[0].Reference {
		t.Fatalf("models: %+v", r)
	}
	if len(r.Rows) != 2 || *r.Rows[0].Values[0] != 67 || r.Rows[1].Values[0] != nil || *r.Rows[1].Values[1] != 90 {
		t.Fatalf("rows: %+v", r.Rows)
	}
	if len(r.Notes) != 1 || r.Notes[0] != "Link-following is on by default." {
		t.Fatalf("notes: %q", r.Notes)
	}
}
