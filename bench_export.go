package main

// bench export: the bench as one JSON file for the project site (site/):
// every case with what KubeHealth puts in front of the model, every snapshot
// with its overview ranking, and the results table from bench/README.md.
// Nothing here calls a model.

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

type benchExport struct {
	Kubehealth string `json:"kubehealth"`
	CtxLen     int    `json:"ctx_len"`
	// What the model is told before the evidence.
	SystemPrompt    string                `json:"system_prompt"`
	RCASystemPrompt string                `json:"rca_system_prompt"`
	Tools           []benchExportTool     `json:"tools"`
	Cases           []benchExportCase     `json:"cases"`
	Snapshots       []benchExportSnapshot `json:"snapshots"`
	Results         *benchExportResults   `json:"results,omitempty"`
}

type benchExportTool struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

type benchExportCase struct {
	benchCase
	Category string             `json:"category"`
	Events   []benchExportEvent `json:"events"`
	// Members: warnings on the objects a workload owns, as the prompts list them.
	Members []string `json:"members,omitempty"`
	// LogsPod: the Pod whose logs the prompts carry (the anchor or a workload's newest failing Pod).
	LogsPod string       `json:"logs_pod,omitempty"`
	Logs    string       `json:"logs,omitempty"`
	Linked  []linkedItem `json:"linked,omitempty"`
	// The first prompts, exactly as KubeHealth builds them for a model with CtxLen.
	RCAPrompt      string `json:"rca_prompt"`
	AnalysisPrompt string `json:"analysis_prompt"`
	// Rank: the anchor's row on the overview (1 = top), 0 when it has none.
	Rank     int    `json:"rank,omitempty"`
	RowKey   string `json:"row_key,omitempty"`
	Manifest string `json:"manifest,omitempty"`
}

type benchExportEvent struct {
	Type    string `json:"type"`
	Reason  string `json:"reason"`
	Message string `json:"message"`
	Count   int32  `json:"count"`
	Age     string `json:"age"`
}

type benchExportSnapshot struct {
	snapshotMeta
	Objects  int       `json:"objects"`
	Logs     int       `json:"logs"`
	Problems []Problem `json:"problems"`
	// The overview note's prompt, about the top problem.
	NotePrompt   string `json:"note_prompt,omitempty"`
	NoteEvidence string `json:"note_evidence,omitempty"`
}

type benchExportResults struct {
	Models []benchExportModel `json:"models"`
	Rows   []benchExportRow   `json:"rows"`
	Notes  []string           `json:"notes,omitempty"`
}

type benchExportModel struct {
	Name      string `json:"name"`
	Reference bool   `json:"reference,omitempty"`
}

type benchExportRow struct {
	Check  string     `json:"check"`
	Values []*float64 `json:"values"`
}

func benchExportCmd(args []string) error {
	fs := flag.NewFlagSet("bench export", flag.ExitOnError)
	casesDir := fs.String("cases", "bench/cases", "case directory")
	snapsDir := fs.String("snapshots", "", "directory of snapshots (default: snapshots next to the cases directory)")
	readme := fs.String("readme", "", "bench README with the results table (default: README.md next to the cases directory)")
	incidents := fs.String("incidents", "", "incident manifests, <dir>/<case id>/manifest.yaml (default: incidents next to the cases directory)")
	out := fs.String("out", "", "JSON file to write")
	ctxLen := fs.Int("ctx", 8192, "context length the prompts are sized for (the embedded build's)")
	fs.Parse(args)
	if *out == "" {
		return errors.New("export: -out is required")
	}
	base := filepath.Dir(filepath.Clean(*casesDir))
	if *snapsDir == "" {
		*snapsDir = filepath.Join(base, "snapshots")
	}
	if *readme == "" {
		*readme = filepath.Join(base, "README.md")
	}
	if *incidents == "" {
		*incidents = filepath.Join(base, "incidents")
	}

	log.SetOutput(io.Discard)
	savedAI, savedPrompt, savedCfg := aiConfig, getSystemPrompt(), cfg
	defer func() {
		aiConfig, cfg, clock = savedAI, savedCfg, time.Now
		setSystemPrompt(savedPrompt)
	}()
	cfg.AllNS, cfg.Namespaces = true, nil
	aiConfig.enabled = false
	setSystemPrompt(defaultSystemPrompt)

	cases, err := loadCases(*casesDir, nil)
	if err != nil {
		return err
	}
	bySnap := map[string][]benchCase{}
	var order []string
	for _, bc := range cases {
		if _, ok := bySnap[bc.Snapshot]; !ok {
			order = append(order, bc.Snapshot)
		}
		bySnap[bc.Snapshot] = append(bySnap[bc.Snapshot], bc)
	}
	sort.Strings(order)

	// The budgets the first prompts get, as runRCAAgent and analyzeObjKey size them.
	rcaSystem := defaultSystemPrompt + rcaSystemPromptSuffix
	tools := rcaToolDefs()
	toolsJSON, _ := json.Marshal(tools)
	rcaBudget := promptBudget(*ctxLen, estimateTokens(rcaSystem)+estimateTokens(string(toolsJSON)))
	analysisBudget := promptBudget(*ctxLen, estimateTokens(defaultSystemPrompt))

	exp := benchExport{Kubehealth: version, CtxLen: *ctxLen, SystemPrompt: defaultSystemPrompt, RCASystemPrompt: rcaSystem}
	for _, t := range tools {
		exp.Tools = append(exp.Tools, benchExportTool{t.Function.Name, t.Function.Description})
	}

	for _, name := range order {
		snap, err := loadSnapshot(filepath.Join(*snapsDir, name))
		if err != nil {
			return err
		}
		c, at, problems, err := replaySnapshot(snap, snap.meta.RecordedAt)
		if err != nil {
			return err
		}
		se := benchExportSnapshot{snapshotMeta: snap.meta, Objects: len(snap.objects), Logs: len(snap.logs), Problems: problems}
		if len(problems) > 0 {
			top := problems[0]
			var logs string
			if top.Pod != "" {
				logs, _ = fetchLogsForAI(c, top.OpenNS, top.Pod)
			}
			se.NotePrompt, se.NoteEvidence = buildNotePrompt(top, logs, *ctxLen/briefBudgetShare)
		}
		exp.Snapshots = append(exp.Snapshots, se)

		for _, bc := range bySnap[name] {
			a := bc.Anchor
			ec := benchExportCase{benchCase: bc, Category: benchCategory(bc)}
			for _, ev := range objectEvents(c, a.NS, a.Kind, a.Name) {
				ec.Events = append(ec.Events, benchExportEvent{ev.Type, ev.Reason, oneLine(ev.Message), ev.Count, ev.Age})
			}
			rc := buildRCAInitialContext(c, a.NS, a.Kind, a.Name, rcaBudget, *ctxLen)
			ec.RCAPrompt, ec.LogsPod, ec.Logs, ec.Linked = rc.text, rc.logsPod, rc.podLogs, rc.linked
			ae := analysisEvidence(c, a.NS, a.Kind, a.Name, *ctxLen)
			ec.AnalysisPrompt, ec.Members = ae.fit(analysisBudget), ae.members
			for i, p := range problems {
				if rowAbout(a, p) {
					ec.Rank, ec.RowKey = i+1, p.Key
					break
				}
			}
			if b, err := os.ReadFile(filepath.Join(*incidents, bc.ID, "manifest.yaml")); err == nil {
				ec.Manifest = string(b)
			}
			exp.Cases = append(exp.Cases, ec)
		}
		_ = at
	}

	if f, err := os.Open(*readme); err == nil {
		exp.Results = parseResultsTable(f)
		f.Close()
	}
	return writeJSONFile(*out, exp)
}

// rowAbout reports whether an overview row is about the anchor: its own row,
// or a pattern row that lists it.
func rowAbout(a benchAnchor, p Problem) bool {
	if noteAbout(a, p.Key) {
		return true
	}
	for _, it := range p.Items {
		if it.Namespace == a.NS && it.Name == a.Name {
			return true
		}
	}
	return false
}

// benchCategories groups the cases by where the root cause lies, for the
// site's coverage chart. A case that isn't listed is placed by its truth.
var benchCategories = map[string]string{
	"buoy-forecast-cpu-request": "scheduling", "kelp-tiler-untolerated-taint": "scheduling",
	"proof-scheduler-anti-affinity": "scheduling", "swell-replica-primary-pending": "scheduling",
	"plate-composer-mq-pending": "scheduling",
	"buoy-uplink-missing-class": "storage", "orders-db-unbound-pvc": "storage", "orders-pvc-storageclass": "storage",
	"sediment-cache-rwx-claim": "storage", "legacy-pv-reclaim": "storage", "orders-worker-cascade": "storage",
	"checkout-api-image-typo": "images", "ingest-gauge-bad-tag": "images", "log-shipper-registry": "images",
	"salinity-model-pull-secret": "images", "glyph-ocr-model-image": "images",
	"fraud-scorer-configmap": "config", "pigment-api-secret-key": "config", "current-mapper-pg-secret": "config",
	"digest-mailer-relay-mount": "config",
	"search-indexer-oom":        "runtime", "quill-reconciler-oom": "runtime", "tally-importer-bad-flag": "runtime",
	"tally-consumer-redis-oom": "runtime", "stencil-renderer-init-schema": "runtime", "vellum-gateway-stuck-rollout": "runtime",
	"storefront-readiness-path": "probes", "hook-courier-readiness-port": "probes", "sms-gateway-liveness-slow-start": "probes",
	"glyph-preview-selector": "services", "dispatch-worker-selector-typo": "services", "inkwell-thumbnailer-target-port": "services",
	"font-syncer-rbac": "policy", "glyph-indexer-service-account": "policy", "ink-queue-limitrange": "policy",
	"palette-sync-quota": "policy", "ink-proofer-queue-down": "policy",
	"catalog-sync-token": "jobs", "fx-snapshot-deadline": "jobs", "quarter-close-missing-input": "jobs",
}

func benchCategory(bc benchCase) string {
	if bc.Healthy {
		return "healthy"
	}
	if c, ok := benchCategories[bc.ID]; ok {
		return c
	}
	t := strings.ToLower(bc.Truth)
	has := func(ws ...string) bool {
		for _, w := range ws {
			if strings.Contains(t, w) {
				return true
			}
		}
		return false
	}
	switch {
	case has("probe"):
		return "probes"
	case has("storageclass", "pvc", "claim", "volume"):
		return "storage"
	case has("image", "pull"):
		return "images"
	case has("configmap", "secret"):
		return "config"
	case has("quota", "limitrange", "rbac", "forbidden", "serviceaccount"):
		return "policy"
	case has("selector", "targetport", "endpoints", "service"):
		return "services"
	case has("taint", "affinity", "nodeselector", "insufficient", "pending"):
		return "scheduling"
	case has("cronjob", "job", "deadline"):
		return "jobs"
	}
	return "runtime"
}

var pctRe = regexp.MustCompile(`^(\d+(?:\.\d+)?)\s*%$`)

// parseResultsTable reads the table under "## Results so far" in the bench
// README, and the paragraph lines after it.
func parseResultsTable(r io.Reader) *benchExportResults {
	sc := bufio.NewScanner(r)
	in, done, para := false, false, false
	var res benchExportResults
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if strings.HasPrefix(line, "## ") {
			if in {
				break
			}
			in = strings.HasPrefix(line, "## Results")
			continue
		}
		if !in {
			continue
		}
		if !strings.HasPrefix(line, "|") {
			if len(res.Rows) > 0 {
				done = true
			}
			// Lines after the table, joined into paragraphs.
			switch {
			case !done:
			case line == "":
				para = false
			case para:
				res.Notes[len(res.Notes)-1] += " " + line
			default:
				res.Notes = append(res.Notes, line)
				para = true
			}
			continue
		}
		cells := strings.Split(strings.Trim(line, "|"), "|")
		for i := range cells {
			cells[i] = strings.TrimSpace(cells[i])
		}
		switch {
		case len(res.Models) == 0:
			for _, h := range cells[1:] {
				ref := strings.Contains(h, "reference")
				h = strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(h, "*reference:*", ""), "*", ""))
				res.Models = append(res.Models, benchExportModel{Name: h, Reference: ref})
			}
		case strings.Trim(cells[0], "-: ") == "":
			// the separator row
		default:
			row := benchExportRow{Check: cells[0]}
			for _, v := range cells[1:] {
				var p *float64
				if m := pctRe.FindStringSubmatch(v); m != nil {
					f, _ := strconv.ParseFloat(m[1], 64)
					p = &f
				}
				row.Values = append(row.Values, p)
			}
			res.Rows = append(res.Rows, row)
		}
	}
	if len(res.Rows) == 0 {
		return nil
	}
	return &res
}
