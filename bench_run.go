package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ---- Results -------------------------------------------------------------------

// benchResults is what `bench run` writes: every item's final text, the
// requests KubeHealth sent for it and the answers it got.
//
// A run with one seed keeps its items at the top level (seed, snapshots,
// notes, cases); a run with several seeds (-seeds 1,2,3) lists them under
// seeds, and its items under runs, one per seed, in the same order.
type benchResults struct {
	Kubehealth string `json:"kubehealth"`
	Model      string `json:"model"`
	LLM        string `json:"llm"`
	// LinkFollow is -link-follow (linked.go); absent in results written
	// before it existed.
	LinkFollow *bool `json:"link_follow,omitempty"`
	benchRun
	Seeds        []int64     `json:"seeds,omitempty"`
	Runs         []*benchRun `json:"runs,omitempty"`
	NoCache      bool        `json:"no_prompt_cache"`
	CtxLen       int         `json:"ctx_len"` // as the server reports it; 0: unknown, nothing trimmed
	SystemPrompt string      `json:"system_prompt"`
	StartedAt    time.Time   `json:"started_at"`
	FinishedAt   time.Time   `json:"finished_at,omitzero"`
}

// benchRun is every item run once, with one seed.
type benchRun struct {
	Seed      int64                       `json:"seed,omitempty"`
	Snapshots map[string]benchSnapshotRun `json:"snapshots,omitempty"`
	Notes     map[string]*benchNoteResult `json:"notes,omitempty"` // snapshot → its overview note
	Cases     []*benchCaseResult          `json:"cases,omitempty"`
}

// runs is the results' runs, one per seed: the top-level items when the run
// had one seed.
func (r *benchResults) runs() []*benchRun {
	if len(r.Runs) > 0 {
		return r.Runs
	}
	return []*benchRun{&r.benchRun}
}

type benchSnapshotRun struct {
	RecordedAt time.Time `json:"recorded_at"`
	ReplayedAt time.Time `json:"replayed_at"` // the replay's now; timestamps moved by the difference
	Problems   int       `json:"problems"`
	TopProblem string    `json:"top_problem,omitempty"`
}

// benchNoteResult is the overview note on a snapshot's most urgent problem.
type benchNoteResult struct {
	Status   string `json:"status"`
	Text     string `json:"text,omitempty"`
	Error    string `json:"error,omitempty"`
	Ref      string `json:"ref,omitempty"` // problem key the note is about
	Label    string `json:"label,omitempty"`
	Pod      string `json:"pod,omitempty"`
	Evidence string `json:"evidence,omitempty"`
	Tokens   int    `json:"tokens,omitempty"`
	Budget   int    `json:"budget,omitempty"`
	// Target is the case marked note_target for this snapshot; OnTarget says
	// whether the note is about its anchor.
	Target    string          `json:"target,omitempty"`
	OnTarget  bool            `json:"on_target"`
	Seconds   float64         `json:"seconds"`
	Exchanges []benchExchange `json:"exchanges"`
}

type benchCaseResult struct {
	Case     benchCase           `json:"case"`
	Analysis benchAnalysisResult `json:"analysis"`
	RCA      benchRCAResult      `json:"rca"`
}

type benchAnalysisResult struct {
	Content   string          `json:"content"`
	Reasoning string          `json:"reasoning,omitempty"`
	Error     string          `json:"error,omitempty"`
	Seconds   float64         `json:"seconds"`
	Exchanges []benchExchange `json:"exchanges"`
}

type benchRCAResult struct {
	Status     string    `json:"status"`
	Result     string    `json:"result"`
	Error      string    `json:"error,omitempty"`
	ErrorType  string    `json:"error_type,omitempty"`
	Iterations int       `json:"iterations"`
	Steps      []RCAStep `json:"steps"`
	// Linked: the objects the first prompt's linked evidence lists
	// (rule:Kind/name), and how many of its lines were trimmed.
	Linked        []string        `json:"linked,omitempty"`
	LinkedTrimmed int             `json:"linked_trimmed,omitempty"`
	Seconds       float64         `json:"seconds"`
	Exchanges     []benchExchange `json:"exchanges"`
}

// benchExchange is one chat-completion request and its answer. Request is
// the body as sent; Response is the server's JSON, or for a streamed answer
// the assembled content and reasoning.
type benchExchange struct {
	Request  json.RawMessage `json:"request"`
	Status   int             `json:"status,omitempty"`
	Response any             `json:"response,omitempty"`
	Error    string          `json:"error,omitempty"`
	Seconds  float64         `json:"seconds"`
}

// benchStream is a streamed answer, assembled.
type benchStream struct {
	Content          string `json:"content"`
	ReasoningContent string `json:"reasoning_content,omitempty"`
	FinishReason     string `json:"finish_reason,omitempty"`
	Chunks           int    `json:"chunks"`
}

// ---- Recording the model's requests -----------------------------------------------

// benchRecorder sits in http.DefaultClient, which every KubeHealth model call
// uses, and keeps each chat-completion request with its answer. With a seed it
// adds "seed" to every chat request, so a server that honours it samples the
// same way on every run; with noCache it adds "cache_prompt": false, since
// llama-server's answers to the same request differ once part of the prompt
// comes from its cache. Servers that don't know a field ignore it.
type benchRecorder struct {
	base    http.RoundTripper
	seed    int64
	noCache bool

	mu   sync.Mutex
	sink *[]benchExchange
}

func (r *benchRecorder) record(sink *[]benchExchange) {
	r.mu.Lock()
	r.sink = sink
	r.mu.Unlock()
}

func (r *benchRecorder) add(ex benchExchange) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sink != nil {
		*r.sink = append(*r.sink, ex)
	}
}

func (r *benchRecorder) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method != http.MethodPost || !strings.HasSuffix(req.URL.Path, "/chat/completions") || req.Body == nil {
		return r.base.RoundTrip(req)
	}
	body, err := io.ReadAll(req.Body)
	req.Body.Close()
	if err != nil {
		return nil, err
	}
	if r.noCache && bytes.HasPrefix(body, []byte("{")) && !bytes.Contains(body, []byte(`"cache_prompt":`)) {
		body = append([]byte(`{"cache_prompt":false,`), body[1:]...)
	}
	if r.seed != 0 && bytes.HasPrefix(body, []byte("{")) && !bytes.Contains(body, []byte(`"seed":`)) {
		body = append([]byte(fmt.Sprintf(`{"seed":%d,`, r.seed)), body[1:]...)
	}
	out := req.Clone(req.Context())
	out.Body = io.NopCloser(bytes.NewReader(body))
	out.ContentLength = int64(len(body))
	out.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(body)), nil }

	var probe struct {
		Stream bool `json:"stream"`
	}
	json.Unmarshal(body, &probe)
	ex := benchExchange{Request: json.RawMessage(body)}
	t0 := time.Now()
	resp, err := r.base.RoundTrip(out)
	if err != nil {
		ex.Error = err.Error()
		ex.Seconds = since(t0)
		r.add(ex)
		return nil, err
	}
	ex.Status = resp.StatusCode
	resp.Body = &recordedBody{ReadCloser: resp.Body, stream: probe.Stream && resp.StatusCode == http.StatusOK,
		done: func(b []byte, stream bool) {
			ex.Seconds = since(t0)
			ex.Response = decodeAnswer(b, stream)
			r.add(ex)
		}}
	return resp, nil
}

func since(t time.Time) float64 { return float64(time.Since(t).Milliseconds()) / 1000 }

// recordedBody keeps what KubeHealth reads of an answer and hands it over
// when KubeHealth closes the body.
type recordedBody struct {
	io.ReadCloser
	buf    bytes.Buffer
	stream bool
	once   sync.Once
	done   func(b []byte, stream bool)
}

func (b *recordedBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.buf.Write(p[:n])
	return n, err
}

func (b *recordedBody) Close() error {
	// Keep what was left unread (an error body KubeHealth only peeked at).
	io.Copy(&b.buf, b.ReadCloser)
	err := b.ReadCloser.Close()
	b.once.Do(func() { b.done(b.buf.Bytes(), b.stream) })
	return err
}

// decodeAnswer turns a response body into something readable: the
// assembled content and reasoning of a stream, the JSON of a completion, or
// the text of an error.
func decodeAnswer(b []byte, stream bool) any {
	if stream {
		var s benchStream
		var content, reasoning strings.Builder
		sc := bufio.NewScanner(bytes.NewReader(b))
		sc.Buffer(make([]byte, 64*1024), 1024*1024)
		for sc.Scan() {
			data, ok := strings.CutPrefix(sc.Text(), "data: ")
			if !ok || data == "[DONE]" {
				continue
			}
			var chunk struct {
				Choices []struct {
					Delta struct {
						Content          string `json:"content"`
						ReasoningContent string `json:"reasoning_content"`
					} `json:"delta"`
					FinishReason string `json:"finish_reason"`
				} `json:"choices"`
			}
			if json.Unmarshal([]byte(data), &chunk) != nil || len(chunk.Choices) == 0 {
				continue
			}
			s.Chunks++
			content.WriteString(chunk.Choices[0].Delta.Content)
			reasoning.WriteString(chunk.Choices[0].Delta.ReasoningContent)
			if fr := chunk.Choices[0].FinishReason; fr != "" {
				s.FinishReason = fr
			}
		}
		s.Content, s.ReasoningContent = content.String(), reasoning.String()
		return s
	}
	if json.Valid(b) {
		return json.RawMessage(append([]byte(nil), b...))
	}
	return string(b)
}

// ---- bench run -----------------------------------------------------------------

func benchRunCmd(args []string) error {
	fs := flag.NewFlagSet("bench run", flag.ExitOnError)
	casesDir := fs.String("cases", "bench/cases", "directory of case files (<id>.yaml)")
	snapsDir := fs.String("snapshots", "", "directory of snapshots (default: snapshots next to the cases directory)")
	llm := fs.String("llm", "", "OpenAI-compatible base URL, e.g. http://127.0.0.1:8080/v1")
	model := fs.String("model", "", "model name sent to the server; also the results' column in the scorecard")
	out := fs.String("out", "", "results file to write (JSON)")
	only := fs.String("only", "", "comma-separated case ids to run (default: all)")
	seed := fs.Int64("seed", 1234, "seed added to every chat request so sampling repeats across runs (0: none)")
	seedList := fs.String("seeds", "", "comma-separated seeds: run every item once per seed, and score the mean (overrides -seed)")
	noCache := fs.Bool("no-prompt-cache", true, `add "cache_prompt": false to every chat request (llama-server answers differently from its prompt cache)`)
	quiet := fs.Bool("quiet", false, "hide KubeHealth's own log lines")
	linkFollow := fs.Bool("link-follow", true, "fetch the objects the evidence names and add them to the first prompts (AI_LINK_FOLLOW)")
	atFlag := fs.String("at", "recorded", "replay time: recorded (same prompts on every run), now, or an RFC 3339 time; timestamps move by (at − recording time)")
	fs.Parse(args)
	if *llm == "" || *model == "" || *out == "" {
		return errors.New("run: -llm, -model and -out are required")
	}
	if *snapsDir == "" {
		*snapsDir = filepath.Join(filepath.Dir(filepath.Clean(*casesDir)), "snapshots")
	}
	cases, err := loadCases(*casesDir, splitList(*only))
	if err != nil {
		return err
	}
	seeds := []int64{*seed}
	if *seedList != "" {
		seeds = nil
		for _, v := range splitList(*seedList) {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return fmt.Errorf("-seeds: %q is not a number", v)
			}
			seeds = append(seeds, n)
		}
		if len(seeds) == 0 {
			return errors.New("-seeds: no seeds")
		}
	}
	if *quiet {
		log.SetOutput(io.Discard)
	}

	// KubeHealth's model settings, as a fresh start with this server.
	savedAI, savedPrompt, savedCfg, savedTransport := aiConfig, getSystemPrompt(), cfg, http.DefaultClient.Transport
	savedLinks := getLinkFollow()
	defer func() {
		aiConfig, cfg, http.DefaultClient.Transport, clock = savedAI, savedCfg, savedTransport, time.Now
		setSystemPrompt(savedPrompt)
		setLinkFollow(savedLinks)
	}()
	setLinkFollow(*linkFollow)
	aiConfig = aiCfgT{baseURL: strings.TrimSuffix(*llm, "/"), model: *model, enabled: true}
	setSystemPrompt(defaultSystemPrompt)
	cfg.AllNS, cfg.Namespaces = true, nil
	rec := &benchRecorder{base: http.DefaultTransport, seed: *seed, noCache: *noCache}
	http.DefaultClient.Transport = rec

	res := &benchResults{
		Kubehealth: version, Model: *model, LLM: aiConfig.baseURL, NoCache: *noCache, LinkFollow: linkFollow,
		CtxLen: modelCtxLen(), SystemPrompt: getSystemPrompt(), StartedAt: time.Now().UTC().Truncate(time.Second),
	}
	if res.CtxLen == 0 {
		fmt.Fprintf(os.Stderr, "bench run: %s does not report its context length; KubeHealth will not trim prompts\n", aiConfig.baseURL)
	}
	save := func() error { return writeJSONFile(*out, res) }

	var order []string
	bySnap := map[string][]benchCase{}
	for _, c := range cases {
		if bySnap[c.Snapshot] == nil {
			order = append(order, c.Snapshot)
		}
		bySnap[c.Snapshot] = append(bySnap[c.Snapshot], c)
	}
	snaps := map[string]*benchSnapshot{}
	for _, name := range order {
		snap, err := loadSnapshot(filepath.Join(*snapsDir, name))
		if err != nil {
			return fmt.Errorf("snapshot %s: %w", name, err)
		}
		snaps[name] = snap
	}
	for _, sd := range seeds {
		run := &res.benchRun
		if len(seeds) > 1 {
			run = &benchRun{}
			res.Seeds = append(res.Seeds, sd)
			res.Runs = append(res.Runs, run)
			fmt.Printf("== seed %d\n", sd)
		}
		run.Seed, run.Snapshots, run.Notes = sd, map[string]benchSnapshotRun{}, map[string]*benchNoteResult{}
		rec.seed = sd
		if err := benchRunSeed(run, order, bySnap, snaps, *atFlag, rec, save); err != nil {
			return err
		}
	}
	rec.record(nil)
	res.FinishedAt = time.Now().UTC().Truncate(time.Second)
	if err := save(); err != nil {
		return err
	}
	fmt.Println()
	fmt.Print(benchScorecard([]benchScore{scoreResults(res)}))
	return nil
}

// benchRunSeed runs every case once, snapshot by snapshot, into run, saving
// the results after every item.
func benchRunSeed(run *benchRun, order []string, bySnap map[string][]benchCase, snaps map[string]*benchSnapshot,
	atFlag string, rec *benchRecorder, save func() error) error {
	for _, name := range order {
		snap := snaps[name]
		at, err := replayTime(snap, atFlag)
		if err != nil {
			return err
		}
		c, at, problems, err := replaySnapshot(snap, at)
		if err != nil {
			return fmt.Errorf("snapshot %s: %w", name, err)
		}
		sr := benchSnapshotRun{RecordedAt: snap.meta.RecordedAt, ReplayedAt: at, Problems: len(problems)}
		if len(problems) > 0 {
			sr.TopProblem = problems[0].Key
		}
		run.Snapshots[name] = sr
		fmt.Printf("== %s: %d problems, recorded %s\n", name, len(problems), snap.meta.RecordedAt.Format(time.RFC3339))

		target := ""
		for _, bc := range bySnap[name] {
			if bc.NoteTarget {
				target = bc.ID
			}
		}
		note := benchNote(c, at, rec)
		note.Target = target
		for _, bc := range bySnap[name] {
			if bc.ID == target && noteAbout(bc.Anchor, note.Ref) {
				note.OnTarget = true
			}
		}
		run.Notes[name] = note
		fmt.Printf("   note  %-7s %5.1fs  %s: %s\n", note.Status, note.Seconds, note.Ref, truncate(note.Text+note.Error, 100))
		if err := save(); err != nil {
			return err
		}

		for _, bc := range bySnap[name] {
			cr := &benchCaseResult{Case: bc}
			benchAnalysis(c, bc.Anchor, rec, &cr.Analysis)
			benchRCA(c, bc.Anchor, rec, &cr.RCA)
			run.Cases = append(run.Cases, cr)
			fmt.Printf("   %-28s analysis %5.1fs | rca %s %5.1fs, %d steps\n", bc.ID,
				cr.Analysis.Seconds, cr.RCA.Status, cr.RCA.Seconds, len(cr.RCA.Steps))
			if err := save(); err != nil {
				return err
			}
		}
	}
	return nil
}

// replayTime is the instant a snapshot is replayed at, from -at: "recorded"
// (its recording time, so nothing moves), "now", or an RFC 3339 time.
func replayTime(snap *benchSnapshot, at string) (time.Time, error) {
	switch at {
	case "", "recorded":
		return snap.meta.RecordedAt, nil
	case "now":
		return time.Now().UTC().Truncate(time.Second), nil
	}
	t, err := time.Parse(time.RFC3339, at)
	if err != nil {
		return time.Time{}, fmt.Errorf("-at %q: want recorded, now or an RFC 3339 time", at)
	}
	return t.UTC(), nil
}

// replaySnapshot builds the snapshot's in-memory cluster as of at and warms
// it like a running KubeHealth. Every timestamp moves by (at − recording
// time), so every age stays what it was, and the clock is held at that
// instant until the next replay. Absolute times that KubeHealth prints (a
// CronJob's Last Schedule, a Job's Start Time) move with the shift, so only
// a fixed at (the default: the recording time) sends the same prompts on
// every run.
func replaySnapshot(snap *benchSnapshot, at time.Time) (*clusterState, time.Time, []Problem, error) {
	clock = func() time.Time { return at }
	c, err := snap.cluster(at)
	if err != nil {
		return nil, at, nil, err
	}
	if err := c.benchWarm(context.Background(), at); err != nil {
		return nil, at, nil, err
	}
	return c, at, c.buildProblems(c.allNamespaceEvents(), at), nil
}

// noteAbout reports whether the problem row ref (a problem key) is about the
// anchor: a namespaced anchor's row is its workload's, "ns:<ns>/<name>"; a
// cluster-scoped anchor's row groups its kind's warnings by reason,
// "cluster:<Kind>/<Reason>".
func noteAbout(a benchAnchor, ref string) bool {
	if a.NS == clusterScopeNS {
		return strings.HasPrefix(ref, "cluster:"+a.Kind+"/")
	}
	return ref == "ns:"+a.NS+"/"+a.Name
}

// benchNote writes the overview note as the Regenerate button does and waits
// for it.
func benchNote(c *clusterState, at time.Time, rec *benchRecorder) *benchNoteResult {
	n := &benchNoteResult{}
	rec.record(&n.Exchanges)
	defer rec.record(nil)
	t0 := time.Now()
	problems := c.regenerateBrief(at)
	for {
		c.brief.Lock()
		st := c.brief.status
		c.brief.Unlock()
		if st != "running" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	n.Seconds = since(t0)
	p := c.briefPayload(problems, at).Note
	if p == nil {
		n.Status = "none"
		n.Error = "no problems in this snapshot"
		return n
	}
	n.Status, n.Text, n.Ref, n.Label, n.Pod = p.Status, p.Text, p.Ref, p.Label, p.Pod
	n.Evidence, n.Tokens, n.Budget = p.Evidence, p.Tokens, p.Budget
	if p.Status != "ready" {
		n.Error = p.Reason
	}
	return n
}

// benchAnalysis runs the ✦ AI analysis of an object, as POST
// /api/ai/analyze/object/<ns>/<kind>/<name> does, and waits for it.
func benchAnalysis(c *clusterState, a benchAnchor, rec *benchRecorder, out *benchAnalysisResult) {
	rec.record(&out.Exchanges)
	defer rec.record(nil)
	key := aiStoreKey(c, "obj:"+a.NS+"/"+a.Kind+"/"+a.Name)
	aiInsightCache.Delete(key)
	t0 := time.Now()
	err := analyzeObjKey(c, key, a.NS, a.Kind, a.Name)
	out.Seconds = since(t0)
	if v, ok := aiInsightCache.Load(key); ok {
		in := v.(*AIInsight)
		out.Content, out.Reasoning, out.Error = in.Content, in.Reasoning, in.Error
	}
	if err != nil && out.Error == "" {
		out.Error = err.Error()
	}
}

// benchRCA runs the RCA agent on an object, as POST
// /api/rca/<ns>/<kind>/<name> does, and waits for it.
func benchRCA(c *clusterState, a benchAnchor, rec *benchRecorder, out *benchRCAResult) {
	rec.record(&out.Exchanges)
	defer rec.record(nil)
	key := rcaCacheKey(c, a.NS, a.Kind, a.Name)
	state := &RCAState{Status: "running", MaxIter: getRCAMaxIter()}
	rcaCache.Store(key, state)
	t0 := time.Now()
	runRCAAgent(c, a.NS, a.Kind, a.Name, state)
	out.Seconds = since(t0)
	state.RLock()
	out.Status, out.Result, out.Error, out.ErrorType = state.Status, state.Result, state.Error, state.ErrorType
	out.Iterations, out.Steps = state.Iteration, append([]RCAStep(nil), state.Steps...)
	out.Linked, out.LinkedTrimmed = append([]string(nil), state.Linked...), state.LinkedTrimmed
	state.RUnlock()
}

func writeJSONFile(path string, v any) error {
	b, err := json.MarshalIndent(v, "", " ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(b, '\n'), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// ---- bench inspect ---------------------------------------------------------------

func benchInspectCmd(args []string) error {
	fs := flag.NewFlagSet("bench inspect", flag.ExitOnError)
	dir := fs.String("snapshot", "", "snapshot directory")
	casesDir := fs.String("cases", "", "case directory: also check this snapshot's cases (anchors, events, logs)")
	atFlag := fs.String("at", "recorded", "replay time: recorded, now, or an RFC 3339 time")
	showLinked := fs.Bool("linked", false, "with -cases: print the linked evidence each case's first prompts get")
	fs.Parse(args)
	if *dir == "" {
		return errors.New("inspect: -snapshot is required")
	}
	log.SetOutput(io.Discard)
	savedAI, savedCfg := aiConfig, cfg
	defer func() { aiConfig, cfg, clock = savedAI, savedCfg, time.Now }()
	cfg.AllNS, cfg.Namespaces = true, nil
	aiConfig.enabled = false
	snap, err := loadSnapshot(*dir)
	if err != nil {
		return err
	}
	at, err := replayTime(snap, *atFlag)
	if err != nil {
		return err
	}
	c, at, problems, err := replaySnapshot(snap, at)
	if err != nil {
		return err
	}
	m := snap.meta
	fmt.Printf("snapshot %s (context %s), recorded %s with %s\n", m.Name, m.Context, m.RecordedAt.Format(time.RFC3339), m.RecordedWith)
	fmt.Printf("  %d objects, %d container logs, namespaces: %s\n", len(snap.objects), len(snap.logs), strings.Join(m.Namespaces, ", "))
	for _, n := range m.Notes {
		fmt.Println("  note:", n)
	}
	fmt.Printf("\nproblems as KubeHealth ranks them (replayed at %s):\n", at.Format(time.RFC3339))
	for i, p := range problems {
		var rs []string
		for _, r := range p.Reasons {
			rs = append(rs, fmt.Sprintf("%s ×%d", r.Reason, r.Count))
		}
		fmt.Printf("%3d. %-8s %-60s %s; last %s ago", i+1, p.Tier, p.Key, strings.Join(rs, ", "), p.Last)
		if p.Pod != "" {
			fmt.Printf("; pod %s", p.Pod)
		}
		fmt.Println()
	}
	if *casesDir == "" {
		return nil
	}
	cases, err := loadCases(*casesDir, nil)
	if err != nil {
		return err
	}
	fmt.Println("\ncases:")
	for _, bc := range cases {
		if bc.Snapshot != m.Name {
			continue
		}
		a := bc.Anchor
		info := c.fetchObjectInfo(a.NS, a.Kind, a.Name)
		state := "found"
		switch {
		case info.NotFound:
			state = "NOT FOUND"
		case info.Error != "":
			state = "error: " + info.Error
		}
		evs := objectEvents(c, a.NS, a.Kind, a.Name)
		warnings := 0
		for _, e := range evs {
			if e.Type == "Warning" {
				warnings++
			}
		}
		fmt.Printf("  %-28s %s: %s, %d events (%d warnings)", bc.ID, a, state, len(evs), warnings)
		if strings.EqualFold(a.Kind, "Pod") {
			logs, _ := fetchLogsForAI(c, a.NS, a.Name)
			fmt.Printf(", logs %d lines", strings.Count(logs, "\n"))
		} else if a.NS != clusterScopeNS {
			var e evidence
			lines, _ := memberEvidence(c, a.NS, a.Kind, a.Name)
			pod := e.addWorkload(c, a.NS, a.Kind, a.Name)
			fmt.Printf(", %d member warnings", len(lines))
			if pod != "" {
				fmt.Printf(", logs from %s (%d lines)", pod, strings.Count(e.logs, "\n"))
			}
		}
		// What link-following adds to the first prompts (linked.go).
		le := analysisEvidence(c, a.NS, a.Kind, a.Name, 0)
		var tags []string
		for _, it := range le.linkedItems {
			tags = append(tags, it.tag())
		}
		if len(tags) > 0 {
			fmt.Printf(", linked %s", strings.Join(tags, " "))
		}
		if *showLinked && le.linked != "" {
			fmt.Print("\n" + strings.TrimSuffix(strings.ReplaceAll(le.linked, "\n", "\n      "), "      "))
		}
		if bc.NoteTarget {
			top := ""
			if len(problems) > 0 {
				top = problems[0].Key
			}
			fmt.Printf(", note target (top problem %s: %v)", top, noteAbout(a, top))
		}
		fmt.Println()
	}
	return nil
}
