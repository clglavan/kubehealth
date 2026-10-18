package main

// kubehealth bench: frozen, replayable incidents for benchmarking the model
// behind KubeHealth's AI features.
//
//	bench record   dump a live cluster into a snapshot directory
//	bench run      replay snapshots in memory and run the overview note, the
//	               ✦ AI analysis and the RCA agent on every case, through the
//	               same functions the UI uses, against an OpenAI-compatible LLM
//	bench score    compare results files in a markdown scorecard
//	bench inspect  show how KubeHealth ranks a snapshot's problems (no LLM)
//
// See "Benchmark" in docs/reference.md.

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"sigs.k8s.io/yaml"
)

func benchMain(args []string) int {
	if len(args) == 0 {
		benchUsage()
		return 2
	}
	var err error
	switch args[0] {
	case "record":
		err = benchRecordCmd(args[1:])
	case "run":
		err = benchRunCmd(args[1:])
	case "score":
		err = benchScoreCmd(args[1:])
	case "inspect":
		err = benchInspectCmd(args[1:])
	case "export":
		err = benchExportCmd(args[1:])
	case "-h", "-help", "--help", "help":
		benchUsage()
		return 0
	default:
		fmt.Fprintf(os.Stderr, "kubehealth bench: unknown command %q\n\n", args[0])
		benchUsage()
		return 2
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "kubehealth bench:", err)
		return 1
	}
	return 0
}

func benchUsage() {
	fmt.Fprint(os.Stderr, `Usage:
  kubehealth bench record  -kubeconfig <path> -context <ctx> [-namespaces a,b] -out bench/snapshots/<name>
  kubehealth bench run     -cases bench/cases -llm <OpenAI-compatible base URL> -model <name> -out <results.json> [-seeds 1,2,3]
  kubehealth bench score   -results a.json,b.json [-out scorecard.md]
  kubehealth bench inspect -snapshot bench/snapshots/<name> [-cases bench/cases]
  kubehealth bench export  -cases bench/cases -out site.json   (the bench as JSON, for the project site)

Run a command with -h for its flags.
`)
}

// ---- Cases -----------------------------------------------------------------

// benchCase is one bench/cases/<id>.yaml: an object in a snapshot, what is
// wrong with it, and the keywords a right answer contains.
type benchCase struct {
	ID       string      `json:"id"`
	Snapshot string      `json:"snapshot"`
	Anchor   benchAnchor `json:"anchor"`
	// Healthy: the right answer is that nothing is broken.
	Healthy bool   `json:"healthy,omitempty"`
	Truth   string `json:"truth"`
	// Keyword groups: a check passes when every group has at least one
	// keyword in the text (case-insensitive substring).
	Cause [][]string `json:"cause,omitempty"` // the root cause
	// Deep: with needs_tool, the cause that only another object's evidence
	// shows; otherwise the decisive detail of a cause the anchor's own
	// evidence shows (a tag, a limit).
	Deep [][]string `json:"deep,omitempty"`
	Fix  [][]string `json:"fix,omitempty"` // the target of the fix
	// NeedsTool: the decisive cause is in another object that is not in
	// KubeHealth's first RCA prompt, so the agent has to call a tool to see it.
	NeedsTool bool `json:"needs_tool,omitempty"`
	// NoteTarget: the overview note should be about this case's problem.
	NoteTarget bool   `json:"note_target,omitempty"`
	Source     string `json:"source"`
	License    string `json:"license"`
}

type benchAnchor struct {
	NS   string `json:"ns"` // clusterScopeNS (_cluster) for cluster-scoped objects
	Kind string `json:"kind"`
	Name string `json:"name"`
}

func (a benchAnchor) String() string { return a.NS + "/" + a.Kind + "/" + a.Name }

// loadCases reads every *.yaml in dir, sorted by file name. only, when not
// empty, keeps the listed case ids.
func loadCases(dir string, only []string) ([]benchCase, error) {
	files, err := filepath.Glob(filepath.Join(dir, "*.yaml"))
	if err != nil {
		return nil, err
	}
	sort.Strings(files)
	keep := map[string]bool{}
	for _, id := range only {
		keep[id] = true
	}
	var out []benchCase
	targets := map[string]string{}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			return nil, err
		}
		var c benchCase
		if err := yaml.UnmarshalStrict(b, &c); err != nil {
			return nil, fmt.Errorf("%s: %w", f, err)
		}
		if want := strings.TrimSuffix(filepath.Base(f), ".yaml"); c.ID != want {
			return nil, fmt.Errorf("%s: id %q does not match the file name", f, c.ID)
		}
		if c.Snapshot == "" || c.Anchor.NS == "" || c.Anchor.Kind == "" || c.Anchor.Name == "" {
			return nil, fmt.Errorf("%s: snapshot and anchor {ns, kind, name} are required", f)
		}
		if !c.Healthy && len(c.Cause) == 0 {
			return nil, fmt.Errorf("%s: a fault case needs cause groups", f)
		}
		if c.NeedsTool && (c.Healthy || len(c.Deep) == 0) {
			return nil, fmt.Errorf("%s: needs_tool is for fault cases, with deep groups for what the other object shows", f)
		}
		if c.NoteTarget {
			if other := targets[c.Snapshot]; other != "" {
				return nil, fmt.Errorf("%s: snapshot %s already has a note target (%s)", f, c.Snapshot, other)
			}
			targets[c.Snapshot] = c.ID
		}
		if len(keep) > 0 && !keep[c.ID] {
			continue
		}
		out = append(out, c)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no cases in %s", dir)
	}
	return out, nil
}

// ---- Snapshot metadata -------------------------------------------------------

// snapshotMeta is a snapshot's snapshot.yaml.
type snapshotMeta struct {
	Name    string `json:"name"`
	Context string `json:"context"`
	// RecordedAt is the time the snapshot's ages are measured against, in
	// whole seconds. Replay moves every timestamp by (replay time − RecordedAt).
	RecordedAt        time.Time `json:"recorded_at"`
	RecordedWith      string    `json:"recorded_with"`
	LogTailLines      int64     `json:"log_tail_lines"`
	Namespaces        []string  `json:"namespaces"`
	SkippedNamespaces []string  `json:"skipped_namespaces"`
	Notes             []string  `json:"notes,omitempty"`
}

const snapshotMetaFile = "snapshot.yaml"

func readSnapshotMeta(dir string) (snapshotMeta, error) {
	var m snapshotMeta
	b, err := os.ReadFile(filepath.Join(dir, snapshotMetaFile))
	if err != nil {
		return m, err
	}
	if err := yaml.Unmarshal(b, &m); err != nil {
		return m, fmt.Errorf("%s: %w", filepath.Join(dir, snapshotMetaFile), err)
	}
	return m, nil
}

func writeYAML(path string, v any) error {
	b, err := yaml.Marshal(v)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}
