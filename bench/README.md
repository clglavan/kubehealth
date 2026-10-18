# KubeHealth-Bench

A benchmark for the model behind KubeHealth's AI features, run **through KubeHealth
itself**: the same prompts, evidence, context budget and read-only tools the UI uses,
on frozen incidents recorded from real Kubernetes clusters. No cluster is needed to
run it.

It answers one question: *if you plug this model into KubeHealth, how good are the
answers people will see?*

## What it measures

KubeHealth asks the model three things. The bench scores all three on every incident:

| Job | What KubeHealth asks | What the bench checks |
|---|---|---|
| **RCA** (root-cause analysis) | "Find out why this object is broken" — with 4 read-only tools (`get_pod_logs`, `get_object_describe`, `get_object_events`, `get_correlated_events`) and up to 5 rounds | the right root cause, the decisive detail, the right fix, whether it investigates when the cause sits elsewhere, how many lookups it makes, whether it finishes |
| **✦ Analysis** | "Is anything wrong with this object? What, why, what to do?" | the right issue, the right action, and — on healthy objects — that it does not invent a fault |
| **Note** | "In one or two sentences, what should the engineer check first?" for the top problem on the overview | it points at the right problem, and it is an answer rather than leaked reasoning |

Scoring reads the diagnosis from the **whole answer** with keyword checks (every
keyword group must match), never its layout: a model is not rewarded or punished for
headings. A fix only counts when the answer also says what to do (create, set, roll
back, `kubectl` …).

## The incidents

**54 cases** in [`cases/`](cases), recorded as frozen snapshots in [`snapshots/`](snapshots):

| | faults | healthy | where from |
|---|---|---|---|
| `shop-staging`, `payments-prod` | 10 | 2 | KubeHealth's demo clusters (`demo/setup.sh`) |
| `bench-a` … `bench-d` | 30 | 12 | a kind cluster built by [`incidents/setup.sh`](incidents) — one manifest per case |
| **total** | **40** | **14** | |

- Faults cover scheduling (taints, selectors, affinity, requests, PVC binding), images
  (bad tags, pull secrets), configuration (missing ConfigMaps/Secrets/keys), runtime
  (OOMKilled, bad arguments, read-only filesystems, probes), Jobs and CronJobs, Services
  (selector/port mismatches), quotas and LimitRanges, RBAC, init containers and stuck
  rollouts.
- **15 cases have the cause in another object** (`needs_tool: true`): the symptom is on
  one workload, the cause in its dependency — a database that can't start because its
  Secret is missing, a claim whose StorageClass doesn't exist. The evidence KubeHealth
  puts in the first prompt is not enough; the model has to go and look.
- **14 healthy objects**, several with harmless noise (a readiness blip at start-up, a
  Job that retried and then succeeded, a scale-down), so "nothing is wrong" is a real
  test.
- Snapshots store no values: ConfigMaps and Secrets keep key names only, literal env
  values and last-applied annotations are dropped.

## Running it

Any OpenAI-compatible chat-completions server works (llama.cpp, LM Studio, vLLM, …).
Example with llama.cpp and an 8k context, as KubeHealth's embedded build uses:

```bash
llama-server -m model.gguf --port 8091 --ctx-size 8192 --n-gpu-layers 999

kubehealth bench run   -cases bench/cases -llm http://127.0.0.1:8091/v1 -model mymodel \
                       -seeds 1,2,3 -out results/mymodel.json
kubehealth bench score -cases bench/cases -results results/base.json,results/mymodel.json \
                       -out scorecard.md
```

- **Deterministic:** replays happen at the recording time, so every run sends identical
  prompts; `-seeds 1,2,3` runs everything once per seed and the scorecard shows the mean
  and the spread across seeds.
- **Records, too:** `kubehealth bench record` snapshots a live cluster (read-only), and
  `kubehealth bench inspect -snapshot … -cases …` checks that each case's anchor has
  the evidence the case expects.

The full reference (flags, case-file format, how timestamps are replayed, how to add a
case) is in [`docs/reference.md`](../docs/reference.md#benchmark).

## Results so far

Mean of 3 seeds. Reference models are much larger hosted models, run through the same
prompts and tools (their tool calls go through a JSON protocol instead of native
function calling) without link-following.

| Check | Qwen3-0.6B (base) | magos v19 | magos v19 + link-following | *reference:* Claude Haiku 5.5 | *reference:* Gemini 3.8 Flash |
|---|---|---|---|---|---|
| RCA: right root cause | 67% | 88% | 87% | 99% | 89% |
| RCA: finds the cause in another object | 2% | 20% | 78% | 44% | 62% |
| RCA: looks further when the cause is elsewhere | 0% | 98% | 91% | 0% | 98% |
| RCA: right fix | 56% | 71% | 87% | 93% | 89% |
| Analysis: right issue | 79% | 91% | 89% | 100% | 97% |
| Healthy object: no invented fault | 86% | 71% | 71% | 95% | 90% |

Link-following is on by default (`AI_LINK_FOLLOW`, `bench run -link-follow`); the
base model and magos v19 columns above ran with it off.
[magos](https://huggingface.co/clglavan/magos-k8s-0.6b) is a 0.6B model built for
KubeHealth; its model card has the full table with an explanation of every check.

## Adding a case

Reproduce the incident on a cluster, record a snapshot (or reuse one), check it with
`bench inspect -cases`, then write `cases/<id>.yaml` with keywords only a right answer
contains. Review the snapshot before committing: no values are stored, but names,
images and log lines are.
