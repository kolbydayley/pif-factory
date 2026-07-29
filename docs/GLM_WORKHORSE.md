# GLM-5.2 Workhorse

Status: **offline canary only**. This adapter uses the OpenCode Go subscription
to call `opencode-go/glm-5.2` as a narrow candidate extractor. It is not wired
to SQLite, the production worker, label publication, or queue mutation.

The division of responsibility is:

1. GLM reads one bounded podcast window and returns only `event_type`,
   `evidence`, and `claim_text`.
2. A dedicated non-coding OpenCode agent receives the full event ontology,
   calibration guidance, episode metadata, neighboring context, and an explicit
   evidence/context boundary.
3. Local code validates the schema, maps harmless Unicode/whitespace-normalized
   spans back to unique exact source substrings, and drops any ambiguous event
   without discarding the other grounded events in the case.
4. A source-aware Codex audit measures transcript support, distinctness, signal
   value, type fit, and keep/revise/drop disposition. A separate reference
   alignment measures holdout coverage only; the reference set is not treated
   as exhaustive enough to measure candidate precision.
5. Existing PIF validators and quality gates remain authoritative. A passing
   canary does not authorize production promotion.

Check readiness without making a model call:

```bash
python3 -m research_factory.pif_cli lab glm-workhorse status
```

Preview the frozen 20-source canary:

```bash
python3 -m research_factory.pif_cli lab glm-workhorse canary
```

Run it explicitly:

```bash
python3 -m research_factory.pif_cli lab glm-workhorse canary --execute
```

The default settings are two concurrent OpenCode calls, a 180-second hard
deadline, no blanket retries, and ten candidate events per window. Each call
uses an isolated ephemeral OpenCode data directory so parallel calls do not
contend on OpenCode's SQLite session database. Only the existing OpenCode
authentication file is copied into that temporary directory; it is deleted
with the worker state after the call. Private jobs, raw output, validated
candidates, and receipts live under `~/Library/Application Support/Podcast
Intelligence Factory/glm-workhorse/`.

After the canary completes, run the private Codex judge using the receipt path:

```bash
python3 -m research_factory.pif_cli lab glm-workhorse judge \
  --receipt '<run-root>/receipt.revalidated.json' --execute
```

Run the primary transcript-visible candidate audit:

```bash
python3 -m research_factory.pif_cli lab glm-workhorse audit \
  --receipt '<run-root>/receipt.revalidated.json' --execute
```

If validator rules improve after a run, reprocess captured answers without
making another provider call:

```bash
python3 -m research_factory.pif_cli lab glm-workhorse revalidate \
  --receipt '<run-root>/receipt.json'
```

The source-aware experimental gate requires all cases usable, at least `0.95`
supported-or-partial candidates, at least `0.75` keep-or-revise candidates, and
at least `0.60` clean keeps. The advisory reference gate requires at least
`0.70` equivalent coverage, `0.80` equivalent-or-partial coverage, and zero
invalid judge pairs. Receipts always record
`production_enabled=false`, `promotion_authorized=false`,
`queue_mutation=false`, and `canonical_db_opened=false`.

## Corrected 20-source canary

The corrected 2026-07-27 run passed both experimental gates:

- `20/20` cases were usable from `20` calls with no retries and no timeouts.
- `119` candidates were returned; event-granular grounding dropped one
  ungrounded quote and retained `118` exact-grounded candidates.
- The source-aware Codex audit found `112/118` fully supported and `6/118`
  partially supported, with zero unsupported candidates.
- It classified `97` candidates as keep, `19` revise, and `2` drop:
  `98.3%` actionable and `82.2%` clean keeps.
- Reference alignment found `83/95` equivalent events (`87.4%` coverage) and
  `86/95` equivalent-or-partial events (`90.5%` coverage).
- GLM calls took `170.228` seconds wall time at concurrency two and reported
  approximately `$0.32294` in nominal provider-equivalent cost.

The initial failed run was a harness failure: the default coding agent lacked
the full ontology/context, a 60-second subprocess deadline killed valid work,
`steps=1` prematurely stopped the custom agent, parallel processes shared one
SQLite session database, validation failed at whole-case rather than event
granularity, and a source-blind reference comparison was incorrectly treated as
candidate precision. The corrected lane is viable as an offline nomination
workhorse, while Codex remains the final semantic authority. Production and
queue integration remain explicitly disabled.

## GPT-5.3-Codex-Spark comparison

On 2026-07-27, the same frozen selection, OpenCode CLI transport, custom agent,
job packets, ontology, context, concurrency, deadline, grounding rules, and
Codex audits were rerun with `openai/gpt-5.3-codex-spark`. OpenCode used the
locally authenticated OpenAI OAuth provider; no Codex CLI transport was used.

Run the Spark variant with:

```bash
python3 -m research_factory.pif_cli lab glm-workhorse canary \
  --model openai/gpt-5.3-codex-spark --execute
```

| Measure | GLM 5.2 | GPT-5.3-Codex-Spark |
|---|---:|---:|
| Usable cases | 20/20 | 20/20 |
| Timeouts | 0 | 0 |
| Wall time | 170.228s | 148.657s |
| Returned events before grounding | 119 | 138 |
| Ungrounded events dropped | 1 | 13 |
| Grounded candidates | 118 | 125 |
| Fully supported | 112 | 119 |
| Partially supported | 6 | 6 |
| Unsupported | 0 | 0 |
| Keep / revise / drop | 97 / 19 / 2 | 106 / 9 / 10 |
| Actionable fraction | 98.3% | 92.0% |
| Clean-keep fraction | 82.2% | 84.8% |
| Low-signal candidates | 2 | 10 |
| Equivalent reference coverage | 83/95 (87.4%) | 70/95 (73.7%) |
| Equivalent-or-partial coverage | 86/95 (90.5%) | 86/95 (90.5%) |
| Provider-reported per-call cost | $0.32294 nominal | $0.00 reported |

Spark passed both experimental gates and was about 12.7% faster. It found the
same broad reference coverage when partial matches count and had a slightly
higher clean-keep rate among retained candidates. GLM was substantially better
at verbatim evidence fidelity, strict semantic equivalence, low-signal pruning,
and overall keep-or-revise yield. For this exact evidence-bound extraction
lane, GLM remains the better default workhorse; Spark is a viable faster
fallback or a useful second-pass diversity model.

## Expanded OpenCode provider and model comparison

The same 20-source harness was subsequently run through OpenCode with the
direct Z.AI coding-plan authentication and the OpenAI subscription's GPT-5.4
and GPT-5.4 Mini models. Extraction for every row below used OpenCode; Codex was
used only as the unchanged offline evaluator.

| Model and OpenCode provider | Wall | Per-call p50 / p90 / max | Grounded / dropped | Equivalent + partial reference | Actionable | Clean keep | Gates |
|---|---:|---:|---:|---:|---:|---:|---|
| `opencode-go/glm-5.2` | **170.2s** | 15.0 / 20.1 / 48.4s | 118 / **1** | **83 + 3** | **98.3%** | 82.2% | pass |
| `zai-coding-plan/glm-5.2` | 254.9s | 23.4 / 33.7 / 56.7s | 108 / **1** | 73 + 13 | 95.4% | 75.9% | pass |
| `openai/gpt-5.3-codex-spark` | **148.7s** | **13.0 / 16.6 / 24.4s** | 125 / 13 | 70 + 16 | 92.0% | **84.8%** | pass |
| `openai/gpt-5.4` | 253.7s | 24.1 / 34.4 / 40.1s | **129 / 0** | 60 + 27 | 82.9% | 79.8% | reference fail |
| `openai/gpt-5.4-mini` | 476.3s | 42.3 / 68.9 / 87.2s | 104 / 8 | 52 + 28 | 86.5% | 76.9% | reference fail |

The direct Z.AI lane did not reduce latency: it was about 49.7% slower than
OpenCode Go for the same nominal GLM 5.2 model. It remains a credible
subscription-capacity overflow lane because all cases completed, both gates
passed, and p90 stayed below 34 seconds, but this test did not measure or
establish either provider's usage ceiling.

GPT-5.4 had perfect event grounding and high transcript support but
over-extracted low-signal material, failed the strict-equivalence gate, and was
as slow as Z.AI. GPT-5.4 Mini was the slowest lane by a wide margin and also
failed strict reference equivalence. Neither is recommended for this
candidate-extraction role under the current harness.

Recommended routing:

1. Use OpenCode Go GLM 5.2 as the default workhorse.
2. Use direct Z.AI GLM 5.2 as the first capacity/quota overflow lane.
3. Use Spark when speed or candidate diversity matters, followed by strict
   quote grounding and Codex pruning.
4. Do not route this task to GPT-5.4 or GPT-5.4 Mini by default.
