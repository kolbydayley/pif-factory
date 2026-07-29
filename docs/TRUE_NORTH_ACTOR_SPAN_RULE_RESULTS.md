# True-North Deterministic Actor Span Rule (T1) — 2026-07-29

## Decision

`docs/TRUE_NORTH_ACTOR_CONTRACT.md` already declares two mechanical rules as
normative. Both were enforced only as **rejection** — a provider answer that
violated the span invariant was refused and the call charged — and never as
**construction**. This implements them as construction, deterministically, with
**zero model calls**.

No gate, gold, consensus, manifest, or scored run was modified. Development
partition only; sealed holdouts untouched. The module is a composition helper;
wiring it into a scored lane is a separate, explicit step.

## The rules (quoted from the contract)

> A non-null value that is not a case-insensitive literal span of the evidence
> is mechanically converted to `null`.

> A direct speaker discussing their own action in their own voice is not a
> `reported_actor`.

## Result — full development population (n = 1,458 atomics, `gold_sha256 02c06a87…7e6221`)

| Composition | Agreement with repaired gold | 95% CI |
|---|---:|---|
| Raw candidate prior | **19.4102%** (283/1458) | 17.46 – 21.52 |
| **+ span enforcement** | **75.0343%** (1094/1458) | 72.75 – 77.19 |
| + span and speaker-self | 75.0343% (1094/1458) | 72.75 – 77.19 |
| Always-null baseline | 69.8217% | — |

**+55.6 points, no model call.**

### It costs nothing where an actor should be emitted

| Population | Raw prior | Span-enforced |
|---|---:|---:|
| Gold non-null actors (n = 440) | 64.3182% | **64.3182%** |

Identical. The rule removes only values the contract already required to be
null; it does not suppress a single correct emission. The entire gain is on the
1,018 atomics whose gold actor is null.

### Where the priors go

| Outcome | Count |
|---|---:|
| Kept (literal evidence span) | 529 |
| Nulled — not a literal span | **805** |
| Nulled — speaker talking about themselves | 0 |
| Already null | 124 |

**55.2% of candidate actor priors are not literal evidence spans.** That single
fact is the field's dominant error mode, and the contract already says what to
do about it.

Speaker-self exclusion fires zero times on this population. It is retained
because the contract requires it and it is free, but it is not load-bearing
here and should not be credited with any of the gain.

## Relationship to the two-stage actor experiment

`docs/TRUE_NORTH_ACTOR_TWO_STAGE_RESULTS.md` (same day, model-based) found
**emission is the limiting half** at 50.4451% stage-1 accuracy, with a partial
composite of 48.0712% against a 43.3234% always-null floor.

This rule attacks that same limiting half with no calls. The two numbers are
**not directly comparable** — the two-stage composite runs through the exact
Search scorer, which also charges atomic-count and alignment misses to
`reported_actor`, whereas this is field agreement on the gold atomic
population. What transfers is the finding, not the percentage: a deterministic
span test decides emission better than a paid binary emission stage, and the
two-stage run's own stage-2 value accuracy where a validated value aligned
(89.1892%) suggests value selection was never the bottleneck.

The natural composition to measure next: deterministic span rule for emission,
cheap model only for value selection among literal spans when several are
present.

## Oracle bound for the remaining half — before anyone funds a value stage

T1 fixed emission. Value selection (64.3182% on gold non-null) is the open half.
`runs/actor-span-rule-20260729/oracle_bound.py` measures what a value stage
could ever achieve, with no model calls.

| Question | Result |
|---|---:|
| Gold actor is a literal span of its own evidence | **440/440 = 100.0%** |
| Unreachable for any span-restricted selector | **0** |
| Prior already correct when reachable | 283/440 = 64.3182% |
| **Residual — the only items a paid value stage can win** | **157** |

**Nothing is out of reach.** Every gold actor is present verbatim in its own
evidence, so a span-restricted selector has no structural ceiling and the whole
problem is picking the right span. The addressable surface is 157 atomics —
10.8% of the 1,458-atomic population.

### The decisive number

| Composition | Field agreement |
|---|---:|
| Span-enforced prior (T1, shipped) | 1094/1458 = 75.0343% |
| **+ a PERFECT value stage on all 157 residual items** | **1251/1458 = 85.8025%** |
| Reported-actor gate | 90.3182% |

**Correction 2026-07-29:** the 85.8025% figure is computed on the **coupled
denominator** — the exact defect T6 diagnoses. It is therefore *evidence for the
decoupling ruling*, not an independent stop-condition. Read it as: under the
current coupled scoring, even a perfect value stage cannot clear the gate, which
is a statement about the scoring rule, not about value selection. Under Ruling 1
(matched-pair scoring) this bound must be recomputed before it means anything
about what a value stage can achieve.

What survives the correction, because neither depends on the denominator:

- **100% reachability** — every gold non-null actor is a literal span of its own
  evidence, computed directly against gold.
- **The residual is 157 atomics** — likewise computed directly against gold.

**Recommendation (unchanged in direction, corrected in basis): do not fund a
paid actor value stage until the field-gate scoring is decoupled per Ruling 1
and this bound is recomputed on matched pairs.** T1 already captured the free
55.6 points; the residual is small and well-characterised, so the value stage
can be sized precisely once the measurement is right.

Caveat: this script's span *option enumerator* (capitalized-run heuristic plus
speaker roster) failed to enumerate the gold span on 255 of 440 items, so its
`span_option_count_distribution` and `residual_option_count_distribution` are
indicative only and understate how often a single-option deterministic pick is
available. Reachability (100%) and the residual count (157) do not depend on the
enumerator — they are computed directly against gold.

## What this does not establish

- Not a gate result. The composite reported-actor gate runs through the Search
  scorer with its own denominator; this measurement does not run that scorer.
  Expect a smaller composite gain than the field-level gain shown here.
- Not evidence that the 90.3182% actor gate is reachable. See
  `docs/TRUE_NORTH_GATE_DENOMINATOR_ALIGNMENT.md`: gold-vs-gold on that gate's
  own denominator reaches only 0.6847, so the gate is currently unpassable
  regardless of extractor quality.
- 64.32% on gold non-null actors is unchanged by this rule and remains the open
  problem for value selection.

## Reproducing

```bash
PYTHONPATH=. python3 runs/actor-span-rule-20260729/measure.py
```

Module: `research_factory/true_north_actor_span_rule.py`.
Tests: `tests/test_true_north_actor_span_rule.py` — 11 passed.
Full true-north regression: 107 passed.

## Provenance

Predicted independently by two frontier shadow passes on 2026-07-28: the Opus
shadow benchmark's recommendation T1
(`docs/TRUE_NORTH_OPUS_SHADOW_20260728.md`) and the actor shadow's
`prior_span_enforced_vs_gold` at 75.2% on a 500-atomic sample
(`docs/TRUE_NORTH_ACTOR_SHADOW_20260728.md`). This run reproduces that at
**75.0343% on the full 1,458-atomic population** with the rule implemented in
the repository rather than in a scratch script.
