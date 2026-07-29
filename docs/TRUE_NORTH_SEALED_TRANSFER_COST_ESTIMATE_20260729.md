# True-North Sealed-Transfer Cost Estimate

Date: 2026-07-29

Scope: Task 2, Step 1 of the closeout plan

Provider calls made: **0**

Sealed gold opened or compiled: **no**

## Verified sealed state

The sealed atomic-gold scaffold is prepared but has no model output:

| Artifact | Count or state |
|---|---:|
| Pass-A jobs | 38 |
| Pass-A schemas | 38 |
| Pass-A outputs | 0 |
| Pass-B jobs | 38 |
| Pass-B schemas | 38 |
| Pass-B outputs | 0 |
| Pass-C adjudication directory | absent |
| Compiled `final/gold.private.json` | absent |
| Compiled `final/consensus.private.json` | absent |

The relation and utility directories contain one specification/question
scaffold apiece. Canonical, canonical-subject, and canonical-proposition gold
do not exist. None is needed to calculate the nine extraction gates.

The two hash-bound bundle records are:

| Episode | Candidates | Candidate-bearing segments |
|---|---:|---:|
| `ep_97a45100ce0d305f58d7dd69` | 312 | 17 |
| `ep_044f1d2d020e021cfaf99e90` | 375 | 21 |
| **Both** | **687** | **38** |

Both records have a bundle SHA-256 and episode-context SHA-256 in the suite
manifest.

## Nine-gate dependency check

The current extraction table is produced by
`true_north_semantic_scoring.score_campaign` plus the consensus atomic
metrics and Ruling-7 disposition/merge accounting. Its model-derived
references are:

- atomic `final/gold.private.json`;
- atomic `final/consensus.private.json`;
- candidate predictions; and
- the bundle speaker maps.

That is sufficient for candidate-state macro F1, retained-value recall,
intrinsic junk escape, acceptable atomic count, claim-text faithfulness,
speaker, reported actor, hallucination, and schema success.

The older whole-benchmark `true_north.score_run` additionally insists on
canonical gold because it continues into canonical and relation metrics.
That requirement does not enter the Ruling-7 nine-gate extraction table.
Canonical, canonical-subject, canonical-proposition, relation, and utility
gold can therefore be omitted from this transfer-price estimate.

## Measurement basis

### Atomic gold

Development atomic gold used 69 segment packets in each of passes A, B, and C:
207 calls and 9,552,192 measured input-plus-output tokens. Per-packet
least-squares fits against the actual candidate count were:

| Pass | Measured token model | R squared |
|---|---|---:|
| A | `30,572.71 + 802.36 × candidates` | 0.907 |
| B | `30,360.06 + 801.82 × candidates` | 0.887 |
| C | `30,275.82 + 1,254.42 × candidates` | 0.929 |

Sources:

- `gold/development/pass-a/jobs/`
- `gold/development/pass-a/outputs/*/receipt.json`
- `gold/development/pass-b/outputs/*/receipt.json`
- `gold/development/pass-c-adjudication/outputs/*/receipt.json`

Compile is local and costs zero provider calls.

### Actor-gold repair

The development actor repair had 1,458 atomics, batches of 75, and measured
112 A/B disagreements. Its A, B, and C receipt totals imply respectively
547.0 tokens per atomic, 559.2 tokens per atomic, and 607.1 tokens per
adjudicated item. The clean call formula is:

```text
2 × ceil(atomic_count / 75) + ceil(disagreement_count / 75)
```

Sources:

- `gold/development/actor-repair/manifest.json`
- `gold/development/actor-repair/result-span-enforced.json`
- `gold/development/actor-repair/pass-*/outputs/*/receipt.private.json`

The sealed atomic and disagreement counts do not exist yet. Actor calls and
tokens below are consequently development-rate projections, not
deterministically knowable pre-inference floors. The projection uses 1.279
atomics per candidate and a 7.68% actor A/B disagreement rate. The deterministic
evidence-span enforcement and compilation after adjudication cost zero calls.

### Certified GLM-only stack

The optimistic runtime topology is:

```text
2 disposition packets per candidate-bearing segment
+ 1 Task-5 packet per candidate-bearing segment
+ 1 split-default packet per segment containing a flagged compound
```

All 17 segments in the smaller episode and all 21 in the larger episode
contain at least one compound-flagged candidate. Retry, validation-repair, and
operational-fallback calls are excluded.

Development receipt fits were:

| Lane | Measured token model | R squared |
|---|---|---:|
| Disposition A | `2,638.07 + 329.47 × candidates` | 0.963 |
| Disposition B | `2,939.84 + 312.26 × candidates` | 0.939 |
| Task 5 | `6,007.33 + 484.33 × eligible candidates` | 0.804 |
| Split-default | `4,115.70 + 1,308.67 × eligible flagged candidates` | 0.826 |

Task-5 eligibility is projected from its measured 241/256 rate. Compound
eligibility is projected from the measured 106/116 rate.

Sources:

- `phase-c/runs/phase-c-disposition-20260728-v1/`
- `phase-c/runs/phase-c-disposition-20260728-v2/`
- `multipass/runs/task5-adjudication-20260729-v1/`
- `multipass/runs/task5-input-split-default-20260729-v1/`

## Priced options

Token figures are measured-size planning estimates. Provider output and
reasoning length are not knowable before inference, so there is no honest
exact token floor analogous to the exact structural call floor.

### A — atomic sealed gold, smaller episode

| Component | Codex calls | GLM calls | Estimated tokens |
|---|---:|---:|---:|
| Atomic A/B/C, 17 packets each | 51 | 0 | 2,442,428 |
| Actor A/B/C, projected 399 atomics / 31 disagreements | 13 | 0 | 460,157 |
| Compile and span enforcement | 0 | 0 | 0 |
| **Option A total** | **64** | **0** | **2,902,585** |

The exact pre-run expression is `51 + actor_calls(N, D)`. The displayed 64 is
the development-calibrated point estimate because sealed `N` and `D` do not
exist yet.

### B — certified GLM-only stack, smaller episode

| Component | Codex calls | GLM calls | Estimated tokens |
|---|---:|---:|---:|
| Disposition A | 0 | 17 | 147,642 |
| Disposition B | 0 | 17 | 147,401 |
| Task 5 | 0 | 17 | 244,518 |
| Split-default | 0 | 17 | 288,515 |
| **Option B total** | **0** | **68** | **828,076** |

One full smaller-episode transfer check, A plus B, is therefore projected at
**64 Codex calls + 68 GLM calls = 132 calls and 3,730,661 tokens**.

### C — both sealed episodes

Pooling actor repair across both episodes is valid because its packets are
atomic batches, not segment-context packets.

| Component | Codex calls | GLM calls | Estimated tokens |
|---|---:|---:|---:|
| Atomic gold A/B/C, 38 packets each | 114 | 0 | 5,429,782 |
| Actor repair, projected 879 atomics / 68 disagreements | 25 | 0 | 1,013,552 |
| Certified GLM-only stack | 0 | 152 | 1,854,703 |
| **Option C total** | **139** | **152** | **8,298,037** |

If actor packets are unnecessarily isolated by episode, the Codex call count
rises from 139 to 142. The larger episode alone projects to 78 Codex calls,
84 GLM calls, and 4,566,067 combined tokens.

## D — cheaper defensible variants

### D1: full smaller episode, seven gates

Run atomic A/B/C and the complete GLM stack, but omit actor-gold repair. Score
the seven gates whose reference remains valid:

- candidate-state macro F1;
- retained-value recall;
- intrinsic junk escape;
- atomic count;
- faithfulness;
- speaker; and
- schema.

Reported actor and hallucination must be marked unavailable; using the
pre-repair actor field would not be defensible.

| Codex calls | GLM calls | Estimated tokens |
|---:|---:|---:|
| 51 | 68 | 3,270,504 |

This is the cheapest unbiased whole-episode check. It saves 13 Codex calls and
approximately 460,157 tokens, but it does not test the Phase-D actor rule.

### D2: deterministic 209-candidate cluster sample

For a cheaper directional signal, precommit the salt
`ai-safety-v1|sealed-transfer-sample-v1`, SHA-256-rank all 17 segment IDs, and
take complete segments until at least 200 candidates are included. On the
current scaffold that selects 11 segments and 209 of 312 candidates without
using gold labels or model results.

| Codex calls | GLM calls | Estimated tokens |
|---:|---:|---:|
| 42 | 44 | 2,453,712 |

This includes projected actor repair and can calculate all nine numbers. It is
meaningful as a directional transfer diagnostic for the common atomic and
field metrics: under an independent-candidate approximation, the worst-case
finite-population 95% margin is about ±3.9 percentage points. Segment-cluster
correlation makes the real interval wider.

It is **not** a defensible pass/fail estimate for intrinsic junk escape or
three-class macro F1. Junk is rare, and an input-blind sample cannot guarantee
enough junk/hold cases. Selecting after seeing gold classes would be
cherry-picking. A defensible full nine-gate transfer table therefore requires
the complete smaller episode.

## Decision-relevant conclusion

- Full smaller episode, all nine gates: approximately **132 calls / 3.73M
  tokens**.
- Both episodes, all nine gates: approximately **291 calls / 8.30M tokens**.
- Full smaller episode, seven gates without actor/hallucination: **119 calls /
  3.27M tokens**.
- Directional 209-candidate sample: **86 calls / 2.45M tokens**, but it cannot
  establish the rare-junk or macro-F1 gates.

No option was executed. The sealed fold remains unopened, and cumulative
campaign spend remains **309 calls / 3,583,991 known ledger tokens**.
