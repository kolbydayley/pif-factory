# True-North Phase C Disposition Result

Date: 2026-07-28

Suite: `ai-safety-v1`

Partition: Search development fold only

Status: **FAILED — stop before Task 5**

## Frozen acceptance rule

- Junk escapes: `0/9`
- False rejects: at most `10`
- Retained-value recall: at least `0.95`

## Bounded runs

| Run | Route | Calls | Tokens | Junk escapes | False rejects | Retained-value recall | Pass |
|---|---|---:|---:|---:|---:|---:|---|
| `phase-c-disposition-20260728-v1` | GLM | 21 | 139,744 | 2 | 12 | 0.949791 | No |
| `phase-c-disposition-20260728-v2` | GLM, one permitted added sentence | 21 | 141,674 | 1 | 12 | 0.949791 | No |
| `phase-c-disposition-20260728-v2-spark-conflicts-v1` | Spark conflict packet | 1 | 30,404 | 6 | 1 | 0.995816 | No |
| Majority recomposition of existing outputs | No model call | 0 | 0 | 3 | 6 | 0.974895 | No |

The raw Spark result exposed a harness composition defect: one Spark judgment
could overwrite two agreeing GLM judgments. The corrected deterministic
composition preserves a two-GLM agreement and uses Spark only to break a
selected GLM disagreement. The correction improved the raw Spark result, but
the frozen zero-junk-escape gate still failed.

## Accounting and isolation

- Phase C paid usage: 43 calls and 311,822 tokens.
- The second GLM run consumed the plan's only permitted additional
  single-sentence prompt iteration.
- No holdout artifact was opened.
- No production database or release state was mutated.
- Suite verification remained clean with manifest
  `f27c15b26388be7773d1457ced2ebd5cfc1c78c8edbecf86398213d7c0140bde`.
- The corrected no-call recomposition is frozen at result SHA-256
  `f1f5ebda6e8c88ddc845767bdeab7f6fdf45a2e9c7aabd434a0a9e4c48a06fcd`.

## Gate consequence

Task 4 did not meet its acceptance rule after the allowed iteration and
bounded conflict escalation. Tasks 5–7 have not been started. Continuing into
decomposition, actor confirmation, or certification would conceal an upstream
disposition failure and violate the ordered gate-closure plan.

## Task 4b amendment result

Task 4b implemented the approved asymmetric junk-verification stage without
changing the frozen Task 4 prompt or composition.

### Deterministic screen

- Retained inputs: 243
- Screened inputs: 170
- Planned GLM packets: 7
- Screen-class memberships:
  - `ensemble_disagreement`: 17
  - `repetition`: 13
  - `fragment`: 157
  - `bare_mention_question`: 17
- Both amendment-named escapes were screened before any paid call.
- Screen SHA-256:
  `a8eedfe9b4a1f2ded9c73616dcfb35c5dd51b245674cb521bf82dac097f0091f`

The frozen fragment rule selected substantially more items than the
amendment's approximate 20–40 expectation. The authorized thresholds were
preserved; compact batches kept the run within seven planned packets.

### Bounded verifier run

- Seven GLM batches completed.
- One additional GLM attempt failed strict validation because its deficiency
  quote was not copied verbatim. The failed receipt was preserved and charged;
  the one remaining authorized GLM call retried that batch successfully.
- Total paid usage: 8 calls, 68,324 tokens, 289.823 receipt seconds.
- Spark escalations: 0.
- Candidates flipped to reject: 2.
- No holdout or production mutation occurred.

### Composed score

| Measure | Task 4b result | Gate | Pass |
|---|---:|---:|---|
| Junk escapes | 3/9 | 0/9 | No |
| False rejects | 8 | ≤10 | Yes |
| Retained-value recall | 0.966527 | ≥0.95 | Yes |

All three remaining junk escapes were screened in and the verifier returned
`confirm_retain` for each. Therefore the amendment's one screen-threshold
adjustment is not applicable: no escaped junk item was screened out. The
verifier also produced no uncorroborated rejection requiring Spark.

Task 4b has reached its explicit second-failure stop. Tasks 5–7 remain
unstarted. The fully accounted result is frozen at SHA-256
`4b4d46878652394b327315469f6ef3818d236daa83a4f013cdb10fd339d8915d`.

## Task 4c Amendment 2a execution stop

The amended deterministic preflight successfully screened all five named junk
fixtures, including `dev_c094b91406c9222943a29eba` through its same-segment
local paraphrase neighbor. It selected 222 of 243 retained candidates into six
GLM packets. No gold, holdout, canonical, or production artifact was opened or
mutated during screening.

The first GLM packet completed and passed schema and scope validation, but its
provider receipt reported 47,323 total tokens: 33,319 input, 1,795 output, and
12,209 reasoning tokens. The second call was interrupted before it produced a
receipt. Task 4c therefore consumed exactly one paid call and 47,323 tokens.

Measured against the first receipt, the six frozen packets project to roughly
283,519 total tokens. The runner's original post-call-only budget check could
have authorized a call that crossed the 150,000-token hard ceiling. Execution
was stopped rather than violate that ceiling, and the harness now performs a
conservative whole-run packet-budget preflight before any provider call.

This is a **mechanical budget-preflight failure**, not a scored semantic Task 4c
result. The 0/9 junk gate, false-reject count, and retained-value recall were
not evaluated from the incomplete output. Suite verification remained clean
at manifest SHA-256
`f27c15b26388be7773d1457ced2ebd5cfc1c78c8edbecf86398213d7c0140bde`;
the production source remained unchanged. No further Task 4c calls are
authorized under the unchanged one-run, 150,000-token contract.

## Task 4d option 2 checkpoint

Task 4c is retired. An offline review swept 36 proximity, shared-stem, and
stem-rarity parameterizations against the real development bundles. Every
setting that admitted all five known junk fixtures selected 946–1,001 of 1,056
retained candidates; selective settings dropped fixtures. This independently
reproduced the Amendment 2a failure mode and established that there is no
surface-screen operating point.

The approved option-2 measurement contract is recorded in the suite manifest:

- Previous manifest SHA-256:
  `f27c15b26388be7773d1457ced2ebd5cfc1c78c8edbecf86398213d7c0140bde`
- Amended manifest SHA-256:
  `519250505f6b458502b8825cb8a6d65c66d93c9b6bd6d7892e94663b3b98d961`
- Measurement-contract SHA-256:
  `dedab8c1cb837229da2d2b38730ee37e76e4eaa7e1b9bb6a4ba4681116ef754f`

The Phase C disposition gate now counts intrinsic junk only. Relational
families (`non_useful_repetition*`, `nonasserted_question_frame`) are excluded
from that gate only conditionally: certification must prove an identifying
canonical merge with a corroborating non-junk peer and ledger category
`merged_duplicate_retained`.

### Coupled gate result

| Measure | Result | Required | Pass |
|---|---:|---:|---|
| Intrinsic junk escapes | 1 | 0 | No |
| False rejects | 8 | ≤10 | Yes |
| Retained-value recall | 0.966527 | ≥0.95 | Yes |
| Relational escapes | 2 | — | — |
| Canonically merged relational escapes | 0 | 2 | No |
| `merged_duplicate_retained` rows | 0 | 2 | No |
| Relational contamination | 2 | 0 | No |

The remaining intrinsic escape is
`dev_6184abb2048d10e9496e6ee4`. The two relational contamination candidates are
`dev_ab9794e907ab6d420d4a9bea` and
`dev_fcec890304c9c2b40332af53`. Both are unmerged, and neither carries
`merged_duplicate_retained`.

The combined checkpoint failed and Task 5 was not authorized or started. No
sealed holdout was opened, no paid model call was made, and the production
source remained unchanged. The checkpoint is frozen at SHA-256
`52b10ea706fb381f9c9bef1c2e9246a290ba408f8441bf96b2a5c8889d8c1c92`.
