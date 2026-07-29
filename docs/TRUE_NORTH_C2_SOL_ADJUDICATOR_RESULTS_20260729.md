# True-North Revision C2: Sol Adjudicator

Date: 2026-07-29

Suite: `ai-safety-v1`

Run: `task5-c2-sol-adjudicator-20260729-v1`

Status: **terminal operational failure; decomposition lane permanently closed**

## Frozen design

C2 changed exactly one semantic seat from Phase C:

- Pass A: frozen GLM Task-5 decompositions, zero calls.
- Pass B: all 106 completed Sol decompositions from Phase C, zero calls.
- Pass C2: `gpt-5.6-sol` through `codex exec --ephemeral`.
- Input: the exact 19 Phase C A/B-plus-evidence adjudication packets.
- Output contract: select a non-empty exact subset of the A/B claim-text
  union; no third structure.
- Unflagged candidates: frozen Task-5 outputs.
- Declared ceiling: 24 calls / 700,000 tokens.
- Whole-run reservation: 19 calls / 665,000 tokens.

## Terminal result

No model inference occurred. Codex's response-format API rejected the Phase C
JSON Schema before execution:

1. The exact OpenCode-compatible schema was rejected because `oneOf` is not
   permitted.
2. One bounded provider-adapter retry flattened only the response schema while
   retaining the exact packet and semantic validator. It was rejected because
   `uniqueItems` is not permitted.

Both responses were HTTP 400-style `invalid_json_schema` failures before model
execution. They are charged as two invocations and zero inference tokens.
Prompts, A/B inputs, candidate scope, union semantics, gold, gates, and scorer
were unchanged.

The second provider-facing adapter restriction could mechanically be removed,
but the directive authorized one revision and one bounded operational retry.
No further adapter or experiment was attempted. This avoids relabeling a chain
of harness revisions as the same one-run experiment.

## Result table

| Measure | Result | Gate | Outcome |
|---|---:|---:|---|
| Validated Sol envelopes | 0 / 19 | 19 / 19 required | Not measurable |
| Full-fold atomic count | n/a | >= 0.900000 | Not scored |
| Flagged-subset atomic count | n/a | diagnostic | Not scored |
| Matched faithfulness | n/a | >= 0.744435 | Not scored |
| Matched hallucination | n/a | <= 0.093684 | Not scored |
| Candidate-state macro F1 | n/a | >= 0.790664 | Not rescored |
| Junk / contamination | unchanged frozen certification | 0 / 0 | No mutation |

Adjudicator selection statistics are unavailable: Sol produced zero semantic
outputs, so it did not choose A, B, or a merged union for any candidate.

## Finding and permanent closure

The gold-class adjudicator recipe did **not transfer operationally through the
current Codex structured-output harness**. This is not evidence that Sol's
semantic adjudication quality is poor; the model never ran. It is evidence
that the full recipe was not reproducible under the frozen one-revision
envelope without additional harness adaptation.

Per the review-loop terminal ruling, the decomposition experiment lane is now
**permanently closed regardless of outcome**. No further decomposition design,
schema adapter, retry, or holdout execution is authorized.

## Spend and isolation

- C2: **2 calls / 0 inference tokens**
- Cumulative campaign: **289 calls / 3,029,113 known tokens**
- Historical actor-gold-repair tokens remain unavailable.
- Result SHA-256:
  `86f991d45b12919d286f03e9897fab46534a0fa8338020b84ff16fccceee440c`
- Holdout opened: `false`
- Production mutation: `false`
