# True-North Spark Split-Default Probe

Date: 2026-07-29

Status: terminal, incomplete, budget-ineligible; decomposition lane stopped.

## Bounded experiment

The experiment used `openai/gpt-5.3-codex-spark` on the exact immutable
flagged packets from `task5-input-split-default-20260729-v1`. Nineteen
semantic packets were deterministically packed into fourteen provider
envelopes. Every returned inner result still had to validate against its
original packet. Unflagged candidates retained their frozen Task-5 outputs.

The declared ceiling was 14 calls and 250,000 tokens.

## Terminal result

| Measure | Result |
|---|---:|
| Calls | 14 / 14 |
| Tokens | 309,122 / 250,000 |
| Token overage | 59,122 |
| Validated provider envelopes | 4 / 14 |
| Validated Spark candidates | 12 / 106 flagged |
| Same-candidate Spark atomic accuracy | 0.916667 |
| Same-candidate GLM atomic accuracy | 0.916667 |
| Partial-fallback full-fold atomic accuracy | 0.784483 |
| Partial-fallback aligned faithfulness | 0.747826 |
| Intrinsic junk escapes | 0 |
| Relational contamination | 0 |

Ten envelopes failed the unchanged local contract, chiefly because
`edit_reason=none` was returned when the proposal was not adopted verbatim.
The run also exceeded its token ceiling because the 17,000-token reservation
per provider envelope understated actual Spark usage. The harness did not
silently accept invalid outputs, truncate the comparison, weaken the
validator, or dispatch a fifteenth call.

The 12-candidate same-item comparison is diagnostic only. It shows no measured
Spark advantage over GLM on that small validated slice. It cannot answer the
full model-versus-input question because 94 flagged candidates lack validated
Spark output. The Task-5 fallback composition is likewise not an eligible
experiment result.

Per Kolby's ruling, failure ends this decomposition lane. The next decision is
strong-lane routing versus certification with a disclosed atomicity
limitation; no architecture choice is made here.

Cumulative known campaign spend after this probe is 250 calls and 2,326,768
tokens. Earlier actor-gold-repair token usage remains excluded where the
historical ledger never captured it, so the token total is explicitly a known
minimum.

Terminal artifact SHA-256:
`6c65cd742a5133775f2f63ddfc7c3a73552e91d1df50e9bb9dd07c785b106657`.

