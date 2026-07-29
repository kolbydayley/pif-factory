# True North Input Split-Default Result

Date: 2026-07-29

Run: `task5-input-split-default-20260729-v1`

Partition: full Search development fold

Sealed holdouts opened: no

Production mutation: no

## Decision

The input-side split-default inversion did not pass and is retired. Per the
review directive, no second input-anchoring design will be attempted.

The deterministic screen selected 106 of 241 Stage-B-eligible candidates in
19 packets. It split each flagged proposal on coordinating conjunctions and
clause-boundary commas, then asked GLM to merge only non-independent
fragments. The 135 unflagged candidates retained their frozen Task-5 outputs.

Seventeen packets containing 91 flagged candidates validated. Two packets
repeatedly violated the unchanged Stage-B edit contract by returning
`edit_reason=none` after changing the proposed one-claim text. The token
preflight refused another wave at 28 calls and 328,682 tokens because the
remaining 21,318 tokens could not reserve two calls safely.

The complete-fold numbers below are explicitly diagnostic: the two missing
flagged packets use their frozen Task-5 outputs. This fallback does not make
the experiment complete or acceptance-eligible.

| Metric | Frozen Task 5 | Split-default diagnostic |
|---|---:|---:|
| Atomic-count accuracy | 78.0172% | **80.6034%** |
| Full-fold faithfulness | 54.7319% | 56.1932% |
| Aligned-subset faithfulness | 74.4900% | **74.5206%** |
| Full-fold speaker exactness | 68.2493% | 70.1493% |
| Flagged atomic-count accuracy | — | **66.6667%** |
| Unflagged atomic-count accuracy | — | **92.1260%** |
| Under-extraction errors | 48 | **45** |
| Over-extraction errors | 3 | **0** |
| Intrinsic junk escapes | 0 | 0 |
| Relational contamination | 0 | 0 |

Among the 91 directly measured flagged candidates:

- 6 moved from wrong to acceptable.
- 0 moved from acceptable to wrong.
- 7 changed predicted count at all.
- 32 remained wrong despite receiving the split-default input.

The mechanism moved atomicity by 2.59 percentage points, not the 11.98 points
needed to clear the 90% gate. It did not cause the anticipated over-splitting
failure; instead, under-extraction remained dominant. The evidence therefore
rejects deterministic input-default inversion as a sufficient solution in
this harness.

## Acceptance

- Complete full-Search measurement: **fail** (17/19 flagged packets).
- Atomic-count accuracy at least 90%: **fail** (80.6034% diagnostic).
- Aligned faithfulness at least 75%: **fail** (74.5206% diagnostic).
- Intrinsic junk escapes zero: **pass**.
- Relational contamination zero: **pass**.

The actor lane remains held. Sealed transfer episodes remain unopened.

## Spend

- Experiment ceiling: 30 calls / 350,000 tokens.
- Actual: **28 calls / 328,682 tokens**.
- Unused ceiling: 2 calls / 21,318 tokens; insufficient for the two-call
  reservation required by the remaining packets.
- Cumulative campaign calls: **236**.
- Known cumulative tokens: **2,017,646**, excluding the earlier 45-call actor
  gold-repair stage whose ledger did not record tokens.

The campaign-wide call total remains report-only by owner authorization.
Per-experiment ceilings remain hard.
