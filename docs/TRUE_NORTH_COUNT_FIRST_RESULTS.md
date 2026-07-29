# True North Count-First Stage-B Result

Date: 2026-07-29  
Run: `task5-count-first-20260729-v1`  
Partition: full Search development fold  
Sealed holdouts opened: no  
Production mutation: no

## Decision

The count-first prompt variant did not improve atomic-count accuracy and does
not pass.

One of 21 packets returned invalid JSON on both its first attempt and its
identical-prompt replacement. The experiment stopped at its declared 25-call
ceiling with 20 packets and 232/256 candidates directly measured. Its
acceptance status is therefore partial and ineligible.

For a full-fold diagnostic only, the missing packet uses its frozen Task-5
output. This fallback does not convert the experiment into a complete run.

| Metric | Frozen Task 5 | Count-first + one-packet fallback |
|---|---:|---:|
| Atomic-count accuracy | 78.0172% | **78.0172%** |
| Full-fold faithfulness | 54.7319% | 55.5442% |
| Aligned-subset faithfulness | 74.4900% | **74.3401%** |
| Full-fold speaker exactness | 68.2493% | 68.4366% |
| Aligned-subset speaker exactness | 87.3800% | 86.1244% |
| Under-extraction errors | 48 | 48 |
| Over-extraction errors | 3 | 3 |
| Intrinsic junk escapes | 0 | 0 |
| Relational contamination | 0 | 0 |

Among the 232 directly measured candidates, the model changed its predicted
atomic count for only five:

- 2 moved from wrong to acceptable.
- 2 moved from acceptable to wrong.
- 1 changed count without changing acceptability.

The prompt hypothesis is rejected for this model and harness. Removing the
verbatim-adoption default did not make GLM systematically count first; its
atomic-count behavior remained almost entirely unchanged.

## Spend

- Experiment ceiling: 25 calls / 300,000 tokens.
- Actual: **25 calls / 249,496 tokens**.
- Cumulative campaign calls: **185**.
- Known cumulative tokens: **1,427,310**, excluding the earlier 45-call actor
  gold-repair stage whose ledger did not record tokens.

The campaign-wide 160-call stop was lifted by owner authorization, but this
experiment's local ceiling remained hard and was enforced.
