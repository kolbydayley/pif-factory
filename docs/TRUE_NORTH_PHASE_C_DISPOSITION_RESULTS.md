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
