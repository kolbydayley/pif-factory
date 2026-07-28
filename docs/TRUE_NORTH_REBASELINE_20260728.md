# True-North Instrument-V2 Re-baseline — 2026-07-28

## Decision

The repaired measurement instrument does **not** make the stored combined-stack
run ready to scale. The best stored variant, `stack-03-all-compatible`, still
passes **3 of 9** extraction gates.

The re-score did expose one large instrument error: the former "unsupported"
proxy reported 84.6% because it treated surface-form and field disagreement as
hallucination. Instrument v2 reports a 3.85% hallucination rate and preserves
the former 84.6% result as a legacy diagnostic. The remaining 3.85% still
misses the 2% gate.

No new model calls were made. The original campaign and score artifacts remain
unchanged. Re-scoring used a labeled audit clone that references the same
hash-bound packets and stored outputs:

```text
~/Library/Application Support/Podcast Intelligence Factory/true-north/
  ai-safety-v1/input-optimization/pif_true_north_input_optimization_v1/
  tnio-20260728-combined-stack-v1-instrument-v2/
```

## Nine-gate comparison

| Gate | Instrument v1 | Instrument v2 | Target | Remaining gap | Result | Attribution |
|---|---:|---:|---:|---:|---|---|
| Candidate-state macro F1 | 97.66% | 97.66% | ≥90% | none | PASS | Model already clears gate |
| Retained-value recall | 100.00% | 100.00% | ≥90% | none | PASS | Model already clears gate |
| Junk escape | 11.11% | 11.11% | ≤2% | 9.11 pp high | FAIL | Real disposition error: 1 of 9 strict junk candidates escaped |
| Acceptable atomic count | 65.12% | 65.12% | ≥90% | 24.88 pp low | FAIL | Real decomposition/count error |
| Claim-text faithfulness proxy | 53.80% | 53.80% | ≥90% | 36.20 pp low | FAIL | Best-consensus-reference scoring was already effectively active; substantial lexical/decomposition mismatch remains |
| Speaker exactness | 64.71% | 64.71% | ≥97% | 32.29 pp low | FAIL | Alias canonicalization did not rescue this stored sample; unmatched atomics and genuine attribution differences dominate |
| Reported-actor exactness | 38.82% | 38.82% | ≥95% | 56.18 pp low | FAIL | Same structural coupling problem, with more missing/extra actor assignments |
| Hallucination rate | n/a (legacy proxy 84.62%) | 3.85% | ≤2% | 1.85 pp high | FAIL | Instrument repair removed divergence false positives; 2 of 52 strict candidates still have hallucination-severity flags |
| Schema parse success | 100.00% | 100.00% | ≥99% | none | PASS | Harness already clears gate |

The 82.69% field-divergence rate is retained as a diagnostic. It is not treated
as hallucination and does not drive the hard unsupported-content gate.

## Gold reliability

The existing pass-C adjudication was already complete. Re-running it would have
spent model quota and attempted to replace immutable artifacts without changing
the adjudication set, so instrument v2 instead repaired the reliability
calculation and regenerated its diagnostic from the frozen pass-A, pass-B,
pass-C, and consensus artifacts.

| Reliability measure | Raw A/B | Strict post-consensus |
|---|---:|---:|
| Items | 1,140 | 1,098 |
| Exact disposition agreement | 80.70% | 83.79% |
| Exact atomic-count agreement | 84.04% | 86.98% |
| Value-state agreement | — | 100.00% |
| Reject-set Jaccard | 53.33% previously reported across contested scope | 100.00% |
| Contested items excluded | — | 42 |
| Reliability gate | FAIL | PASS |

Exact `retain` versus `revise` and exact atomic count remain visible as
diagnostics. The reliability gate binds to stable value/junk/hold truth and the
strict reject set, matching the consensus policy.

## Instrument changes verified

- Speaker and reported-actor comparisons canonicalize known aliases through
  each candidate's actual episode speaker map.
- Enum-like fields normalize through the repository's existing label-pack
  vocabulary; unknown values remain disagreements.
- Unsupported flags distinguish `hallucination` from `divergence`.
- The hard gate uses hallucination only; the legacy aggregate remains auditable.
- Faithfulness chooses the best frozen consensus decomposition, and contested
  candidates are excluded from strict denominators.
- Gold reliability excludes consensus-contested items while retaining raw
  agreement diagnostics.
- The focused benchmark regression suite passes: **86 tests passed**.

## Checkpoint conclusion

Phase 1 is complete. Its measurable gain is a truthful unsupported-content
gate, not a quality breakthrough. The stored single-pass GLM output still has
real decomposition and attribution coupling failures, and it remains below the
scale-up threshold.

Per the implementation plan, no Phase 2 model calls should begin until this
checkpoint is reviewed. If approved, the next bounded experiment is the
three-pass design: disposition, proposition inventory/decomposition, then
closed-set attribution.
