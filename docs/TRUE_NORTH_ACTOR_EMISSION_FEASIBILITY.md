# True-North Task 6 Actor-Emission Feasibility

## Decision

Task 6 was **not built or run** because its independently measured achievable
ceiling is below the approved reported-actor gate.

No model calls were made for this audit. No sealed holdout or production
database was opened.

## Post-repair inter-annotator ceiling

The audit reloaded the span-enforced pass-A, pass-B, and pass-C actor-repair
artifacts, verified that pass C covers every A/B disagreement, and verified
that the promoted development gold equals the A/B agreement plus pass-C
adjudication.

| Measurement | Result |
|---|---:|
| Repaired atomics | 1,458 |
| Pass-A/pass-B agreements | 1,346 |
| Disagreements | 112 |
| Post-repair ceiling | 92.3182% |
| Phase B rule | `min(95%, ceiling - 2pp)` |
| Ceiling-referenced gate | **90.3182%** |

This independently reproduces the approved Phase B repaired-field gate. The
pre-repair 66.2% agreement is not used.

## Upper bounds

Promoted development gold contains:

- 440/1,458 non-null reported actors: 30.1783%
- 1,018/1,458 null reported actors: 69.8217%

On gold-non-null atomics, the frozen candidate `reported_actor` prior exactly
matches 283/440 values, or 64.3182%.

| Hypothetical system | Correct atomics | Upper bound |
|---|---:|---:|
| Perfect emission + existing prior values | 1,018 nulls + 283 values | **89.2318%** |
| Perfect emission + perfect values | 1,458 | **100.0000%** |

Perfect values provide 10.7682 percentage points of headroom above the
existing-prior upper bound.

The gold-atomic-weighted candidate-prior emission rate is 88.3402%; the
`reported_actor OR actor_name` fallback emits on 100% of atomics. Emission rate
does not affect the upper-bound calculation because the hypothetical emission
decision is perfect; only value accuracy on gold-positive atomics matters.

## Gate comparison

- Achievable with the specified existing prior values: 89.2318%
- Approved gate: 90.3182%
- Shortfall: **1.0864 percentage points**

The specified binary emission Task 6 cannot pass the approved gate even with a
perfect emission classifier. Per the review directive, the gate was not
recalibrated and no Task 6 model or prompt was built.

## Reproducibility

- Audit schema: `pif_true_north_actor_emission_feasibility_v1`
- Audit SHA-256:
  `b121b5bb68afa42d4baa8fc486d72fe24d4460996db7661daad7d584cbc25dcd`
- Local artifact:
  `diagnostics/task6-actor-emission-feasibility.json`
- Focused actor/calibration tests: 17 passed
