# True-North Transfer-Collapse Diagnostic

Date: 2026-07-29

Provider calls: **0**

Second sealed episode opened: **no**

## Conclusion

The collapse is partly a macro-F1 class-prior artifact and partly a genuine
disposition failure. It is not caused by the hold-admission policy: that rule
was never invoked on the sealed episode because neither GLM disposition pass
emitted a hold.

## Candidate-state distribution and class performance

Rates use strictly scoreable candidates. Development is the 256-candidate
Ruling-7 Search scope; 248 are strict and 8 contested. Sealed has 306 strict
and 6 contested candidates.

| Gold class | Development count | Development rate | Sealed count | Sealed rate |
|---|---:|---:|---:|---:|
| Value | 239 | 96.371% | 290 | 94.771% |
| Junk | 9 | 3.629% | 15 | 4.902% |
| Hold | 0 | 0.000% | 1 | 0.327% |

| Class | Development precision | Development recall | Development F1 | Sealed precision | Sealed recall | Sealed F1 |
|---|---:|---:|---:|---:|---:|---:|
| Value | 0.991489 | 0.974895 | 0.983122 | 0.991667 | 0.820690 | 0.898113 |
| Junk | 0.538462 | 0.777778 | 0.636364 | 0.212121 | 0.933333 | 0.345679 |
| Hold | 1.000000* | 1.000000* | 1.000000* | 1.000000 | 0.000000 | 0.000000 |

`*` Development contains no strict hold examples and no predicted holds. The
exact scorer's empty-class convention assigns precision, recall, and F1 of
1.0. One sealed gold hold predicted as value changes hold F1 discontinuously
from 1.0 to 0.0.

The observed macro-F1 decline is `0.873162 - 0.414597 = 0.458565`. To separate
class priors from conditional decisions, the sealed true-class conditional
prediction rates were held fixed while the true-class counts were reweighted
to development's 239/9/0 distribution. That counterfactual scores `0.726363`.

- Prior/scorer contribution: `0.726363 - 0.414597 = 0.311766`, or **68.0%**
  of the observed drop.
- Conditional decision contribution:
  `0.873162 - 0.726363 = 0.146799`, or **32.0%**.

The second component is real. Value recall fell 15.42 points. Junk precision
fell 32.63 points because 52 gold-value candidates were rejected, even though
junk recall improved.

## Hold policy

| Reading | Held candidates |
|---|---:|
| GLM disposition pass A | 0 |
| GLM disposition pass B | 0 |
| Union of A and B | 0 |
| Ensemble before hold resolution | 0 |
| Admitted by `admit_hold_unless_intrinsic_v1` | 0 |
| Blocked by the intrinsic rule | 0 |

The sealed gold rate among policy-input holds is therefore **undefined (0/0)**,
not a degraded analogue of development's 17/18 gold-value base rate. The
policy was fitted to a development risk, but it was dormant in this runtime
composition on sealed data. The one strict gold hold was classified as value
before the policy boundary.

## Retained-value recall attribution

There are 290 strict gold-value candidates:

| Outcome | Count | Share of strict gold value |
|---|---:|---:|
| Retained with at least one atomic | 238 | 82.069% |
| Rejected at disposition | 52 | 17.931% |
| Lost through terminal hold handling | 0 | 0.000% |
| Accepted but produced zero atomics | 0 | 0.000% |

All 52 recall misses originate in disposition rejection. Hold handling and
zero-atomic decomposition contribute none.

## Diagnostic ablations

These ablations are **diagnosis only**.

### Hold resolution disabled

Because the policy received zero holds, disabling it changes nothing.

| Gate | Result | Pass |
|---|---:|:---:|
| Candidate-state macro F1 | 0.414597 | No |
| Retained-value recall | 0.820690 | No |
| Intrinsic junk escape | 0.000000 | Yes |
| Atomic-count accuracy | 0.722689 | No |
| Faithfulness | 0.719775 | No |
| Speaker | 1.000000 | Yes |
| Reported actor | 0.734694 | No |
| Hallucination | 0.075163 | Yes |
| Schema | 1.000000 | Yes |

Result: **4/9**, identical to the frozen stack.

### Phase-D actor suppression disabled

This retains the deterministic evidence-span rule but restores all 80
span-valid actor emissions instead of suppressing 48 at risk >= 2.

| Gate | Result | Pass |
|---|---:|:---:|
| Candidate-state macro F1 | 0.414597 | No |
| Retained-value recall | 0.820690 | No |
| Intrinsic junk escape | 0.000000 | Yes |
| Atomic-count accuracy | 0.722689 | No |
| Faithfulness | 0.719775 | No |
| Speaker | 1.000000 | Yes |
| Reported actor | 0.726531 | No |
| Hallucination | 0.133987 | No |
| Schema | 1.000000 | Yes |

Result: **3/9**. The fitted suppression threshold remains directionally useful
on sealed data: it improves actor exactness by 0.008163 and hallucination by
0.058824, restoring the hallucination gate. It nevertheless does not preserve
the joint development result because actor remains 0.000691 below its gate.

## Absolute interpretation boundary

The ablations are diagnostic only. No variant may be adopted, and no prompt,
threshold, rule, policy, composition, scorer, or gate may change on the basis
of these sealed results. The stack may not be re-certified using a
sealed-derived configuration.

Any apparently better ablation is future-work evidence requiring fresh
authorization and fresh held-out data. The second sealed episode
`ep_044f1d2d020e021cfaf99e90` is permanently closed under this campaign and
may not validate any diagnosis-informed change.
