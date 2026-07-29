# True-North Final Nine-Gate Development Report

> Historical v6 report. Superseded by
> `docs/TRUE_NORTH_CERTIFICATION_20260729.md` and measurement contract v7.

Date: 2026-07-29

This is the consolidated result under measurement contract v6. The selected
eligible baseline is frozen Task 5 plus the contract-normative deterministic
actor-span rule. The incomplete Spark probe is not substituted into this
table.

| Gate | Result | Requirement | Coupled diagnostic | Pass |
|---|---:|---:|---:|:---:|
| Candidate-state macro F1 | 0.863223 | >= 0.900000 | n/a | No |
| Retained-value recall | 0.970711 | >= 0.900000 | n/a | Yes |
| Intrinsic junk escape rate | 0.000000 | <= 0.020000 | n/a | Yes |
| Acceptable atomic-count rate | 0.780172 | >= 0.900000 | n/a | No |
| Claim-text faithfulness | 0.726634 | >= 0.744435 | 0.529081 | No |
| Speaker exactness | 0.991379 | >= 0.954615 | 0.682493 | Yes |
| Reported-actor exactness | 0.780172 | >= 0.735385 | 0.537092 | Yes |
| Hallucination proxy | 0.133065 | <= 0.020000 | n/a | No |
| Schema parse success | 1.000000 | >= 0.990000 | n/a | Yes |

Overall: **5 of 9 gates pass. The stack is not certified to scale.**

## Final gold-class probe context

The subsequent `gpt-5.6-sol` probe was incomplete and therefore does not
replace the eligible baseline above. Its 96 validated flagged candidates
scored 0.631579 atomic-count accuracy, below the 0.652632 GLM/Task-5 control on
the same candidates. Errors remained directionally under-split. The
decomposition lane is stopped under the owner's terminal rule.

The hallucination gate also has a newly measured gold-vs-gold conflict:
pass-A against pass-B scores 0.145614 under the exact live proxy and 0.073684
under the narrower matched-pair-only diagnostic. Both exceed the unchanged
0.02 gate. This is reported as a measurement-contract finding, not used to
change the gate.

Safety accounting remains intact: intrinsic escapes are zero, relational
contamination is zero, the one materialized relational merge is auditable,
and no sealed holdout or production database was opened or mutated.

The unresolved bottleneck is not hidden by the denominator ruling. Atomic
count remains 11.98 percentage points below gate. Matched-pair faithfulness is
1.78 points below gate. Candidate-state macro F1 and the hallucination proxy
also fail independently. Neither Spark nor the gold-author-class probe supplied
a complete eligible replacement. Certification must therefore disclose the
measured atomicity and hallucination limitations.

Eligible baseline rescore SHA-256:
`84b82fc289b8f288297fe682eebc04d94a57634f2f044ba6c4281328dfd5889c`.
