# True-North Decoupled Scoring Results

> Historical v6 result. Hallucination scoring and final certification are
> superseded by contract v7 and
> `docs/TRUE_NORTH_CERTIFICATION_20260729.md`.

Date: 2026-07-29

Status: frozen development-fold result; sealed holdouts unopened.

## Owner ruling and contract

Kolby's review-thread ruling adopts matched-pair denominators for speaker,
reported actor, and claim-text faithfulness. The former coupled-denominator
readings remain mandatory diagnostics, but no longer determine those three
gates. The atomic-count range-acceptance gate remains 0.90.

The gates were recomputed from the frozen pass-A/pass-B artifacts using
`min(previous_target, matched_pair_ceiling_mean - 0.04)`:

| Field | Matched-pair ceiling | Gate |
|---|---:|---:|
| Speaker | 0.994615 | 0.954615 |
| Reported actor | 0.775385 | 0.735385 |
| Claim-text faithfulness | 0.784435 | 0.744435 |

The directive displayed 0.9700 for the speaker result, but the authoritative
formula yields 0.954615. This arithmetic correction is recorded in the
manifest and budget ledger.

The suite manifest is now contract v6, retains the complete option-2
disposition contract as history, and binds the deterministic actor-span rule
as the zero-call actor baseline. The current manifest SHA-256 is
`c77634f40b07dbf554610d726f99db6c50ae275197490dd959638b1442fdae77`;
the v6 contract SHA-256 is
`61f5f4d24eb86ff1e20d7ac9e480ee5967c13419b99770c6e2c249c68f93fbc0`.

## Frozen-lane rescoring

All rows below were recomputed from hash-bound local outputs with zero
provider calls. Field values are matched-pair gate readings; the parenthetical
values are the retained coupled diagnostics.

| Frozen lane | Atomic count | Faithfulness | Speaker | Actor | Hallucination | Gates |
|---|---:|---:|---:|---:|---:|---:|
| Task 5 adjudication | 0.780172 | 0.726634 (0.529081) | 0.991379 (0.682493) | 0.271552 (0.186944) | 0.592742 | 4/9 |
| Split-default partial | 0.806034 | 0.729336 (0.541858) | 0.991561 (0.701493) | 0.265823 (0.188060) | 0.596774 | 4/9 |
| Actor two-stage partial | 0.780172 | 0.726634 (0.529081) | 0.991379 (0.682493) | 0.698276 (0.480712) | 0.092742 | 4/9 |
| Prior floor | 0.581395 | 0.694850 (0.383058) | 0.976744 (0.500000) | 0.279070 (0.142857) | 0.519231 | 4/9 |
| Task 5 plus deterministic actor span | 0.780172 | 0.726634 (0.529081) | 0.991379 (0.682493) | 0.780172 (0.537092) | 0.133065 | 5/9 |

The actor-span rule is strictly better than unconditional actor priors under
the adopted gate: its matched-pair actor exactness is 0.780172 against
0.735385. It does not solve atomicity, faithfulness, candidate-state macro F1,
or the hallucination proxy.

Option-2 disposition certification is applied to complete Search-fold lanes:
resolved-hold retained-value recall is 0.970711, intrinsic junk escapes are
zero, relational contamination is zero, and one relational escape is a
materialized canonical merge. The independent three-class candidate-state
macro F1 remains 0.863223 and still fails its 0.90 gate.

Rescore index SHA-256:
`9ca9fff218b1cc5faf8f2235f41308d0c0688d7aa332e2951e93fdd5770124c8`.
