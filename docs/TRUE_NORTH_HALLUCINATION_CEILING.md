# True-North Hallucination Gold-vs-Gold Calibration

Date: 2026-07-29

Status: zero-call measurement-contract finding; gate unchanged.

Pass A was scored as prediction against pass B as reference with the exact
current semantic scorer.

| Reading | Flagged candidates | Rate | Current gate |
|---|---:|---:|---:|
| Live hallucination proxy | 166 / 1,140 | 0.145614 | <= 0.020000 |
| Matched-pair-only diagnostic | 84 / 1,140 | 0.073684 | diagnostic |

The live proxy includes unmatched predicted claims because that is what the
current hard gate scores. The matched-only reading excludes those claims but
retains hallucination-severity flags on aligned pairs. Both readings exceed
2%.

The requested `min(current_target, observed gold-vs-gold rate)` framing returns
0.02, so it supplies no automatic threshold change. More importantly, the gold
process itself fails the current gate by 12.56 percentage points. This is a
measurement-contract finding for Kolby. The gate remains unchanged pending an
explicit owner ruling.

No provider call, holdout access, production mutation, or gate change occurred.

Result SHA-256:
`f9b99c77045e0ad5daa183badc49426a17fdc9510bc7d414298479fc4c877ed8`.

