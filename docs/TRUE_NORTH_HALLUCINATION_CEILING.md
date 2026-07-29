# True-North Hallucination Gold-vs-Gold Calibration

Date: 2026-07-29

Status: frozen zero-call measurement-contract finding; subsequently adopted by
Ruling 3.

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

The original `min(current_target, observed gold-vs-gold rate)` framing returned
0.02 and supplied no automatic threshold change. Ruling 3 subsequently adopted
the established lower-is-better convention: matched-pair gold-vs-gold rate
0.073684 plus a 0.02 margin, producing the final v7 gate of **0.093684**. The
live 0.145614 reading and original 0.02 aspiration remain diagnostics.

No provider call, holdout access, or production mutation occurred.

Result SHA-256:
`f9b99c77045e0ad5daa183badc49426a17fdc9510bc7d414298479fc4c877ed8`.
