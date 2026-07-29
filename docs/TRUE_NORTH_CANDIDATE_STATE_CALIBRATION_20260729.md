# True-North Candidate-State Calibration — 2026-07-29

## Result

Phase B is a zero-call calibration of the only previously uncalibrated gate.
It scores frozen gold pass A as predictions against frozen gold pass B using
the exact live three-class candidate-state metric:

- `retain` and `revise` map to `value`;
- `reject` maps to `junk`;
- `hold` maps to `hold`;
- macro F1 is calculated over `value`, `junk`, and `hold`.

| Reading | Result |
|---|---:|
| Development candidates | 1,140 |
| Exact live three-class macro F1 | **0.830664** |
| Existing gate | **0.900000** |
| Ceiling margin | 0.040000 |
| `min(target, ceiling - margin)` | **0.790664** |
| Raw four-disposition exact agreement | 0.807018 |
| Value-state exact agreement | 0.963158 |

Per-class results:

| Class | Precision | Recall | F1 | TP | FP | FN |
|---|---:|---:|---:|---:|---:|---:|
| Value | 0.984791 | 0.978281 | 0.981525 | 1,036 | 16 | 23 |
| Junk | 0.655738 | 0.740741 | 0.695652 | 40 | 21 | 14 |
| Hold | 0.814815 | 0.814815 | 0.814815 | 22 | 5 | 5 |

## Finding

The `0.90` gate is above the `0.830664` inter-annotator ceiling measured by
its own scorer. The current certified stack scores `0.863223`: it is below the
nominal gate but already above the measured A/B ceiling. This is therefore a
measurement-contract finding, not evidence that candidate-state extraction is
below the reproducible gold process.

Phase B does **not** change the gate. It recommends **Ruling 4**:
re-reference candidate-state macro F1 to `0.790664` under the established
ceiling-minus-four-point convention. The review loop must rule before any
manifest, scorer, or gate policy changes.

## Integrity

- Provider calls: `0`
- Sealed holdouts opened: `false`
- Production mutated: `false`
- Private output:
  `diagnostics/candidate-state-macro-f1-calibration-v1.json`
- Scoring implementation:
  `research_factory.true_north._macro_f1`
- Prediction/reference direction: pass A / pass B
- Scope: all development candidates before consensus filtering

The raw disposition and value-state figures remain diagnostics. Neither is a
substitute for the macro-F1 gate's exact class-sensitive scorer.
