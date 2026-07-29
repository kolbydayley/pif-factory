# True-North Gate Denominator Alignment (T6) — 2026-07-29

## Decision

**No gate was changed.** This adds a diagnostic that measures each semantic
gate's target against a ceiling computed on *the denominator that gate actually
scores on*. Adopting any threshold below is a measurement-contract change and
requires Kolby's explicit approval.

Zero paid model calls. Development partition only; sealed holdouts untouched.
No gold, consensus, manifest, or run artifact was modified.

## The defect

`_field_metric` (`research_factory/true_north_semantic_scoring.py:477,499`)
scores a field as:

```python
denominator = max(predicted_atom_count, gold_atom_count)
score = correct / denominator
```

Unmatched atomics — a decomposition difference — therefore count against a
*field* metric. But `compute_ceiling_document`
(`research_factory/true_north_gate_calibration.py`) built its speaker,
reported-actor, and faithfulness ceilings **only from
`scored["alignment"]` matched pairs**, then derived the recommended threshold as
`min(current_target, ceiling_mean − 0.04)`.

The two denominators are not the same measurement. The recalibration compared
the gate against an easier number than the gate applies, so an unpassable gate
survived the repair unchanged.

## Measured on the real development gold (n = 1,140 candidates, 1,300 matched pairs)

| Gate | Matched-pair ceiling | Ceiling on the gate's own denominator | Live target | Gold passes its own gate? | Recommended @ gate denominator |
|---|---:|---:|---:|---|---:|
| `speaker_exactness` | 0.9946 | **0.8917** | 0.9700 | **No** | 0.8517 |
| `reported_actor_exactness` | 0.7754 | **0.6847** | 0.9500 | **No** | 0.6447 |
| `claim_text_faithfulness_proxy` | 0.7844 | **0.7226** | 0.9000 | **No** | 0.6826 |

Speaker is the clearest case. Matched-pair agreement is 0.9946, so the
recalibration computed `min(0.97, 0.9946 − 0.04) = 0.97` and left the gate
exactly where it was. On the denominator the gate scores against, gold-vs-gold
reaches only 0.8917 — **5.8 points below the gate**. No extractor can pass it,
because the gold process does not pass it.

All three semantic gates sit above the agreement their own gold achieves.

## What this does and does not establish

- **Does:** these three gate readings certify nothing about the extractor. A
  failure against them is not evidence of an extraction defect.
- **Does not:** say the extractor is good. Atomic-count accuracy (78.02% vs
  ≥90%) is measured on a denominator that is not in dispute, and the junk and
  contamination gates are unaffected by this finding.
- **Does not:** propose that decomposition go unmeasured. The coupled
  denominator exists to stop a system from hiding missing atomics inside field
  scores. The correct split is: `acceptable_atomic_count_rate` carries
  decomposition, field gates carry attribution — measured on matched pairs and
  calibrated against a matched-pair ceiling. Coupling both into one number
  produces a metric neither gold nor extractor can satisfy.

## The decision this puts in front of Kolby

For each of the three gates, one of:

1. **Re-reference the threshold** to the ceiling on the gate's own denominator
   (the last column). Honest, and immediately usable.
2. **Decouple the metric**: score fields on matched pairs, let
   `acceptable_atomic_count_rate` carry decomposition alone. Under this,
   speaker measures 0.9752 on the Opus shadow column — above the 0.97 gate.
3. **Demote to diagnostic** and certify on the remaining gates.

Option 2 is the one that preserves the gates' original intent; it needs the
scorer change plus a re-score, not just a threshold edit.

## Reproducing

```python
from pathlib import Path
from research_factory import true_north, true_north_gate_calibration as cal

base = Path.home() / (
    "Library/Application Support/Podcast Intelligence Factory/"
    "true-north/ai-safety-v1/gold/development"
)
pass_a, pass_b = true_north._load_independent_atomic_gold(base)
doc = cal.compute_ceiling_document(pass_a, pass_b)
print(doc["gate_denominator_alignment"])
```

Tests: `tests/test_true_north_gate_calibration.py` (7 passed). Full true-north
regression: 96 passed across
`test_true_north{,_gate_calibration,_relational_merge,_prior_adoption,_task4c_amendment2a}.py`.

## Provenance

Independently predicted by the Opus shadow benchmark
(`docs/TRUE_NORTH_OPUS_SHADOW_20260728.md`, recommendation T6), which named
`_field_metric` and `compute_ceiling_document` as the mechanism before this
measurement existed. This document confirms it with the repository's own
scorer against the real gold rather than a shadow prediction.
