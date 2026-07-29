# True North Gate Closure v2 — Approval Checkpoint

Date: 2026-07-28

Status: **Awaiting explicit approval**

No live gate, canonical gold artifact, production database, release ledger, or
holdout artifact has been changed.

## Phase A: zero-call prior floor

The pure-prior adapter used the stored `stack-03-all-compatible` dispositions
and frozen candidate fields. It made zero model calls and passed 3 of 9 gates
on the exact 60-candidate comparison cohort.

| Gate | Current target | Prior floor | Pass |
|---|---:|---:|:---:|
| Candidate-state macro F1 | ≥0.90 | 0.9766 | Yes |
| Retained-value recall | ≥0.90 | 1.0000 | Yes |
| Junk escape | ≤0.02 | 0.1111 | No |
| Atomic-count accuracy | ≥0.90 | 0.5814 | No |
| Claim-text faithfulness | ≥0.90 | 0.3968 | No |
| Speaker exactness | ≥0.97 | 0.5000 | No |
| Reported-actor exactness | ≥0.95 | 0.2500 | No |
| Hallucination rate | ≤0.02 | 0.2115 | No |
| Schema success | ≥0.99 | 1.0000 | Yes |

The floor confirms that disposition/value retention is nearly solved, but
candidate priors alone cannot supply decomposition or actor truth.

## Phase B1: faithfulness calibration

Calibration used all 1,140 development candidates from independent gold passes
A and B, including 1,300 matched atomic pairs.

| Measurement | Result |
|---|---:|
| Matched-atomic lexical ceiling mean | 0.784435 |
| Median | 0.801834 |
| Fraction at current 0.90 target | 0.281538 |
| Formula result: `ceiling mean - 0.04` | 0.744435 |
| Count-coupled campaign diagnostic | 0.674041 |

The proposed live threshold is a conservative round-up to **0.75**, paired
with the unchanged hallucination gate of **≤0.02**. The 0.90 aspiration remains
visible as a diagnostic.

This separates two already-independent requirements:

- lexical faithfulness on matched atomics; and
- completeness/decomposition through the ≥0.90 atomic-count gate.

## Phase B2: reported-actor repair

The actor contract was independently applied in clean pass A and pass B, with
pass C resolving disagreements. Only `reported_actor` could be emitted or
changed.

| Measurement | Result |
|---|---:|
| Existing adjudicated atomics | 1,458 |
| Pass-A packets | 20 |
| Pass-B packets | 20 |
| Pass-C packets | 5 |
| Actual calls / hard ceiling | 45 / 60 |
| A/B agreements after contract enforcement | 1,346 |
| A/B disagreements | 112 |
| Repaired agreement ceiling | 0.923182 |
| Formula result: `min(0.95, ceiling - 0.02)` | **0.903182** |
| Final non-null actors | 440 |
| Final actor equals direct speaker | 0 |
| Final actor absent from exact evidence | 0 |
| Atomics whose actor value changed from old gold | 773 |
| Old non-null actor changed to null | 661 |

An invariant audit caught that the initial model outputs often copied actor
names embedded in the adjudicated claim even when the exact evidence used only
a pronoun. A symmetric deterministic rule now converts any non-null actor that
is not a case-insensitive literal evidence span—or that equals the direct
speaker—to null before A/B agreement and C compilation. The unenforced draft is
retained as history but explicitly superseded.

The proposed actor-only gold revision changes no other atomic field. It remains
under `gold/development/actor-repair/final-span-enforced/`; canonical
`gold/development/final/` is untouched.

## Exact proposed gate changes

| Gate | Current | Proposed |
|---|---:|---:|
| Claim-text faithfulness | ≥0.90 | **≥0.75** |
| Hallucination rate | ≤0.02 | **unchanged ≤0.02** |
| Reported-actor exactness | ≥0.95 | **≥0.903182** |

Every other gate remains unchanged. Approval would authorize only:

1. switching these two live thresholds;
2. promoting the span-enforced actor-only gold revision while preserving its
   predecessor;
3. bumping the scorer/gate and gold hashes with this justification;
4. re-scoring the zero-call floor under the final gates; and
5. beginning Phase C's bounded model calls.

Without approval, none of those actions will occur.

## Reproducibility

- Calibration:
  `~/Library/Application Support/Podcast Intelligence Factory/true-north/ai-safety-v1/diagnostics/gate-calibration.json`
- Actor result:
  `~/Library/Application Support/Podcast Intelligence Factory/true-north/ai-safety-v1/gold/development/actor-repair/result-span-enforced.json`
- Actor result SHA-256:
  `83f77773dcdfa930362fd23b80111a63bb40f076a78032c9e23737d58969e6ae`
- Proposed actor gold SHA-256:
  `76ae641e829e09a2e8d1068748fb36b47d96837421437d248272ed8116d2bc2a`
- Focused regression suite: 111 passed.
