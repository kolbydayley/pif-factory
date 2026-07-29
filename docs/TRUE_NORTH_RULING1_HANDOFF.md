# Ruling 1 handoff — stood down, findings for the review loop

Date: 2026-07-29
Author: Claude orchestrator lane, to the Codex review loop (single driver).

## Stood down — no collision

I began implementing Ruling 1 (decouple field gates onto matched pairs) and
stopped on discovering the review lane already has it **in flight, uncommitted**:

- `research_factory/true_north_semantic_scoring.py` — +108 lines
- `research_factory/true_north_gate_calibration.py` — +16 lines
- `tests/test_true_north_semantic_scoring.py` — +24 / −2

Confirmed from the working tree: `_field_metric` now reports the matched-pair
score as `score` with the coupled value moved to `coupled_diagnostic`. That is
the ruling, correctly implemented.

Two exploratory test methods I had added to
`tests/test_true_north_semantic_scoring.py` were **removed again**; the file now
contains only the review lane's edits. No code file was left modified by this
lane. Nothing of the in-flight work was staged, committed, reverted, or stashed.

**Ruling 1 is yours. I will not touch the scorer, calibration module, or their
tests.**

## Accepted corrections

1. **Atomic-count ceiling.** `TRUE_NORTH_ATOMIC_CEILING_RECONCILIATION.md`
   (`f2d97b1`) is accepted in full. My comparator was exact A/B agreement
   against a range-acceptance gate — holding the gate to a stricter rule than it
   applies, the mirror of the defect T6 found in the field gates. Withdrawn in
   `TRUE_NORTH_GATE_DENOMINATOR_ALIGNMENT.md` (`039ce88`): the fairness claim,
   the "~6 unreachable points", and "further paid decomposition experiments
   cannot close this gate". Standing: the 0.90 gate is fair; 78.02%/80.60% is a
   real shortfall concentrated in compound candidates.
2. **Actor oracle bound.** The 85.8025% figure is computed on the coupled
   denominator, so it is evidence *for* Ruling 1, not an independent
   stop-condition. Corrected in `TRUE_NORTH_ACTOR_SPAN_RULE_RESULTS.md`.

## Two integration items for your re-score

### 1. The actor oracle bound must be recomputed on matched pairs

Once the decoupled scorer lands, `85.8025%` is stale and should not be cited.
Reproduce with:

```bash
PYTHONPATH=. python3 runs/actor-span-rule-20260729/oracle_bound.py
```

The bound's inputs that **survive** the change, because they are computed
directly against gold and not through any denominator:

- every gold non-null actor (440/440) is a literal span of its own evidence —
  100% reachable, no structural ceiling;
- the residual a paid value stage could win is **157 atomics**.

Only the 85.80% composed-ceiling line needs recomputing. If it clears 90.32%
under matched-pair scoring, the "don't fund a value stage yet" recommendation
should be revisited — it was conditioned on the coupled bound.

### 2. `gate_denominator_alignment` may need re-pointing

`research_factory/true_north_gate_calibration.py` (mine, `db2aa65`) added
`gate_denominator_alignment`, which reports each gate's target against a ceiling
on the **coupled** denominator. Its purpose was to prove the coupled gates were
unpassable — the evidence behind Ruling 1.

After decoupling, that block describes a denominator the gates no longer use. It
should either be re-pointed at matched-pair ceilings, or explicitly retained as
a historical diagnostic with a note saying so. Your +16 lines in that file may
already handle this; flagging it so it is a decision rather than an oversight.
`live_gate_changed: False` in that block is now potentially misleading, since
Ruling 1 does change the live gate.

## Status of this lane's committed work

| Commit | Content | Status |
|---|---|---|
| `db2aa65` | `gate_denominator_alignment` diagnostic | landed; see item 2 |
| `4d3fbe2` | T1 deterministic actor span rule | accepted by Kolby |
| `0588df8` | Actor oracle bound | corrected; see item 1 |
| `b57ae8e` | Full ceiling audit | atomic row withdrawn |
| `039ce88` | Corrections + rulings recorded | current |

Also open from the earlier wave, unrelated to Ruling 1 and untouched:
**Task 5 fix round 1** — two Important review findings on the stage-B harness
(`b9d1ac9`) remain unaddressed: the composition-parity rationale (adjudication
mode sources speaker/actor from stage-C attribution while the Task 1 floor uses
priors, so a stage-C regression would be misread as a stage-B failure), and the
duplicated prior contract across modules with no binding test. Details in
`.superpowers/sdd/2026-07-28-true-north-gate-closure-v2/progress.md`.
