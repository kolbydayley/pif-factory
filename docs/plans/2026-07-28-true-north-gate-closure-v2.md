# True-North Gate Closure v2 — Adjudicate-and-Repair Architecture

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. This plan supersedes Phases 2–3 of `2026-07-28-true-north-gate-closure.md`; Phase 1 of that plan (instrument v2) is complete and stays.

**Goal:** Pass all nine extraction gates by re-founding the workhorse task as *candidate adjudication with minimal repair* (adopt frozen upstream priors by default), and by recalibrating the two gates that are provably above their own gold's inter-annotator ceiling.

**Architecture:** Three moves. (1) A zero-model-call **prior-adoption simulation** that composes stored dispositions with the frozen candidates' own `speaker`/`actor`/`claim_text`/enum priors and scores the result — establishing each gate's floor before any spend. (2) A **measurement-contract repair** for the two gates the data proves impossible (lexical faithfulness ≥0.90 vs inter-gold ceiling 0.792; reported-actor ≥95% vs inter-gold agreement 66.2%), one via ceiling-referenced recalibration, one via a bounded gold field re-adjudication with a written definition. (3) A **narrowed GLM role**: disposition plus split/no-split minimal-edit decomposition anchored on candidate wording — the only decisions priors cannot make — with Spark escalation on detected conflicts.

**Tech Stack:** unchanged (`research_factory`, OpenCode transports, shadow SQLite, hash-bound packets).

## The measured facts this plan is built on (verified 2026-07-28, development gold, n=1,140 candidates / 1,458 gold atomics)

| Signal | Value | Consequence |
|---|---:|---|
| Gold `raw_speaker` == frozen candidate `speaker.name` | **99.4%** (1449/1458) | Speaker gate (≥97%) is passed by **deterministically adopting the prior**; no model call. Multipass withheld this prior and scored 58–63%. |
| Gold-A vs gold-B `raw_speaker` agreement | 99.4% | Confirms 99.4% is the instrument ceiling; adoption sits at it. |
| Gold-A vs gold-B lexical faithfulness (1v1 pairs, scorer's own proxy) | **mean 0.792, only 28.4% ≥0.90** | The ≥0.90 lexical gate is **above the ceiling of the gold process itself**. No extractor, including Codex, can pass it. It must be recalibrated. |
| Candidate `claim_text` verbatim vs final gold, same proxy | mean 0.700 | Anchoring output on candidate wording starts 30pp above the multipass rewriter (0.40) and near the 0.79 ceiling. |
| Gold-A vs gold-B `reported_actor` agreement | **66.2%** (344/520) | The ≥95% gate is uncertifiable against a field gold itself can't reproduce. The field conflates focal actor with reported-speech source and needs re-adjudication under a written definition. |
| Gold `reported_actor` ∈ candidate `{actor_name, reported_actor}` priors | 68.5% | Priors match final gold better than gold passes match each other — after field re-adjudication, prior + confirmation is a plausible ≥95% path. |
| Gold stance / time_horizon / certainty == candidate priors | 93.3% / 90.7% / 85.2% | Adopt-by-default with evidence-gated override; these feed divergence diagnostics, not hard gates. |
| Gold atomic-count distribution | 0:89, 1:734, 2:251, 3:49, ≥4:17 | 70% of value candidates are single-atom. Split/no-split is a binary classification with a strong prior, not free decomposition. Inter-gold count agreement post-consensus is 86.98% with min–max range acceptance. |
| Junk denominators (Search fold) | 9 strict junk, 1 escape = 11.1% | The junk gate is a **small-sample zero-tolerance** gate: ≤2% of 9 means zero escapes. Target the specific escape classes; also report the Wilson interval so a 0/9 pass isn't over-read. |

Root cause of the multipass failure, in one sentence: it re-derived from clipped evidence what the frozen pipeline had already answered with full episode context, and it was scored against two gates set above their own instrument ceiling.

## Global Constraints

All constraints from `2026-07-28-true-north-gate-closure.md` carry over verbatim (shadow-only writes, sealed holdouts stay sealed, evidence harness-bound, no answer keys in model inputs, budget ledger, focused regression suite before each commit). Additions:

- Gate-threshold and gold-field changes are **measurement-contract changes**: they require Kolby's explicit approval at the Task 3 checkpoint before adoption, and the manifest records old threshold, new threshold, and the ceiling evidence justifying it.
- Candidate priors (`speaker`, `actor_name`, `reported_actor`, `stance`, `time_horizon`, `certainty`, `claim_text`) are legitimate model inputs — they are frozen upstream hypotheses produced without access to gold, already present in the hash-pinned bundles. They are not answer keys. Gold-derived data remains forbidden in packets.
- Before any new code, commit the existing untracked Phase-1/Phase-2 work: `git add` the instrument-v2 scorer changes, multipass modules, tests, and the three result docs as at least two commits (scorer/instrument, multipass harness) so this plan's diffs are reviewable. Do not include `~/Library/Application Support` artifacts.

---

# Phase A — Zero-cost floor: prior-adoption simulation

### Task 1: Build the prior-adoption composer and score it offline

**Files:**
- Create: `research_factory/true_north_prior_adoption.py`
- Test: `tests/test_true_north_prior_adoption.py`
- Create: `docs/TRUE_NORTH_PRIOR_FLOOR.md` (results)

**Interfaces:**
- Produces: `compose_prior_adoption(bundle, dispositions) -> list[dict]` returning canonical `canonical_atomic_output_v1` rows where, for every candidate the disposition source retained/revised: `claim_text` = candidate `claim_text` verbatim; `raw_speaker` = candidate `speaker.name`; `reported_actor` = candidate `reported_actor.name` or `actor_name`; `stance`/`time_horizon`/`certainty`/`polarity` = candidate priors (enum-normalized); one atomic claim per candidate; evidence harness-bound as usual.
- Disposition source: the stored best single-pass stack outputs (`stack-03-all-compatible`, campaign `tnio-20260728-combined-stack-v1`) — dispositions there hit 97.66% F1. No model calls anywhere in this task.

- [ ] **Step 1: Write failing tests** — composer emits one atom per value candidate with prior fields verbatim; enum priors normalize through `normalize_enum_field`; reject/hold candidates emit disposition-only rows; structured `speaker`/`reported_actor` dicts flatten to `.name`.
- [ ] **Step 2: Run tests, verify fail; implement; verify pass.**
- [ ] **Step 3: Score the composed output with instrument v2** on both Search episodes and the 60-candidate comparison cohort. Zero paid calls.
- [ ] **Step 4: Write `docs/TRUE_NORTH_PRIOR_FLOOR.md`** — the nine-gate table for the pure-prior floor next to single-pass and multipass columns. Expected (to be confirmed by the run): speaker ≈99% PASS, actor ≈68% (pending Phase B), faithfulness ≈0.70, atomic-count accuracy = fraction of candidates whose acceptable consensus range includes 1.
- [ ] **Step 5: Commit** `git commit -m "feat(true-north): prior-adoption composer and zero-call floor measurement"`

This task is the certainty instrument for everything after it: every later intervention must beat this floor on the same cohort or it is rejected.

# Phase B — Measurement-contract repair (requires Kolby approval at Task 3)

### Task 2: Ceiling-referenced faithfulness gate

**Files:**
- Modify: `research_factory/true_north_semantic_scoring.py`, gate wiring in `research_factory/true_north.py`
- Create: `research_factory/true_north_gate_calibration.py` (computes and stores inter-gold ceilings per metric from pass-A/pass-B artifacts)
- Test: `tests/test_true_north_gate_calibration.py`

**Interfaces:**
- Produces: `compute_interannotator_ceilings(suite) -> dict` writing `diagnostics/gate-calibration.json` with, per metric: A-vs-B mean, median, fraction ≥ current threshold, and the derived recommended threshold `min(current_target, ceiling_mean − 0.04)`. Faithfulness gate becomes: **mean lexical faithfulness ≥ (inter-gold mean − 0.04) = 0.75** AND hallucination rate ≤2% (the existing severity-split gate, which is the actual anti-fabrication protection). The 0.90 aspiration stays visible as a diagnostic.
- Rationale recorded in the file: a gate above the instrument ceiling certifies nothing; 0.75 + zero-tolerance hallucination is strictly more informative than an unpassable 0.90.

- [ ] **Step 1: Write failing tests** with synthetic pass-A/pass-B fixtures (known mean 0.80 → recommended 0.76; ceiling above target → target unchanged).
- [ ] **Step 2: Implement; run tests; verify pass.**
- [ ] **Step 3: Generate `diagnostics/gate-calibration.json` from the real artifacts. Do not switch the live gate yet — that flips at the Task 3 checkpoint.**
- [ ] **Step 4: Commit** `git commit -m "feat(true-north): inter-annotator ceiling calibration for semantic gates"`

### Task 3: Reported-actor field re-adjudication + approval checkpoint

**Files:**
- Modify: `research_factory/true_north.py` (a new bounded gold phase `actor-repair`)
- Create: written field definition in `docs/TRUE_NORTH_ACTOR_CONTRACT.md`

**Interfaces:**
- The written definition (use verbatim in the adjudication packet and all future prompts): *"`reported_actor` is the focal actor: the named person, organization, or collective whose action, decision, state, or outcome the claim describes, when that actor is explicitly named in the evidence and is not the direct speaker speaking in their own voice about themselves. It is not restricted to sources of reported speech. If no such actor is explicitly named, the field is null."*
- Adjudication scope: Spark/Codex lane, development partition only, exactly the `reported_actor` field of existing consensus atomics — every other gold field is read-only. Output is a new consensus revision with provenance; the old field values remain in history. Budget: ≤60 calls (batched packets), declared in the ledger.
- After re-adjudication, recompute A/B/C-derived agreement for the repaired field; the gate binds to the repaired field at `min(95%, repaired_ceiling − 2pp)`.

- [ ] **Step 1: Implement the `actor-repair` gold phase** reusing the pass-C adjudication machinery (packet shape, receipts, compile step). Tests: scope enforcement (only `reported_actor` may differ post-compile), development-only, budget declared.
- [ ] **Step 2: Run the re-adjudication; recompute field agreement; record the repaired ceiling.**
- [ ] **Step 3: CHECKPOINT — present to Kolby:** the prior-floor table (Task 1), the calibration file (Task 2), the repaired actor ceiling, and the exact proposed gate changes (faithfulness 0.90→0.75+hallucination≤2%; reported-actor 95%→repaired-ceiling-referenced). **No gate switches and no Phase C model calls before explicit approval.**
- [ ] **Step 4: On approval, flip gate wiring, bump the manifest's scorer/gate hash with the recorded justification, re-score the Task 1 floor under final gates, and commit.**

# Phase C — Narrowed GLM role: only the decisions priors cannot make

The prior floor decides this phase's scope. GLM is needed only where the floor fails. Expected residual work: disposition (junk zero-tolerance), split/no-split + minimal-edit decomposition, and actor confirmation. Speaker needs **no model** (prior adoption at 99.4%).

### Task 4: Disposition pass, junk-class hardened

**Files:**
- Modify: `research_factory/true_north.py` (stage-A prompt revision only; harness unchanged)

**Interfaces:**
- Keep the stage-A disposition-only call and schema from multipass (mechanically proven: 100% parse, 91.6% recall). Two evidence-driven prompt changes: (a) the false-reject classes were `question_or_setup` 9, `fragment` 7, `repetition` 2 — add: *"A question, fragment, or repeated construction is junk only when the evidence provides no recoverable asserted proposition; a rhetorical or self-answered question, a fragment whose assertion is completed within its own evidence, a repeated but substantive assertion, and an explicit forecast or skeptical stance are all value."* (b) the single junk escape class gets the mirror-side instruction retained from v1.
- Acceptance on Search fold: junk escapes 0/9 AND false rejects ≤10 (recall ≥95%).

- [ ] **Step 1: Update prompt constant; adjust prompt-hash tests.**
- [ ] **Step 2: Run stage A on the two Search episodes (≤25 calls, declared); score; verify acceptance; one further single-sentence iteration permitted if a named class still dominates.**
- [ ] **Step 3: Commit.**

### Task 5: Split/no-split minimal-edit decomposition

**Files:**
- Modify: `research_factory/true_north.py` (replace stage-B contract)
- Test: `tests/test_true_north.py`

**Interfaces:**
- New stage-B output per value candidate: `{"candidate_id", "split": bool, "atomic_claims": [{"claim_text"}], "edit_reason": "none"|"pronoun_expansion"|"attribution_embed"|"compound_split"|"qualifier_repair"}` with the packet carrying the candidate's `claim_text` prominently and this contract:
- Stage-B system prompt (final text, verbatim):

```
You are a claim adjudication editor for a private podcast research corpus. Do
not use tools. Each candidate arrives with a proposed claim_text hypothesis
and its exact evidence. Your default action is to adopt the proposed
claim_text verbatim as one atomic claim; most candidates need exactly this.
Depart from the proposal only for a concrete, evidence-grounded reason. Split
into multiple claims only when the proposal asserts two or more conclusions
that could independently be true or false — separate list items, separate
forecasts, a separately asserted cause and effect, or distinct stances; when
you split, reuse the proposal's own wording for each part, changing only what
grammar requires. Edit wording only to expand an unclear pronoun to its
explicit referent from the evidence, to repair a qualifier the proposal
dropped or added relative to the evidence, or to remove content the evidence
does not state. Never introduce vocabulary absent from both the proposal and
the evidence, and never compress or restyle wording that is already accurate.
Report the edit reason for every claim. Return only the exact schema-valid
JSON requested by the packet.
```

- Acceptance on Search fold vs the Task 1 floor: atomic-count accuracy ≥90%, faithfulness ≥ recalibrated gate, hallucination ≤2%, and **no gate below the prior floor**.

- [ ] **Step 1: Write failing schema/validator tests (`edit_reason` closed set; `split=false` requires exactly one claim; adopted-verbatim claims must byte-match the proposal when `edit_reason=none`).**
- [ ] **Step 2: Implement; tests pass.**
- [ ] **Step 3: Run on Search fold (≤25 calls, declared); score; verify acceptance.**
- [ ] **Step 4: Commit.**

### Task 6: Actor confirmation pass (only if the Phase B repaired-field floor fails its gate)

**Interfaces:**
- Skip entirely if prior adoption of the repaired field passes at the Task 3 recomputation — measure first.
- If needed: per-claim closed choice `{"candidate_id", "claim_index", "actor_decision": "adopt_prior"|"replace"|"null_field", "replacement_freetext": str|null}` under `docs/TRUE_NORTH_ACTOR_CONTRACT.md`'s definition, with the prior displayed. Escalate `replace` decisions to Spark for confirmation (both models must agree, else prior stands). Acceptance: repaired-field exactness ≥ gate on Search fold.

- [ ] **Step 1: Measure the repaired-field prior floor; decide skip/build.**
- [ ] **Step 2 (conditional): tests → implement → bounded run (≤25 calls) → score → commit.**

### Task 7: Certification

- [ ] **Step 1: Freeze configuration (composition rules, prompts, gates, hashes).**
- [ ] **Step 2: Two identical-config development passes; both must pass all nine (post-approval) gates.**
- [ ] **Step 3: Open sealed transfers once under the parent gate. Failure returns to Kolby with the falloff ledger; no post-inspection edits.**
- [ ] **Step 4: Final certification report with cost accounting vs all-Codex baseline; commit.**

## Budget

Declared ceilings for the whole plan: ≤160 paid calls, ≤2.5M tokens, ≤5h provider wall time (Phase A: 0; Phase B: ≤60 Spark; Phase C: ≤75 GLM + ≤25 Spark escalation/confirmation). Enforced through the existing reservation ledger.

## Assumptions

- Stored `stack-03-all-compatible` dispositions are reusable for the Task 1 simulation (hash-bound outputs on disk).
- Candidate `speaker`/`reported_actor` structured priors are populated across all five suite episodes at rates comparable to the development measurement (spot-check per episode in Task 1).
- Gold field re-adjudication is procedurally allowed as a new consensus revision with provenance (the benchmark forbids silent rewrites, not versioned corrections); if Kolby rules otherwise at the checkpoint, the fallback is demoting reported-actor to a diagnostic and certifying on the remaining eight gates.
- The utility gate (22/25 research questions) is unaffected by these changes; it re-runs in Task 7 certification as-is.
