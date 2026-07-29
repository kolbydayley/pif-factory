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

### Task 4b (AMENDMENT 2026-07-28): Asymmetric junk-verification stage

Task 4 failed its frozen acceptance after its permitted iteration (best GLM: 1 escape / 12 false rejects / 94.98% recall; corrected GLM×2+Spark-tiebreak ensemble: 3 escapes / 6 false rejects / 97.49% recall — see `docs/TRUE_NORTH_PHASE_C_DISPOSITION_RESULTS.md`). Root cause, verified against run artifacts: two escaped junk items (`dev_c094b91406c9222943a29eba` non_useful_repetition, `dev_d7f6bd87ab720be875111f97` truncated fragment) were retained by **both** GLM runs, so no tiebreak composition can ever reject them, and they belong to precisely the classes Task 4's prompt softened to protect recall. Recall and junk-precision cannot share one prompt. This amendment keeps the zero-junk gate and authorizes one precision-tilted verification stage over retained items. Prompt iterations on Task 4's disposition prompt remain exhausted — the disposition stage is frozen as the corrected ensemble composition (result SHA `f1f5ebda…a06fcd`).

**Files:**
- Modify: `research_factory/true_north.py` (add `junk-verify` stage after disposition composition)
- Test: `tests/test_true_north.py`

**Interfaces:**
- **Deterministic screen** selects which retained candidates get verified (no model involvement in scoping). A retained candidate is screened in when any of: (a) either GLM disposition run or Spark rejected it (value-state disagreement anywhere in the ensemble); (b) repetition screen — token-Jaccard of its `claim_text`+`evidence_text` against any earlier candidate in the same episode ≥0.6; (c) fragment screen — `evidence_text` does not end in terminal punctuation, or is under 120 characters; (d) bare-mention/question screen — `claim_text` contains no finite verb outside a name/title, or evidence is interrogative without a declarative continuation. Thresholds are frozen constants; record screened-in count per class in the run report. Expected scope: ~20–40 of ~230 retained candidates.
- **Verifier call** (GLM, batched screened candidates, closed schema): `{"candidate_id", "verdict": "confirm_retain"|"reject", "junk_reason": <same closed set as stage A>|null, "deficiency_quote": str|null}` where `reject` requires both `junk_reason` and a `deficiency_quote` copied verbatim from the evidence.
- **Asymmetric flip rule** (deterministic composition): a retained candidate flips to reject only when the verifier rejects AND at least one corroborator agrees — either (i) some ensemble member had already rejected it, or (ii) the verifier's `junk_reason` matches the deterministic screen class that selected the candidate (e.g. verifier says `repetition` on a repetition-screened item). A verifier reject with no corroboration escalates that single candidate to Spark with the same verifier contract; Spark's verdict then decides. Unanimity-retained, unscreened candidates are never touched.
- Verifier system prompt (final text, verbatim):

```
You are a junk auditor for a private podcast research corpus. Do not use
tools. Every candidate you receive has been provisionally retained, and most
are genuinely valuable; your task is to catch the rare junk that slipped
through. Reject a candidate only when you can quote a concrete deficiency
from its exact evidence: the evidence merely repeats an assertion already
made elsewhere without adding new content; the evidence is a truncated
fragment whose assertion cannot be completed from the text present; the
evidence only names a person, product, or document without asserting anything
about it; the evidence is an unanswered question or setup with no recoverable
assertion; or the evidence is page chrome, navigation, or metadata. If the
evidence contains any complete, substantive asserted proposition a researcher
could verify, compare, contradict, or qualify, confirm the retention even
when the candidate is also partly repetitive or fragmentary. For each reject,
name the junk class and copy the deficient text verbatim as the deficiency
quote. Return only the exact schema-valid JSON requested by the packet.
```

- Budget for this amendment: ≤8 GLM calls + ≤3 Spark single-candidate escalations, ≤120k tokens, declared in the ledger before the first call.
- Acceptance on the Search fold (frozen composed pipeline = ensemble disposition + junk-verify flips): junk escapes 0/9, false rejects ≤10, retained-value recall ≥0.95. One screen-threshold adjustment (constants only, no prompt change) is permitted if a screened-out junk item escapes; a second failure stops Phase C and returns to Kolby with the falloff ledger.

- [ ] **Step 1: Write failing tests** — screen classes select the two known both-run escapes from stored v1/v2 outputs; flip rule refuses an uncorroborated verifier reject without Spark; `reject` without `deficiency_quote` fails validation; unscreened candidates cannot be flipped.
- [ ] **Step 2: Implement screen, verifier stage, and composition; tests pass.**
- [ ] **Step 3: Dry-run the screen offline** against stored v1/v2 outputs and report per-class screened-in counts — the two known escapes MUST be screened in before any paid call; if not, fix screen constants first.
- [ ] **Step 4: Run the verifier on the Search fold; compose; score against the frozen acceptance rule.**
- [ ] **Step 5: Write results appendix into `docs/TRUE_NORTH_PHASE_C_DISPOSITION_RESULTS.md`; commit** `git commit -m "feat(true-north): asymmetric junk-verification stage for Phase C disposition"`

### Task 4c (AMENDMENT 2 — 2026-07-28): Marginal-utility verification with neighbor and segment context

Task 4b failed at its stop condition: all three residual escapes were screened in, and the verifier confirmed each (3/9 escapes, 8 false rejects, 96.65% recall — see the Task 4b appendix in `docs/TRUE_NORTH_PHASE_C_DISPOSITION_RESULTS.md`). Root cause, verified against the bundles and gold: the residual junk is **relational, not intrinsic**. "Nilay summarizes Fable as Mythos with security guardrails" is a substantive sentence in isolation; it is junk only because the same proposition is already carried elsewhere in the retained set (gold reason codes: `non_useful_repetition`, `non_useful_repetition_of_model_definition`, `nonasserted_question_frame` — the question's proposition exists verbatim in a retained candidate at 0.41 claim-token Jaccard). A verifier shown one candidate alone answered the wrong question correctly. Task 4b's intrinsic-junk contract is retired; this task replaces the packet contents and the question being asked. No further iterations of the 4b design are authorized.

**Files:**
- Modify: `research_factory/true_north.py` (`junk-verify` stage: packet builder + contract v2)
- Test: `tests/test_true_north.py`

**Interfaces:**
- **Scope** (deterministic, tightened — the 4b fragment screen at 157/243 was uninformative and is retired): a retained candidate is screened in when any of: (a) repetition — token-Jaccard of its `claim_text ∪ evidence_text` against another retained candidate's `claim_text ∪ evidence_text` ≥0.45 (pairs recorded); (b) chrome/bare-mention — `evidence_text` contains bracketed link text (`[...]`), or `claim_text`'s main verb is a reference verb (references, mentions, cites, names, points to) with no further predicate; (c) pure-interrogative — `evidence_text` consists only of interrogative sentences; (d) dangling fragment — no terminal punctuation AND begins lowercase AND under 160 characters; (e) any ensemble value-state disagreement. Constants frozen; per-class counts reported. Expected scope ≈30–50.
- **Packet per screened candidate** (all drawn from the same frozen bundle — no new sources): the candidate; the full text of its segment cropped ±400 characters around the evidence span (original offsets preserved); and its top-3 nearest **retained** neighbors by the same token-Jaccard, each with `candidate_id`, `claim_text`, `evidence_text`. For repetition-screened items the matched pair is always included.
- **Verifier contract v2 output**: `{"candidate_id", "verdict": "confirm_retain"|"reject", "junk_reason": <stage-A closed set>|null, "duplicate_of": <candidate_id>|null, "deficiency_quote": str|null}` — a `repetition` reject must name `duplicate_of` from the shown neighbors; a fragment/question/bare-mention reject must include the verbatim `deficiency_quote`.
- **Verifier system prompt v2 (final text, verbatim):**

```
You are a marginal-utility auditor for a private podcast research corpus. Do
not use tools. Each packet shows one provisionally retained candidate, the
surrounding transcript text of its segment, and the most similar already
retained candidates. Most inputs are valuable; your task is to catch the rare
candidate that adds nothing to the corpus. Reject a candidate only in these
cases. Repetition: every proposition in its evidence is already carried by
one of the shown retained neighbors, in the same or different words, and the
candidate adds no new subject, predicate, outcome, qualifier, or attribution;
name which neighbor carries it. Non-assertion: read the surrounding segment
text and confirm the evidence is a question, setup, or fragment whose
recoverable content is either absent or already carried by a shown neighbor.
Bare reference: the evidence only points to a document, product, page
element, or name without asserting any verifiable proposition about it. If
the candidate contributes any independently citable proposition that no shown
neighbor carries, confirm the retention, even when partly repetitive,
interrogative, or fragmentary. Quote the deficiency verbatim for every
non-repetition reject. Return only the exact schema-valid JSON requested by
the packet.
```

- **Composition rule** (asymmetry preserved, one addition): flips still require corroboration (prior ensemble reject, screen-class match, or a named `duplicate_of` whose pair similarity ≥0.45). New: a `confirm_retain` verdict on a repetition-screened candidate whose pair similarity is ≥0.60 gets one Spark second opinion with the identical packet; junk-reject requires the two models to agree, else retain stands. Unscreened unanimous retains remain untouchable.
- **Route:** GLM batched verification (≤6 calls); Spark only for the high-similarity confirm double-checks and uncorroborated-reject escalations (≤4 calls). Budget ≤10 calls / ≤150k tokens, declared before the first call.
- **Acceptance** (unchanged): 0/9 escapes, ≤10 false rejects, recall ≥0.95 on the Search fold with the frozen ensemble + 4c flips.
- **Hard stop:** one run, no prompt or constant iteration. On failure, proceed directly to the Task 4d decision checkpoint — do not retry.

- [ ] **Step 1: Write failing tests** — screen selects all five known development junk candidates from stored outputs (`dev_c094…`, `dev_d7f6…`, `dev_ab97…`, `dev_6184…`, `dev_fcec…`) without referencing gold in runtime code (test-only fixture IDs); packet builder includes segment crop with correct offsets and the matched repetition pair; `repetition` reject without `duplicate_of` fails validation; high-similarity confirm triggers the Spark double-check.
- [ ] **Step 2: Implement; tests pass; commit code before the paid run.**
- [ ] **Step 3: Offline dry-run of screen + packet builder; verify the five known junk items are screened in and their packets contain the relevant neighbor or segment context.**
- [ ] **Step 4: Execute the bounded run; compose; score.**
- [ ] **Step 5: Append results; commit** `git commit -m "feat(true-north): marginal-utility junk verification with neighbor context"`

### Task 4d (decision checkpoint if 4c fails): gate-semantics ruling

If 4c fails, the remaining escapes are items two independent Codex gold passes both rejected but which survive neighbor-context adjudication by two different models. That is no longer an extraction defect; it is a disagreement about where duplicate suppression belongs. Present Kolby one decision with two options:

1. **Keep the 0/9 disposition gate as-is** and accept that the extraction stack cannot certify; the campaign ends with the falloff ledger (status quo, no further spend).
2. **Move relational junk downstream**: reclassify `non_useful_repetition*` and `nonasserted_question_frame` escapes as acceptable at disposition **if and only if** the downstream canonicalization stage merges them into their duplicate's canonical group (the ledger already defines `merged_duplicate_retained`). The disposition gate then binds to intrinsic junk only (chrome, bare mention, fragment); a new certification-time check asserts every relational escape was actually merged, so corpus contamination stays zero. This is a measurement-contract change: record it in the manifest with this evidence trail if approved.

No implementation happens in 4d without Kolby's explicit choice in the current conversation.

### Task 5: Split/no-split minimal-edit decomposition

Precondition (amended): Task 4b acceptance passed — superseded: Task 4c acceptance passed, or Task 4d option 2 approved and implemented.

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

Amendment accounting (2026-07-28): Phase C has consumed 43 calls / 311,822 tokens through Task 4, plus 8 calls / 68,324 tokens in Task 4b (51 calls total). Task 4c adds ≤10 calls / ≤150k tokens inside the Phase C ceiling; the remaining envelope after Task 4c is reserved for Tasks 5–7. Task 4d spends nothing.

## Assumptions

- Stored `stack-03-all-compatible` dispositions are reusable for the Task 1 simulation (hash-bound outputs on disk).
- Candidate `speaker`/`reported_actor` structured priors are populated across all five suite episodes at rates comparable to the development measurement (spot-check per episode in Task 1).
- Gold field re-adjudication is procedurally allowed as a new consensus revision with provenance (the benchmark forbids silent rewrites, not versioned corrections); if Kolby rules otherwise at the checkpoint, the fallback is demoting reported-actor to a diagnostic and certifying on the remaining eight gates.
- The utility gate (22/25 research questions) is unaffected by these changes; it re-runs in Task 7 certification as-is.
