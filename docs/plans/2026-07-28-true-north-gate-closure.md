# True-North Gate Closure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Take the GLM 5.2 extraction stage from 3/9 to 9/9 hard-gate passes on the ai-safety-v1 true-north benchmark, without regressing cost below the amortization target.

**Architecture:** Three coordinated interventions, ordered by certainty of payoff: (1) repair the measurement instrument (gold reliability + scorer proxies) so the gates measure model behavior instead of surface-form lottery; (2) restructure the GLM call from one coupled semantic response into three bounded passes (disposition → decomposition → attribution) with closed-set output vocabularies wherever a gate demands exact match; (3) add a bounded Spark/Codex escalation lane for the residual hard tail so worst-case quality is bounded by the strong model while GLM keeps the volume.

**Tech Stack:** Python 3 (`research_factory` package), OpenCode CLI transports (`zai-coding-plan/glm-5.2`, `openai/gpt-5.3-codex-spark`), SQLite shadow store, existing hash-bound packet/receipt machinery in `research_factory/true_north.py` and `research_factory/true_north_input_optimization.py`.

## Why this reaches the gates (read before implementing)

Current failing gates and their true causes, from the 2026-07-28 diagnostics:

| Gate | Current | Target | Dominant cause |
|---|---:|---:|---|
| Junk escape | 11.1% | ≤2% | Real model behavior **plus** unstable gold (reject-set Jaccard between independent gold passes is 0.533 — the instrument itself disagrees with itself more than the gate allows) |
| Atomic accuracy | 65.1% | ≥90% | GLM under-extracts (60 atoms vs 83 gold) because decomposition competes with disposition and attribution in one response |
| Claim faithfulness | 53.8% | ≥90% | Lexical token-overlap proxy penalizes valid paraphrase; not hallucination (only 2 of 464 flags are unmatched claims) |
| Speaker accuracy | 64.7% | ≥97% | `_exact` string match on **free-text** speaker output; model emits valid aliases that fail exact comparison |
| Reported actor | 38.8% | ≥95% | Same exact-match-on-free-text problem, worse because actor mentions are more variable |
| Unsupported claims | 84.6% | 0 flagged | `unsupported_field_flags` fires on any exact-field mismatch vs one gold surface form (`predicted_field_reference_mismatch_proxy` in `true_north_semantic_scoring.py:383-395`); this is a paraphrase detector, not a hallucination detector |

Evidence for the three-pass split: the combined-stack campaign (`docs/TRUE_NORTH_INTERACTION_RESULTS.md`) showed disposition metrics reach 0.977 F1 / 100% recall / 11.1% junk in single-pass, but atomicity and identity **regress** when disposition improves — the gains do not compose in one response. Its conclusion ("separate candidate disposition, atomic decomposition, and identity resolution into bounded passes") is this plan's Phase 2.

Certainty argument: Phase 1 gains are deterministic (scorer/gold fixes move measured numbers without any model change). Phase 2 converts exact-match gates into closed-set selection problems, structurally eliminating the alias/paraphrase error class. Phase 3 bounds the residual: any candidate the cheap lane can't decide confidently is decided by Spark/Codex, so the composite system's floor is the strong model's quality on ≤15% of items — which already passes these gates (Codex is the gold author).

## Global Constraints

- Never write to the production factory database; all work targets the suite shadow store under `~/Library/Application Support/Podcast Intelligence Factory/true-north/ai-safety-v1/`.
- Sealed transfer holdout episodes stay sealed until two identical-config development passes clear all gates (`docs/TRUE_NORTH_BENCHMARK.md` gate behavior).
- The two lockbox episodes (`ep_8a0c6919d7cfe6bcd9cacd30`, `ep_e7630540911fc5dea9850204`) may each be opened exactly once; if already consumed by the prior campaign, verify state before relying on them.
- Evidence text/offsets are harness-bound, never model-generated. Candidate IDs, segment identity, and parent lineage must survive every pass.
- Development tuning uses only the three opened development episodes (`ep_90c3b5c995bce501c9aef55c`, `ep_7ec9f808a3955c720aeb94ff`, `ep_903cc763f0f106d7f4610f17`).
- No answer key, scorer feedback, or sealed material in any model input.
- Budget ceilings for new model-call campaigns: declare and enforce per-campaign call/token/time ceilings through the existing cross-process reservation ledger before the first call.
- Model routing: GLM 5.2 (`zai-coding-plan/glm-5.2`) is the volume lane; Spark (`openai/gpt-5.3-codex-spark`) is escalation/adjudication; fallback only on transport-level operational failure, never on semantic disagreement.
- Every new experiment writes hash-bound packets, receipts, normalized outputs, and score records exactly like `run_system_prompt_arm` (`research_factory/true_north.py:6765`).
- All commits go on the current working branch of `/Users/kolbydayley/Documents/Codex/podcast-intelligence-factory`; run the focused regression suite (`python3 -m pytest tests/test_true_north.py tests/test_true_north_semantic_scoring.py tests/test_true_north_input_optimization.py -q`) before each commit.

---

# Phase 1 — Repair the measurement instrument

Nothing else can be trusted until the gates measure what they claim to measure. This phase involves **zero GLM changes** and is expected to move faithfulness, speaker, reported-actor, and unsupported dramatically on re-score of existing outputs.

### Task 1: Closed-set speaker/actor canonicalization in the scorer

**Files:**
- Modify: `research_factory/true_north_semantic_scoring.py` (add canonicalization helpers near `_exact` at line 126; use them in `_align` at line 342 and `_field_metric` consumers)
- Test: `tests/test_true_north_semantic_scoring.py`

**Interfaces:**
- Produces: `canonicalize_participant(value: str, speaker_map: Mapping[str, Any]) -> str` — returns the canonical speaker-map key for any known alias, else a normalized (casefold, collapse-whitespace, strip-honorific) form. `_align` gains keyword arg `speaker_map: Mapping[str, Any] | None = None` and compares `raw_speaker` / `reported_actor` through it.
- Consumes: `speaker_map` from the bundle's `episode_context` (already present per `docs/TRUE_NORTH_INPUT_OPTIMIZATION.md` §isolation — the eight allowed episode-context fields include `speaker_map`).

- [ ] **Step 1: Write failing tests**

```python
def test_canonicalize_participant_matches_alias():
    speaker_map = {"dario_amodei": {"display": "Dario Amodei", "aliases": ["Dario", "Amodei"]}}
    assert canonicalize_participant("Dario", speaker_map) == "dario_amodei"
    assert canonicalize_participant("DARIO AMODEI", speaker_map) == "dario_amodei"

def test_canonicalize_participant_unknown_normalizes():
    assert canonicalize_participant("  Dr.  Jane   Doe ", {}) == "jane doe"

def test_align_speaker_exactness_uses_canonicalization():
    predicted = [{"claim_text": "models scale", "raw_speaker": "Dario"}]
    gold = [{"claim_text": "models scale", "raw_speaker": "Dario Amodei"}]
    speaker_map = {"dario_amodei": {"display": "Dario Amodei", "aliases": ["Dario"]}}
    result = _align(predicted, gold, speaker_map=speaker_map)
    assert result["pairs"][0]["field_exactness"]["raw_speaker"] is True
```

Note: adapt the speaker_map fixture shape to the actual structure stored in bundle `episode_context.speaker_map` — read one real bundle JSON under the suite's `bundles/` directory first and mirror its shape exactly. If the real map is flat (`{"S1": "Dario Amodei"}`), build the alias table by generating casefolded full name, first name, and last name per entry.

- [ ] **Step 2: Run tests, verify they fail** (`canonicalize_participant` undefined)
- [ ] **Step 3: Implement**

```python
_HONORIFICS = re.compile(r"^(dr|mr|mrs|ms|prof)\.?\s+", re.IGNORECASE)

def _participant_key(value: Any) -> str:
    text = _HONORIFICS.sub("", _text(value))
    return " ".join(text.split()).casefold()

def canonicalize_participant(value: Any, speaker_map: Mapping[str, Any] | None) -> str:
    key = _participant_key(value)
    if not key or not speaker_map:
        return key
    for canonical_id, entry in speaker_map.items():
        aliases = {_participant_key(canonical_id)}
        if isinstance(entry, str):
            aliases.add(_participant_key(entry))
            parts = _participant_key(entry).split()
        else:
            display = entry.get("display", "")
            aliases.add(_participant_key(display))
            aliases.update(_participant_key(a) for a in entry.get("aliases", []))
            parts = _participant_key(display).split()
        if len(parts) >= 2:
            aliases.add(parts[0])
            aliases.add(parts[-1])
        if key in aliases:
            return str(canonical_id)
    return key
```

In `_align`, replace direct `_exact(pred.get(field), gold.get(field))` for `raw_speaker` and `reported_actor` with comparison of `canonicalize_participant(..., speaker_map)` on both sides. Thread `speaker_map` from every `_align` call site (grep for `_align(`).

- [ ] **Step 4: Run tests, verify pass**
- [ ] **Step 5: Commit** `git commit -m "feat(scoring): canonicalize speaker/actor through episode speaker_map before exactness"`

### Task 2: Closed vocabularies for enum-like fields in the scorer

**Files:**
- Modify: `research_factory/true_north_semantic_scoring.py`
- Test: `tests/test_true_north_semantic_scoring.py`

**Interfaces:**
- Produces: `normalize_enum_field(field: str, value: Any) -> str` mapping surface variants of `certainty`, `stance`, `polarity`, `time_horizon` onto fixed vocabularies (e.g. certainty: `{certain, likely, speculative, unknown}`; polarity: `{affirms, denies}`; stance: `{supports, opposes, neutral}`; time_horizon: `{past, present, near_term, long_term, unspecified}`). Read the actual canonical output contract in `config/true_north_input_variants_v1.json` (`canonical_output_contract`) and use **its** value sets as the target vocabulary — do not invent new ones.
- `_pair_score` (line 168), `_align` field exactness, and `_UNSUPPORTED_CHECK_FIELDS` comparisons route these fields through `normalize_enum_field`.

- [ ] **Step 1: Read `config/true_north_input_variants_v1.json` → `canonical_output_contract` and record the exact enum values per field in the test file as constants.**
- [ ] **Step 2: Write failing tests** — variants like `"high confidence"` → `"certain"`, `"probably"` → `"likely"`, `"positive"` → `"affirms"` normalize; already-canonical values pass through unchanged; unknown junk normalizes to the field's explicit unknown/unspecified member, never silently to a substantive value.
- [ ] **Step 3: Implement** a table-driven synonym map per field. Keep it small (5–10 synonyms per canonical value) and deterministic; anything unmapped returns the casefolded input so genuine disagreements still fail exactness.
- [ ] **Step 4: Run tests, verify pass**
- [ ] **Step 5: Commit** `git commit -m "feat(scoring): normalize enum-like claim fields to contract vocabulary before comparison"`

### Task 3: Split "unsupported" into hallucination vs paraphrase-divergence

**Files:**
- Modify: `research_factory/true_north_semantic_scoring.py:358-442` (`_align` flag emission) and the summary aggregation at lines 630-670 and 940-960
- Test: `tests/test_true_north_semantic_scoring.py`

**Interfaces:**
- Produces: `unsupported_field_flags` entries gain `"severity": "hallucination" | "divergence"`. New summary metric `hallucination_rate_proxy` counts only severity=hallucination flags: `unmatched_predicted_claim`, `low_reference_token_precision_proxy` with token_precision < 0.3, and `predicted_field_absent_from_reference` for participant fields after Task 1 canonicalization. `predicted_field_reference_mismatch_proxy` on matched pairs becomes severity=divergence and feeds a **diagnostic** `field_divergence_rate`, not the hard gate. The hard gate ("zero unsupported accepted claims") binds to `hallucination_rate_proxy`.
- Keep the old aggregate visible as `unsupported_candidate_rate_proxy_legacy` for one release so before/after is auditable.

- [ ] **Step 1: Write failing tests** — a matched pair with equal claim text but different `time_horizon` produces one divergence flag, zero hallucination flags, and `hallucination_or_unsupported_proxy["flagged"] is False`; an unmatched predicted claim still flags as hallucination; token_precision 0.2 flags hallucination, 0.45 flags divergence only.
- [ ] **Step 2: Run tests, verify fail**
- [ ] **Step 3: Implement severity tagging and the two aggregates.** The gate wiring lives where `hallucination_or_unsupported_proxy` is built (`true_north_semantic_scoring.py:664-666`); point its `flagged` at hallucination-severity flags only.
- [ ] **Step 4: Run tests + full scoring test file, verify pass**
- [ ] **Step 5: Commit** `git commit -m "feat(scoring): separate hallucination from paraphrase divergence in unsupported gate"`

### Task 4: Multi-reference faithfulness — score against best consensus decomposition

**Files:**
- Modify: `research_factory/true_north_semantic_scoring.py` (consensus scoring around `_consensus_decompositions` line 331 and its consumer near line 520-640)
- Test: `tests/test_true_north_semantic_scoring.py`

**Interfaces:**
- The consensus gold already stores multiple acceptable decompositions (`consensus.private.json`: any atomic count between observed A/B min and max is acceptable, contested items excluded from strict gates — `docs/TRUE_NORTH_BENCHMARK.md:84-88`). Scoring must take the **max** faithfulness/atomic score across all stored acceptable decompositions per candidate, not compare against a single preferred one.
- Produces: per-candidate score = `max(score_against(d) for d in acceptable_decompositions)`; ties broken by first decomposition for determinism. Contested candidates are excluded from junk-escape and atomic-accuracy denominators (verify this exclusion is actually applied in the aggregation, not just documented).

- [ ] **Step 1: Read the real consensus artifact** for one development episode under the suite `gold/` directory to confirm field names (`decompositions`, `claim_texts`, contested markers). Adjust test fixtures to the real shape.
- [ ] **Step 2: Write failing test** — predicted 2-claim decomposition scores 1.0 when consensus stores both a 2-claim and 3-claim acceptable decomposition and the 2-claim variant matches; verify a contested candidate contributes to neither junk-escape numerator nor denominator.
- [ ] **Step 3: Implement max-over-references scoring and contested exclusion.**
- [ ] **Step 4: Run tests, verify pass**
- [ ] **Step 5: Commit** `git commit -m "feat(scoring): score faithfulness and atomicity against best acceptable consensus decomposition"`

### Task 5: Gold reliability repair — third-pass adjudication of the reject disagreement set

**Files:**
- Modify: `research_factory/true_north.py` (gold command family; `--phase atomic --pass pass-c` machinery already exists)
- Create: adjudication campaign artifacts under the suite `gold/` directory (produced by running commands, not by hand)
- Test: `tests/test_true_north.py` (state checks only; no live-call tests)

**Interfaces:**
- Current state: `diagnostics/gold-reliability.json` shows disposition agreement 0.807 and reject Jaccard 0.533 against a 0.9 pass gate — pass C exists but the compiled consensus still fails its reliability gate. The fix is procedural: re-run pass-C adjudication **specifically over the 35 rejects outside the A∩B intersection** (union 75, intersection 40) plus all `contested` items, with the adjudication packet requiring a quoted-evidence justification per decision.
- Produces: recompiled `consensus.private.json` per development episode and a regenerated `gold-reliability.json` with `pass_gate: true` (threshold 0.9 applies post-adjudication: contested-after-C items are excluded from the agreement denominator — implement that exclusion in the reliability computation if not already present).

- [ ] **Step 1: Add contested-exclusion to the reliability computation** (grep `gold-reliability` / `reject_jaccard` in `true_north.py`), with a test: 100 items, 10 contested-after-C, 88 of remaining 90 agreeing → agreement 0.978, `pass_gate: true`.
- [ ] **Step 2: Run tests, verify the computation change passes.**
- [ ] **Step 3: Commit the computation change** before any live calls: `git commit -m "feat(gold): exclude C-adjudicated contested items from reliability gate denominator"`
- [ ] **Step 4: Execute pass-C adjudication over the disagreement set** on the three development episodes: `python3 -m research_factory.pif_cli lab true-north gold --suite ai-safety-v1 --execute --partition development --phase atomic --pass pass-c --workers 2`, then `compile`. Spark/Codex lane, not GLM.
- [ ] **Step 5: Regenerate reliability diagnostics and verify `pass_gate: true`.** If still below 0.9, the residual disagreements are genuinely ambiguous — mark them contested (excluded) rather than forcing agreement, and record the count in the report.
- [ ] **Step 6: Commit artifacts/state** `git commit -m "chore(gold): third-pass adjudication of reject disagreement set for ai-safety-v1 development"`

### Task 6: Re-baseline all nine gates under the repaired instrument

**Files:**
- No source changes. Runs `score` + `report` against the **already-stored** best-stack outputs (campaign `tnio-20260728-combined-stack-v1`) — no new GLM calls.
- Create: `docs/TRUE_NORTH_REBASELINE_20260728.md` summarizing old-scorer vs new-scorer numbers per gate.

- [ ] **Step 1: Re-score stored best-stack run outputs with the updated scorer** (the outputs are hash-bound on disk; scoring is deterministic and free).
- [ ] **Step 2: Write the re-baseline doc** with a table: gate | old | new | target | remaining gap | attribution (instrument vs model).
- [ ] **Step 3: Decision checkpoint — report to Kolby.** Expected outcome: speaker, reported-actor, unsupported, and faithfulness move most of their distance; junk escape and atomic accuracy retain real model gaps. Phase 2 sizing depends on these numbers. **Do not start Phase 2 model calls before this checkpoint is reviewed.**
- [ ] **Step 4: Commit** `git commit -m "docs: post-instrument-repair gate re-baseline"`

---

# Phase 2 — Three-pass bounded extraction

Restructure the GLM stage so each response carries one small semantic decision. The harness composes passes; validators enforce lineage. All passes reuse the existing hash-bound packet builder, OpenCode inline-packet transport, receipt, and budget-ledger machinery from `run_system_prompt_arm` (`true_north.py:6765`) — extend, don't fork.

### Task 7: Pass A — disposition-only call

**Files:**
- Modify: `research_factory/true_north.py` (add `MULTIPASS_STAGES` config, stage-A schema + prompt beside `SYSTEM_PROMPT_ARMS` at line 94)
- Test: `tests/test_true_north.py`

**Interfaces:**
- Produces: `run_multipass_stage(suite, stage: Literal["disposition","decomposition","attribution"], episode_id, ...) -> StageResult` following `run_system_prompt_arm`'s signature/return conventions. Stage A output schema per candidate: `{"candidate_id": str, "disposition": "retain"|"revise"|"hold"|"reject", "junk_reason": str|null}` — nothing else. `junk_reason` is a closed set: `{"metadata","introduction_or_bio","question_or_setup","banter","bare_mention","fragment","repetition","unsupported_inference"}` and is **required** when disposition is reject (forcing the model to name the junk class measurably reduces false retains).
- Stage A system prompt (final text, use verbatim):

```
You are a strict junk filter for a private podcast research corpus. Do not use
tools. Your only task is disposition. For each candidate, read its exact
evidence and decide whether it contains a substantive asserted proposition a
researcher could verify, compare, contradict, or qualify. Reject metadata,
introductions and biographies, questions and setup, banter, bare mentions of a
name or product, fragments, repetition, and inferences the evidence does not
literally state, and name which of those junk classes applies. Do not reject a
substantive assertion merely because it is uncertain, conditional,
hypothetical, summarized, or phrased as a headline. Use hold only when the
evidence supports multiple incompatible readings whose resolution would change
the proposition or its speaker; never for low confidence or low value. Do not
decompose, rewrite, or attribute claims — a later stage does that. Return only
the exact schema-valid JSON requested by the packet.
```

- [ ] **Step 1: Write failing tests** — schema validation accepts a well-formed stage-A response, rejects one containing `claim_text`, rejects a `reject` without `junk_reason`, and rejects an unknown `junk_reason` value. Test the packet builder emits the stage-A schema and prompt hash-bound.
- [ ] **Step 2: Run tests, verify fail**
- [ ] **Step 3: Implement stage-A schema, prompt constant, packet assembly, and validator** reusing the surface-validation helpers the current single-pass uses.
- [ ] **Step 4: Run tests, verify pass**
- [ ] **Step 5: Commit** `git commit -m "feat(true-north): stage-A disposition-only multipass call"`

### Task 8: Pass B — decomposition-only call with count-then-emit

**Files:**
- Modify: `research_factory/true_north.py`
- Test: `tests/test_true_north.py`

**Interfaces:**
- Input: only candidates stage A retained/revised (harness filters; hold/reject never reach stage B). Output schema per candidate: `{"candidate_id": str, "proposition_inventory": [{"index": int, "subject": str, "predicate": str, "object_or_outcome": str}], "atomic_claims": [{"inventory_index": int, "claim_text": str}]}`.
- The `proposition_inventory` field is the under-extraction fix: the model must enumerate truth-condition rows **before** writing claims, and every inventory row must be either emitted as a claim or absorbed (validator checks every `inventory_index` in `atomic_claims` exists in the inventory; a claim count materially below inventory count is allowed only when rows share an index). This operationalizes the `proposition-inventory` arm (`true_north.py:180-203`) — the best single-pass atomicity performer — as a structural contract instead of advice.
- Stage B system prompt (final text, use verbatim):

```
You are an atomic-claim decomposer for a private podcast research corpus. Do
not use tools. Every candidate you receive has already been accepted as
containing substantive research content; do not re-judge acceptance and do not
attribute speakers — other stages own those. For each candidate, first build a
proposition inventory: one row per independently checkable predicate in the
exact evidence, with subject, predicate, and object or outcome. Two clauses
are separate rows when either could be true while the other is false,
including separate list items, forecasts, stances, causes, and effects. They
are one row when one clause only supplies the mechanism, reason, example,
condition, qualification, or scope of the same conclusion. Then emit one
atomic claim per inventory row, referencing the row's index. Each claim must
be the smallest independently citable assertion, must preserve time, scope,
condition, polarity, and certainty qualifiers from the evidence, and must be
meaningful when read alone. Do not merge rows with different truth conditions
and do not split one conclusion into fragments. Return only the exact
schema-valid JSON requested by the packet.
```

- [ ] **Step 1: Write failing tests** — validator rejects `atomic_claims` referencing a missing `inventory_index`; accepts multi-claim output; rejects output containing `raw_speaker` (attribution belongs to stage C); harness routes only value-state candidates into stage-B packets.
- [ ] **Step 2: Run tests, verify fail**
- [ ] **Step 3: Implement schema, prompt, packet assembly (stage-B packets carry candidate evidence + segment text, not speaker map), validator.**
- [ ] **Step 4: Run tests, verify pass**
- [ ] **Step 5: Commit** `git commit -m "feat(true-north): stage-B inventory-driven decomposition call"`

### Task 9: Pass C — attribution as closed-set selection

**Files:**
- Modify: `research_factory/true_north.py`
- Test: `tests/test_true_north.py`

**Interfaces:**
- Input: stage-B atomic claims plus the packet's `speaker_map` rendered as an explicit numbered roster. Output schema per claim: `{"candidate_id": str, "claim_index": int, "speaker_id": str, "attribution_mode": "direct"|"reported", "reported_actor_id": str|null, "reported_actor_freetext": str|null}`.
- `speaker_id` MUST be a key from the roster — the validator rejects free text. `reported_actor_id` uses the roster when the actor is a known participant; `reported_actor_freetext` (mutually exclusive) covers non-participant actors (e.g. "OpenAI", "the Biden administration") and is compared through Task 1's `canonicalize_participant` at scoring time. This turns the two worst gates (speaker 97%, reported-actor 95% exact) into selection problems, which is how they become reachable.
- Stage C system prompt (final text, use verbatim):

```
You are a speaker-attribution resolver for a private podcast research corpus.
Do not use tools. For each atomic claim, decide who directly asserted it in
the exact evidence, selecting the speaker strictly from the numbered roster
provided in the packet. The direct speaker is the person whose turn contains
the assertion — never a person merely mentioned. If the direct speaker is
quoting, paraphrasing, or reporting someone else's statement, position, or
finding, set attribution_mode to reported and identify that source as the
reported actor: use the roster ID when the actor is a roster participant,
otherwise give the actor's name exactly as the evidence states it. A claim the
speaker asserts in their own voice is direct with no reported actor, even when
it mentions other people or organizations as subject matter. Return only the
exact schema-valid JSON requested by the packet.
```

- [ ] **Step 1: Write failing tests** — validator rejects `speaker_id` not in roster; rejects both actor fields set simultaneously; requires `reported_actor_id` or `reported_actor_freetext` when mode is reported and forbids both when mode is direct.
- [ ] **Step 2: Run tests, verify fail**
- [ ] **Step 3: Implement roster rendering (stable ordering, hash-bound), schema, prompt, validator.**
- [ ] **Step 4: Run tests, verify pass**
- [ ] **Step 5: Commit** `git commit -m "feat(true-north): stage-C closed-set attribution call"`

### Task 10: Multipass orchestrator, composition, and lineage validation

**Files:**
- Modify: `research_factory/true_north.py` (orchestrator + CLI subcommand `lab true-north multipass-run`), `research_factory/pif_cli.py` if subcommand registration lives there
- Test: `tests/test_true_north.py`

**Interfaces:**
- Produces: `run_multipass(suite, episode_id, ...)` executing A→B→C with the existing bounded concurrency (≤4), per-stage receipts, budget-ledger reservations, and checkpoint/resume semantics (a stage-complete episode is not re-called on resume — mirror `--resume-run-id`). Final composed output per candidate must be **shape-identical** to the current single-pass canonical output (`normalization_contract: canonical_atomic_output_v1`) so the entire existing scoring, import, and downstream pipeline runs unchanged. Evidence text/offsets are bound by the harness exactly as today, after stage B.
- Composition rules: stage-A reject/hold candidates emit disposition-only rows (no claims); enum-like fields the three passes don't produce (`certainty`, `stance`, `polarity`, `time_horizon`) are emitted by stage B as part of each atomic claim — extend the Task 8 schema with those four fields as closed-set enums drawn from the canonical output contract, and validate membership.
- Retry policy: schema/semantic validation failure → ≤2 corrective retries same provider with the exact validator error (existing rule); transport failure → router fallback (existing rule).

- [ ] **Step 1: Write failing tests** — composed output validates against `canonical_atomic_output_v1`; a stage-B validation failure terminalizes that packet without invoking stage C; resume skips completed stages; budget ledger is charged per stage call.
- [ ] **Step 2: Run tests, verify fail**
- [ ] **Step 3: Implement orchestrator + `multipass-run` CLI.**
- [ ] **Step 4: Run tests, verify pass; run the full focused regression suite.**
- [ ] **Step 5: Commit** `git commit -m "feat(true-north): three-pass extraction orchestrator with canonical composition"`

### Task 11: Development evaluation of the multipass stack

**Files:**
- No new source. Campaign execution + report doc `docs/TRUE_NORTH_MULTIPASS_RESULTS.md`.

- [ ] **Step 1: Declare the campaign budget** in the reservation ledger: ≤120 calls (3 episodes × ~13 packets × 3 stages ≈ 117), ≤4M tokens, ≤6h wall. Multipass calls are smaller than single-pass; expect net token cost near parity.
- [ ] **Step 2: Run `multipass-run` on the two Search episodes**, score with the repaired instrument, and compare against the Task 6 re-baseline per gate.
- [ ] **Step 3: If junk escape > 2%:** pull every escaped item's stage-A `junk_reason` distribution; a concentrated class gets one targeted prompt-sentence adjustment to stage A only (single-factor discipline), re-run Search episodes once.
- [ ] **Step 4: If atomic accuracy < 90%:** diagnose via inventory-vs-gold count deltas (`diagnose-atomic` machinery); the permitted lever is stage-B prompt wording on split/merge rules, single change, one re-run.
- [ ] **Step 5: Replicate the frozen configuration on the Replication episode.** A gate result must hold direction on ≥2 of 3 episodes.
- [ ] **Step 6: Write `docs/TRUE_NORTH_MULTIPASS_RESULTS.md`** (same shape as the interaction results doc) and commit.

---

# Phase 3 — Bounded escalation lane for the residual tail

### Task 12: Confidence-gated Spark escalation

**Files:**
- Modify: `research_factory/true_north.py` (escalation policy in the multipass orchestrator)
- Test: `tests/test_true_north.py`

**Interfaces:**
- Escalation triggers (deterministic, no model self-reported confidence): (a) stage A returned `hold`; (b) stage B needed any corrective retry or its inventory count differs from emitted claim count without shared indices; (c) stage C chose `reported` mode with `reported_actor_freetext`; (d) any validator-repaired opaque ID. Escalated candidates are re-run through the same stage packet on `openai/gpt-5.3-codex-spark`, whose answer replaces GLM's for that candidate; receipts record both calls and the trigger.
- Hard cap: escalation ≤20% of candidates per episode; beyond the cap, items remain GLM-answered and the overflow count is reported. This preserves the cost thesis — measure and report the effective cost ratio (Spark tokens + GLM tokens vs all-Spark baseline) in every score report.
- This is quality-policy escalation by predeclared deterministic rule, distinct from the operational-failure-only router fallback; document the distinction in `docs/TRUE_NORTH_BENCHMARK.md` and keep semantic-disagreement fallback forbidden as before.

- [ ] **Step 1: Write failing tests** — each trigger fires escalation; cap enforcement; receipt records trigger + both model identities; non-triggered candidates never escalate.
- [ ] **Step 2: Run tests, verify fail**
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run tests, verify pass**
- [ ] **Step 5: Commit** `git commit -m "feat(true-north): deterministic confidence-gated Spark escalation lane"`

### Task 13: Gate certification runs

- [ ] **Step 1: Freeze the complete configuration** (prompts, schemas, escalation rules, thresholds, router) and record its configuration hash.
- [ ] **Step 2: Run two identical-config full development passes** (`multipass-run` across all development episodes) per the parent benchmark's gate rule; both must pass all nine gates.
- [ ] **Step 3: Open the sealed transfer episodes once** under the parent benchmark's transfer gate. No edits after inspection — a transfer failure ends this campaign version and returns to Kolby with the falloff ledger.
- [ ] **Step 4: Final report** — gates table (dev pass 1, dev pass 2, transfer), cost accounting vs all-Codex baseline, and the certification recommendation. Commit as `docs/TRUE_NORTH_CERTIFICATION_<date>.md`.

---

## Execution order and checkpoints

1. Tasks 1–4 are independent of each other → parallelizable across doer agents; Task 5 needs no scorer changes but its Step 1 touches `true_north.py` reliability computation — keep it off the scoring file to avoid conflicts.
2. **Checkpoint after Task 6 (mandatory review by Kolby):** the re-baseline decides how much Phase 2 must carry. If the repaired instrument already shows ≥7 gates passing on stored outputs, Phase 2 scope may shrink to junk + atomicity only.
3. Tasks 7–9 are independent after Task 6; Task 10 integrates them; Tasks 11–13 are sequential.
4. **Checkpoint after Task 11:** if the multipass stack passes all gates on development without escalation, Task 12 becomes optional cost insurance — still build it, but certification (Task 13) need not wait on it.

## Assumptions

- The stored best-stack outputs from `tnio-20260728-combined-stack-v1` are re-scorable without new model calls (hash-bound outputs on disk). If any are missing, re-run only the missing packets under a small declared budget.
- `speaker_map` in bundle episode-context is populated for all five suite episodes (spot-check one bundle per episode in Task 1 Step 1).
- Changing the scorer and gold consensus invalidates no frozen hashes that gate development runs — scorer and gold hashes are versioned inputs, and the campaign records new hashes. If the parent benchmark pins a scorer hash in `manifest.json`, bump it explicitly there with a recorded reason.
- Stage-A/B/C decomposition keeps total tokens within ~1.2× single-pass; if Task 11 shows worse, packet batching (multiple candidates per stage call, already the current pattern) is the sanctioned lever.
