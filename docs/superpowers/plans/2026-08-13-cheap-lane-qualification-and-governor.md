# Cheap-Lane Qualification + Budget Governor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Take Grok 4.6 and GLM-5.2 from load-test-validated drafters to gate-passing qualified bulk lanes, with a Codex weekly budget governor enforcing the 30% cap.

**Architecture:** Three phases. (A) Empirical: harden the drafting prompts against the exact judge failure notes, re-run qualification rounds on fresh segments until gates pass. (B) Deterministic: build `pif_budget_governor` (weekly usedPercent delta attribution + allowance pacing) with unit tests. (C) Production adapters: shared cheap-lane adapter module + validators, then a bulk draft runner writing to a shadow table until promotion flips `provider_policy.json`.

**Tech Stack:** Python 3.9 stdlib (matches research_factory), sqlite3, Grok Build CLI 1.0.3 headless, opencode 1.18.8, codex exec for judging.

## Global Constraints

- No canonical `data/factory.sqlite` mutation before promotion; shadow outputs live under `work/` or a dedicated shadow sqlite.
- Fail-closed: probe/parse failures stop the lane, never fall back silently to Codex (`provider_policy.json` ruling 2026-08-10).
- All Codex spend (judging + audits) counts inside the 30% weekly budget.
- Qualification gates (from spec): judged support ≥ 0.95 mean, junk ≤ 0.10 mean, coverage ≥ 0.95 mean, strict pass rate ≥ 0.9, schema pass 100%, evidence grounding handled event-granularly.
- Never `git add -A`; stage owned paths only.
- Sealed True-North artifacts and `ep_044f1d2d020e021cfaf99e90` stay untouched.

---

### Task A1: Hardened drafting prompt v2

**Files:**
- Create: `work/loadtest-20260813/prompt_v2.py` (shared PROMPT_V2 constant + build_prompt())

**Interfaces:**
- Produces: `PROMPT_V2` template with `{SEGMENT_TEXT}` slot; used by A2 runner and Task C1 adapters.

- [ ] **Step 1:** Write PROMPT_V2 adding, verbatim rules targeting the judge failure notes:
  - "Do NOT extract: jokes, parodies, recited poems/speeches, host banter, episode housekeeping (intros, outros, 'take it away', scheduling), sponsor or advertising reads (any product pitch delivered as promotion), calls to like/subscribe/support."
  - "claim_text must not assert MORE than the evidence supports. Never add attribution, numbers, or causes that the evidence does not state. If the speaker is uncertain or speculative, the claim_text must preserve that uncertainty."
  - "A claim about a sponsor's product is junk unless the hosts editorially discuss it outside the ad read."
- [ ] **Step 2:** Keep every rule from the load-test prompt (exact-substring evidence, claim types, full-name entities).
- [ ] **Step 3:** Commit.

### Task A2: Fresh qualification manifest + round runner

**Files:**
- Create: `work/loadtest-20260813/qualify.py`
- Reuse: `loadtest.py` lane runners, `judge.py` judge_one + snapshots.

**Interfaces:**
- Consumes: `run_grok`, `run_glm`, `judge_one`, PROMPT_V2.
- Produces: `qualification_round_N.json` with per-lane gate metrics: `{lane: {support, junk, coverage, pass_rate, schema_pass, grounding, n_drafted, n_judged, gates_passed: bool}}`.

- [ ] **Step 1:** Manifest: 80 ready-labeled segments NOT in the load-test manifest (offset past the 220 or excluded by id), balanced across sources.
- [ ] **Step 2:** Round runner: draft 40/lane with PROMPT_V2, judge 15/lane sampled, compute gate metrics vs Global Constraints thresholds.
- [ ] **Step 3:** Run round 1. Expected: improvement over load-test baselines (Grok junk < 0.062, GLM support > 0.961).
- [ ] **Step 4:** Commit round receipt.

### Task A3: Iterate to green (≤3 rounds)

- [ ] **Step 1:** If a lane fails a gate, read every failing judge note, classify (junk-inclusion / overstatement / grounding / schema), amend PROMPT_V2 with a targeted rule or few-shot example.
- [ ] **Step 2:** Re-run round on FRESH segments (never reuse judged segments for tuning-then-measuring in the same round).
- [ ] **Step 3:** Stop when both lanes green, or after 3 rounds report per-field diagnosis and recommend next move.

### Task B1: Budget governor module

**Files:**
- Create: `research_factory/pif_budget_governor.py`
- Test: `tests/test_pif_budget_governor.py`

**Interfaces:**
- Produces:
  - `read_weekly_snapshot(session_root: Path) -> dict | None` — newest `{used_percent: float, resets_at: int}` from `~/.codex/sessions/**/rollout-*.jsonl` token_count events.
  - `class WeeklyLedger(db_path)` — `.record(before: dict, after: dict, run_id: str)` appends attributed points; `.points_used(resets_at: int) -> float`.
  - `allowance(snapshot, ledger, *, cap_points=30.0, now: int) -> dict` — `{points_remaining, points_today, allowed: bool}` with pacing points_today = remaining / days_left (min 1 day), borrow ≤ 2×.

- [ ] **Step 1:** Failing tests: snapshot parse from a fixture JSONL; ledger accumulation across two windows (reset rollover isolates windows); allowance hard-stop at cap; pacing math; malformed snapshot → allowed=False.
- [ ] **Step 2:** Run tests, verify fail. **Step 3:** Implement. **Step 4:** Tests pass. **Step 5:** Commit.

### Task C1: Cheap-lane adapter module

**Files:**
- Create: `research_factory/cheap_lane_adapters.py`
- Test: `tests/test_cheap_lane_adapters.py` (subprocess mocked)

**Interfaces:**
- Produces:
  - `draft_grok(segment_text: str, *, timeout=240) -> dict` and `draft_glm(segment_text: str, state_root: Path, *, timeout=300) -> dict`, both returning `{ok, label, elapsed, error}` with the `{"text": ...}` unwrap (grok) and fenced-JSON repair (glm).
  - `validate_label(label: dict, segment_text: str) -> dict` — schema check, event-granular drop of ungrounded evidence, claim/evidence overlap flag; returns `{label, dropped, schema_ok}`.

- [ ] **Step 1:** Failing tests: unwrap shapes, JSON repair on fenced output, ungrounded-event drop keeps grounded siblings, schema rejection. **Steps 2-5:** standard TDD cycle, commit.

### Task C2: Shadow bulk draft runner

**Files:**
- Create: `research_factory/pif_bulk_draft_runner.py` (CLI: `python -m research_factory.pif_bulk_draft_runner --lane grok --count N`)
- Shadow store: `work/bulk-drafts/drafts.sqlite` (table `draft_labels(segment_id, lane, label_json, validation_json, created_at)`)

**Interfaces:**
- Consumes: B1 governor (audit budget), C1 adapters, canonical DB read-only (pending extractor queue join for segment selection).
- Produces: shadow drafts + per-run receipt `{lane, drafted, validated, dropped_events, audit_sample, audit_agreement}`.

- [ ] Steps: TDD on selection query (read-only), runner loop with concurrency (grok 6 / glm 3), Codex audit sample via judge prompt inside governor allowance, receipt write, commit.

### Task C3: Promotion + tier state (post-green only)

**Files:**
- Modify: `config/provider_policy.json` (the one-line flip — only on green gates)
- Create: `work/bulk-drafts/tier_state.json` (`{lane, tier, streak_days, last_green_date}`)

- [ ] Steps: promotion commit routine (policy edit + receipt reference + Telegram notice via existing channel), tier ladder 100→400→1,600→uncapped on 7-day calendar-continuous green streaks, auto-freeze revert path. Canonical label writes only after this task's promotion gate.

## Self-Review Notes

- Spec coverage: governor (B1), qualification (A1-A3), auto-promote/freeze (C3), tiered ramp (C3), adapters/validators (C1), shadow-first (C2). Transcript-acquisition expansion is explicitly out of scope per spec.
- Types consistent: adapters return the same `{ok, label, elapsed, error}` shape the load-test runners proved out.
- Phase A precedes C3 promotion strictly; C2 runs shadow regardless of gate state.
