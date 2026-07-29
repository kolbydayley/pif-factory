# True-North Campaign Closeout Plan

> **For agentic workers:** This plan is written for an executor with ZERO prior
> context. It closes out the True-North extraction campaign in four tasks and
> then ends the campaign. Execute tasks strictly in order. Each task ends with
> a report and a checkpoint. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Validate the certified GLM-only extraction stack against sealed
holdout data, obtain a real production cost/drift measurement, deliver a final
honest scoreboard with a go/no-go production recommendation to Kolby, and end
the campaign.

**Architecture:** The executor (you) does not write extraction code. You drive
an existing, fully-equipped Codex session (the "doer") by sending bounded
directives, then independently verify every claim in its reports against
frozen artifacts on disk before acting on them. The doer has the complete
harness: hash-bound packets, budget ledger, validators, scorers, shadow
stores.

**Tech Stack:** Codex CLI (`codex exec resume`), the `research_factory` Python
package in this repo, SQLite artifacts under `~/Library/Application Support/
Podcast Intelligence Factory/true-north/ai-safety-v1/`, provider lanes
`zai-coding-plan/glm-5.2` (Z.AI subscription) and `gpt-5.6-sol` /
`openai/gpt-5.3-codex-spark` (Codex subscription).

---

## Context you must internalize before Task 1

### What this campaign is

The Podcast Intelligence Factory (PIF) extracts atomic claims from podcast
transcripts. Production historically used expensive Codex-class models for
everything. The campaign's founding objective: move extraction work onto the
cheap subsidized GLM lane without unacceptable quality loss. The True-North
benchmark (suite `ai-safety-v1`) measures quality on 5 development episodes
with adjudicated gold, plus sealed transfer episodes that have NEVER been
evaluated against.

### Current state (as of 2026-07-29, commit `d9bd045` + in-flight Ruling 7)

- **7 of 9 gates pass** on the development fold under measurement contract v8+.
  Passing: candidate-state macro F1 (0.863 ≥ 0.791), retained-value recall
  (0.971), intrinsic junk (0 escapes), speaker (0.992), reported actor (0.782 ≥
  0.735), hallucination (0.040 ≤ 0.094), schema (1.0). Failing permanently:
  atomicity (~0.78–0.81 vs 0.90) and faithfulness (~0.73 vs 0.744) — proven to
  be ONE root cause (decomposition alignment) across six designs and three
  model families. The decomposition lane is PERMANENTLY CLOSED. Do not reopen
  it under any framing.
- **Five of nine gates were re-referenced** to measured gold-vs-gold ceilings
  during the campaign (Rulings 1–6, recorded in the suite manifest and
  `config/true_north_campaign_budget_ledger.json`). The original aspirational
  targets remain visible as diagnostics.
- **Ruling 7 (COMPLETE, commit `a3bf2d9`):** the certified stack is now the
  GLM-only composition — GLM split-default decomposition (frozen Task-5
  fallback) + deterministic actor-span suppression (threshold `risk >= 2`) +
  certified option-2 disposition ensemble + speaker prior adoption. It holds
  7/9 with several gates IMPROVING over the sol-assisted variant (macro F1
  0.8732, actor 0.7932, recall 0.9749) at ZERO runtime Codex-lane calls
  (~56 GLM calls per representative episode vs 17 all-Codex baseline calls;
  100% Codex-lane reduction). The sol-assisted best-of-lanes variant remains
  documented but not recommended: +0.008 atomicity, no additional gate, ≥21
  Codex calls. Task 1 below is therefore a VERIFICATION task, not an
  execution task.
- **Phase E (production comparison) failed closed**: the July 20 snapshot
  (`data/factory.sqlite`, now hydrated and readable read-only) has no token
  usage in baseline outputs (0/17) and 0 atomic claims in production tables,
  so retrospective drift/cost is unmeasurable. A prospective design is needed
  (Task 3).
- **Spend to date:** ~309 provider calls / ~3.58M tokens, all flat-rate
  subscription quota. Campaign hard stop was lifted by Kolby; per-experiment
  declared ceilings remain mandatory and are enforced by the harness ledger.
- **Key documents** (read these before Task 1):
  - `docs/TRUE_NORTH_CERTIFICATION_20260729.md` — the terminal certification.
  - `config/true_north_campaign_budget_ledger.json` — rulings + spend.
  - `docs/TRUE_NORTH_BENCHMARK.md` — the benchmark protocol, including the
    sealed-holdout progression rule this plan explicitly overrides (below).
  - `docs/TRUE_NORTH_DECOUPLED_SCORING_RESULTS.md`,
    `docs/TRUE_NORTH_PHASE_D_ACTOR_SUPPRESSION_20260729.md`,
    `docs/TRUE_NORTH_C2_SCHEMA_FREE_FINAL_RESULTS_20260729.md` — the frozen
    lane results the certified composition draws from.

### How to drive the doer session

The doer is Codex session `019f84e7-f0ad-7b43-9be8-95f9ca559fa5` (thread
"Research local open model labeling"), running `gpt-5.6-sol`, working in this
repo. Send work like this:

```bash
codex exec resume 019f84e7-f0ad-7b43-9be8-95f9ca559fa5 \
  -o /tmp/doer-reply-N.txt - < /tmp/doer-instruction-N.md \
  > /tmp/doer-run-N.log 2>&1
```

Rules, all mandatory, learned the hard way:

1. **Never send while a turn is active.** Check `ps aux | grep 'codex exec'`
   (ignore the dream automation, which runs `--ephemeral` in the kolby-os dir)
   and confirm the previous invocation's `-o` reply file exists. A concurrent
   writer can corrupt the session rollout.
2. **Every directive must begin with the no-Goals clause and end with an
   explicit turn terminator.** Template: open with "Do NOT create, adopt,
   continue, or infer any Codex Goal from this directive. This is a bounded
   task list; when its tasks are reported, END YOUR TURN." Codex has a
   first-class Goals feature (`~/.codex/goals_1.sqlite`); an earlier directive
   phrased as a standing objective spawned an unbounded self-continuing goal
   that Kolby had to delete manually. Never phrase anything as an objective,
   mission, or standing goal. Never write to Codex SQLite databases.
3. **Verify every doer claim against artifacts before acting.** Score files
   live under `~/Library/Application Support/Podcast Intelligence Factory/
   true-north/ai-safety-v1/` (runs, rescoring, certification, diagnostics,
   calibration). The campaign caught ~a dozen material errors this way — in
   both directions (doer errors AND reviewer errors). A confident,
   well-formatted report with a wrong headline number is the expected failure
   mode, not a rarity.
4. **Every experiment declares calls/token ceilings in the ledger BEFORE its
   first provider call.** Per-envelope token reservations: GLM 25k, sol 35k
   (measured; smaller reservations caused overruns). Include a mandatory
   single-packet smoke test before any batch with a new schema or transport.
   Do not pass provider-side structured-output schemas to sol via the Codex
   lane — its validator rejects `oneOf`, `uniqueItems`, and untyped fields;
   specify JSON shape in the prompt and validate locally.
5. **Monitor commits while a turn runs**: poll `git log` in this repo (~60s);
   the doer commits as it completes work. Silence >90 min during an active
   turn warrants inspection of the run log, not blind waiting.
6. **Pre-commit stop conditions in every directive** ("one run, no
   self-iteration; on failure stop and report") and forbid choosing whichever
   metric reading makes numbers smaller. The doer honors these when stated.

### Authority and its limits

Kolby has delegated execution authority to the review loop, including
measurement-contract rulings (Rulings 1–7 precedent), EXCEPT:

- **No re-referencing atomicity (0.90) or faithfulness (0.744435).** Both are
  final; their shortfalls are certified disclosed limitations. Ruling 6 forbids
  reopening faithfulness; the decomposition closure forbids new decomposition
  designs.
- **No production mutation, ever.** Production DB is read-only; all writes go
  to isolated shadow stores. Any isolation ambiguity = stop.
- **Privacy:** no transcripts, gold content, secrets, or answer keys in model
  inputs beyond what the frozen packet builders already include; reports carry
  metrics and identifiers, not transcript text.
- **Sealed holdouts:** this plan explicitly authorizes ONE transfer evaluation
  (Task 2) as an owner-approved exception to the progression rule, for
  overfitting measurement — labeled as such, never as "passing the benchmark."
  No second opening. If Task 2's cost estimate is prohibitive, holdouts stay
  sealed and the plan records why.

## Global Constraints

- Doer session UUID: `019f84e7-f0ad-7b43-9be8-95f9ca559fa5`. Repo:
  `/Users/kolbydayley/Documents/Codex/podcast-intelligence-factory`.
- All spend is subscription quota ("calls" = quota/rate-limit consumption, not
  dollars). Report cumulative campaign spend with every result.
- The focused regression suite must pass before any commit the doer makes:
  `python3 -m pytest tests/ -k true_north -q` (or the doer's equivalent
  focused invocation; currently ~276 tests).
- Certification updates go to `docs/TRUE_NORTH_CERTIFICATION_20260729.md`
  with ruling provenance; the manifest versioning pattern (bump + preserve
  prior in history) is established — follow it.
- The headless queue lane (dispatcher `headless-codex-dispatcher.js`) may land
  its own commits. Its standing orders: non-colliding work only, findings via
  in-repo docs. If its work collides with yours, stop and reconcile via a
  committed doc, not a merge-around.

---

### Task 1: Confirm Ruling 7 re-certification

The Ruling 7 directive was in flight when this plan was written. First
determine its outcome; complete it if unfinished.

- [ ] **Step 1:** Check whether the doer's turn completed
  (`/tmp`-equivalent reply file from the prior operator may not exist for you;
  instead check `git log` for a commit after `d9bd045` touching
  `docs/TRUE_NORTH_CERTIFICATION_20260729.md`, and read the certification
  doc's current gate table).
- [ ] **Step 2:** Verify from artifacts, not the doc: the GLM-only composition
  rescore file under `.../ai-safety-v1/rescoring/` (or the path the
  certification cites). Confirm: (a) 7/9 gates hold — specifically that
  speaker, actor, hallucination, macro F1, recall, junk, schema did NOT
  regress versus the sol-assisted composition; (b) the runtime path contains
  zero Codex-lane calls; (c) atomicity/faithfulness readings are reported
  honestly as failing.
- [ ] **Step 3:** If any passing gate regressed or the doer did not adopt the
  GLM-only stack, read its stated reason. If the reason is evidence-based
  (e.g. suppression threshold incoherent against GLM-only decomposition),
  accept the best coherent stack it identified instead — the objective is
  maximum gates at zero Codex-lane runtime calls; do not force an incoherent
  composition.
- [ ] **Step 4:** If unfinished, send a directive (with the mandatory no-Goals
  header and END YOUR TURN footer) instructing exactly the Ruling 7 task as
  specified in the ledger's `owner-delegated` entries, zero provider calls.
- [ ] **Step 5 (checkpoint):** Record the confirmed certified composition and
  its gate table in your report. Do not proceed to Task 2 until the
  certification doc, the rescore artifact, and your independent read of the
  gate numbers agree.

### Task 2: Sealed-transfer overfitting check (the single most important task)

Everything measured so far was tuned on 3–5 development episodes. The Phase D
suppression threshold, gate recalibrations, and composition choices are all
fitted to that fold. The sealed transfer episodes exist precisely to measure
this. This plan authorizes ONE evaluation.

- [ ] **Step 1 (zero calls): cost estimate.** Direct the doer to report,
  without executing anything: (a) the state of sealed-holdout gold —
  directories exist under `.../ai-safety-v1/gold/sealed-holdout/` (pass-a,
  pass-b, relations, utility observed present; completeness unknown); (b) if
  gold is incomplete, the call floor to compile it (pass-A + pass-B + pass-C
  adjudication + compile, per the layered protocol in
  `docs/TRUE_NORTH_BENCHMARK.md`) — note this gold work runs on the Codex
  lane by protocol; (c) the call floor to run the certified GLM-only stack on
  the sealed episodes (GLM lane); (d) which sealed artifacts (bundles,
  packets) already exist hash-pinned versus need building.
- [ ] **Step 2 (decision gate):** If total estimated cost ≤ 120 calls /
  3,000,000 tokens, proceed. If above, STOP this task, record the estimate in
  the certification as "transfer check priced but not executed," and continue
  to Task 3 — do not shave the protocol to fit.
- [ ] **Step 3 (execute):** Declare the measured ceiling in the ledger. Compile
  any missing sealed gold FIRST (it is scoring infrastructure, not tuning —
  but it must never enter any extraction model's input). Then run the frozen
  certified stack ONCE on the sealed episodes. No prompt, threshold, rule, or
  composition changes are permitted after seeing any sealed result — a
  transfer result explains, it never tunes. Anything learned goes in the
  report as future work.
- [ ] **Step 4 (score and compare):** Report the full nine-gate table on
  sealed episodes next to the development-fold table, with per-gate deltas.
  Expected honest outcomes: small deltas (dev numbers generalize — the
  campaign's conclusions stand) or large drops on the tuned components
  (Phase D suppression, recalibrated fields) — which would mean the dev-fold
  certification overfits, and the certification must say so prominently.
- [ ] **Step 5 (checkpoint):** Update the certification with a "Transfer
  generalization" section either way. This is the plan's core deliverable.

### Task 3: Prospective shadow cost/drift measurement (replaces failed Phase E)

Retrospective comparison is impossible (snapshot lacks receipts and atomic
claims). Measure prospectively instead.

- [ ] **Step 1 (zero calls): feasibility.** Direct the doer to determine how
  new episodes arrive (the production pipeline is event-driven; check the
  queue/automation configs in this repo, e.g. `automation/` and the app-server
  pipeline) and whether a dual-run is possible: when a new episode is labeled
  by production, run the certified GLM-only stack on the same episode in
  shadow, both sides producing receipted outputs. Report the design: trigger,
  isolation, per-episode call cost for both lanes, and where shadow results
  land.
- [ ] **Step 2 (bounded pilot):** If feasible, authorize up to 3 prospective
  episodes, ceiling 90 calls / 1,800,000 tokens (measured single-episode GLM
  floor was 42 calls; the certified stack no longer uses sol). Per-lane
  accounting is mandatory: GLM calls/tokens, Codex-lane calls/tokens (expected
  0 for the hybrid runtime), production baseline calls/tokens, all per
  retained valid atomic, plus per-stage agreement between the two lanes'
  outputs. Never present a blended figure without the lane split.
- [ ] **Step 3:** If new episodes will not arrive within the working window,
  record the ready-to-run design + declared ceiling in the certification as
  "prospective trial armed, awaiting episodes," and proceed. Do not
  manufacture fake episodes to force a measurement.
- [ ] **Step 4 (checkpoint):** Report the cost ratio (or the armed design).

### Task 4: Final scoreboard, decision memo, campaign end

- [ ] **Step 1:** Direct the doer to produce
  `docs/TRUE_NORTH_FINAL_SCOREBOARD_20260729.md` containing three columns per
  gate: ORIGINAL aspirational target, final calibrated gate (with its measured
  gold-vs-gold ceiling), and the certified result on development AND sealed
  folds. No spin: the reader must be able to see in one table that e.g. actor
  is 0.78 against an original 0.95 aspiration and a 0.735 measured ceiling.
  Include: the cost structure (Codex-lane calls 17 → 0, GLM calls added), the
  decomposition process-property finding, total campaign spend, and the
  complete ruling provenance chain (Rulings 1–7 + this plan's transfer
  exception).
- [ ] **Step 2:** Write the decision memo section addressed to Kolby: a
  concrete go/no-go recommendation on wiring the certified stack into
  production shadow permanently, conditioned explicitly on the Task 2 transfer
  result (generalizes → recommend go; collapses → recommend no-go and state
  what the collapse implicates). Production cutover itself remains Kolby's
  decision alone — recommend, never execute.
- [ ] **Step 3:** Update the canonical project memory
  (`~/.codex/memories/projects/podcast-intelligence-factory--023b01/`
  project.md and notes.md) with the final state, per its existing conventions.
- [ ] **Step 4:** Final commit; confirm regression suite passes; confirm
  worktree clean; tag `campaign-closed-20260729`.
- [ ] **Step 5 (end):** Deliver the scoreboard and memo to Kolby. The campaign
  is then CLOSED. Do not open new experiments, lanes, or recalibrations. Any
  further work (production wiring, the two-model-consensus research idea,
  actor value selection over the 157-atomic residual) is future work requiring
  Kolby's fresh authorization.

## Self-review notes for the executor

- The single biggest risk in this plan is Task 2 revealing overfitting. Do not
  soften that result if it appears; it is the most valuable possible finding.
- The second biggest risk is scope creep back into closed lanes. Six designs
  failed decomposition across three model families; a seventh idea will occur
  to you and it will feel novel. It is not authorized. Record it as future
  work.
- Historical error patterns to watch in yourself, all committed by the prior
  operator: budget ceilings set by estimate instead of measurement (compute
  floors first), certifying on quality when the objective is cost, trusting a
  well-formatted report without opening the artifact, and objective-shaped
  language spawning a Codex Goal.
