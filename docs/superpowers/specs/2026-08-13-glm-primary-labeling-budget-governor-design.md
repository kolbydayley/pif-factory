# GLM-Primary Labeling with Codex Budget Governor — Design

Date: 2026-08-13
Status: Approved by Kolby (conversation, 2026-08-13)
Authorization: This document is the fresh authorization required by the
True-North closure terms for a second cheap-lane campaign.

## Problem

All production labeling today is Codex `gpt-5.5` via the subscription lane at a
bounded 25 segments/day (~2.4M tokens/day) against a backlog of ~77,700 pending
extractor jobs (~8.5 years at current pace). Kolby uses Codex for other work,
so PIF must not consume more than **30% of the weekly Codex quota**, while the
bulk of labeling moves to cheap lanes — primarily GLM (`opencode-go/glm-5.2`
via the OpenCode Go subscription), with Grok 4.6 low-reasoning (SuperGrok Lite
plan, grok CLI) as a second candidate lane for extra capacity.

Constraints carried forward:

- Kolby ruling 2026-08-10 (`config/provider_policy.json`): Codex is
  judge/audit-only in the durable system; a cheap lane takes a bulk stage ONLY
  after a Phase-3 agreement gate measures it, with auto-freeze on breach.
  Stage flips are one-line reviewed policy edits, never env vars.
- True-North closure (2026-07-29): any second campaign requires fresh
  authorization, multi-fold validation, and fresh held-out data. The sealed
  episode `ep_044f1d2d020e021cfaf99e90` stays closed.
- Fail-closed provider routing: unknown stages/providers raise; no silent
  fallback to Codex.

Evidence base at design time:

- GLM narrow 3-field canary (2026-07-27, corrected harness): PASSED —
  20/20 usable, 98.3% actionable, 87.4% reference coverage.
- GLM contract-fair rescore (2026-07-31): narrow contract NOT viable as a
  Codex replacement (event_count_ratio 0.594, recall gap 0.804) and cannot
  produce production-complete `ai_discourse_v3_1` records (missing claim_type,
  actor, entities, metrics, attribution, quality flags).
- Conclusion: qualification must test the FULL v3.1 contract with the
  improved harness, not the narrow contract.

## Decisions (made by Kolby, 2026-08-13)

1. **Path**: qualify cheap lanes on the full v3.1 contract, then promote.
2. **Promotion**: automatic on all-green gates; Telegram notice after.
3. **Ramp**: tiered with 7-day green-streak gates (100 → 400 → 1,600 →
   uncapped), reusing scale-gate streak machinery.
4. **Grok**: test Grok 4.6 (low reasoning) via the grok CLI as an additional
   candidate lane alongside GLM. GLM remains the preferred bulk provider.

## Component 1 — Codex weekly budget governor

Purpose: keep PIF's total Codex consumption ≤ 30 percentage points of the
weekly Codex quota window, measured against live account state.

- **Quota source**: extend `parse_rate_limit_snapshot`
  (`research_factory/app_server_capacity.py`) to parse the weekly/secondary
  window (`usedPercent`, `resetsAt`) in addition to the primary window, from
  the existing `account/rateLimits/read` probe. Probe never starts a thread,
  turn, or sidecar.
- **Attribution by delta**: snapshot weekly `usedPercent` immediately before
  and after each PIF Codex batch; the delta is PIF-attributed. Accumulate into
  a new ledger table (`pif_codex_weekly_points_ledger`) keyed by the weekly
  window's `resetsAt` timestamp, alongside the existing token ledger
  (`pif_subscription_budget_ledger`). Sub-point deltas are recorded with the
  probe pair's raw values so rounding never hides spend.
- **Policy** (fail-closed):
  - Hard stop: PIF-attributed points ≥ 30 in the current window → no further
    PIF Codex calls until the window resets.
  - Pacing: remaining PIF allowance is spread across remaining days in the
    window (allowance/day = remaining_points / remaining_days); a day may
    borrow at most 2× its pace share.
  - Priority when allowance is thin: production audit sampling first,
    qualification judging second, anything else last.
  - Probe unreachable or malformed → no Codex calls that cycle.
- **Accounting scope**: qualification judge spend, production audits, and any
  residual Codex labeling all count inside the 30%.

## Component 2 — Full-contract qualification campaign (per candidate lane)

Offline; zero production mutation; receipts record
`production_enabled=false` until promotion.

- **Candidates**: (a) GLM `opencode-go/glm-5.2` via OpenCode with the
  corrected harness that passed the 07-27 canary: dedicated non-coding agent,
  full ontology + calibration guidance, adequate deadlines, isolated ephemeral
  OpenCode data dirs. (b) Grok `grok-4.6` low-reasoning via the grok CLI with
  an equivalent harness. Precondition for (b): install and authenticate the
  grok CLI (not present on the machine at design time) and build a small
  transport adapter mirroring the OpenCode one.
- **Task**: draft complete `ai_discourse_v3_1` labels on bounded windows.
- **Reference**: segments already labeled by production Codex gpt-5.5
  (existing ground truth), plus a Codex judge for field-level agreement on
  fresh folds. Multi-fold; held-out segments never before used for tuning;
  holdout exclusion by episode ID and text hash.
- **Gates** (all required, per lane):
  - Field-level agreement ≥ 0.9 (the `agreement_threshold` already declared in
    `provider_policy.json` for `label_segment`).
  - 100% exact-evidence grounding through existing validators (event-granular
    drop of ungrounded events is allowed, whole-case failure is not).
  - Schema pass on every emitted record.
  - Quarantine rate not materially worse than production Codex (baseline
    ~1.2%).
- **Failure output**: per-field disagreement diagnosis so iteration targets
  harness/prompt fixes; a failed lane can re-run after fixes without touching
  holdout data already spent.
- **Lane selection**: GLM is preferred; if both pass, GLM takes the bulk lane
  and Grok is configured as overflow capacity (used when the GLM/OpenCode
  quota is exhausted or the GLM lane is paused for non-quality reasons). If
  only Grok passes, Grok takes the bulk lane and GLM returns to candidate
  status with a diagnosis.

## Component 3 — Auto-promotion and auto-freeze

- **Promote**: on all-green gates, the system edits `provider_policy.json`
  (`label_segment.provider` → winning lane), commits the one-line change with
  the scoreboard receipt path in the message, and sends a Telegram notice.
- **Freeze**: any production audit breach (agreement below gate, grounding
  failure spike, quarantine spike beyond 2× baseline) reverts the policy to
  Codex bounded 25/day, commits the revert, and notifies. Freeze is one-way:
  un-freezing requires a passed re-qualification, never a timer.
- Both paths write immutable receipts; the policy file remains the single
  runtime authority (no env-var overrides).

## Component 4 — Production bulk lane with tiered ramp

- Wire the cheap-lane adapter (currently offline-only GLM workhorse) into the
  daily controller's bounded extraction stage as a real queue-consuming lane;
  Codex becomes judge/audit-only for this stage.
- **Tier ladder**: 100/day → 400 → 1,600 → uncapped. Each promotion requires a
  7-day all-green calendar-continuous streak (existing scale-gate semantics:
  a no-run day resets the streak; a missed day cannot be backfilled).
- **Audit**: source-aware Codex audit on a 10–15% random daily sample of
  cheap-lane output. The governor may shrink sampling to a floor of 5%; below
  the floor the cheap lane pauses rather than running unaudited. First day at
  each new tier is a canary: half volume, double audit rate.
- **Quota behavior**: OpenCode/Grok quota exhaustion pauses that lane; no
  silent fallback to Codex. If an overflow lane is qualified, it may take the
  volume its own quota allows.
- **Observability**: daily receipts extend with cheap-lane volume by provider,
  audit sample size and agreement, PIF weekly Codex points used/remaining,
  tier, and streak. Telegram alerts on freeze, budget-halt, streak reset, and
  tier promotion.

## Error handling summary

| Failure | Behavior |
| --- | --- |
| Rate-limit probe fails | No Codex calls that cycle (fail-closed) |
| PIF weekly points ≥ 30 | Codex halt until window reset; cheap lane continues only while audited backlog coverage ≥ floor |
| Audit agreement breach | Auto-freeze to Codex 25/day + revert commit + Telegram |
| Cheap-lane quota exhausted | Lane pauses; overflow lane may take volume; never silent Codex fallback |
| No-run calendar day | Streak resets (existing scale-gate rule); alarm on absence, not just failure |

## Testing

- Governor math unit-tested against synthetic rate-limit snapshots (window
  rollover, mid-batch reset, malformed payloads, delta attribution).
- Dry-run mode for the full daily cycle with both lanes stubbed.
- Qualification harness validated on 2–3 already-judged segments before any
  paid fold runs.
- Tier-canary days (half volume, double audit) at every tier boundary.

## Out of scope

- Transcript acquisition backlog (~6,800 fetchable episodes) — separate work.
- Any change to the sealed True-North artifacts or holdout episodes.
- Replacing Codex as judge/auditor.
