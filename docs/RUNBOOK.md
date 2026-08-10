# PIF Runbook — live tree `~/pif-factory`

> Rewritten 2026-08-10 under the durability plan. The previous runbook still
> pointed at the abandoned `~/Documents` tree; it is preserved verbatim at
> `docs/RUNBOOK-legacy-20260810.md` — its efficiency-backtest and model-matrix
> history (chunk sweeps, Sol anchor, distillation stop-rules) remains the
> record of what was tried and rejected. **Never run anything against the
> Documents tree**: it is a frozen, non-authoritative snapshot.

## Standing rules (Kolby's rulings, 2026-08-10)

1. **Codex is judge/audit-only.** Bulk extraction runs on GLM or waits.
2. **Hard budget: 5,000,000 subscription tokens/day** across all lanes,
   enforced before dispatch. At 120% a `KILL` file stops everything.
3. **Provider routing** lives in `config/provider_policy.json` only. Flipping
   a stage to GLM requires the Phase-3 agreement gate; a breach auto-freezes
   back to Codex.
4. **Quota days freeze the scale streak** (never reset it).

## Deploy / Schedule

The single scheduler is the Claude scheduled task **`pif-daily-cycle`**
(`~/.claude/scheduled-tasks/pif-daily-cycle/SKILL.md`), which runs:

```bash
cd /Users/kolbydayley/pif-factory && /Users/kolbydayley/.local/libexec/pif-sdk-pipeline-controller --project-root /Users/kolbydayley/pif-factory serve --once
```

- There is deliberately **no launchd agent**. The stale plist (pointing at
  Documents) was removed to `work/pif-ops/removed-launchagents/`. Do not
  reinstall it; if launchd ever returns, regenerate from
  `automation/com.kolby.pif.sdk-pipeline-controller.plist.template` (already
  repointed at the live tree) and *first* disable the scheduled task — never
  run both.
- The nine legacy `pif-*` jobs in `~/.codex/memories/automation/jobs/` stay
  disabled. Backfill returns only as a GLM-lane task after the agreement gate
  holds.

## Budget

- Ledger: `pif_subscription_budget_ledger` (append-only) in
  `data/factory.sqlite`; module `research_factory/subscription_budget.py`.
- Daily receipt: `work/pif-ops/budget/<day>/budget-receipt.json`.
- Check remaining budget:

```bash
cd ~/pif-factory && PYTHONPATH=. python3 -c "
import sqlite3
from research_factory.subscription_budget import budget_gate
from research_factory.util import now_iso
conn = sqlite3.connect('data/factory.sqlite'); conn.row_factory = sqlite3.Row
print(budget_gate(conn, day=now_iso()[:10]))"
```

- **KILL engaged** (`work/pif-ops/budget/KILL`): every dispatch refuses, on
  every day, until the file is removed. Diagnose what bypassed the
  pre-dispatch gate before deleting it — removing KILL without a diagnosis is
  how the next billion-token wave happens.

## Outage / quota exhaustion

Signature: `codex exec` exits rc=1 in ~10s, log ends with *"You've hit your
usage limit … try again at <time>"*, zero `turn.completed` events.

- Handled automatically: the failed call reclassifies to `codex_usage_limit`,
  the remaining wave returns `provider_quota_exhausted`, **no durable
  attempts are spent**, and the day records as a `skipped` stage with a named
  blocker.
- The scale streak **freezes** over such days (`quota_frozen` in the scale
  receipt); no action needed to protect it.
- `usage_limit_retry_at` in the stage receipt says when the subscription
  resets.

## Provider policy / GLM

- Routing truth: `config/provider_policy.json`. The worker asserts stage
  models against it; the controller resolves its model from it; `--model` is
  an explicit override (None = policy).
- GLM executor: `research_factory/headless_codex.py::_run_glm_opencode` —
  opencode binary, identical output-file contract, metered as
  `provider_lane=opencode_glm` (does not consume the Codex cap).
- Agreement loop: `research_factory/provider_agreement.py`;
  `pif_provider_agreement` rows; per-stage thresholds in the policy file
  (episode_context ≥ 0.95, label_segment ≥ 0.90).
- **Flipping a stage to GLM**: set the stage's `provider`/`model` in the
  policy file (keep `fallback_provider`/`fallback_model`), commit, and ensure
  daily agreement sampling runs. A threshold breach writes
  `work/pif-ops/agreement/FREEZE_<stage>` and `effective_stage_policy`
  downgrades dispatch to the fallback automatically. Freezes never auto-lift:
  review the breach, then delete the freeze file.

## Review backlog

- Resolutions: `label_review_resolutions` — append-only side table; the
  canonical `labels` rows are never mutated. The 2026-08-10 bulk ruling
  resolved 6,302 (5,885 advisory, 417 bootstrap); actionable backlog ~2,039
  (402 defect-family, 80 no-reason, ~1,557 unique-prose).
- Status:

```bash
cd ~/pif-factory && PYTHONPATH=. python3 -c "
import sqlite3
from research_factory.review_drain import actionable_backlog
conn = sqlite3.connect('file:data/factory.sqlite?mode=ro', uri=True)
conn.row_factory = sqlite3.Row
print(actionable_backlog(conn))"
```

- Reverse a bulk pass:
  `DELETE FROM label_review_resolutions WHERE method = '<method-tag>'`.

## Watchdog

```bash
cd ~/pif-factory && PYTHONPATH=. python3 -m research_factory.ops_watchdog
```

Exit 1 on any alert. Checks: lane dead (>30h without a headless log), budget
KILL, agreement freezes, missing-yesterday daily receipt (streak risk).
Report: `work/pif-ops/watchdog/<day>.json`.

## Verification (after any change)

```bash
cd ~/pif-factory && PYTHONDONTWRITEBYTECODE=1 python3 -B -m pytest -p no:cacheprovider \
  tests/test_headless_codex_quota.py tests/test_subscription_budget.py \
  tests/test_daily_quota_freeze.py tests/test_provider_policy.py \
  tests/test_glm_provider_executor.py tests/test_provider_agreement.py \
  tests/test_review_drain.py tests/test_ops_watchdog.py \
  tests/test_daily_extraction_phase_a.py tests/test_daily_acceptance_gate.py \
  tests/test_sdk_pipeline_controller.py -q
```

Known pre-existing red (unrelated, artifact-dependent, other lane's domain):
`tests/test_app_server_adoption_alignment_measured_score.py`.

## Privacy

Unchanged: transcripts and prompt files are private local artifacts; do not
move `corpus/` into public hosting; publish only sanitized snapshots/reports
citing segment IDs and short evidence spans. `python3 -m research_factory
privacy-scan` before any export.
