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

### Signal Desk clean-corpus rebuild exception

The ordinary 5M/day authority above is unchanged. The Signal Desk rebuild has
a separate, scope-bound grant at
`config/signal_desk_rebuild_budget_grant.json`: 20M GPT-5.5 tokens/day for
campaign `signal-desk-clean-corpus-2026-08-31`, expiring after 45 days or the
first clean release. The rebuild governor is
`research_factory/signal_desk_rebuild_budget.py`; other lanes cannot use it.

The exception is GPT-5.5-only. GPT-5.6-sol gold authoring stays under the
ordinary 5M/day governor and uses lane `gpt_5_6_sol_gold_authoring`. Freeze its
split-isolated plan and inputs without spending tokens:

```bash
cd ~/pif-factory && python3 -B scripts/pif_signal_desk_rebuild_plan_gold.py \
  --materialize-inputs
```

The expected receipt is 2,493 gold calls (804 each for A/B/C plus an 81-window
blind audit), with validation, holdout, and audit packets under
`sealed_item_storage`. Never copy those item files into development paths.
The semantic canary reserve is 39,657 tokens/call. Keep a 10% safety margin:
no more than 113 gold calls in one otherwise-empty 5M budget window. Every
started call, including failures, must append usage to the shared subscription
ledger with lane `gpt_5_6_sol_gold_authoring`. Do not run bulk gold until the
leased worker enforces both reservation and append-only metering.

Before full rebuild, the campaign must record 5 consecutive useful-work days
(7 preferred) at 18M-20M tokens without a provider quota failure. A file named
`KILL-signal-desk-clean-corpus-2026-08-31.json` means the 120% campaign limit
tripped. Stop dispatch, reconcile every contributing ledger, identify the
bypass, and record the diagnosis. It does not clear at midnight. Only Kolby or
an operator explicitly delegated by Kolby in the current turn may remove it.

### Gold runner keepalive

`scripts/pif_signal_desk_gold_resume.py` is fail-closed: any resumable stop
(one provider timeout, a weekly-gate stall, "checkpoint preserved") exits the
process. A contract failure on a single window (excerpt not exact at its
declared offsets) gets one fresh attempt as a resurrected lineage
(`MAX_GOLD_CONTRACT_ATTEMPTS = 2`; the rejected output is never reused, its
sidecar is archived as `.semantic-rejected`); a second failure quarantines the
window and the swarm continues. At phase start the runner resurrects any
quarantined window that still has an attempt to spare. Windows quarantined
after both attempts need an explicit `resurrect_task` or a gate ruling before
the downstream all-window stages. Two layers relaunch a dead runner:

- tmux session `signal-desk-gold`, window `keepalive`: `pif_signal_desk_gold_keepalive.py --loop` (60s).
- codex-cron job `pif-gold-keepalive`: the same script with `--once` every 5 min; survives a reboot.

Policy (`research_factory/signal_desk_gold_keepalive.py`): relaunch only when
`pgrep -f 'python3 -B scripts/pif_signal_desk_gold_resume.py'` finds nothing
AND `artifacts/gold-resume-supervisor.json` is resumable. It never relaunches
over `KILL-signal-desk-gold-authoring.json`, a `complete` checkpoint, or an
operator-required error (hash verification, symlink/0700, authorization,
unknown split/phase, phase incomplete, non-complete receipt). Backoff 60s
doubling to 30 min, reset after 30 min of healthy uptime. State and log:
`artifacts/keepalive-state.json`, `logs/keepalive.log`. To hold it
deliberately, drop the KILL receipt or `codex-cron disable pif-gold-keepalive`
and kill the `keepalive` tmux window.

Throughput: the adaptive limiter (`signal_desk_adaptive_concurrency`, lane
`gold`, bounds 2-8) drops 2 slots on `rate_limit>2%`, `timeout>5%`,
`parse_schema>2%`, or p95>900s over a 600s window, then climbs back 1 slot
per three healthy evaluations (every 2 min 02-13 UTC, every 10 min on the
shoulder, never during the 22-02 UTC peak). At full speed a window holds ~13
calls, so a single mis-classified event trips it. Contract failures are
recorded as the neutral `failure` outcome for that reason. If the campaign is
crawling, check `signal_desk_adaptive_concurrency_state.effective_limit` and
`last_trip_reason` before suspecting the provider; true concurrency is the
overlap of `started_at..completed_at` in the dispatch DB, not the count of
capacity leases (those live 30 minutes).

Leftover sidecars: any sidecar left under `sealed-gold-results/<split>/sidecars/`
by a stopped turn (timeout `interrupted`, `failed` with an unknown provider
error, a killed runner's `in_progress`, a `completed` turn whose output was
rejected) is archived into `recovery-sidecars/<turn>/` and the window retried
on its next lease, **provided no output artifact exists** under
`<split>/<turn>/` — the runner writes the output only after validation, so its
absence proves nothing accepted can be lost. With an output present nothing is
archived. If a runner ever flaps on `AppServerRecoveryRequired: turn sidecar
already exists`, that invariant is being violated somewhere; inspect before
moving anything by hand.

Limiter outcomes are keyed on the exception class, never on message text:
recovery errors embed the sidecar path, and `.../validation/sidecars/...` once
matched the `parse_schema` token "validation". A trip is charged once per
event: later evaluations only count events newer than the last trip, otherwise
one bad event re-trips every 120s until it ages out of the 600s window and
drains the lane to its minimum. To restore a falsely tripped lane use
`set_effective_limit(conn, lane="gold", effective_limit=8, reason=...)`, not
a direct UPDATE, and only after the offending events are older than 600s.

Liveness: a bare `pgrep -f` count lies — it also matches the tmux server that
was started with the runner command and any shell whose script mentions it.
Require the executable to be `python`/`Python`/`caffeinate` (that is what the
keepalive does). To restart the runner on new code, `kill -INT` the **python**
pid (not caffeinate, not tmux); the client writes `cancelled` sidecars that are
retried on the same lineage. Then let the keepalive relaunch it: it first frees
the dead runner's 30-minute capacity admissions, otherwise the relaunched
runner defers on `gold_model_capacity_slots_full` until they lapse.

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
