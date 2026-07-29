# PIF native goal supervisor

## Outcome

The former babysitter and shell watchdog are disabled. The active control path is
the scheduler-managed deterministic command `pif-native-goal-supervisor`, every
240 minutes with run-at-load catch-up and a 30-minute failure retry. It supervises
the exact evaluation thread `019f4cf1-c46e-7db3-acd2-bf03c4459a10` without using
`codex exec`, API-key billing, raw session tokens, or a second target-thread
writer.

The Desktop-native actuator remains
`pif-evaluation-goal-resumer-v2`. Its operator-owned prompt, active status,
four-hour recurrence, and target thread are restored from a checksum-bound
canonical copy on every healthy supervisor cycle.

## Semantic plan gate

Dispatch liveness and semantic progress are separate postconditions. A native
`task_started` event, Goal context, heartbeat context, generic reasoning, or a
tool call proves only that Desktop accepted a turn. It does not prove that an
evaluation decision advanced.

Before mutating a blocked or paused Goal, the supervisor now validates the
installed `~/.codex/lib/pif-evaluation-semantic-plan-v1.json`, its executable
step, and the SHA-256-bound directive named by that step. The exact plan epoch,
step ID, model-call cap, token cap, expected receipt path, and accepted terminal
receipt states are recorded in the sanitized supervisor receipt.

- An executable step with no terminal receipt may resume the Goal and dispatch
  the native heartbeat.
- A matching terminal `pif_semantic_plan_step_receipt_v1` is a safe continuation
  checkpoint. On an idle unfinished task, the supervisor resumes the Goal and
  dispatches one native heartbeat so the task can consume the receipt's
  passed/rejected/waiting branch without replaying the completed model call.
  A fresh open turn remains a no-op, so this never creates a second writer.
- A missing, malformed, mismatched, or hash-drifted plan/directive fails closed
  before Goal mutation.
- `native_turn_started` or `dispatch_activity_verified` never implies
  `semantic_progress_verified`. Only a matching receipt created after that
  supervisor cycle began proves the latter; a pre-existing receipt is reported
  as branch input, not new progress.

Semantic plans are immutable and versioned. The builder installs the unique
highest plan epoch, while prior plans and receipts remain checksum-bound
evidence. A terminal receipt therefore cannot permanently strand the overall
Goal while a versioned successor is being prepared. The `0.97` strict macro
quality and `0.28` production-token gates remain unchanged.

## Fifteen-boundary audit

Times are America/New_York. These are the last fifteen relevant stop/restart
boundaries, reconstructed from the target rollout, goal database, scheduler
history, watchdog receipts, and process ownership.

| # | Time | Observed boundary | Diagnosis |
|---|---|---|---|
| 1 | Jul 13 08:33 | One automatic continuation started immediately after completion. | Native same-owner continuation worked. |
| 2 | Jul 13 08:42 | The goal was explicitly set to `blocked`; the turn then completed. | A blocked goal and an idle turn became separate persistent states. |
| 3 | Jul 13 10:19 | A manual status turn completed without reactivating the goal. | Prompt activity was mistaken for goal control. |
| 4 | Jul 13 10:28 | A recovery turn aborted after about 19 minutes. | No postcondition proved a replacement turn remained live. |
| 5 | Jul 13 10:47 | A turn started, emitted no terminal event, and was followed by roughly five hours of silence. | Rollout lifecycle alone could not distinguish a live Desktop owner from abandoned detached state. |
| 6 | Jul 13 15:49 | A babysitter turn started and aborted at 17:15. | The babysitter itself timed out instead of supervising independently. |
| 7 | Jul 13 17:33 | A manual recovery completed at 18:49 while the goal remained blocked. | Turn restart did not imply goal restart. |
| 8 | Jul 13 21:01 | Another manual resume completed at 22:12 with the goal still blocked. | Same missing goal transition. |
| 9 | Jul 14 00:08 | A shell watchdog launched a detached continuation. | `codex exec resume` used the wrong transport owner. |
| 10 | Jul 14 07:52 | Another detached watchdog continuation completed. | Filesystem progress was reported as Desktop liveness. |
| 11 | Jul 14 09:02 | A short detached continuation completed and immediately started another. | Overlapping writers contaminated lifecycle evidence. |
| 12 | Jul 14 09:09 | A second short detached continuation repeated the pattern. | No single-owner lease or same-turn receipt. |
| 13 | Jul 14 09:20 | A third short continuation repeated it. | The watchdog was launching, not proving recovery. |
| 14 | Jul 14 09:25 | A detached start had no terminal before the native start at 09:32. | The target rollout contained competing transport histories. |
| 15 | Jul 14 13:46-17:25 | The native turn completed, then the replacement native actuator had to be forced due to start the current turn. | The old same-thread heartbeat had paused itself and changed cadence; no independent actuator repaired it. |

Supporting aggregate evidence:

- `pif-pipeline-babysitter`: 10 scheduler runs, 5 successful and 5 failed.
- `pif-pipeline-watchdog`: 127 scheduler runs, 58 successful and 69 failed.
- Watchdog receipts include 64 `recycle_failed` results and seven detached
  `resume_launched` results.
- The goal remained persisted as `blocked` from Jul 13 08:42 until the corrected
  native recovery path reactivated it on Jul 14.

## Root causes

1. Goal, turn, automation, and scheduler state were treated as one state machine,
   but they are four independent control planes.
2. A same-thread heartbeat cannot supervise its own disabled configuration, and
   its prompt was allowed to pause itself and change its recurrence.
3. `codex exec resume` appended to the same rollout through a different runtime;
   rollout growth therefore did not prove the Desktop chat was running.
4. The old supervisors performed broad artifact scans and model work before the
   liveness decision. Timeouts, resource deadlocks, and stale artifacts could
   stop the supervisor itself.
5. No restart transaction required all postconditions: goal active, exact native
   heartbeat accepted, same replacement turn, fresh progress, and one Desktop
   writer.
6. The four-hour cadence was sometimes expected to provide immediate recovery.
   Four hours is the intentional normal upper bound; failures retry after 30
   minutes.

## Corrected transaction

The external command now performs one bounded transaction:

1. Acquire a lifetime `flock` and classify the newest bounded rollout tail.
2. Require exactly one writable rollout owner and prove it is the Desktop Codex
   app-server.
3. On every cycle, including a fresh open turn, query the goal through the
   official app-server under managed ChatGPT auth. If it is `blocked`, set only
   its status to
   `active`, then verify that objective, token budget, and accounting are
   unchanged.
4. For a fresh open turn, repair the actuator configuration but do not make it
   due or dispatch another turn.
5. For terminal or stale state, restore the exact operator-owned heartbeat and
   transactionally set only that
   native automation due.
6. Recheck Desktop ownership immediately before dispatch.
7. Accept dispatch only when one new turn contains the same `task_started`,
   active-goal context, exact automation ID, and fresh activity, with one
   Desktop writer. Report semantic progress separately from the plan-step
   receipt. If the installed step already has a terminal receipt, dispatch only
   the continuation that selects and freezes its declared successor; never
   replay the completed call.
8. Append a sanitized fsynced JSONL receipt. A missing postcondition exits
   nonzero, invokes the scheduler's 30-minute retry, and escalates on the first
   failure.

If the goal is verified complete, the supervisor sends the deduplicated
completion notification first and then disables its own external schedule.

## Owner boundary

The Desktop app-server is shared and communicates with Electron through private
stdio pipes. A separately launched or managed app-server is not the live turn's
owner. It may safely change the persisted goal status, but it must never call
`thread/resume`, `turn/steer`, `turn/interrupt`, or `turn/start` for this same
Desktop-owned thread.

For a stale open turn, the supervisor first queues the exact Desktop-native
actuator and requires proof. If Desktop does not accept it within the bounded
verification window, the run fails and alerts; it does not report a false
restart, kill the shared Desktop process, or create a second writer. A hard
owner-directed interrupt is not exposed as a safe unattended local CLI for a
Desktop stdio thread.

Future unattended extraction must start under the persistent managed app-server
owner from its first turn. The worker and watchdog must share that exact owner
transport and thread lease; then the watchdog can safely use `turn/steer`,
`turn/interrupt`, `thread/goal/set`, and `turn/start` with compare-and-swap turn
IDs. Do not migrate the current thread while Desktop owns an active turn.

## Installed surfaces

- Source: `research_factory/app_server_thread_supervisor.py`
- Tests: `tests/test_app_server_thread_supervisor.py`
- Builder: `automation/build_pif_native_goal_supervisor_runtime.py`
- Semantic plan source: `automation/pif-evaluation-semantic-plan-v1.json`
- Current directive: `automation/pif-evaluation-shared-reference-repair-v1.json`
- Runtime: `~/.codex/lib/pif-native-goal-supervisor.pyz`
- Installed semantic plan: `~/.codex/lib/pif-evaluation-semantic-plan-v1.json`
- Wrapper: `~/.codex/bin/pif-native-goal-supervisor`
- Scheduler job: `~/.codex/memories/automation/jobs/pif-native-goal-supervisor.json`
- Receipts: `~/.codex/memories/automation/pif-native-goal-supervisor/events.jsonl`

## Verification

```bash
cd /
/Users/kolbydayley/.codex/bin/pif-native-goal-supervisor run
/Users/kolbydayley/.codex/bin/codex-cron run pif-native-goal-supervisor
python3 -m pytest -q /Users/kolbydayley/.codex/tests/test_scheduler.py
```

A healthy live target returns `fresh_open_noop`, `desktop_owned=true`, and
`writer_count=1`. A real restart is accepted only as `native_trigger_verified`.
`supervisor_failed` is a failed recovery, not liveness.

## Operator holds and accidental pauses

Goal status is not hold authority. An intentional pause is respected only when
`~/.codex/memories/automation/pif-native-goal-supervisor/operator-hold.json`
exists and validates as `pif_native_operator_hold_v1` for the exact target
thread. The marker contains only a UUID, timestamp, authorization identity, and
SHA-256 of the private reason; it never contains the reason itself.

The checksum-bound runtime reads the marker before any goal mutation. A valid
marker plus a paused goal returns `intentional_operator_hold_noop`. A paused,
unfinished goal with an absent or malformed marker is an accidental pause: the
runtime uses managed ChatGPT app-server auth for a status-only paused-to-active
mutation, then requires a newly appended `task_started`, matching goal and
heartbeat context, progress, and one Desktop-owned rollout writer before it can
report `accidental_pause_recovered_native_trigger_verified`.

The shell wrapper performs checksum verification only. It cannot infer a hold
from automation TOML or bypass runtime validation. The legacy babysitter and
watchdog jobs remain disabled, and the four-hour native supervisor remains the
only evaluation supervisor.
