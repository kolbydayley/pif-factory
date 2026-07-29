# Pipeline supervision watchdog (retired)

> **Current state, 2026-07-15:** both `pif-pipeline-watchdog` and
> `pif-pipeline-babysitter` are disabled. They are retained only as incident
> evidence and rollback code. The active replacement is documented in
> `PIF_NATIVE_GOAL_SUPERVISOR.md`. Do not re-enable either shell supervisor.

The former fallback supervisor was the scheduler-managed job
`pif-pipeline-watchdog`. It checked deterministic pipeline state every five
minutes and also received a best-effort immediate kick when a detached Codex turn
exited.

## Ownership boundary

- `codex-scheduler` decides when the job is due.
- `codex-cron` is the only scheduler execution boundary; it applies the job lock,
  working directory, timeout, environment, logs, and run history.
- `~/.codex/bin/pif-pipeline-watchdog` verifies and execs the checksum-bound
  home-scoped archive `~/.codex/lib/pif-pipeline-runtime.pyz` while starting
  from `/Users/kolbydayley`. It never imports code from the project directory.
- The watchdog observes deterministic state and launches at most one bounded
  control turn. A normal Codex `task_complete` is not pipeline completion.
- The pipeline phase and verified immutable artifacts govern completion. The
  target thread's historical goal-database status can lag execution.

## Why the home-scoped packaged runtime is mandatory

On 2026-07-14, two separate launchd-native failure modes were sampled. The first
hung before Python executed any module code, in
`_PyPathConfig_ComputeSysPath0 -> _Py_wgetcwd -> getcwd -> open$NOCANCEL` while
its current directory was the project folder. Moving the working directory home
fixed that path, but an untouched later check hung during repository package
import in `os_listdir -> opendir -> open$NOCANCEL`. This established that the
project filesystem could stall both startup and import before the watchdog's
Python signal deadline could protect it.

Do not change the watchdog job's `working_directory` back to the repository.
Do not restore repository code to its startup `PYTHONPATH`. The scheduler-facing
runtime contains only the babysitter and watchdog modules in a home-scoped
zipapp. Project-backed milestone and SQLite observations run in disposable
children with hard deadlines; a stuck child yields a sanitized unavailable
snapshot and cannot wedge the parent. Control launches also use the archive and
a bounded child runner whose timeout never performs an unbounded final wait.

Rebuild the archive after changing either supervision module:

```bash
/opt/homebrew/bin/python3 \
  /Users/kolbydayley/Documents/Codex/podcast-intelligence-factory/automation/build_pif_watchdog_runtime.py
```

The builder atomically replaces the archive and its SHA-256 manifest. The
home-scoped wrapper refuses to execute a missing or mismatched archive.

## Failure behavior

- One scheduler error creates a high-severity local operator item.
- A successful recovery resolves that item.
- The scheduler records `last_success_at` and separately alerts if the critical
  watchdog has no success for 660 seconds.
- Calendar jitter has bounded interval leeway so a heartbeat a few seconds early
  does not turn a five-minute cadence into ten minutes.
- Overall local observation is capped at 30 seconds, each project-backed probe
  at 10 seconds, and a control launch at 45 seconds.
- A live outer process is recycled only after 30 minutes with no session,
  milestone, or queue progress. This is deliberately above the managed judge's
  20-minute turn deadline but below the former one-hour blind spot.
- A receipt-owned process that never emits its own `task_started` is a startup
  stall after 10 minutes. An older turn's `task_complete` timestamp is never
  attributed to a newer launch.
- Runtime path, version, SHA-256, and OpenAI code signature are verified before a
  fallback CLI launch and recorded in the launch/exit receipts.
- A raw open turn can be overridden only when the prior detached owner is proven
  dead, its terminal receipt matches the exact thread/session (and launch ID for
  new launches), and no newer manual/app turn began after that owner exited.
- Recovery is a two-phase transaction. The babysitter writes a UUID-bound
  `pending_control_launch` under a brief flock, releases the lock, and only then
  invokes tmux. The runner starts in `$HOME`, writes its launch-ID receipt there,
  and passes `--cd <project>` to Codex before `resume`; no project access or
  external process call occurs while the pipeline state lock is held.
- An exact turn-start event or exact-thread Codex process promotes the pending
  reservation. Process-based acceptance additionally requires the launch-ID
  receipt's exact `codex_pid`; an unrelated/manual process cannot commit the
  reservation. Proven auth/CLI pre-dispatch exit classes can retry. A timeout,
  generic exit, or otherwise ambiguous launch remains pending and fails closed,
  so no later heartbeat can blindly submit the same semantic turn twice.
- Every new detached Codex child runs in a dedicated process group and records
  that PGID in its launch receipt. Recycling requires an exact launch ID,
  thread, PID, PGID, pinned-runtime match, and unchanged kernel start identity.
  The watchdog sends bounded TERM/KILL only to that verified group, proves it is
  gone, and writes a terminal `watchdog_recycled` receipt. tmux is cleanup, not
  proof of ownership or successful termination.
- Control turns initialize from the home-scoped `pif-control-workspace`; the
  recovery capsule supplies the absolute project root and requires explicit
  project workdirs. This keeps project FileProvider access out of CLI startup.
- Recent raw-open lifecycle state and unavailable process liveness never count
  as inactivity. Only a stale, exact-owner orphan with available/dead process
  evidence reaches the recovery path.

## Verification

```bash
cd /
/Users/kolbydayley/.codex/bin/pif-pipeline-watchdog run --debounce-checks 1
/Users/kolbydayley/.codex/bin/codex-cron run pif-pipeline-watchdog
jq '.tasks["pif-pipeline-watchdog"]' \
  /Users/kolbydayley/.codex/memories/automation/scheduler/state.json
tail -n 5 \
  /Users/kolbydayley/.codex/memories/automation/pif-pipeline-watchdog/events.jsonl
```

A healthy active evaluation reports `active_progress` or `active_wait`, exits
zero, and updates `last_success_at`. The forced wrapper and scheduler-native
paths should both finish in seconds. Routine successful checks do not send
Telegram messages.

The 2026-07-14 launchd verification deliberately exercised the bad path:
successive untouched heartbeats encountered `milestone_snapshot_timeout`,
returned in about 12 seconds, kept the target active, recorded fresh successful
scheduler state, resolved the prior failure/freshness alerts, and left no hung
watchdog process.

The immediate exit kick was also raced against another `codex-cron run` on
purpose. One invocation performed the check and the other returned a structured
zero-exit `skipped_locked` record; neither raised, launched a duplicate, or
generated a false scheduler failure.

### 2026-07-14 orphaned-process correction

A later live incident exposed two bugs that the first hardening pass did not
cover. A new launch at 02:25 emitted no lifecycle events, but the watchdog read
the prior turn's 02:22 `task_complete` and killed tmux five minutes later. The
shell runner had backgrounded Codex, so tmux exited while Codex PID 13127 and its
child survived under PID 1. The receipt stayed `running/codex_dispatched` and 64
successive scheduler cycles returned `recycle_failed` because the old recycle
path attempted only `tmux kill-session` even though no tmux server remained.

The corrected path binds completion to the current launch, detects pre-turn
startup stalls separately, and treats an exact receipt-owned process group as
the termination boundary. A controlled recovery terminalized the legacy launch
as `watchdog_recycled` and started replacement launch
`6624d576-5ce2-499d-8276-7bf382f86595` in dedicated PGID 73090. Its event stream
emitted `thread.started`, `turn.started`, and fresh rollout activity immediately.
The next untouched launchd heartbeat at 07:55 returned `active_progress`, reset
the scheduler's consecutive-error count to zero, and refreshed `last_success_at`.
The focused watchdog, launch, SDK-controller, and scheduler suite passed 72 tests.

### 2026-07-14 desktop-ownership correction

The process-level repair above still did not satisfy desktop-visible liveness.
`codex exec resume` owned a separate transport and could append to the target
rollout while the Codex app's own thread registry correctly reported the thread
as `idle`. Rollout growth, a live tmux session, or watchdog `active_progress`
therefore must not be presented as proof that the desktop chat is running.

The local `pif-pipeline-watchdog` schedule was disabled and detached launch
`78581ee4-af13-46fc-85c3-7d9d96cf3ccb` was terminated with receipt exit code
143. Continuation moved to a Codex-native thread turn for
`019f4cf1-c46e-7db3-acd2-bf03c4459a10`; the app registry then reported
`active`, desktop-owned turn `019f60d4-1f6f-7e81-97bc-e4de53d09cc7` began, and
the desktop app-server became the sole rollout writer.

The original app-native heartbeat later failed for two independent reasons. Its
prompt allowed the evaluated thread to disable its own supervisor after declaring
an external blocker, and a scheduled thread turn did not reactivate the separate
Goal-mode record. The target used the first escape hatch after v66, left its goal
persisted as `blocked`, paused the heartbeat, and restored a stale five-minute
recurrence. Consequently, no four-hour run was ever due.

The replacement continuation owner is
`~/.codex/automations/pif-evaluation-goal-resumer-v2/automation.toml`. It remains
`ACTIVE` on a four-hour interval and targets the exact evaluation thread. Every
scheduled turn must first run the sanitized app-server goal-control preflight,
which performs `thread/goal/get`, status-only `thread/goal/set` to `active` when
the goal is blocked, and a verifying `thread/goal/get`. Prompt text alone is not
a goal resume. Evaluation artifacts are evidence only and cannot authorize
pausing, disabling, or rescheduling this operator-owned heartbeat. An external
blocker leaves the heartbeat active for a later recheck; only verified completion
of the full workflow or a direct user instruction may stop it.

Do not re-enable the shell watchdog or use `codex exec` to resume this thread.
For this workflow, accepted liveness requires both an authoritative goal status
of `active` and the Codex app registry to report `active` for the target with
fresh activity from its native turn; filesystem activity alone is insufficient.

## Prospective replacement

`sdk_pipeline_controller.py` is the non-activated replacement path. It uses the
official Python Codex SDK/app-server with managed ChatGPT authentication and a
sanitized durable ledger. Keep its LaunchAgent disabled until the disposable
turn, crash-reconciliation, sleep/wake, and single-owner chaos tests in
`SDK_PIPELINE_CONTROLLER.md` pass.
