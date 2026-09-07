# Gold completion control

Owner: this Codex task, `01a04040-77ee-77f2-9d26-a5ca1ae56986`.
Authorization updated by Kolby on 2026-09-07: goals, exit hooks, and a scheduled
backstop are explicitly authorized. Older blanket scheduling prohibitions no
longer apply to the one backstop below.

## Three layers

1. **Active task goal:** remains active across intermediate process exits and
   substage completion. An authored dataset is not an accepted dataset.
2. **Process-exit wrapper:** writes child exit/output receipts and sends a
   continuation message to this task. Owner discovery retries before dispatch;
   an uncertain start-turn delivery is not resent. A separate task acknowledgement
   is evidence of wake-up, not the wrapper's success flag.
3. **Native 15-minute heartbeat:** `signal-desk-completion-monitor` is the sole
   active backstop. It checks actual work, detects missed continuation, and resumes
   the next authorized bounded stage. The older gold supervisor remains paused
   because its operating instructions are obsolete.

Heartbeat recovery must be labeled heartbeat recovery, never retroactively
reported as successful process-hook delivery. An already-running worker is not
relaunched. Capacity backoff is honored. A stalled process is diagnosed before
interruption; app hosts are not restarted. All budget reservations, operator
stops, grant expiry, privacy and sealed-split controls remain binding.

## Completion sequence

- Finish bounded shared-rubric A/B/C authoring; qualify expected judgments with
  GPT-5.5 and measure consistency across authors, auditor and final approval.
- Resolve rubric/scorer defects with versioned experiments, not repeated voting
  until an event passes. Preserve the frozen benchmark, rejected records and
  original audit denominators. Quarantine is not successful labeling.
- Finish required gold work and independent per-split audit slices. Validation
  and holdout item-level answers remain sealed. Report each actual gate result.
- Run A1 only on accepted dev gold; derive gates from measured frontier bounds.
  Complete A2 scorer qualification under the correct version before tournament
  progression. No threshold may be bypassed to declare completion.
- Produce a final reconciliation of authored, accepted, quarantined, unresolved
  and audited counts with evidence receipts. State exactly what remains outside
  the completed scope; do not imply production was rebuilt or deployed unless
  independently verified.

An unavailable provider, exhausted grant, missing source evidence or unresolved
user choice is a real blocker to report—not completion. Do not create new spending
authority or conceal a blocked lane. Pause the backstop only after verified goal
completion or a subsequent explicit user stop.

## Limits

Local execution requires the Mac and Codex services to be available. Sleeping,
offline or closed-host periods can delay both workers and wakeups. This design
adds recovery paths; it is not a guarantee of uninterrupted execution. No-change
healthy checks stay quiet; report milestones, failed gates, persistent stalls,
required decisions and completion.
