# Podcast Intelligence Factory Guidance

## Parallel-agent checkout — September 8, 2026

- This independent clone is `/Users/kolbydayley/pif-factory-agent`, on branch
  `codex/signal-desk-agent-handoff`. Kolby requested it so another agent can work
  without altering the original checkout.
- Frozen baseline: `7f72e6bf44bfc1fcdfae93a4f033fb119bb1bd48`, tagged
  `snapshot/signal-desk-2026-09-08-7f72e6b` in both local repositories.
- Do not edit, run jobs against, or publish from `/Users/kolbydayley/pif-factory`.
  Several scripts and inherited instructions contain that absolute path or use
  `Path.home() / "pif-factory"`. Audit and isolate paths before executing them.
- Private ignored databases, transcripts, sealed gold, credentials, operational
  receipts, and locks were NOT copied. Their absence is not permission to access
  or modify the original campaign, bypass seals, or launch replacement workers.
- This clone does not authorize paid jobs, production deployments, or resuming
  the original task's hooks/monitors. Obtain direction for those actions.
- Begin with `docs/signal-desk/comprehensive-chat-handoff-2026-09-08.md` and
  `docs/signal-desk/repository-handoff-snapshot-2026-09-08.md`. Runtime observations
  there are timestamped history, not current liveness checks.

- Keep this project private-analysis-only by default.
- Do not publish raw transcripts, long copyrighted excerpts, secrets, tokens, or private financial rows.
- Use the local SQLite queue as the source of truth for jobs.
- Prefer Codex app prompt/output handoffs over OpenAI API billing unless Kolby explicitly asks for API usage.
- Keep observer UI snapshots sanitized: operational counts, queues, active jobs, failed jobs, artifacts, and intervention flags only.
- Do not touch OpenClaw or Finance production lanes from this project.
