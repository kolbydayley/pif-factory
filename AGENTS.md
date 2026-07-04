# Podcast Intelligence Factory Guidance

- Keep this project private-analysis-only by default.
- Do not publish raw transcripts, long copyrighted excerpts, secrets, tokens, or private financial rows.
- Use the local SQLite queue as the source of truth for jobs.
- Prefer Codex app prompt/output handoffs over OpenAI API billing unless Kolby explicitly asks for API usage.
- Keep observer UI snapshots sanitized: operational counts, queues, active jobs, failed jobs, artifacts, and intervention flags only.
- Do not touch OpenClaw or Finance production lanes from this project.

