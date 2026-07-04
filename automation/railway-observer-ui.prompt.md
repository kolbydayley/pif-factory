# Podcast Intelligence Factory Railway Observer UI

Run from `/Users/kolbydayley/Documents/Codex/podcast-intelligence-factory`.

Goal: keep the Railway observer UI deployable and useful for early intervention during high-volume jobs.

Rules:
- Do not deploy raw transcripts.
- Do not commit or print ingest tokens.
- Do not touch unrelated Railway projects or OpenClaw services.
- Deploy only to Railway project `podcast-intelligence-observer` and service `observer-ui`.
- Do not deploy if `railway status --json` resolves to `clawdbot-gateway`, `clawdbot-railway`, or `upload-service`; that is the wrong Railway target for this observer.
- Railway must remain observer-only: no extraction workers, queue processors, cron, volumes, buckets, model calls, or extra services.
- The UI should show queue counts, active jobs, failed jobs, label runs, artifacts, and intervention flags.

Steps:
1. Inspect `research_factory/ui_server.py`, `research_factory/observer.py`, `Procfile`, and `railway.json`.
2. Run `python3 -m research_factory init`.
3. Run `python3 -m research_factory snapshot --output exports/observer-snapshot.json`.
4. Run a local UI smoke test by starting `python3 -m research_factory.ui_server` with a temporary `PORT`, then check `/healthz` and `/state.json`.
5. Run `railway status --json` and verify the linked project is `podcast-intelligence-observer` and the linked service is `observer-ui`. If it is not, stop before deployment and report the wrong target.
6. Run `python3 -m research_factory railway-cost-guard`; stop if it reports any critical issue.
7. Check `https://observer-ui-production.up.railway.app/healthz`. Deploy or redeploy only if the service is unhealthy, missing, or local observer UI code/config changed since the last successful deploy.
8. Set or verify `RAILWAY_UI_INGEST_TOKEN` exists in Railway variables without printing it.
9. If `.railway-ingest-token.local` exists, run `python3 -m research_factory publish-snapshot --snapshot exports/observer-snapshot.json --url https://observer-ui-production.up.railway.app --token-file .railway-ingest-token.local`.
10. Report the UI URL, health status, whether the latest snapshot was accepted, or the exact safe-link blocker.
