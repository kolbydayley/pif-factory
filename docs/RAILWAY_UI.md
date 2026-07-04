# Railway Observer UI

The observer UI is a small Python service that can run on Railway and display sanitized operational state from the local factory.

Railway is intentionally minimal here. It is not an extraction, ingestion, queue processing, semantic review, graph, cron, or model-calling tier.

Live URL: `https://observer-ui-production.up.railway.app`

Railway target:

- Project: `podcast-intelligence-observer`
- Service: `observer-ui`
- Environment: `production`

## Environment

- `RAILWAY_UI_INGEST_TOKEN`: required secret for local snapshot uploads.
- `RAILWAY_UI_STATE_DIR`: optional writable state directory, defaults to `ui_state`.
- `PORT`: provided by Railway.

## Deploy

From this project:

The service has already been created and linked locally. Before deploying, verify:

```bash
railway status --json
railway service list --json
python3 -m research_factory railway-cost-guard
```

Railway service filesystem state is ephemeral across deploys. After every redeploy, republish the latest local snapshot.

After deployment:

```bash
python3 -m research_factory snapshot
python3 -m research_factory publish-snapshot \
  --url https://observer-ui-production.up.railway.app \
  --token-file .railway-ingest-token.local
```

## Security Model

- The service never receives raw transcripts.
- The ingest endpoint rejects snapshots missing the privacy marker.
- The token should be kept in local shell/automation env, not committed.
- The service should have exactly one Railway service, no cron, no volume, no buckets, no worker process, and no model/API extraction code path.
- `sleepApplication=false` is acceptable while live observer availability matters; do not add Railway compute to compensate for sleeping/cold starts.
