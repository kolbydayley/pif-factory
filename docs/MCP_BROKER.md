# MCP Broker

The MCP broker exposes the Podcast Intelligence Factory to ChatGPT Apps and other MCP-capable AI surfaces without moving canonical compute off the Mac.

## Role

- Local SQLite remains the source of truth.
- Local scheduled Codex jobs run extraction, review, validation, graph work, and observer publishing.
- Railway may host the MCP broker and observer UI.
- Railway must not run model extraction, review, graph judgment, transcription, queue processing cron, or long-running workers.

Phase 1 is control/status only. The enabled MCP tools are:

- `get_status`
- `get_queue_summary`
- `get_worker_policy`
- `request_local_cycle`
- `get_observer_url`

The extraction tools `claim_work`, `get_work_context`, `get_work_chunk`, `submit_work_notes`, `get_work_notes`, `submit_work_output`, `heartbeat_work`, and `release_work` are reserved for Phase 2.

## Run Locally

```bash
PIF_MCP_PUBLIC_URL=http://127.0.0.1:8081 \
PIF_MCP_AUTH_SECRET=dev-secret \
PIF_MCP_INGEST_TOKEN=dev-ingest \
PORT=8081 \
python3 -m research_factory.mcp_server
```

Health:

```bash
curl -fsS http://127.0.0.1:8081/healthz
```

MCP endpoint:

```text
http://127.0.0.1:8081/mcp
```

## Railway Service

Create a separate Railway service from the same repo with start command:

```bash
python3 -m research_factory.mcp_server
```

Required environment:

- `PIF_MCP_PUBLIC_URL`: public base URL of the MCP broker service.
- `PIF_MCP_AUTH_SECRET`: token signing secret.
- `PIF_MCP_INGEST_TOKEN`: secret used by local Codex bridge sync to publish sanitized state.
- `PIF_MCP_BROKER_DB`: optional broker SQLite path; use Railway Postgres adapter before enabling Phase 2.
- `PORT`: provided by Railway.

The existing observer service remains:

```bash
python3 -m research_factory.ui_server
```

## Local Bridge Publish

Local Codex jobs publish sanitized state to the broker:

```bash
python3 -m research_factory mcp-broker publish-snapshot \
  --url https://<mcp-broker-service>.up.railway.app \
  --token-file .mcp-broker-ingest-token.local
```

The bridge snapshot is sanitized and does not include active job target IDs, prompt paths, output paths, raw transcript text, or local artifact lists.

## ChatGPT App Setup

Use the MCP server URL:

```text
https://<mcp-broker-service>.up.railway.app/mcp
```

Use OAuth authentication. The broker exposes:

- `/.well-known/oauth-protected-resource`
- `/.well-known/oauth-authorization-server`
- `/oauth/register`
- `/oauth/authorize`
- `/oauth/token`

Scopes:

- Phase 1: `factory.status`, `factory.control`
- Reserved for Phase 2: `factory.claim`, `factory.submit`

## Phase 2 Context Contract

Phase 2 uses a manifest/chunk/notes workflow instead of returning a full transcript from `get_work_context`.

- `get_work_context` returns compact instructions, output schema, a chunk manifest, and `first_chunk_id`.
- `get_work_chunk` returns one bounded transcript chunk and `next_chunk_id`.
- `submit_work_notes` stores compact per-chunk notes for the current worker.
- `get_work_notes` returns compact accumulated notes and chunk coverage.
- `submit_work_output` remains the only final remote output path; local import still performs canonical validation before SQLite mutation.

Default chunk size is `8000` characters and can be adjusted with `PIF_MCP_CONTEXT_CHUNK_CHARS`.

## Safety Gate For Phase 2

Do not enable extraction tools until:

- OAuth has been tested from ChatGPT.
- Broker audit logs are visible in observer output.
- Local importer validates remote submissions idempotently.
- One `full_text_allowed` job completes end-to-end with local import.
- Railway cost guard passes with only observer, MCP broker, and approved storage services.
