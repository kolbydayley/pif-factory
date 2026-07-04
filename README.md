# Research Intelligence Factory

Local-first research factory for building a private, extensible technology discourse corpus. Podcasts are the first production adapter; the queue and content model are designed to extend to blogs, Substacks, YouTube captions, papers, release notes, docs, and newsletters.

It is designed for high-volume Codex usage without turning the result into loose summaries:

- SQLite queue with idempotent jobs and leases
- portable queue envelopes for local headless Codex workers and a future Railway MCP/ChatGPT worker seam
- content abstractions for sources, items, artifacts, spans, context packages, and extraction runs
- 57 verified podcast feeds across AI, engineering, venture, markets, security, and tech strategy
- creator RSS or official public transcript ingestion by default
- local raw transcript storage
- strict versioned label-pack schemas
- Codex app prompt/output handoffs for subscription-backed labeling
- sanitized Railway observer UI snapshots
- repeatable exports for trends and graphs

Active roadmap: [docs/ROADMAP.md](docs/ROADMAP.md). The next graph layer turns v3.1 discourse events into governed canonical identity, expert influence, agreement/disagreement, and authority-score surfaces. Raw mentions stay separate from canonical entities, and high-impact merges require GPT-5.5 judge rationale plus deterministic validation.

## Quickstart

```bash
cd /Users/kolbydayley/Documents/Codex/podcast-intelligence-factory
python3 -m research_factory init
python3 -m research_factory preflight --model gpt-5.4
python3 -m research_factory enqueue --lane podcast --since 2026-06-01 --source-list config/sources.yaml
python3 -m research_factory queue sync-envelopes
python3 -m research_factory queue status --by lane,content_type,role,status
python3 -m research_factory railway-cost-guard
python3 -m research_factory transcript-candidates --lane podcast --limit 8
python3 -m research_factory run --lane podcast --limit 20 --model gpt-5.4
python3 -m research_factory snapshot
```

If feeds do not expose `<podcast:transcript>` links, use the candidate list as a bounded Codex research queue. Attach only verified official transcript pages or public creator-channel captions:

```bash
python3 -m research_factory attach-transcript \
  --episode-id ep_... \
  --transcript-url https://official.example/transcript \
  --transcript-type text/html \
  --source-kind official_show_transcript
```

If `run` reaches label jobs, it writes prompt/output handoffs under `runs/`. A Codex app worker should read each prompt, write only valid JSON to the named output path, then submit it:

```bash
python3 -m research_factory submit --job-id 123 --output-json runs/outputs/run_abc.json
```

For tests and bootstrapping only:

```bash
python3 -m research_factory run --lane podcast --limit 20 --local-draft
```

## Local Headless Worker Shape

The current scaling path is local headless Codex workers, not remote MCP workers:

```bash
python3 -m research_factory worker run --role acquisition --lane podcast --limit 4 --worker-id headless-acquisition
python3 -m research_factory worker run --role extractor --lane podcast --limit 4 --model gpt-5.5 --label-pack ai_discourse_v3_1 --worker-id headless-extractor
python3 -m research_factory worker run --role reviewer --lane quality --limit 4 --model gpt-5.5 --label-pack ai_discourse_v3_1 --worker-id headless-reviewer
```

`remote-queue` commands exist only as a disabled contract smoke surface for a later Railway MCP/ChatGPT scheduled-task adapter. The local SQLite database remains canonical.

```bash
python3 -m research_factory remote-queue claim-job --worker-id future-chatgpt-task --capability extractor
```

## Local Production Cycle

Production compute runs locally on the Mac. Railway is only a thin observer/storage edge.

```bash
python3 -m research_factory production-cycle --worker-mode none
python3 -m research_factory production-cycle --worker-mode bounded --publish
```

`--worker-mode none` syncs queue envelopes, writes the scale report, generates a sanitized snapshot, and privacy-scans exports. `--worker-mode bounded` also runs small local worker batches. Snapshot publishing is opt-in with `--publish`.

## Railway Observer UI

Live observer:

```text
https://observer-ui-production.up.railway.app
```

This repo includes a small no-dependency Python web service:

```bash
python3 -m research_factory.ui_server
```

It serves a dashboard at `/`, a health check at `/healthz`, and a token-protected snapshot ingest endpoint at `/ingest-snapshot`.

Railway must stay observer-only. Before and after deploys:

```bash
python3 -m research_factory railway-cost-guard
```

The guard fails if Railway has unexpected services, cron, volumes, more than one replica, or a start command other than the observer UI server.

Local publishing flow:

```bash
python3 -m research_factory snapshot --output exports/observer-snapshot.json
python3 -m research_factory publish-snapshot \
  --snapshot exports/observer-snapshot.json \
  --url https://observer-ui-production.up.railway.app \
  --token "$(cat .railway-ingest-token.local)"
```

The snapshot intentionally contains operational state only: counts, queue status, active/failed jobs, recent label runs, artifacts, and intervention flags. It does not publish raw transcripts.

## Outputs

```bash
python3 -m research_factory export trend-report --topic agi --window month
python3 -m research_factory export graph --type guest_network
python3 -m research_factory privacy-scan
```

## Boundaries

- Private analysis only by default.
- No app-only/paywalled transcript scraping.
- Attach only verified official transcript/caption URLs.
- Remote workers may only claim jobs whose privacy tier explicitly allows remote context.
- Railway runs no extraction workers, no queue processors, no cron jobs, and no model calls.
- No full transcript publishing.
- No raw finance transactions.
- No trade recommendations.
- No OpenClaw production mutations from this project.
