# Research Radar

Private, local, event-driven research intelligence. The first workspace is
**AI & Technology Radar**. Its product unit is an evidence-linked briefing when
something materially changes—not an evaluator receipt, per-source summary, or
scheduled digest.

The v1 implementation provides:

- an isolated SQLite authority store outside Documents/FileProvider storage;
- explicit provisional, verified, amended, retracted, and dismissed states;
- exact evidence offsets and release lineage for every semantic record;
- bounded source probation, queue leases, retries, item budgets, and cycle budgets;
- graph-backed brief, timeline, entity-position, source-ledger, and operations queries;
- a loopback-only local dashboard and JSON API with briefing feedback;
- frozen product, schema, benchmark, workspace, and budget contracts; and
- a deterministic five-item product slice that contains no copied source text.

The prior Podcast Intelligence Factory ingestion and transcript backbone remains
available for recovery and future adapters, but its evaluator epochs and pending
label queue are not Research Radar authority.

See [the frozen v1 contract](docs/RESEARCH_RADAR_V1.md) for product boundaries,
acceptance gates, and explicit deferrals.

The isolated post-extraction podcast benchmark is documented in
[AI-Safety True-North Downstream Benchmark](docs/TRUE_NORTH_BENCHMARK.md). It
uses hash-pinned candidates, OpenCode workhorses, Codex-owned gold, and
shadow-only transactional commits. Its consensus-aware contract preserves
independent-gold disagreement, accepts valid atomic-count ranges, and makes
research utility the primary certification gate.

## Quickstart

```bash
cd /Users/kolbydayley/Documents/Codex/podcast-intelligence-factory
python3 -m research_factory.radar_cli validate-contract
python3 -m research_factory.radar_cli init
python3 -m research_factory.radar_cli seed-demo
python3 -m research_factory.radar_cli serve --host 127.0.0.1 --port 8767
```

The default database is
`~/Library/Application Support/Research Radar/research-radar.sqlite3`. Put
`--db /absolute/test.sqlite3` before a subcommand to use an isolated database.
The equivalent installed entry point is `pif radar ...`.

The demo produces five provisional briefings so the inbox, timeline, evidence,
position history, source controls, and feedback can be inspected. It does not
verify sample claims. Verification always requires an explicit acceptance event
and the configured source-evidence rule.

The scheduler and model transport are intentionally disabled. They remain gated
on the frozen 30-item benchmark and accepted seven-day live shadow.

## Legacy recovery surfaces

The commands below document the preserved ingestion/evidence backbone. They are
not the Research Radar production path, and frozen evaluator epochs must not be
resumed or imported into the new authority store.

If a public feed does not expose a transcript link, attach only verified official
transcript pages or public creator-channel captions:

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
