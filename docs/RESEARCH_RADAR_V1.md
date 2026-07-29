# Research Radar v1

Status: **frozen pilot contract** (2026-07-20)

Research Radar is a private, local, event-driven research product. Its unit of
value is an evidence-linked briefing about a meaningful development, not an
extractor run, evaluator score, source summary, or scheduled digest.

The first workspace is **AI & Technology Radar**. It asks:

1. What materially changed?
2. Which people or organizations changed position?
3. What evidence supports or contradicts the development?
4. What remains uncertain, and what should be watched next?

Briefings use neutral synthesis. They preserve competing interpretations,
position changes, unresolved questions, exact evidence, amendments, and
retractions. They do not recommend actions or maintain beliefs for the user.

## Authority and privacy boundary

- The Research Radar SQLite database is new authority. It does not import the
  legacy pending queue, deterministic claim clusters, authority scores, or
  experimental graphs.
- The default database is
  `~/Library/Application Support/Research Radar/research-radar.sqlite3`, outside
  Documents and macOS FileProvider storage. `--db` supplies an explicit test or
  recovery path.
- Provisional records remain visibly provisional. Only explicit acceptance may
  produce a verified record. Amendments and retractions are new visible states,
  never silent overwrites.
- Source text, evidence, graph content, and brief content remain local. Railway
  may receive sanitized operational counts and health only.
- Public sources are allowed. Authenticated, private, paywalled, and paid
  transcription sources are excluded from v1.
- Semantic extraction, entity resolution, claim grouping, and position
  comparison are LLM-owned. Deterministic code owns transport-independent
  validation, hashes, exact offsets, schemas, state transitions, budgets,
  lineage, SQLite queries, and literal FTS. Embeddings and deterministic
  semantic matching are prohibited.

The machine-readable authority is in `config/research_radar/`:

- `product_contract_v1.json`
- `schema_contract_v1.json`
- `budget_policy_v1.json`
- `benchmark_v1.json`
- `workspace_ai_technology_v1.json`
- `demo_five_items_v1.json`

Run `python3 -m research_factory.radar_cli validate-contract` before initializing
or changing pilot behavior. Validation checks the bundle together, including
the five demo evidence offsets and cross-file caps.

## Bounded pipeline

The only production-shaped flow is:

`discover → fetch → normalize → extract evidence → reconcile document → reconcile graph → score significance → publish provisional → verify/amend/retract`

One item is capped at 75,000 input tokens, 15 minutes, four coherent windows,
one schema/evidence repair, and one transport retry. One cycle is capped at 25
new items or two hours. New sources begin in probation, require three sampled
items, and are subject to the 50-source pilot cap and three promotions per day.

The scheduler is deliberately disabled. `run-cycle` is a manual deterministic
queue transition for development and does not invoke a model or start a
scheduler. Production scheduling remains gated on the representative benchmark
and accepted seven-day shadow.

## Compact CLI

```bash
python3 -m research_factory.radar_cli validate-contract
python3 -m research_factory.radar_cli init
python3 -m research_factory.radar_cli seed-demo
python3 -m research_factory.radar_cli status
python3 -m research_factory.radar_cli run-cycle --max-items 5
python3 -m research_factory.radar_cli serve --host 127.0.0.1 --port 8767
```

Place `--db /absolute/path.sqlite3` before the command to use an isolated test
database. `serve` delegates only to the loopback-only Research Radar dashboard.
It refuses Railway and non-loopback binds.

The bundled demo is idempotent, deterministic, and intentionally synthetic. It
contains five short original paraphrases, reserved `.example.invalid` URLs, and
no raw transcript or copied publication passage. It exists to exercise the
complete product surface without treating sample content as real intelligence.

## Delivery gates

1. A five-item vertical slice must end in useful dashboard briefings with
   evidence drill-down and feedback.
2. The frozen 30-item benchmark uses 20 development items and ten untouched
   holdout items, with six items in each source-format group. At most two
   extractor configurations may be tried; one is frozen before opening holdout.
3. Holdout acceptance requires complete resolving evidence, at least 95%
   attribution and temporal accuracy, at least 90% entity and position-change
   accuracy, no unsupported verified statement, and at most 10% duplicate
   developments.
4. A seven-day live shadow requires at least 80% retained/useful briefs, no
   critical source misattribution, dashboard queries under two seconds, and
   complete budget compliance.
5. Production enablement requires a signed receipt with benchmark, model/prompt,
   source-set, quality, token, wall-time, failure, and release lineage.

Full historical backfill; authority, consensus, contrarian, and forecast
rankings; team accounts; private data; paid transcription; external delivery;
and efficiency optimization are explicitly deferred.
