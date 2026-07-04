# Runbook

## Normal Controller Cycle

```bash
cd /Users/kolbydayley/Documents/Codex/podcast-intelligence-factory
python3 -m research_factory init
python3 -m research_factory verify-sources --source-list config/sources.yaml
python3 -m research_factory enqueue --lane podcast --since 2025-01-01 --source-list config/sources.yaml --label-pack ai_discourse_v3_1
python3 -m research_factory queue sync-envelopes
python3 -m research_factory queue status --by lane,content_type,role,status
python3 -m research_factory railway-cost-guard
python3 -m research_factory transcript-candidates --lane podcast --limit 40 --claim --worker-id transcript-discovery
python3 -m research_factory run --lane podcast --limit 100 --job-types fetch_transcript,prepare_transcript --no-claim-prompts --max-label-prompts 0 --model gpt-5.5 --label-pack ai_discourse_v3_1 --worker-id codex-controller
python3 -m research_factory prepare-transcripts --lane podcast --limit 50 --label-pack ai_discourse_v3_1 --enqueue-labels --priority 25 --pilot-id controller-v31
python3 -m research_factory run --lane podcast --limit 25 --job-types episode_context,label_segment --model gpt-5.5 --label-pack ai_discourse_v3_1 --worker-id codex-controller
python3 -m research_factory scale-batch-report --pilot-id controller-v31
python3 -m research_factory snapshot --output exports/observer-snapshot.json
```

For v3.1, GPT-5.5 should read full episode context before extraction. Deterministic code may validate schemas, offsets, privacy, queue state, and graph invariants, but it should not replace AI-led extraction with regex matching.

Do not invoke Codex `/fast` for GPT-5.5 extraction, reviewer, identity-judge, or claim-judge work. The fast QA loop, failure-bank checks, and delta audits are still correct; `/fast` mode is not.

The roadmap for canonical expert/guest graphing, identity resolution, claim agreement, and authority scoring lives in `docs/ROADMAP.md`.

## Production Cycle

The production control plane is local. Use Railway only for the sanitized observer snapshot.

Observer-only local cycle:

```bash
python3 -m research_factory production-cycle --worker-mode none
```

Bounded local compute cycle:

```bash
python3 -m research_factory production-cycle --worker-mode bounded --publish
```

This sequence syncs queue envelopes, optionally runs bounded local workers, refreshes scale readiness, writes a sanitized snapshot, privacy-scans exports, and publishes only when `--publish` is explicit. Do not run extraction, review, graph, acquisition, queue processing, cron, or model calls on Railway.

## Local Headless Worker Cycle

Use these commands to run separate bounded local Codex-style workers. They share the same SQLite queue and record `worker_runs` for observer visibility:

```bash
python3 -m research_factory worker run --role acquisition --lane podcast --limit 4 --model gpt-5.5 --label-pack ai_discourse_v3_1 --worker-id headless-acquisition
python3 -m research_factory worker run --role extractor --lane podcast --limit 4 --model gpt-5.5 --label-pack ai_discourse_v3_1 --worker-id headless-extractor
python3 -m research_factory worker run --role reviewer --lane quality --limit 4 --model gpt-5.5 --label-pack ai_discourse_v3_1 --worker-id headless-reviewer
```

Default worker runs are capped at `4` jobs. Use `--burst` only for small clean remediation batches up to `6`.

The future remote/MCP queue contract is intentionally disabled but testable:

```bash
python3 -m research_factory remote-queue claim-job --worker-id future-chatgpt-task --capability extractor --max-items 1
```

Only jobs with a remote-allowed privacy tier can be claimed through that future contract. Local SQLite remains the source of truth, and remote-style submissions must be validated/imported locally before canonical tables change.

## Broad Format Expansion

Do not broad-scale new content formats until the podcast v3.1 gate passes. Then add adapters in this order:

1. public blog/article pages
2. public Substack/newsletter pages
3. public creator YouTube captions
4. PDFs/papers
5. release notes and docs pages

Each adapter must emit normalized `content_sources`, `content_items`, `content_artifacts`, and `content_spans` with provenance, privacy tier, policy flags, parser quality, and acquisition state. Use `tech_discourse_v1` only after a mixed-format holdout gate validates quality.

## Scale-Readiness Gate

Do not call v3.1 scale ready just because every selected episode has a GPT-5.5 episode-context run. Scale readiness requires complete selected-segment extraction plus reviewer and graph gates:

```bash
python3 -m research_factory scale-batch-report --pilot-id scale-gate-v31-2026-07-02
python3 -m research_factory retry-failed-labels --label-pack ai_discourse_v3_1 --mode repair-or-requeue
python3 -m research_factory run --lane podcast --limit 25 --job-types label_segment --max-label-prompts 25 --model gpt-5.5 --label-pack ai_discourse_v3_1 --worker-id codex-controller
python3 -m research_factory audit --sample 1.0 --label-pack ai_discourse_v3_1 --model gpt-5.5
python3 -m research_factory run --lane quality --limit 250 --job-types audit_label --no-claim-prompts --max-label-prompts 0 --model gpt-5.5 --label-pack ai_discourse_v3_1 --worker-id codex-controller-quality
python3 -m research_factory reviewer-audit --pilot-id scale-gate-v31-2026-07-02 --episodes 10 --model gpt-5.5
python3 -m research_factory judge-identities --pilot-id scale-gate-v31-2026-07-02 --model gpt-5.5 --limit 250
python3 -m research_factory cluster-claims --pilot-id scale-gate-v31-2026-07-02 --model gpt-5.5
python3 -m research_factory judge-claim-edges --pilot-id scale-gate-v31-2026-07-02 --model gpt-5.5 --limit 100
python3 -m research_factory scale-batch-report --pilot-id scale-gate-v31-2026-07-02
```

`reviewer-audit` creates local GPT-5.5 review handoffs. A reviewer must read the prompt, write strict JSON to the output path, and submit it with `submit-reviewer-audit`. Candidate claim edges are not final semantic judgments until a GPT-5.5 judge rationale is stored with evidence.

If the semantic reviewer gate fails, do not scale. Use the remediation loop:

```bash
python3 -m research_factory reviewer-findings --pilot-id scale-gate-v31-2026-07-02 --severity P0,P1
python3 -m research_factory requeue-reviewed-segments --pilot-id scale-gate-v31-2026-07-02 --mode reviewed-episodes
python3 work/complete_v31_pilot_labels.py --pilot-id scale-gate-v31-2026-07-02 --model gpt-5.5 --concurrency 4 --max-jobs <pending-remediation-count>
python3 -m research_factory audit --sample 1.0 --label-pack ai_discourse_v3_1 --model gpt-5.5
python3 -m research_factory run --lane quality --limit 250 --job-types audit_label --no-claim-prompts --max-label-prompts 0 --model gpt-5.5 --label-pack ai_discourse_v3_1 --worker-id codex-controller-quality
python3 -m research_factory reviewer-audit --pilot-id scale-gate-v31-2026-07-02 --episodes 10 --model gpt-5.5 --fresh
python3 work/run_reviewer_audits.py --pilot-id scale-gate-v31-2026-07-02 --model gpt-5.5 --concurrency 2
```

`reviewer-findings` is sanitized and does not print transcript excerpts. Use `failed-review-only` for a narrow P0/P1 segment retry; use `reviewed-episodes` when reviewer failure is broad across identity usefulness, precision, or product/market coverage. Stop on Codex usage-limit signals and resume later; `complete_v31_pilot_labels.py` releases missing-output handoffs before exiting.

Only after the 25-episode pilot passes should the controller enqueue a controlled 100-episode batch: `60` high-signal ready AI episodes, `20` newly recovered/acquired transcripts, and `20` broad-tech AI-heavy episodes. Keep concurrency at `4` GPT-5.5 workers until failed jobs, leases, and reviewer scores stay healthy.

## Transcript Discovery Cycle

When the observer shows `manual_transcript_required` jobs, use bounded source discovery before labeling:

```bash
python3 -m research_factory transcript-candidates --lane podcast --limit 40 --claim --worker-id transcript-discovery
python3 -m research_factory attach-transcript --episode-id <id> --transcript-url <official-url> --transcript-type text/html --source-kind official_show_transcript
python3 -m research_factory record-transcript-attempt --episode-id <id> --method youtube_caption --status youtube_caption_blocked --source-kind youtube_captions --error-class youtube_caption_blocked --notes "<short safe reason>"
python3 -m research_factory mark-transcript-exhausted --episode-id <id> --worker-id transcript-discovery --notes "<official pages/RSS/YouTube exhausted>"
python3 -m research_factory enqueue-transcription --lane podcast --provider voyager --label-pack ai_discourse_v3_1 --limit 20 --dry-run
python3 -m research_factory run --lane podcast --limit 75 --job-types fetch_transcript --no-claim-prompts --max-label-prompts 0 --model gpt-5.5 --label-pack ai_discourse_v3_1 --worker-id transcript-discovery
```

Only attach creator RSS transcript links, official public transcript pages, or public creator-channel captions. Leave candidates pending when provenance is unclear. Remove `--dry-run` from `enqueue-transcription` only when paid transcription fallback has been explicitly enabled for that run. The transcribe worker will fail closed unless `ALLOW_PAID_TRANSCRIPTION=true` and the configured provider credentials are present.

If `run` creates prompt handoffs, a Codex app worker should process a bounded batch:

1. Run `python3 -m research_factory claim --lane podcast --label-pack ai_discourse_v3_1 --model gpt-5.5`.
2. Read the returned `prompt_path`.
3. Write only strict JSON to the returned `output_path`.
4. Run `python3 -m research_factory submit --job-id <id> --output-json <output_path>`.
5. If the output cannot be produced cleanly, run `python3 -m research_factory fail --job-id <id> --reason "<short reason>"`.

## Observer Snapshot Cycle

```bash
python3 -m research_factory railway-cost-guard
python3 -m research_factory snapshot --output exports/observer-snapshot.json
python3 -m research_factory publish-snapshot \
  --snapshot exports/observer-snapshot.json \
  --url https://observer-ui-production.up.railway.app \
  --token-file .railway-ingest-token.local
```

The live observer is `https://observer-ui-production.up.railway.app`.

## Intervention Checks

- `failed > 0`: inspect failed jobs before increasing concurrency.
- many `claimed` jobs with expired leases: run a small controller cycle to reclaim leases.
- segments without labels: workers are not submitting model outputs.
- missing transcript jobs: run transcript discovery; source feeds did not expose direct transcript links.
- `youtube_caption_blocked`: use browser/manual public discovery first; do not jump straight to transcription.
- `transcription_eligible`: public online routes were exhausted and audio fallback can be queued if paid transcription is intentionally enabled.

## Privacy Checks

```bash
python3 -m research_factory privacy-scan
```

Do not move `corpus/` into public hosting. Publish only `exports/observer-snapshot.json` or generated reports that cite segment IDs and short evidence spans.
