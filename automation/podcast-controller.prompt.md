# Podcast Intelligence Factory Controller

Run from `/Users/kolbydayley/Documents/Codex/podcast-intelligence-factory`.

Goal: advance the private podcast corpus safely with bounded work and no user-facing chatter.

Rules:
- Use subscription-backed Codex in this Codex app session for label generation. Do not use OpenAI API keys or paid Batch API unless Kolby explicitly asks.
- Do not invoke Codex `/fast` for extraction, reviewer, identity-judge, or claim-judge work. Fast QA loops are fine; `/fast` runtime mode is not.
- Do not publish raw transcripts or long excerpts.
- Do not touch OpenClaw, Finance, or unrelated project files.
- Do not send Telegram messages.
- Keep each run bounded: first run deterministic fetch/audit work only, then claim at most 25 label prompts, and submit every prompt claimed in that same run.
- "Scale ready" means complete-episode extraction: every selected episode has GPT-5.5 full context, every selected segment has a v3.1 GPT-5.5 label, deterministic audits pass, GPT-5.5 reviewer audits pass, and quality/graph blockers are drained.
- Do not treat one validated segment per episode as a passed scale gate.
- Until the active 100-episode pilot passes, target `scale-batch-v31-2026-07-05-500-a`; do not create a new `controller-v31` pilot.

Steps:
1. Run `python3 -m research_factory init`.
2. Run `python3 -m research_factory preflight --model gpt-5.5` and record whether the CLI is healthy; continue through app-based labeling even if CLI preflight fails.
3. Run `python3 -m research_factory verify-sources --source-list config/sources.yaml` and stop if any feed fails.
4. Run `python3 -m research_factory enqueue --lane podcast --since 2025-01-01 --source-list config/sources.yaml --label-pack ai_discourse_v3_1`.
5. Run `python3 -m research_factory quarantine-contaminated --apply` before any new label claims.
6. Run `python3 -m research_factory run --lane podcast --limit 100 --job-types fetch_transcript,prepare_transcript --no-claim-prompts --max-label-prompts 0 --model gpt-5.5 --label-pack ai_discourse_v3_1 --worker-id codex-controller`.
7. Run `python3 -m research_factory scale-batch-report --pilot-id scale-batch-v31-2026-07-05-500-a` and inspect the label counts before claiming work.
8. Run `python3 -m research_factory cleanup-pilot-label-claims --pilot-id scale-batch-v31-2026-07-05-500-a` before any new model calls.
9. If pending label jobs remain, run one bounded supervised wave: `python3 -m research_factory run-pilot-labels --pilot-id scale-batch-v31-2026-07-05-500-a --model gpt-5.5 --concurrency 4 --max-jobs 25 --timeout-seconds 1800 --worker-prefix codex-controller-v31`.
10. Run `python3 -m research_factory retry-failed-labels --pilot-id scale-batch-v31-2026-07-05-500-a --label-pack ai_discourse_v3_1 --mode repair-or-requeue`.
11. Run `python3 -m research_factory cleanup-pilot-label-claims --pilot-id scale-batch-v31-2026-07-05-500-a` again so the run exits with zero empty claimed jobs.
12. Run `python3 -m research_factory audit --sample 1.0 --label-pack ai_discourse_v3_1 --model gpt-5.5 --fresh` only after the scale report shows every selected segment is labeled.
13. Run `python3 -m research_factory run --lane quality --limit 1000 --job-types audit_label --no-claim-prompts --max-label-prompts 0 --model gpt-5.5 --label-pack ai_discourse_v3_1 --worker-id codex-controller-quality` only after step 12 queues audits.
14. Run `python3 -m research_factory reviewer-audit --pilot-id scale-batch-v31-2026-07-05-500-a --episodes 12 --model gpt-5.5 --fresh --mode full` when the selected segments are fully labeled and deterministic audits are complete.
15. If reviewer scores fail the scale gate, run `python3 -m research_factory reviewer-findings --pilot-id scale-batch-v31-2026-07-05-500-a --severity P0,P1`, requeue with `python3 -m research_factory requeue-reviewed-segments --pilot-id scale-batch-v31-2026-07-05-500-a --mode reviewed-episodes`, and stop after reporting the pending remediation count.
16. Run `python3 -m research_factory judge-identities --pilot-id scale-batch-v31-2026-07-05-500-a --model gpt-5.5 --limit 250`.
17. Run `python3 -m research_factory cluster-claims --pilot-id scale-batch-v31-2026-07-05-500-a --model gpt-5.5`.
18. Run `python3 -m research_factory judge-claim-edges --pilot-id scale-batch-v31-2026-07-05-500-a --model gpt-5.5 --limit 100`.
19. Run `python3 -m research_factory discover-concepts --window month --min-evidence 2 --min-source-diversity 1`.
20. Run `python3 -m research_factory detect-shifts --window month --min-support 2`.
21. Run `python3 -m research_factory export signal-report --window month --limit 50`.
22. Run `python3 -m research_factory export graph --type concept_network`.
23. Run `python3 -m research_factory scale-batch-report --pilot-id scale-batch-v31-2026-07-05-500-a`.
24. Run `python3 -m research_factory snapshot --output exports/observer-snapshot.json`.
25. If `.railway-ingest-token.local` exists, run `python3 -m research_factory publish-snapshot --snapshot exports/observer-snapshot.json --url https://observer-ui-production.up.railway.app --token-file .railway-ingest-token.local`; otherwise leave the local snapshot artifact only.

Final response should be concise: counts, labels submitted, failures, and observer snapshot path.
