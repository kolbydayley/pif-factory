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

Steps:
1. Run `python3 -m research_factory init`.
2. Run `python3 -m research_factory preflight --model gpt-5.5` and record whether the CLI is healthy; continue through app-based labeling even if CLI preflight fails.
3. Run `python3 -m research_factory verify-sources --source-list config/sources.yaml` and stop if any feed fails.
4. Run `python3 -m research_factory enqueue --lane podcast --since 2025-01-01 --source-list config/sources.yaml --label-pack ai_discourse_v3_1`.
5. Run `python3 -m research_factory quarantine-contaminated --apply` before any new label claims.
6. Run `python3 -m research_factory run --lane podcast --limit 100 --job-types fetch_transcript,prepare_transcript --no-claim-prompts --max-label-prompts 0 --model gpt-5.5 --label-pack ai_discourse_v3_1 --worker-id codex-controller`.
7. Run `python3 -m research_factory prepare-transcripts --lane podcast --limit 50 --label-pack ai_discourse_v3_1 --enqueue-labels --priority 25 --pilot-id controller-v31`.
8. Run `python3 -m research_factory run --lane podcast --limit 25 --job-types episode_context,label_segment --max-label-prompts 25 --model gpt-5.5 --label-pack ai_discourse_v3_1 --worker-id codex-controller`.
9. For every claimed `episode_context` or `label_segment` prompt returned by step 8, read the returned prompt, write strict JSON to its output path, and submit it with `submit-context` or `submit` as appropriate. Do not leave claimed prompts without outputs.
10. Run `python3 -m research_factory retry-failed-labels --label-pack ai_discourse_v3_1 --mode repair-or-requeue`.
11. Run `python3 -m research_factory audit --sample 0.10 --label-pack ai_discourse_v3_1 --model gpt-5.5`.
12. Run `python3 -m research_factory run --lane quality --limit 25 --job-types audit_label --no-claim-prompts --max-label-prompts 0 --model gpt-5.5 --label-pack ai_discourse_v3_1 --worker-id codex-controller-quality`.
13. Run `python3 -m research_factory reviewer-audit --pilot-id controller-v31 --episodes 10 --model gpt-5.5` when the selected segments are fully labeled.
14. If reviewer scores fail the scale gate, run `python3 -m research_factory reviewer-findings --pilot-id controller-v31 --severity P0,P1`, requeue with `python3 -m research_factory requeue-reviewed-segments --pilot-id controller-v31 --mode reviewed-episodes`, and stop after reporting the pending remediation count.
15. Run `python3 -m research_factory judge-identities --pilot-id controller-v31 --model gpt-5.5 --limit 250`.
16. Run `python3 -m research_factory cluster-claims --pilot-id controller-v31 --model gpt-5.5`.
17. Run `python3 -m research_factory judge-claim-edges --pilot-id controller-v31 --model gpt-5.5 --limit 100`.
18. Run `python3 -m research_factory discover-concepts --window month --min-evidence 2 --min-source-diversity 1`.
19. Run `python3 -m research_factory detect-shifts --window month --min-support 2`.
20. Run `python3 -m research_factory export signal-report --window month --limit 50`.
21. Run `python3 -m research_factory export graph --type concept_network`.
22. Run `python3 -m research_factory scale-batch-report --pilot-id controller-v31`.
23. Run `python3 -m research_factory snapshot --output exports/observer-snapshot.json`.
24. If `.railway-ingest-token.local` exists, run `python3 -m research_factory publish-snapshot --snapshot exports/observer-snapshot.json --url https://observer-ui-production.up.railway.app --token-file .railway-ingest-token.local`; otherwise leave the local snapshot artifact only.

Final response should be concise: counts, labels submitted, failures, and observer snapshot path.
