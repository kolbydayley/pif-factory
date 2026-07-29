# Podcast Intelligence Transcript Source Discovery

Run from `/Users/kolbydayley/Documents/Codex/podcast-intelligence-factory`.

Goal: convert manual transcript candidates into verified transcript-fetch jobs using only allowed public/official sources.

Rules:
- Use subscription-backed Codex in this app session; do not use paid API billing.
- Do not copy full transcript text into chat, memory, reports, commits, or exports.
- Use only creator-provided RSS transcript links, official show transcript pages, or publicly available YouTube captions when the episode/source policy allows it.
- Avoid app-only, paywalled, or private transcript surfaces.
- Record every meaningful public-search outcome with `record-transcript-attempt`; do not let failed discovery disappear.
- Mark an episode `transcription_eligible` only after official pages, source homepage, episode page, RSS metadata, and public YouTube-caption routes have been exhausted.
- Audio transcription fallback is allowed only through the configured `enqueue-transcription`/`transcribe_audio` path and only when paid transcription is explicitly enabled in the environment.
- Do not touch OpenClaw, Finance, or unrelated project files.
- Keep the run bounded: inspect at most 40 claimed candidates and attach at most 20 transcript URLs.
- Do not claim label jobs or create label prompt handoffs. Labeling belongs to the controller/label-worker lane only.
- Do not edit code, run migrations, or perform direct SQL changes from this discovery automation. Use only the CLI commands listed below.
- Treat transcript discovery as an ongoing source-level pipeline, not a one-time pull. Consult `config/transcript_strategies.json` and `docs/TRANSCRIPT_ACQUISITION_PIPELINE.md` before browser work so each source follows its established lane, retry rule, and failure state.

Steps:
1. Run `python3 -m research_factory init`.
2. Run `python3 -m research_factory transcript-strategy-report` and prioritize high-yield automatic/official lanes before manual browser work.
3. Run `python3 -m research_factory enqueue-transcript-backlog --lane podcast --label-pack ai_discourse_v3_1 --limit 250` to refill known safe transcript fetch work. Do not add `--include-youtube-captions` or `--include-document-transcripts` unless that lane has been explicitly enabled.
4. Run `python3 -m research_factory run --lane podcast --limit 75 --job-types fetch_transcript,prepare_transcript --no-claim-prompts --max-label-prompts 0 --model gpt-5.5 --label-pack ai_discourse_v3_1 --worker-id transcript-discovery`.
5. Run `python3 -m research_factory release-transcript-candidates --lane podcast --expired-only --limit 200`.
6. Run `python3 -m research_factory transcript-candidates --lane podcast --limit 40 --claim --worker-id transcript-discovery --max-claimed 120 --per-source-limit 10`.
7. For each candidate, inspect the source strategy first, then the episode URL, source homepage, official show pages, RSS metadata, official archive/sitemap, and public creator-channel YouTube surfaces when that source is in the caption lane.
8. When a valid official transcript source is found, run `python3 -m research_factory attach-transcript --episode-id <id> --transcript-url <url> --transcript-type <type> --source-kind official_show_transcript`.
9. Use `--source-kind youtube_captions` only for public captions from the creator's official YouTube channel. If caption fetch is blocked, record it with `python3 -m research_factory record-transcript-attempt --episode-id <id> --method youtube_caption --status youtube_caption_blocked --source-kind youtube_captions --error-class youtube_caption_blocked --notes "<short safe reason>"`.
10. If public/official online transcript routes are truly exhausted and the episode has an audio URL, run `python3 -m research_factory mark-transcript-exhausted --episode-id <id> --worker-id transcript-discovery --notes "<short searched-sources summary>"`.
11. Run `python3 -m research_factory enqueue-transcription --lane podcast --provider voyager --label-pack ai_discourse_v3_1 --limit 20 --dry-run` and report candidates; remove `--dry-run` only when Kolby has explicitly enabled paid transcription fallback for this run.
12. Run `python3 -m research_factory release-transcript-candidates --lane podcast --worker-id transcript-discovery --limit 40` so unresolved candidates return to pending; do not invent transcript URLs.
13. Run `python3 -m research_factory snapshot --output exports/observer-snapshot.json`.
14. If `.railway-ingest-token.local` exists, run `python3 -m research_factory publish-snapshot --snapshot exports/observer-snapshot.json --url https://observer-ui-production.up.railway.app --token-file .railway-ingest-token.local`.

Final response should include candidate count, transcript URLs attached, blocked caption attempts, episodes marked transcription-eligible, fetch/segment jobs completed, and any source-policy uncertainty.
