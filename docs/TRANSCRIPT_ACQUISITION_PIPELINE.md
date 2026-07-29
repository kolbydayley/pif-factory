# Transcript Acquisition Pipeline

## Goal

Keep transcript acquisition as an ongoing source-by-source pipeline, not a one-time pull. Each source should have a stable route, retry rule, failure state, and monitoring count before the system escalates to manual discovery or paid transcription fallback.

## Source Strategy Catalog

The durable strategy registry lives at `config/transcript_strategies.json`.

Use it to separate sources into recurring lanes:

- `automatic_fetch`: creator RSS transcript tags, official direct transcript files, and explicit official transcript pages that can be fetched without browser research.
- `official_archive_discovery`: official archives, sitemaps, and episode pages that need source-specific URL matching or section parsing.
- `caption_discovery`: public captions from verified creator-owned YouTube channels only.
- `document_transcript_fetch`: official PDF/DOC/DOCX transcript assets, disabled until document parsing is quality-gated.
- `approval_required`: official gated transcript surfaces that must not be ingested until policy approval.
- `manual_exhaustion`: sources with no allowed public transcript route, where the job is to record negative discovery before optional local transcription fallback.

Run:

```bash
python3 -m research_factory transcript-strategy-report
```

The report is sanitized and shows only operational counts by source and strategy.

## Recurring Acquisition Loop

The normal loop should be bounded and repeatable:

```bash
python3 -m research_factory backfill-episodes \
  --lane podcast \
  --source-list config/sources.yaml \
  --label-pack ai_discourse_v3_1 \
  --fetch-concurrency 8 \
  --enqueue-transcript-jobs

python3 -m research_factory enqueue-transcript-backlog \
  --lane podcast \
  --label-pack ai_discourse_v3_1 \
  --limit 250

python3 -m research_factory run \
  --lane podcast \
  --limit 250 \
  --job-types fetch_transcript,prepare_transcript \
  --no-claim-prompts \
  --max-label-prompts 0 \
  --model gpt-5.5 \
  --label-pack ai_discourse_v3_1 \
  --worker-id transcript-acquisition
```

Do not enable YouTube captions in the generic backlog refill. Caption work is a separate lane because caption availability, creator-channel verification, and local fetch blocking need separate audit states.

Do not enable document transcripts until DOCX/PDF parsing is quality-gated:

```bash
python3 -m research_factory enqueue-transcript-backlog --include-document-transcripts
```

Only use that after the document parser has tests for no raw PDF/DOCX residue.

## Current High-Yield Lanes

Automatic linked backlog:

- Odd Lots: Omny RSS transcripts; prefer VTT over JSON/TXT/SRT when available.
- Talk Python To Me and Python Bytes: RSS VTT transcripts.
- Changelog and JS Party: official Changelog transcript pages.
- Software Engineering Daily: official TXT links in RSS body/content.
- Microsoft Research, Practical AI, Security Now: RSS transcript links or official transcript/article pages.

Official archive adapters to build next:

- EconTalk: official episode pages with transcript sections.
- AI Daily Brief: official `/e/YYYY-MM-DD/transcript.md`.
- Training Data: Sequoia official pages from podcast sitemap.
- Developer Tea, Syntax, Stack Overflow, CoRecursive, Pragmatic Engineer: official archives/sitemaps and transcript sections.
- Google Cloud Platform Podcast and Kubernetes Podcast from Google: official sitemap episode pages with transcript sections.
- Decoder: official Verge feed/archive/article content.

Blocked or gated:

- No Priors, Waveform, Real Python, Tech Brew Ride Home, Animal Spirits, Equity, Risky Business: creator-caption lane, blocked when local caption fetch is blocked.
- Founders and Business Breakdowns: official but gated Colossus transcripts; report availability only until explicit approval.
- Eye On AI and NVIDIA AI Podcast: official DOCX/PDF assets; disabled until document parsing is ready.

Exhaustion-first:

- How I Built This, Search Engine, Marketplace Tech, Data Skeptic, and sparse NVIDIA routes. Record official negative searches before marking transcription eligible.

## Failure States

Use specific failure states instead of flattening everything into missing transcript:

- `stale_transcript_link`: official/RSS transcript URL has durable 404/410 after repeated refreshes.
- `official_page_blocked`: official page exists but this environment cannot fetch it.
- `official_page_no_transcript`: official page fetched and has no transcript marker.
- `official_transcript_pending`: new/recent official page exists but transcript may lag.
- `youtube_caption_blocked`: public creator captions may exist, but local fetch is blocked.
- `official_gated`: official transcript exists behind login/register and needs approval.
- `no_public_transcript`: RSS, official page/archive, and verified public caption routes are exhausted.

Only `no_public_transcript` should move toward local transcription eligibility.

## Safety Rules

- Attach only creator-provided RSS transcript links, official show transcript pages/assets, or public creator captions that verify from this machine.
- Do not ingest Apple native transcripts, third-party transcript mirrors, app-only pages, paywalled pages, or guest reposts as canonical transcripts.
- Do not expose raw transcript text, long excerpts, local paths, prompt bodies, secrets, or output JSON in observer snapshots, broker snapshots, memory, or final reports.
- Railway remains observer-only; all transcript acquisition and parsing stays local.
