# Signal Desk — strategic intelligence handoff

Signal Desk is a public, mobile-first research interface for answering four
questions about the technical-podcast corpus:

1. What changed?
2. Why does it matter?
3. Where do credible people disagree?
4. What evidence and coverage limitations should I trust?

The production URL is
<https://signal-desk-production-edf4.up.railway.app/pif-signal-desk.html>.
Public pages contain bounded excerpts and source links, never full
transcripts.

## Architecture

| Component | Path | Responsibility |
|---|---|---|
| Topic registry | `research_factory/topic_canonicalizer.py` | Stable issue IDs, accepted aliases, versioned assignments, and reviewable relationships |
| Aggregator | `research_factory/pif_discourse_aggregates.py` | Read-only corpus aggregation plus episode-context speaker attribution |
| Trust layer | `research_factory/signal_desk_intelligence.py` | Publishability, evidence quality, identity/source canonicalization, briefs, and split payloads |
| HTML shell | `scripts/pif_dashboard_build.py` | Small accessible application shell and asset/payload build |
| Frontend | `scripts/signal_desk_assets/` | Briefing, Issues, Voices, Ask, Evidence, and Coverage & Trust views |
| Publisher | `scripts/pif_dashboard_publish.py` | Freshness/schema guard, legacy upload, site staging, commit, push, and public verification |
| Railway image | `site/Dockerfile` | Static production image with every split payload packaged beside the app |

The V5 build emits:

- `dashboard.html` — lightweight shell;
- `signal-desk.css` and `signal-desk.js`;
- `signal-desk-index.json` — briefing and searchable indexes;
- `signal-desk-issues.json` — stable issue briefs and evidence ledgers;
- `signal-desk-voices.json` — canonical voice profiles;
- `signal-desk-network.json` — explicitly labeled co-appearance edges;
- `pif-signal-desk-funnel.json` — dynamic Coverage & Trust data.

The split files are fetched only by the views that need them. The Coverage
payload is fetched with cache disabled and refreshed every 60 seconds.

## Trust and publication contract

The public model fails closed.

- Evidence is `accepted`, `uncertain`, or `quarantined`.
- A voice excerpt is accepted only when the latest episode-context artifact
  verifies that person as a direct speaker. Missing context, quoted/reported
  actors, third-person mentions, and ambiguous legacy attribution never
  appear under **What they say**.
- Page chrome, sponsor copy, short fragments, missing original sources, and
  unresolved/composite identities lower or block publication.
- Each evidence record carries attribution type/confidence, publishability,
  quality score/reasons, stable source location, and a deduplication key.
- A decision-grade brief needs at least three accepted excerpts from three
  episodes, two shows, and two distinct verified voices. Everything below the
  threshold is labeled **Watchlist** and states the limitation.
- Authority and Network reach are distinct. Reach is co-appearance
  connectivity and is never presented as correctness or expertise.
- Co-mentioned issues are **Research leads**, not relationships. Typed edges
  remain hidden until explicitly adjudicated.
- Brief sentences contain evidence IDs as citations. Unsupported synthesis is
  omitted rather than filled with plausible prose.

The speaker gate is derived in the aggregator from the latest completed
`episode_context_runs.speaker_map_json`. New artifacts use
`direct_speaker`; older artifacts are accepted only for unambiguous
host/guest/interviewer/narrator roles with no quoted, reported, referenced,
mentioned, producer, or external marker.

## Information architecture

Primary navigation:

- **Briefing** — New, Accelerating, Changing consensus, Fading, and Watchlist;
- **Issues** — alias-aware discovery plus source-grounded issue briefs;
- **Voices** — direct claims separated from third-party mentions;
- **Ask** — canonical issue resolution and cited, deterministic synthesis.

**Coverage & Trust** is secondary navigation. It shows the full
shows → episodes → segments funnel, losses by dimension, duplicate resolved
feeds, quarantines, ingestion gaps, source search, and enrollment preflight.

Stable routes:

- `#briefing`, `#issues`, `#voices`, `#ask/<question>`, `#coverage`;
- `#issue/<stable-id>` with optional `month/<yyyy-mm>`;
- `#voice/<stable-id>`;
- `#evidence/<evidence-id>/<origin>/<origin-id>`.

Legacy hashes (`#home/shifts`, `#home/topics`, `#home/people`, `#topic`,
`#person`, and `#funnel`) redirect through the alias registry. Unknown or
retired IDs show an origin-aware unavailable state instead of a dead page.

## Data semantics

- Discourse time is `episodes.published_at`, never processing time.
- Charts use three-month smoothed share of discourse. Raw counts remain in
  accessible month labels/tooltips, and low-coverage months are visibly dim.
- Topic vocabulary is open, but only accepted canonical assignments and
  precision-first normalization merge topics.
- Supportive/promotional/bullish collapse to positive;
  skeptical/warning/bearish to negative; everything else is neutral.
- The funnel canonicalizes resolved RSS URLs, exposes aliases, and keeps raw
  and canonical show totals so duplicate feeds cannot silently inflate
  coverage.

## Build, test, and publish

Regenerate the full public data and application:

```sh
python3 -B -m research_factory.cli --db data/factory.sqlite canonicalize-topics --limit 5000
python3 -B -m research_factory.pif_discourse_aggregates
python3 -B scripts/pif_dashboard_build.py
python3 -B scripts/pif_dashboard_publish.py
```

Focused verification:

```sh
python3 -B -m pytest -q \
  tests/test_signal_desk_intelligence.py \
  tests/test_pif_dashboard_build.py \
  tests/test_pif_dashboard_publish.py \
  tests/test_pif_discourse_aggregates.py
node --check scripts/signal_desk_assets/signal-desk.js
```

The publisher requires schema `signal_desk_v5`, non-empty issue data, a fresh
payload, and every split companion file. The Railway `signal-desk` service
deploys the `site/` directory from branch
`codex/pif-working-system-rebuild-20260720`.

Deployment is complete only after checking the public HTML, CSS, JavaScript,
all JSON payloads, representative deep links, and rendered DOM at the Railway
URL. A git push or Railway build receipt alone is not proof of production.

## Accessibility and responsive contract

- The mobile application shell reserves a separate safe-area-aware region for
  bottom navigation; it never overlays research content.
- 390px and 430px layouts must have no horizontal overflow.
- Interactive targets are at least 44px, with visible focus and semantic
  `aria-current`/`aria-pressed` states.
- Charts are keyboard-operable buttons with complete nonvisual month labels.
- Stance and confidence always have text labels; color is supplemental.
- Search and validation results use live status text.

The browser audit should always include Briefing, one watchlist issue, one
decision-grade issue, a person with both direct and mentioned evidence, Ask,
an evidence citation, Coverage stages, duplicate feeds, empty search states,
browser back, 390px, 430px, and a desktop viewport.
