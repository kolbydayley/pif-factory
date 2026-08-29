# Signal Desk — discourse dashboard handoff

One-page brief for any agent (Codex) taking ownership of the Signal Desk.
Everything referenced is in this repo unless marked external. Built
2026-08-26 in a Claude session; Kolby-directed design decisions inline.

## What it is

A subject-agnostic **technical-podcast discourse research navigator**:
mobile-first shifts, open-vocabulary topic research, rich person profiles,
and every evidence excerpt linked to its original source. It supports both a
two-minute scan and progressively deeper research without requiring an
account. Single self-contained HTML file, regenerated nightly. NOT an AI
dashboard — swap the podcast sources to a new sector and it re-molds itself
with zero configuration (explicit Kolby requirement).

## Components

| Piece | Path | Role |
|---|---|---|
| Aggregator | `research_factory/pif_discourse_aggregates.py` | Read-only over `data/factory.sqlite` → `work/pif-ops/dashboard/data.json` (~1MB) |
| Renderer | `scripts/pif_dashboard_build.py` | Embeds research data into the HTML and emits a separate live funnel JSON resource beside it |
| Nightly job (external) | codex-cron `pif-dashboard-refresh`, daily 21:30, cwd this repo | Runs aggregator then renderer; fires after `pif-canonical-promotion` (21:00) so each night's labels are included |

## Live dashboard

Canonical (GitHub auto-deploy):
<https://signal-desk-production-edf4.up.railway.app/pif-signal-desk.html>
Legacy blob host (kept in sync by the same publish run):
<https://dashboards-production-dcba.up.railway.app/d/pif-signal-desk.html>

Publish pipeline (2026-08-27): `scripts/pif_dashboard_publish.py` guards the
fresh build, PUTs it to the legacy host, copies it to `site/`, commits, and
pushes to `github.com/kolbydayley/pif-factory` (private). The Railway
`signal-desk` service (project keystone-dashboards, service
82d5e774-bb35-4534-b927-bfcd7c5ea572, root `/site`, branch
`codex/pif-working-system-rebuild-20260720`) auto-deploys on every push —
git push is the single publish surface.

The Railway `keystone-dashboards` host serves the current published artifact.
Kolby authorized automated publishing on 2026-08-27 (Signal Desk 10x
project): `scripts/pif_dashboard_publish.py` guard-checks the fresh build
(schema `signal_desk_v4`, non-empty topics, <=2 days old), PUTs it to the
host, and verifies the public GET is byte-identical. Append it as the third
step of the `pif-dashboard-refresh` codex-cron job.

Regenerate manually:

    python3 -B -m research_factory.pif_discourse_aggregates
    python3 -B scripts/pif_dashboard_build.py

## Data contracts (the load-bearing decisions)

- **Discourse time = `episodes.published_at`** (feed-reported release date).
  Never label/transcription time. Back-filling a 2019 episode lands in 2019.
- **Data-frontier anchoring**: detector windows anchor to the newest month
  with >= 50 labeled rows (`data_frontier()`), not the calendar. An
  ingestion freeze reads as a coverage gap, never as "everything faded".
  The dashboard shows an amber ingestion-lag chip when frontier > 21 days old.
- **Open vocabulary**: topics come from label `topics[].topic` strings and
  `actor_positions.concept_name`, normalized by `norm_topic()`. No enum
  anywhere. The old `ai_discourse_v1` pack's hardcoded topic enum is data,
  not schema. V3 removes junk buckets and clusters near-duplicates with a
  bounded, volume-headed token-containment/Jaccard pass applied to both topic
  series and people positions.
- **Stance collapsing**: supportive/promotional/bullish → positive;
  skeptical/warning/bearish → negative; else neutral (`stance_group()`).
- **Sources**: labels (all packs incl. `ai_discourse_bulk_v1`) for topic
  series; `actor_positions` (guest/host/person) for the people board;
  `expert_authority_scores` × `canonical_people` for authority badges;
  evidence excerpts come from `actor_positions.evidence_json.evidence` and
  are whitespace-normalized and capped at 240 characters for public display.
- **Related issues**: ranked from topics co-occurring in the same episodes,
  with shared-show breadth used before shared-episode count.
- **Honest chart magnitude**: weekly chart height is the three-week smoothed
  share of all discourse mentions, not a raw count that inherits corpus
  coverage swings. Exact counts remain in week labels; weeks with fewer than
  150 total mentions are visibly de-emphasized.
- **Trust honesty**: authority-scored voices rank first. When no scored person
  is attached to a topic, the UI says so and presents evidence without making
  a trust claim.
- **People quality**: a person must appear in at least two distinct episodes.
  Browse order starts with sustained episode presence; a changed position
  requires different episodes at least 14 days apart.

## Progressive disclosure routes

The research views remain self-contained, while the Podcast Funnel fetches
`pif-signal-desk-funnel.json` from the server with caching disabled and
refreshes it every 60 seconds. The nightly publish stages that JSON beside the
HTML, so funnel counts and the enrolled-show roster always come from the
latest published processing snapshot rather than build-time values embedded
in the page.

The renderer uses hash routes, so every research layer has a
stable browser-history state without needing a server-side router:

- `#home/shifts`, `#home/people`, `#home/topics`
- `#funnel` for the enrolled-show roster, episode processing funnel, and
  private show-enrollment intake
- `#topic/<topic>` and topic slices for `stance/<group>` or `week/<week>`
- `#person/<person>` for recurring claims, meaningful cross-episode position
  changes, disagreements with the field, and evidence
- `#evidence/<position-id>/<topic>` for excerpt context and the original link

On mobile, topic pages put major issues and trusted-voice status before the
long trend and evidence record. Person pages put biggest recurring claims
first. Desktop retains the two-column research layout.

The Podcast Funnel separates catalogued feed inventory from transcript
attempts, segmented episodes, and intelligence-ready episodes. Because the
published desk is intentionally public and static, its add-show form opens a
prefilled issue in the private repository; an operator must still verify the
RSS feed, check aliases, and run a bounded ingestion dry run before a source
enters production.

## Statistical layer (V4, 2026-08-27)

`research_factory/discourse_stats.py` (pure stdlib) backs every detector:
exact conditional binomial rate tests for emerging/fading (exposure =
weekly labeled-mention totals, so corpus-coverage swings never read as
surges), permutation/chi-square stance-shift tests, a lopsided-null
contested test, Wilson CIs on weekly stance shares (`pos_ci`/`neg_ci` in
each series cell), and Benjamini-Hochberg FDR gating. Every detector hit
carries `p_value`, an effect size with CI, and a `tier`
(strong/moderate/weak). Weak = passes the legacy raw-count rule but fails
significance; the front page never shows weak. `low_sample` is now derived
from the corpus (week_total < 0.25 x median) instead of the inert fixed
150. Effect floors are provisional pending the Phase-2 backtest sweep.

## Detectors (subject-blind statistics, in the aggregator)

- **emerging**: pulse volume >= 8 in the last 4 anchored weeks with
  baseline rate < 0.5/week over the prior 20.
- **shifting**: total-variation divergence >= 0.3 between pulse and
  baseline stance distributions (needs >= 8 pulse, >= 10 baseline rows).
- **contested**: within the pulse window, min(pos,neg)/max >= 0.5 with
  >= 8 positions.
- **fading**: baseline peak week >= 8, pulse <= 2 mentions.

Tuning lives in the constants at the top of the aggregator
(`PULSE_WEEKS`, `BASELINE_WEEKS`, thresholds inline).

## Design system (renderer)

Editorial "signals desk": Fraunces display serif + IBM Plex Sans/Mono,
warm paper surface, dataviz-validated palette (positive `#2a78d6`,
negative `#d84b4a`, neutral `#b9b4a8`, accent `#178b62`). Mobile uses a
persistent three-destination bottom navigation and 44px-or-larger targets.
Categorical hues are fixed, never cycled; colors follow entities.

## Known limits / roadmap (in priority order)

1. **Cluster aliases are not yet displayed**: V3 resolves near-duplicates to
   a volume-headed canonical topic, but the detail page does not yet expose
   which raw terms were folded into that topic.
2. **Bulk labels aren't in `actor_positions` yet** — the people board deepens
   automatically when the tier-100 intelligence campaign runs claim/position
   extraction over the `ai_discourse_bulk_v1` pack (8.5k+ labels waiting).
3. Prediction track records / consensus-formation views once
   outcome-resolution data matures.
4. Automate Railway publishing after the nightly local build once continuous
   public publishing is explicitly authorized.

## Verification habits that caught real bugs during the build

- Render and LOOK (screenshot) before shipping — layout bugs don't show in
  code review.
- Check date semantics end-to-end with a query (labeled-today/released-2019
  rows must chart in 2019).
- The detectors degenerating to "everything fading" = window anchored past
  the coverage frontier.

## Data-source attribution

Episode catalog discovery for truncated feeds was assisted by the
Podcast Index API (https://podcastindex.org). Per its terms, API
responses are treated as transient discovery pointers: episode metadata
of record comes from the shows' own public RSS feeds, raw API response
caches are not retained, and credentials live only in environment
variables.
