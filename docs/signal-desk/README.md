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
| Renderer | `scripts/pif_dashboard_build.py` | Embeds data.json into a template → `work/pif-ops/dashboard/dashboard.html` |
| Nightly job (external) | codex-cron `pif-dashboard-refresh`, daily 21:30, cwd this repo | Runs aggregator then renderer; fires after `pif-canonical-promotion` (21:00) so each night's labels are included |

## Live dashboard

Production URL: <https://dashboards-production-dcba.up.railway.app/d/pif-signal-desk.html>

The Railway `keystone-dashboards` host serves the current published artifact.
The nightly job regenerates the local artifact only; publishing a new snapshot
is still a separate, explicit step.

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

The self-contained renderer uses hash routes, so every research layer has a
stable browser-history state without needing a server-side router:

- `#home/shifts`, `#home/people`, `#home/topics`
- `#topic/<topic>` and topic slices for `stance/<group>` or `week/<week>`
- `#person/<person>` for recurring claims, meaningful cross-episode position
  changes, disagreements with the field, and evidence
- `#evidence/<position-id>/<topic>` for excerpt context and the original link

On mobile, topic pages put major issues and trusted-voice status before the
long trend and evidence record. Person pages put biggest recurring claims
first. Desktop retains the two-column research layout.

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
