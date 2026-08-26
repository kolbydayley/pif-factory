# Signal Desk — discourse dashboard handoff

One-page brief for any agent (Codex) taking ownership of the Signal Desk.
Everything referenced is in this repo unless marked external. Built
2026-08-26 in a Claude session; Kolby-directed design decisions inline.

## What it is

A subject-agnostic **technical-podcast discourse dashboard**: people-first
(who moved, who dissents), open-vocabulary topics grown from the corpus,
proactive trend detectors, and every data point traceable to a verbatim
quote. Single self-contained HTML file, regenerated nightly. NOT an AI
dashboard — swap the podcast sources to a new sector and it re-molds itself
with zero configuration (explicit Kolby requirement).

## Components

| Piece | Path | Role |
|---|---|---|
| Aggregator | `research_factory/pif_discourse_aggregates.py` | Read-only over `data/factory.sqlite` → `work/pif-ops/dashboard/data.json` (~1MB) |
| Renderer | `scripts/pif_dashboard_build.py` | Embeds data.json into a template → `work/pif-ops/dashboard/dashboard.html` |
| Nightly job (external) | codex-cron `pif-dashboard-refresh`, daily 21:30, cwd this repo | Runs aggregator then renderer; fires after `pif-canonical-promotion` (21:00) so each night's labels are included |

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
  not schema.
- **Stance collapsing**: supportive/promotional/bullish → positive;
  skeptical/warning/bearish → negative; else neutral (`stance_group()`).
- **Sources**: labels (all packs incl. `ai_discourse_bulk_v1`) for topic
  series; `actor_positions` (guest/host/person) for the people board;
  `expert_authority_scores` × `canonical_people` for authority badges;
  evidence quotes come from `actor_positions.evidence_json.evidence`.

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
negative `#e34948`, neutral `#c9c5ba`, accent `#1baf7a`). Stance chart is
a weekly diverging stack centered on neutral (Likert-style), one axis,
hover tooltips, direct labels + legend. Categorical hues fixed, never
cycled; colors follow entities.

## Known limits / roadmap (in priority order)

1. **Topic clustering**: person↔topic matching is exact normalized-string;
   near-duplicate topics ("ai agents" vs "agents") should cluster. This is
   why some topic pages show no voices.
2. **"shifting" detector fires rarely** until recent-coverage density rises
   (transcript acquisition frozen since 2026-07-18 — separate campaign).
3. **Bulk labels aren't in `actor_positions` yet** — the people board deepens
   automatically when the tier-100 intelligence campaign runs claim/position
   extraction over the `ai_discourse_bulk_v1` pack (8.5k+ labels waiting).
4. Prediction track records / consensus-formation views once
   outcome-resolution data matures.
5. Optional: publish to the Railway dashboard host instead of a local file.

## Verification habits that caught real bugs during the build

- Render and LOOK (screenshot) before shipping — layout bugs don't show in
  code review.
- Check date semantics end-to-end with a query (labeled-today/released-2019
  rows must chart in 2019).
- The detectors degenerating to "everything fading" = window anchored past
  the coverage frontier.
