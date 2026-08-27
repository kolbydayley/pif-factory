# Signal Desk backtest report

Generated 2026-08-27T09:11:36 · window 2026-01-05 -> 2026-07-01 · 13 as-of replays · 150 distinct firings · 31 mapped truth topics (Wikipedia pageviews).

## Emerging-detector precision vs external ground truth (by tier)

Only `emerging` claims an external attention rise; shifting/contested/fading are scored via event recall, not this table.

| tier | firings scored | precision | median lead (weeks) |
|---|---|---|---|
| strong | 6 | 0.5 | 0 |
| moderate | 1 | 1.0 | -1 |

Unmapped-topic firings skipped: 20 (extend config/backtest_truth.json mappings to cover them).

## Event recall

No curated events in window.

## Lead/lag correlation (share_smooth vs truth)

| topic | best lag (weeks, + = we lead) | r |
|---|---|---|
| agents | +8 | 0.33 |
| agi | +3 | 0.55 |
| ai alignment | -1 | 0.41 |
| ai coding | +8 | 0.63 |
| ai consciousness | +3 | 0.31 |
| ai existential risk | -7 | 0.50 |
| ai for science | -8 | 0.34 |
| ai in advertising | -4 | 0.49 |
| ai regulation | -8 | 0.61 |
| ai safety governance | -6 | 0.59 |
| ai timelines | -2 | 0.50 |
| autonomous vehicle technology maturity | -1 | 0.41 |
| claude code | -6 | 0.72 |
| codex | +5 | 0.67 |
| creator economy | -8 | 0.46 |
| enterprise ai | +8 | 0.20 |
| event sourcing | -4 | -0.04 |
| inference scaling | -8 | 0.28 |
| open source ai | -5 | 0.29 |
| open weights | -8 | 0.28 |
| recursive self improvement | +6 | 0.60 |
| reward hacking | +7 | 0.31 |
| robotaxis | -7 | 0.52 |
| safety alignment | -7 | 0.49 |
| self hosting | +2 | 0.45 |
| singularity | -2 | 0.48 |
| spacex ipo | +2 | 0.65 |
| tailscale | +2 | 0.17 |
| test time compute | +3 | -0.24 |
| us china ai competition | +8 | 0.29 |
| webassembly | -4 | 0.49 |

## Reading it

- Precision: share of firings where the truth series rose >=25% vs its trailing 8-week baseline within [-2,+4] weeks.
- Lead: distance from our firing to the truth series' CUSUM changepoint (negative = we were early).
- The tuning loop: strong-tier precision must beat weak-tier. If it doesn't, adjust effect floors in research_factory/discourse_stats.py (see --sweep).
