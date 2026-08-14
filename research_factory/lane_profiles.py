"""Per-lane drafting profiles: each cheap-lane agent gets its own branched
config over the shared infrastructure (windowing, merge, validation, audit).

A profile pins the exact configuration a lane qualified with; changing one is
a re-qualification event, not a tweak. Shared infra stays in
``cheap_lane_adapters``; lane-specific knobs live only here.

Qualification evidence:
- glm: work/loadtest-20260813/qual_round_4/report_recalibrated.json
       (promoted 2026-08-13, commit 98cc307)
- grok: work/loadtest-20260813/qual_round_4/rejudge_v2.json +
        qual_round_5 report_v2 (full-visibility judge_v2; the earlier 0.728
        coverage was a judge-truncation artifact, not a model gap)
"""
from __future__ import annotations

from typing import Any, Dict

# Appended to the drafting prompt for the omission audit pass. {PRIOR_CLAIMS}
# is replaced with a bulleted list of first-pass claim_texts.
OMISSION_SUFFIX = """

A first extraction pass over this segment produced the claims listed below. Your job is the OMISSION AUDIT: find substantive claims the first pass MISSED. Re-read the segment carefully. Output the SAME JSON structure, containing ONLY genuinely new substantive claims (do not repeat or rephrase the ones below; entities/topics/summary may repeat). If nothing substantive was missed, return an empty claims array.

Claims already extracted:
{PRIOR_CLAIMS}
"""

# NOTE: a banter/junk-guard prompt addendum was tried for grok
# (work/loadtest-20260813/grok_tune/r5_low1junkguard) and measured-rejected:
# it did not reduce junk on the chatter-heavy segments (which the reference
# extractor fails identically) and cost 4 points of coverage.

LANE_PROFILES: Dict[str, Dict[str, Any]] = {
    "glm": {
        # Live production lane — behavior identical to the promoted config.
        # "model" selects the opencode billing route: the same glm-5.2 weights
        # are reachable as opencode-go/glm-5.2 (Go subscription) or
        # zai-coding-plan/glm-5.2 (z.ai coding plan). Kolby holds both subs
        # (2026-08-14) and will keep one; flip this knob when a route retires.
        "model": "opencode-go/glm-5.2",
        "concurrency": 3,
        "omission_passes": 0,
        "window_chars": 6000,
        "reasoning_effort": None,   # opencode has no effort knob
        "prompt_addendum": "",
    },
    "glm-zai": {
        # Second GLM lane: identical model + contract as "glm", billed to the
        # z.ai coding plan instead of OpenCode Go so both subscriptions drain
        # in parallel. Route gate: work/loadtest-20260813/glm_tune/zai52.
        "model": "zai-coding-plan/glm-5.2",
        "concurrency": 3,
        "omission_passes": 0,
        "window_chars": 6000,
        "reasoning_effort": None,
        "prompt_addendum": "",
    },
    "grok": {
        # Qualified config: LOW effort, NO omission pass (re-qualified
        # 2026-08-14, work/loadtest-20260813/qual_round_6/report_v2.json:
        # coverage 0.902 vs 0.889 codex baseline, support 0.996, junk 0.010,
        # pass 1.0, 402 segs/hr). Dropping the omission pass halved SuperGrok
        # pool burn (~2x weekly capacity) for -0.04 coverage still above
        # baseline. Measured-rejected: medium effort (~4x latency), compact
        # output contract (pass_rate 0.80), omission pass (r5: +0.04 coverage
        # for 2x pool burn — not worth it under the Lite weekly cap).
        "concurrency": 6,
        "omission_passes": 0,
        "window_chars": 6000,
        "reasoning_effort": "low",
        "prompt_addendum": "",
    },
}
