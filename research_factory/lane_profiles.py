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
        "concurrency": 3,
        "omission_passes": 0,
        "window_chars": 6000,
        "reasoning_effort": None,   # opencode has no effort knob
        "prompt_addendum": "",
    },
    "grok": {
        # Qualified config: LOW effort + 1 omission pass. Medium effort was
        # measured (grok_tune/medium1density) at ~4x the latency for +0.006
        # coverage and more junk — low wins.
        "concurrency": 6,
        "omission_passes": 1,
        "window_chars": 6000,
        "reasoning_effort": "low",
        "prompt_addendum": "",
    },
}
