# Source-recovery flags: measured diagnostic failure and isolated response

The current frozen diagnostic repeatedly returns readable, speaker-unresolved
claims with `role=substantive_claim` and `needs=audio_or_source`. The existing
validator rejects this combination: only `research_limitation` permits a non-none
recovery need. The author rules describe recovery needs for limitations but do
not explicitly state the biconditional or the difference between unknown voice,
missing factual corroboration, and unrecoverable proposition meaning.

Verified examples include TSMC C, Stoica B, the AI-governance audit, and the
27-record flattened translation/medical-AI/embodied-AI window
`sdw_f367e118b794f35d05f9`. This is an observed failure pattern and a plausible
prompt contributor, not proof that a wording change solves it.

`signal_desk_source_need_prompts.py` adds one clarification to the frozen v5
instructions, with unchanged schema and validator. It is isolated from the
question-representation variant: do not silently combine the two changes and
claim a one-change experiment. No calls have run under this variant. No existing
result, repair approval, or first-pass score transfers to it.

Before promotion, evaluate fresh A/B/C/AUDIT calls on the same complete sixteen
development sources, with exact source and request provenance and full independent
record/lineage review. Keep unresolved speakers null. Test clipped antecedents,
garbled text, and withheld mechanisms so that improved structural validity does
not come from suppressing legitimate recovery needs. Compare first-pass failures,
attribution, meaning fidelity, and coverage separately. Preserve all original
failures and all 804 benchmark windows; this diagnostic is not a reliability gate.

The frozen original run remains unchanged. This unqualified clarification is a
candidate for the next prompt experiment, not a reason to apply semantic repairs
without independent review or to replace the required per-split audits/A1/A2.
