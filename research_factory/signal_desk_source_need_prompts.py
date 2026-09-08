"""One-change, unqualified clarification of the existing recovery-need invariant."""
from . import signal_desk_full_event_v5_prompts as parent
from .signal_desk_rubric_reference_packets import digest

FAMILY='full-event-v5-source-need-clarification-v1'
CLARIFICATION='''
RECOVERY NEED IS NOT ATTRIBUTION CONFIDENCE. evidence_role.needs is non-none
if and only if evidence_role.role is research_limitation. Every substantive_claim,
supporting_context, voice_source_metadata, or promotion_housekeeping record must
use needs=none. This is the existing output contract, not permission to conceal
missing evidence. A readable claim with an unidentified speaker can remain a
substantive claim with null transcript_voice, null owner where appropriate, and
uncertain publishability; do not invent a speaker and do not mark the claim as
source-corrupt just because the voice is unresolved. Lack of independent factual
corroboration is not a transcript-recovery need: preserve the statement as the
speaker's claim, with source scope and uncertainty, not as verified world fact.
When clipping, garbling, a missing antecedent, or unintelligible wording prevents
faithful recovery of the proposition itself, use research_limitation with a
specific wider_context or audio_or_source need and the required quarantine.
Do not mechanically clear needs on genuinely unreadable content or turn every
unknown voice into a research limitation. Decide meaning recoverability first,
voice identity separately, then validate the role/needs combination before return.
'''


def prompts():
    return {role:text+'\n'+CLARIFICATION for role,text in parent.prompts().items()}


def receipt():
    return dict(family=FAMILY,parent=parent.receipt(),changed_dimension='explicit_existing_recovery_need_invariant',
        validator_changed=False,schema_sha256=digest(parent.contract.schema()),
        role_hashes={role:digest(text) for role,text in prompts().items()},
        qualified=False,gold_accepted=False,dispatch_enabled=False,
        requires_fresh_full_population_all_role_qualification=True)
