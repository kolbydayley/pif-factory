"""Unqualified question-role boundary experiment; frozen question v1 is untouched."""
from copy import deepcopy
from . import signal_desk_question_contract as previous
from .signal_desk_rubric_reference_packets import digest

VERSION = 'pif_signal_desk_semantic_records_question_boundary_v2'
QUESTION_ROLES = frozenset({'supporting_context', 'promotion_housekeeping', 'research_limitation'})
RULES = previous.RULES + """
QUESTION ROLE BOUNDARIES: Useful substantive-discussion inquiries remain
supporting_context. A show-format or promotional inquiry is promotion_housekeeping;
a clipped or garbled inquiry needing additional source is research_limitation.
Neither boundary role becomes a substantive claim just because it is a question.
Apply the unchanged recovery-need and publication rules for the chosen role.
Do not change the role merely to pass validation; decide it from the source.
All questions retain questioned proposition status and their own voice. They
never count as factual claims, endorsements, or another person's answer.
"""


def schema():
    value = deepcopy(previous.schema())
    value['title'] = VERSION
    value['properties']['schema_version']['const'] = VERSION
    return value


def validate(value, *, source, window_id):
    if not isinstance(value, dict) or value.get('schema_version') != VERSION:
        raise ValueError('wrong question boundary contract version')
    projected = deepcopy(value)
    projected['schema_version'] = previous.parent.VERSION
    for event in projected.get('events', []):
        if event.get('speech_act') != 'question':
            continue
        if event.get('evidence_role', {}).get('role') not in QUESTION_ROLES:
            raise ValueError('question requires context, housekeeping, or research-limitation role')
        if event.get('attitude', {}).get('proposition_status') != 'questioned':
            raise ValueError('inquiry requires questioned proposition status')
        # Internal compatibility projection only, identical to question v1.
        # No output is relabelled, and all underlying v5 gates still run.
        event['speech_act'] = 'assertion'
    previous.parent.validate(projected, source=source, window_id=window_id)
    return value


def receipt():
    return {'family': VERSION, 'parent': previous.receipt(),
            'schema_sha256': digest(schema()), 'rules_sha256': digest(RULES),
            'changed_dimension': 'non_substantive_question_role_boundaries_only',
            'qualified': False, 'gold_accepted': False, 'dispatch_enabled': False,
            'automatic_migration': False, 'frozen_runs_unchanged': True,
            'requires_fresh_all_role_qualification': True,
            'source_need_clarification_included': False}
