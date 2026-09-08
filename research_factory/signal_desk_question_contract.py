"""Isolated question speech-act experiment; does not mutate frozen v5 runs."""
from copy import deepcopy
from . import signal_desk_full_event_v5 as parent
from .signal_desk_rubric_reference_packets import digest

VERSION = 'pif_signal_desk_semantic_records_question_v1'
RULES = """QUESTION SPEECH ACT: A speaker asking an unanswered question is not
asserting its answer. Use speech_act=question for an inquiry. Keep useful questions
as supporting_context, linked when the answer is present or with missing-parent
status when the answer is outside the window. Preserve uncertainty. Their attitude.proposition_status
must be questioned. An actual_position classification means the speaker really
asked it, not that the premise or answer is true. A rhetorical assertion must be
evaluated from its actual source meaning, not punctuation alone. Separate an
asserted claim from an accompanying inquiry when they are distinct propositions.
Questions never add supporting/opposing factual claims, votes, or evidence of a
guest's view. Preserve the questioner's own voice; do not assign it to the answerer.
All existing exact-source, attribution, context, and publication gates still apply.
"""


def schema():
    result = deepcopy(parent.schema())
    result['title'] = VERSION
    result['properties']['schema_version']['const'] = VERSION
    result['properties']['events']['items']['properties']['speech_act']['enum'].append('question')
    return result


def validate(value, *, source, window_id):
    if not isinstance(value, dict) or value.get('schema_version') != VERSION:
        raise ValueError('wrong question contract version')
    projected = deepcopy(value)
    projected['schema_version'] = parent.VERSION
    for event in projected.get('events', []):
        if event.get('speech_act') != 'question':
            continue
        if event.get('evidence_role', {}).get('role') != 'supporting_context':
            raise ValueError('inquiry cannot be a substantive assertion')
        if event.get('attitude', {}).get('proposition_status') != 'questioned':
            raise ValueError('inquiry requires questioned proposition status')
        # Internal compatibility projection only, never emitted or used as labels.
        # All unchanged parent validators run; question semantics checked above.
        event['speech_act'] = 'assertion'
    parent.validate(projected, source=source, window_id=window_id)
    return value


def receipt():
    return {'family': VERSION, 'parent': parent.receipt(), 'schema_sha256': digest(schema()),
        'rules_sha256': digest(RULES), 'qualified': False, 'gold_accepted': False,
        'frozen_runs_unchanged': True, 'automatic_migration': False,
        'independent_all_role_qualification_required': True,
        'public_consumers_must_exclude_questions_from_claim_counts': True}
