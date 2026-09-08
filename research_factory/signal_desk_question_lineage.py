"""Question-aware adjudication envelope, isolated from frozen lineage v1."""
from copy import deepcopy
from . import signal_desk_adjudication_lineage as parent
from . import signal_desk_question_contract as records
from . import signal_desk_question_prompts as authors
from .signal_desk_rubric_reference_packets import digest

VERSION = 'pif_signal_desk_question_lineage_v1'


def schema():
    value = deepcopy(parent.schema())
    value['properties']['schema_version']['const'] = VERSION
    value['properties']['records'] = records.schema()
    return value


def _checked_parent_projection(value, source, window_id):
    records.validate(value, source=source, window_id=window_id)
    projection = deepcopy(value)
    projection['schema_version'] = records.parent.VERSION
    for event in projection['events']:
        if event['speech_act'] == 'question':
            event['speech_act'] = 'assertion'
    return projection


def validate(value, *, source, window_id, author_a, author_b):
    if not isinstance(value, dict) or value.get('schema_version') != VERSION:
        raise ValueError('wrong question lineage version')
    projection = deepcopy(value)
    projection['schema_version'] = parent.VERSION
    projection['records'] = _checked_parent_projection(value['records'], source, window_id)
    a = _checked_parent_projection(author_a, source, window_id)
    b = _checked_parent_projection(author_b, source, window_id)
    # Internal projection solely reuses the unchanged complete lineage checks.
    # No emitted question changes speech act or acquires assertion authority.
    parent.validate(projection, source=source, window_id=window_id, author_a=a, author_b=b)
    return value


def system():
    return authors.prompts()['C'] + '\n' + parent.RULES + '\nReturn the question-lineage envelope with question-contract records nested under records.'


def packet(source_packet, *, author_a, author_b):
    original = authors.packet(source_packet, 'C', author_a=author_a, author_b=author_b)
    value = {k: v for k, v in original.items() if k not in {'packet_sha256', 'system_sha256', 'schema_sha256', 'schema_version'}}
    value.update(schema_version=VERSION, system_sha256=digest(system()), schema_sha256=digest(schema()),
                 predecessor_packet_sha256=original['packet_sha256'])
    value['packet_sha256'] = digest(value)
    return value


def receipt():
    return {'family': VERSION, 'parent': parent.receipt(), 'records_contract': records.receipt(),
            'schema_sha256': digest(schema()), 'system_sha256': digest(system()),
            'qualified': False, 'gold_accepted': False, 'dispatch_enabled': False,
            'old_calls_are_not_new_family_evidence': True, 'complete_input_accountability_required': True}
