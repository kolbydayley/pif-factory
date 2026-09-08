"""Unqualified question representation family; never rewrites frozen requests."""
from . import signal_desk_full_event_v5_prompts as parent
from . import signal_desk_question_contract as contract
from .signal_desk_rubric_reference_packets import digest


def prompts():
    return {role: text + '\n' + contract.RULES for role, text in parent.prompts().items()}


def packet(source_packet, role, *, author_a=None, author_b=None):
    if role not in {'A', 'B', 'C', 'AUDIT'}:
        raise ValueError('invalid role')
    if role != 'C' and (author_a is not None or author_b is not None):
        raise ValueError('independent roles cannot see answers')
    sha = source_packet['packet_sha256']
    if digest({k: v for k, v in source_packet.items() if k != 'packet_sha256'}) != sha:
        raise ValueError('source lineage changed')
    p = {'window_id': source_packet['window_id'], 'transcript_window': source_packet['transcript_window'],
         'transcript_structure': source_packet['transcript_structure'], 'role': role,
         'original_source_packet_sha256': sha, 'schema_version': contract.VERSION,
         'system_sha256': digest(prompts()[role]), 'schema_sha256': digest(contract.schema())}
    if role == 'C':
        if author_a is None or author_b is None:
            raise ValueError('both independent authors required')
        for key, value in (('author_a', author_a), ('author_b', author_b)):
            p[key] = contract.validate(value, source=p['transcript_window'], window_id=p['window_id'])
    p['packet_sha256'] = digest(p)
    return p


def receipt():
    return {'family': contract.VERSION + '_all_roles', 'parent': parent.receipt(),
            'contract': contract.receipt(), 'role_hashes': {r: digest(s) for r, s in prompts().items()},
            'qualified': False, 'gold_accepted': False, 'dispatch_enabled': False,
            'requires_question_aware_lineage_envelope': True,
            'requires_full_original_16_window_qualification': True,
            'historical_results_cannot_be_relabelled_as_fresh_calls': True}
