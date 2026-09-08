"""Independent full-source question-family record and disposition reviews."""
from . import signal_desk_full_event_v4_review as records_review
from . import signal_desk_full_event_v5_review as parent_review
from . import signal_desk_lineage_review as ledger_review
from . import signal_desk_question_contract as records
from . import signal_desk_question_lineage as lineage
from .signal_desk_rubric_reference_packets import digest

SYSTEM = parent_review.SYSTEM + '\n' + records.RULES + '''
QUESTION FAMILY REVIEW: An inquiry is not an assertion or forecast of its answer.
Evaluate questioner and answerer separately; check exact linked answer support.
Rhetorical assertions and mixed inquiry/assertion turns need semantic judgment,
not punctuation heuristics. Flag questions laundered into substantive claims,
invented answers, omitted consequential claims, and fabricated speaker bindings.
Unanswered questions can remain useful context without being factual evidence.
Give coverage_status independently of candidate correctness: no_omissions_found,
possible_omissions, or unusable_context. Assess the full source against the whole
record_index, not just assigned candidates. coverage_notes explains that verdict;
possible_omissions or unusable_context requires a substantive explanation. An
empty-window missed_records verdict requires possible_omissions; unusable requires
unusable_context. A reassuring note alone is not an omission finding. This review
is not a replacement for the independently sampled reliability audit.
'''
LEDGER_SYSTEM = ledger_review.SYSTEM + '\n' + records.RULES


def schema(packet):
    from copy import deepcopy
    result=deepcopy(parent_review.schema(packet))
    result['required'].append('coverage_status')
    result['properties']['coverage_status']={'type':'string','enum':[
        'no_omissions_found','possible_omissions','unusable_context']}
    return result


def validate_review(value,packet):
    from .signal_desk_full_event_experiment import _shape
    _shape(value,schema(packet))
    parent_review.validate_review({k:v for k,v in value.items() if k!='coverage_status'},packet)
    status=value['coverage_status'];empty=value['empty_window_verdict']
    if status!='no_omissions_found' and not value['coverage_notes'].strip():
        raise ValueError('coverage finding requires explanation')
    if empty=='missed_records' and status!='possible_omissions':
        raise ValueError('missing records contradict coverage status')
    if empty=='unusable' and status!='unusable_context':
        raise ValueError('unusable source contradicts coverage status')
    if empty=='supported_empty' and status!='no_omissions_found':
        raise ValueError('supported empty contradicts coverage status')
    return value


def packets(output, *, source, window_id, token_count):
    return records_review.packets(output,source=source,window_id=window_id,token_count=token_count,
        system=SYSTEM,schema_for_packet=schema,output_validator=records.validate)


def ledger_packets(output, author_packet, *, token_count):
    return ledger_review.packets(output,author_packet,token_count=token_count,
        system=LEDGER_SYSTEM,output_validator=lineage.validate)


def receipt():
    return {'family':'question-independent-source-review-v2','system_sha256':digest(SYSTEM),
            'ledger_system_sha256':digest(LEDGER_SYSTEM),'records':records.receipt(),'lineage':lineage.receipt(),
            'qualified':False,'gold_accepted':False,'requires_all_four_roles_and_complete_ledger':True}
