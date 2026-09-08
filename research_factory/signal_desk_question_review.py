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
'''
LEDGER_SYSTEM = ledger_review.SYSTEM + '\n' + records.RULES
schema = parent_review.schema
validate_review = parent_review.validate_review


def packets(output, *, source, window_id, token_count):
    return records_review.packets(output,source=source,window_id=window_id,token_count=token_count,
        system=SYSTEM,schema_for_packet=schema,output_validator=records.validate)


def ledger_packets(output, author_packet, *, token_count):
    return ledger_review.packets(output,author_packet,token_count=token_count,
        system=LEDGER_SYSTEM,output_validator=lineage.validate)


def receipt():
    return {'family':'question-independent-source-review-v1','system_sha256':digest(SYSTEM),
            'ledger_system_sha256':digest(LEDGER_SYSTEM),'records':records.receipt(),'lineage':lineage.receipt(),
            'qualified':False,'gold_accepted':False,'requires_all_four_roles_and_complete_ledger':True}
