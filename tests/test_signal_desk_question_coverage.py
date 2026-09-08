import pytest
from research_factory import signal_desk_question_review as review


def packet():
    return {'candidates':[],'transcript_window':'No consequential claims here.'}


def value(status='no_omissions_found',empty='supported_empty',notes='No omissions found.'):
    return dict(decisions=[],empty_window_verdict=empty,empty_window_rationale='No consequential claim in source.',
                coverage_notes=notes,coverage_status=status)


def test_clear_coverage_note_is_not_a_problem():
    v=value();assert review.validate_review(v,packet())==v


@pytest.mark.parametrize('status,empty',[
    ('no_omissions_found','missed_records'),('no_omissions_found','unusable'),
    ('possible_omissions','supported_empty')])
def test_contradictory_empty_judgment_fails(status,empty):
    with pytest.raises(ValueError,match='contradict'):
        review.validate_review(value(status,empty),packet())


def test_omission_requires_explanation():
    with pytest.raises(ValueError,match='requires explanation'):
        review.validate_review(value('possible_omissions','missed_records',''),packet())
