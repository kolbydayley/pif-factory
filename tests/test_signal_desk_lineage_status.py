import json
from scripts import pif_signal_desk_lineage_status as status


def test_held_raw_failure_stays_in_completed_denominator(tmp_path, monkeypatch):
    packet = {'packet_sha256': 'original', 'role': 'A'}
    (tmp_path/'original.output.json').write_text(json.dumps({'events': [{}, {}]}))
    monkeypatch.setattr(status.run, 'verify_provider', lambda directory, p: None)
    def invalid(raw, p):
        raise ValueError('unresolved source attribution')
    monkeypatch.setattr(status.run, 'validate', invalid)
    row = dict(state='held', **status.original_quality(tmp_path, packet))
    assert row['original_first_pass_valid'] is False
    assert row['original_record_count'] == 2
    summary = status.quality_summary([row, dict(state='not_started')])
    assert summary['completed_raw_quality']['first_pass_invalid'] == 1
    assert summary['completed_raw_quality']['raw_outputs_present'] == 1
    assert summary['completed_raw_quality']['held_raw_outputs'] == 1
    assert summary['verified_output_quality']['first_pass_invalid'] == 0


def test_repair_does_not_convert_original_failure_into_success():
    rows = [dict(state='authored_not_accepted', original_raw_present=True,
                 original_first_pass_valid=False, repaired_output=True),
            dict(state='held', original_raw_present=True),
            dict(state='waiting_for_parents')]
    summary = status.quality_summary(rows)['completed_raw_quality']
    assert summary['raw_outputs_present'] == 2
    assert summary['first_pass_invalid'] == 1
    assert summary['first_pass_unavailable'] == 1
    assert summary['explicitly_repaired'] == 1
    assert summary['first_pass_valid'] == 0


def test_unverified_provider_is_reported_not_scored(tmp_path, monkeypatch):
    (tmp_path/'x.output.json').write_text('{"events": []}')
    def mismatch(directory, p):
        raise ValueError('provider mismatch')
    monkeypatch.setattr(status.run, 'verify_provider', mismatch)
    result = status.original_quality(tmp_path, {'packet_sha256': 'x', 'role': 'A'})
    assert result['original_raw_present']
    assert 'original_first_pass_valid' not in result
    assert result['original_quality_unavailable_reason'] == 'provider mismatch'
