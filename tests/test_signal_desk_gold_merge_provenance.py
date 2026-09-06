import hashlib

import pytest

from scripts.pif_signal_desk_gold_merge_provenance import checked_text, immutable_json


def test_source_contract_and_protected_split(tmp_path):
    text = "Host: Hello world"
    (tmp_path / "source.txt").write_text(text)
    row = {"split": "development", "transcript_path": "source.txt", "start_char": 0,
           "end_char": len(text), "transcript_sha256": hashlib.sha256(text.encode()).hexdigest(),
           "text_sha256": hashlib.sha256(text.encode()).hexdigest()}
    assert checked_text(row, tmp_path) == (text, text)
    for change in ({"split": "sealed_holdout"}, {"split": "validation"},
                   {"text_sha256": "stale"}, {"transcript_sha256": "stale"}):
        with pytest.raises(ValueError):
            checked_text({**row, **change}, tmp_path)


def test_append_only_projection(tmp_path):
    path = tmp_path / "C" / "w.json"
    immutable_json(path, {"speaker_id": "source label"})
    immutable_json(path, {"speaker_id": "source label"})
    with pytest.raises(ValueError):
        immutable_json(path, {"speaker_id": "invented"})
