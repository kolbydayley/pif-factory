import importlib.util
from pathlib import Path

import pytest

_spec = importlib.util.spec_from_file_location(
    "pif_gold_diag_turn", Path(__file__).resolve().parents[1] / "scripts/pif_gold_diag_turn.py"
)
diag = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(diag)


def test_diagnostic_refuses_what_the_runner_refuses():
    ok = dict(split="sealed_holdout", turn="A", allow_sealed_holdout=True, live_capacity=1)
    diag.validate_request(**ok)  # allowed
    with pytest.raises(SystemExit, match="sealed-holdout"):
        diag.validate_request(**{**ok, "allow_sealed_holdout": False})
    with pytest.raises(SystemExit, match="limited to A or B"):
        diag.validate_request(**{**ok, "turn": "C"})
    with pytest.raises(SystemExit, match="unknown split"):
        diag.validate_request(**{**ok, "split": "holdout"})
    # Never a 9th concurrent provider call.
    with pytest.raises(SystemExit, match="cap is full"):
        diag.validate_request(**{**ok, "live_capacity": diag.PROVIDER_CAP})
