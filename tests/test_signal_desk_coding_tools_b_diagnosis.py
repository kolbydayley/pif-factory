import json
from unittest.mock import patch
import pytest
from research_factory import signal_desk_coding_tools_b_diagnosis as diagnosis
from scripts import pif_signal_desk_coding_tools_b_capacity_retry as retry


def test_missing_retry_cannot_be_complete():
    with patch.object(diagnosis, 'verify', side_effect=ValueError('retry not verified')):
        with pytest.raises(ValueError, match='retry not verified'):
            diagnosis.verified_diagnosis()


def test_combined_population_cannot_shrink():
    with patch.object(diagnosis, 'verify', return_value=([], [])):
        with pytest.raises(ValueError, match='population changed'):
            diagnosis.verified_diagnosis()
