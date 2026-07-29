from __future__ import annotations

from research_factory import true_north
from research_factory.true_north_count_first import (
    _COUNT_FIRST_REPLACEMENT,
    _REMOVED_DEFAULT,
    count_first_system_prompt,
)


def test_count_first_changes_only_the_authorized_framing() -> None:
    frozen = true_north.MULTIPASS_SYSTEM_PROMPTS["adjudication"]
    variant = count_first_system_prompt()

    assert _REMOVED_DEFAULT in frozen
    assert _REMOVED_DEFAULT not in variant
    assert _COUNT_FIRST_REPLACEMENT in variant
    assert variant == frozen.replace(
        _REMOVED_DEFAULT, _COUNT_FIRST_REPLACEMENT, 1
    )


def test_count_first_preserves_closed_contract_vocabulary() -> None:
    variant = count_first_system_prompt()

    assert "Depart from the proposal only" in variant
    assert "Report the edit reason for every claim." in variant
    assert "Never introduce vocabulary absent" in variant
