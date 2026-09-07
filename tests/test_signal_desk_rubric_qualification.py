import pytest
from scripts.pif_signal_desk_gold_rubric_qualification import select_windows
from research_factory.signal_desk_gold_prompt_variant import system_prompts_for_variant,baseline_system_prompts
from research_factory.signal_desk_gold_shared_rubric import RUBRIC_ID,TEXT


def test_selection_is_source_only_and_seal_safe():
    rows=[{"window_id":f"{s}-{i}","show_id":f"{s}-{i}","split":"development","transcript_structure":s} for s in
          ("speaker_turn","paragraph","flattened","asr_diarized") for i in range(5)]
    rows.append({"window_id":"sealed","show_id":"sealed","split":"sealed_holdout","transcript_structure":"flattened"})
    selected=select_windows(rows)
    assert len(selected)==16 and len({r["show_id"] for r in selected})==16
    assert all(r["split"]=="development" for r in selected)
    assert selected==select_windows(list(reversed(rows)))


def test_one_rubric_in_all_roles_without_baseline_mutation():
    before=baseline_system_prompts()
    prompts=system_prompts_for_variant(RUBRIC_ID)
    assert set(prompts)=={"A","B","C","AUDIT"}
    assert all(p.endswith(TEXT) for p in prompts.values())
    assert baseline_system_prompts()==before


def test_insufficient_source_coverage_fails_closed():
    with pytest.raises(ValueError):select_windows([])
