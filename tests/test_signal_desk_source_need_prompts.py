from research_factory import signal_desk_source_need_prompts as variant


def test_single_append_only_dimension_no_schema_or_validator_change():
    old=variant.parent.prompts();new=variant.prompts()
    assert set(old)==set(new)
    for role in old:
        assert new[role]==old[role]+'\n'+variant.CLARIFICATION
    receipt=variant.receipt()
    assert not receipt['validator_changed']
    assert not receipt['qualified'] and not receipt['gold_accepted'] and not receipt['dispatch_enabled']
    assert receipt['requires_fresh_full_population_all_role_qualification']


def test_rule_preserves_unknown_voice_and_genuine_recovery_need():
    rule=variant.CLARIFICATION
    assert 'null transcript_voice' in rule and 'uncertain publishability' in rule
    assert 'if and only if' in rule and 'research_limitation' in rule
    assert 'Do not mechanically clear needs' in rule
    assert 'not as verified world fact' in rule
