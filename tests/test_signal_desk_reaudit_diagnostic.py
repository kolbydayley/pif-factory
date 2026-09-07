from scripts.pif_signal_desk_gold_reaudit_diagnostic import cohort


def test_cohorts_distinguish_changed_gold_from_new_audit_fields():
    previous={"gold":{"speaker_id":"A"},"fields":["stance"]}
    assert cohort(previous,None,"")=="newly_disputed_gold_events"
    assert cohort(previous,previous,"gold_supported").endswith("gold_supported")
    assert cohort({"gold":{"speaker_id":"B"},"fields":["stance"]},previous,"")=="changed_gold_disputed_again"
    assert cohort({"gold":previous["gold"],"fields":["speaker"]},previous,"")=="unchanged_gold_new_disputed_fields"
