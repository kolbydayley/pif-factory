import pytest
from research_factory.signal_desk_qualification_audit import validate_qualification_scope
from research_factory.signal_desk_gold_shared_rubric import RUBRIC_ID, receipt


def inputs():
    ids = [str(i) for i in range(16)]
    return dict(plan={"rubric": receipt(), "manifest_sha256": "m", "window_ids": ids},
        manifest={"manifest_sha256": "m", "windows": [{"window_id": i, "split": "development"} for i in ids]},
        split="development", phase_order=("AUDIT",), target_window_ids=ids,
        prompt_variant_id=RUBRIC_ID, task_namespace="shared-rubric-qualification-audit-v1")


def test_qualification_is_never_reliability_gate():
    assert validate_qualification_scope(**inputs())["reliability_gate_eligible"] is False


@pytest.mark.parametrize("field,value", [("split", "sealed_holdout"), ("phase_order", ("A",)),
    ("prompt_variant_id", None), ("task_namespace", "development"), ("target_window_ids", ["0"])])
def test_scope_cannot_expand_or_shrink(field, value):
    kw = inputs(); kw[field] = value
    with pytest.raises(ValueError): validate_qualification_scope(**kw)


def test_protected_member_rejected():
    kw = inputs(); kw["manifest"]["windows"][0]["split"] = "validation"
    with pytest.raises(ValueError, match="protected"): validate_qualification_scope(**kw)


def test_entrypoint_keeps_existing_metered_runner_and_isolated_dispatch():
    from scripts.pif_signal_desk_rubric_independent_audit import run_arguments
    plan = inputs()["plan"]
    args = run_arguments(plan)
    assert args["qualification_audit_plan"] is plan
    assert args["turn_type"] == "AUDIT" and args["concurrency"] == 2
    assert args["dispatch_database"].name == "independent-audit-dispatch.sqlite"
    assert args["grant_path"].name == "signal_desk_gold_authoring_budget_grant.json"
    assert args["target_window_ids"] == plan["window_ids"]
