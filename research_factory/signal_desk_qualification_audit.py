"""Scope guard for diagnostic role checks; never a powered reliability audit."""
from .signal_desk_gold_shared_rubric import RUBRIC_ID, receipt


def validate_qualification_scope(plan, *, manifest, split, phase_order,
                                 target_window_ids, prompt_variant_id, task_namespace):
    if (split != "development" or tuple(phase_order) != ("AUDIT",)
            or prompt_variant_id != RUBRIC_ID
            or task_namespace != "shared-rubric-qualification-audit-v1"):
        raise ValueError("qualification audit requires isolated development-only AUDIT scope")
    if plan.get("rubric") != receipt() or plan.get("manifest_sha256") != manifest["manifest_sha256"]:
        raise ValueError("qualification audit requires frozen rubric and manifest")
    ids = plan.get("window_ids", [])
    if len(ids) != 16 or len(set(ids)) != 16 or set(ids) != set(target_window_ids or ()):
        raise ValueError("qualification audit requires all sixteen frozen members")
    rows = {r["window_id"]: r for r in manifest["windows"]}
    if any(i not in rows or rows[i]["split"] != "development" for i in ids):
        raise ValueError("qualification audit cannot read protected splits")
    return {"reliability_gate_eligible": False, "windows": 16}
