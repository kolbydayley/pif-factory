from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

import pytest

from research_factory import app_server_canonical_v31_development_freeze as development_freeze
from research_factory import app_server_canonical_v31_epoch7_controller as controller
from research_factory import app_server_canonical_v31_production_contract as production_contract
from research_factory import app_server_canonical_v31_promotion_lineage as lineage
from tests import test_app_server_canonical_v31_development_freeze as freeze_fixtures
from tests import test_app_server_canonical_v31_epoch7_controller as fixtures


@pytest.fixture(scope="module")
def frozen_bundle(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    root = tmp_path_factory.mktemp("canonical-promotion-lineage")
    patcher = pytest.MonkeyPatch()
    extraction_client = freeze_fixtures._UniqueCostLiveFixtureFactory()
    patcher.setattr(controller.matrix.adapter, "_client_factory", extraction_client)
    bundle = fixtures._fixture(root)
    quality = fixtures._terminal_quality_fixture(bundle, patcher)
    controller.freeze_quality_terminal_receipt(
        bundle["controller_root"],
        quality["quality_root"],
        project_root=bundle["project"],
    )

    def verified_live_quality(
        quality_root: Path, *, project_root: Path | None = None
    ) -> dict[str, Any]:
        return controller.verify_quality_terminal_receipt(
            bundle["controller_root"],
            quality_root,
            project_root=project_root,
        )

    patcher.setattr(
        development_freeze.quality_runtime,
        "verify_quality_runtime",
        verified_live_quality,
    )
    freeze_root = bundle["project"] / "work" / "development-freeze"
    frozen = development_freeze.freeze_development_winner(
        controller_root=bundle["controller_root"],
        quality_root=quality["quality_root"],
        freeze_root=freeze_root,
        operator_authorization_id="kolby-promotion-lineage-fixture",
        project_root=bundle["project"],
    )
    yield {**bundle, "quality": quality, "freeze_root": freeze_root, "frozen": frozen}
    patcher.undo()


def _freeze_lineage(
    bundle: Mapping[str, Any], name: str = "promotion-lineage"
) -> dict[str, Any]:
    return lineage.freeze_development_promotion_lineage(
        development_freeze_root=bundle["freeze_root"],
        lineage_root=bundle["project"] / "work" / name,
        project_root=bundle["project"],
    )


def test_verified_epoch7_freeze_emits_production_consumable_development_lineage(
    frozen_bundle: Mapping[str, Any],
) -> None:
    result = _freeze_lineage(frozen_bundle)
    receipt = result["receipt"]
    assert receipt["state"] == "development_lineage_ready_untouched_holdout_closed"
    assert receipt["winner_variant_id"] == frozen_bundle["frozen"][
        "receipt"
    ]["winner_variant_id"]
    assert production_contract._verify_matrix_receipt(result["matrix_lineage"])
    assert production_contract._verify_quality_receipt(
        result["quality_lineage"],
        matrix=result["matrix_lineage"],
    )
    assert result["quality_lineage"]["matrix_lineage_sha256"] == result[
        "matrix_record"
    ]["sha256"]
    assert receipt["semantic_model_call_count_added"] == 0
    assert receipt["holdout_inspected"] is False
    assert receipt["holdout_authorized"] is False
    assert receipt["production_authorized"] is False
    assert receipt["production_mutated"] is False


def test_lineage_tamper_is_rejected_by_adapter_and_production_contract(
    frozen_bundle: Mapping[str, Any],
) -> None:
    result = _freeze_lineage(frozen_bundle, "promotion-lineage-tamper")
    root = Path(result["receipt"]["lineage_root"])
    quality_path = root / lineage.QUALITY_LINEAGE_FILENAME
    quality = json.loads(quality_path.read_text())
    quality["selected_variant_id"] = production_contract.EXPECTED_VARIANT_IDS[0]
    quality_path.write_text(
        json.dumps(quality, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )
    with pytest.raises(
        lineage.CanonicalV31PromotionLineageError,
        match="quality lineage drifted",
    ):
        lineage.verify_development_promotion_lineage(
            root,
            project_root=frozen_bundle["project"],
        )


def test_partial_or_nested_lineage_root_fails_before_artifact_creation(
    frozen_bundle: Mapping[str, Any],
) -> None:
    nested = frozen_bundle["freeze_root"] / "lineage"
    with pytest.raises(
        lineage.CanonicalV31PromotionLineageError,
        match="must be disjoint",
    ):
        lineage.freeze_development_promotion_lineage(
            development_freeze_root=frozen_bundle["freeze_root"],
            lineage_root=nested,
            project_root=frozen_bundle["project"],
        )
    assert not nested.exists()

    partial = frozen_bundle["project"] / "work" / "partial-lineage"
    partial.mkdir(parents=True)
    (partial / "foreign.json").write_text("{}", encoding="utf-8")
    with pytest.raises(
        lineage.CanonicalV31PromotionLineageError,
        match="partial or not fresh",
    ):
        lineage.freeze_development_promotion_lineage(
            development_freeze_root=frozen_bundle["freeze_root"],
            lineage_root=partial,
            project_root=frozen_bundle["project"],
        )


def test_lineage_cli_freeze_status_verify_are_zero_call_and_holdout_closed(
    frozen_bundle: Mapping[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    root = frozen_bundle["project"] / "work" / "promotion-lineage-cli"
    monkeypatch.setattr(lineage, "PROJECT_ROOT", frozen_bundle["project"])
    assert lineage.main(
        [
            "freeze",
            "--development-freeze-root",
            str(frozen_bundle["freeze_root"]),
            "--lineage-root",
            str(root),
        ]
    ) == 0
    frozen = json.loads(capsys.readouterr().out)
    assert frozen["result"]["receipt"]["semantic_model_call_count_added"] == 0
    assert lineage.main(["status", "--lineage-root", str(root)]) == 0
    status = json.loads(capsys.readouterr().out)
    assert status["result"]["holdout_authorized"] is False
    assert lineage.main(["verify", "--lineage-root", str(root)]) == 0
    verified = json.loads(capsys.readouterr().out)
    assert verified["result"]["production_authorized"] is False
