from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any, Mapping

import pytest

from research_factory import app_server_canonical_v31_development_freeze as freeze
from research_factory import app_server_canonical_v31_epoch7_controller as controller
from tests import test_app_server_canonical_v31_development_matrix_runtime as runtime_fixtures
from tests import test_app_server_canonical_v31_epoch7_controller as fixtures


class _UniqueCostLiveFixtureClient(runtime_fixtures.LiveFixtureClient):
    async def run_structured_turn(self, **kwargs: Any) -> Any:
        result = await super().run_structured_turn(**kwargs)
        if kwargs["batch_size"] == 8 and kwargs["thread_mode"] == "same_thread":
            sidecar_path = Path(kwargs["sidecar_path"])
            sidecar = json.loads(sidecar_path.read_text())
            sidecar["usage"]["input_tokens"] -= 1
            sidecar["usage"]["total_tokens"] -= 1
            thread_total = self.thread_totals[sidecar["thread_id"]]
            thread_total["input_tokens"] -= 1
            thread_total["total_tokens"] -= 1
            sidecar["thread_total_usage"] = copy.deepcopy(thread_total)
            sidecar_path.write_text(
                json.dumps(sidecar, sort_keys=True) + "\n", encoding="utf-8"
            )
        return result


class _UniqueCostLiveFixtureFactory:
    def __init__(self) -> None:
        self.client = _UniqueCostLiveFixtureClient()

    def __call__(self) -> _UniqueCostLiveFixtureClient:
        return self.client


@pytest.fixture(scope="module")
def passed_quality_bundle(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    root = tmp_path_factory.mktemp("canonical-development-freeze")
    patcher = pytest.MonkeyPatch()
    extraction_client = _UniqueCostLiveFixtureFactory()
    patcher.setattr(controller.matrix.adapter, "_client_factory", extraction_client)
    bundle = fixtures._fixture(root)
    quality = fixtures._terminal_quality_fixture(bundle, patcher)
    terminal = controller.freeze_quality_terminal_receipt(
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
        freeze.quality_runtime, "verify_quality_runtime", verified_live_quality
    )
    yield {**bundle, "quality": quality, "terminal": terminal}
    patcher.undo()


def _freeze(
    bundle: Mapping[str, Any], name: str = "development-freeze"
) -> dict[str, Any]:
    return freeze.freeze_development_winner(
        controller_root=bundle["controller_root"],
        quality_root=bundle["quality"]["quality_root"],
        freeze_root=bundle["project"] / "work" / name,
        operator_authorization_id=f"kolby-{name}-authorization",
        project_root=bundle["project"],
    )


def test_epoch7_plan_binds_selection_policy_before_semantic_work(
    passed_quality_bundle: Mapping[str, Any],
) -> None:
    loaded = controller.load_epoch7_controller(
        passed_quality_bundle["controller_root"],
        project_root=passed_quality_bundle["project"],
    )
    policy = controller.development_selection_policy()
    assert loaded["contract"]["development_selection_policy_sha256"] == policy[
        "policy_sha256"
    ]
    assert controller._load_object(
        loaded["records"]["development_selection_policy"],
        label="selection policy",
    ) == policy["policy"]
    assert policy["policy"]["exact_metric_tie_policy"] == (
        "reject_no_unique_development_winner"
    )
    assert policy["policy"]["holdout_authorized"] is False


def test_passed_live_quality_freezes_one_exact_development_configuration(
    passed_quality_bundle: Mapping[str, Any],
) -> None:
    result = _freeze(passed_quality_bundle)
    receipt = result["receipt"]
    analysis = result["selection_analysis"]
    configuration = result["winner_configuration"]
    assert receipt["state"] == (
        "development_winner_frozen_holdout_and_production_closed"
    )
    assert receipt["winner_variant_id"] == analysis["ranking"][0]
    assert configuration["winner_variant_id"] == receipt["winner_variant_id"]
    assert configuration["batch_size"] in {3, 5, 8}
    assert configuration["thread_mode"] in {"new_thread", "same_thread"}
    assert configuration["quality_evaluator_frozen"] is True
    assert configuration["shared_reference_frozen"] is True
    assert receipt["development_winner_frozen"] is True
    assert receipt["semantic_model_call_count_added"] == 0
    assert receipt["holdout_authorized"] is False
    assert receipt["production_mutated"] is False
    assert freeze.verify_development_winner_freeze(
        Path(receipt["freeze_root"]),
        project_root=passed_quality_bundle["project"],
    )["receipt"] == receipt


def test_exact_quality_cost_tie_rejects_selection_without_variant_id_tiebreak(
    passed_quality_bundle: Mapping[str, Any],
) -> None:
    inputs = freeze._quality_inputs(
        controller_root=passed_quality_bundle["controller_root"],
        quality_root=passed_quality_bundle["quality"]["quality_root"],
        project_root=passed_quality_bundle["project"],
    )
    altered = copy.deepcopy(inputs)
    current = freeze._selection_analysis(inputs)
    rows = altered["terminal"]["receipt"]["arm_results"]
    winner = next(
        row for row in rows if row["variant_id"] == current["winner_variant_id"]
    )
    challenger = next(
        row for row in rows if row["variant_id"] != current["winner_variant_id"]
    )
    challenger["strict_full_field_macro"] = winner["strict_full_field_macro"]
    challenger["production_amortized_total_token_ratio"] = winner[
        "production_amortized_total_token_ratio"
    ]
    with pytest.raises(
        freeze.CanonicalV31DevelopmentFreezeError, match="no unique winner"
    ):
        freeze._selection_analysis(altered)


def test_nonlive_or_offline_quality_verification_cannot_freeze(
    passed_quality_bundle: Mapping[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        freeze.quality_runtime,
        "verify_quality_runtime",
        lambda *_args, **_kwargs: {
            "state": "offline_fixture_completed_non_promotable",
            "receipt": {"promotable": False},
        },
    )
    with pytest.raises(
        freeze.CanonicalV31DevelopmentFreezeError,
        match="verified live passing quality terminal",
    ):
        freeze.freeze_development_winner(
            controller_root=passed_quality_bundle["controller_root"],
            quality_root=passed_quality_bundle["quality"]["quality_root"],
            freeze_root=passed_quality_bundle["project"] / "work" / "offline-reject",
            operator_authorization_id="kolby-offline-reject-authorization",
            project_root=passed_quality_bundle["project"],
        )


def test_malformed_quality_runtime_failure_is_typed_and_cli_fails_closed(
    passed_quality_bundle: Mapping[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def malformed_runtime(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        raise freeze.quality_runtime.CanonicalV31QualityRuntimeError(
            "quality runtime lineage drifted"
        )

    monkeypatch.setattr(
        freeze.quality_runtime, "verify_quality_runtime", malformed_runtime
    )
    root = passed_quality_bundle["project"] / "work" / "malformed-runtime"
    monkeypatch.setattr(freeze, "PROJECT_ROOT", passed_quality_bundle["project"])
    code = freeze.main(
        [
            "freeze",
            "--controller-root",
            str(passed_quality_bundle["controller_root"]),
            "--quality-root",
            str(passed_quality_bundle["quality"]["quality_root"]),
            "--freeze-root",
            str(root),
            "--operator-authorization-id",
            "kolby-malformed-runtime-authorization",
        ]
    )
    assert code == 2
    error = json.loads(capsys.readouterr().err)
    assert error == {"error": "quality runtime lineage drifted", "ok": False}
    assert not root.exists()


def test_non_boolean_arm_pass_state_cannot_enter_selection(
    passed_quality_bundle: Mapping[str, Any],
) -> None:
    inputs = freeze._quality_inputs(
        controller_root=passed_quality_bundle["controller_root"],
        quality_root=passed_quality_bundle["quality"]["quality_root"],
        project_root=passed_quality_bundle["project"],
    )
    altered = copy.deepcopy(inputs)
    altered["terminal"]["receipt"]["arm_results"][0]["passed"] = 1
    with pytest.raises(
        freeze.CanonicalV31DevelopmentFreezeError,
        match="development quality checks drifted",
    ):
        freeze._selection_analysis(altered)


def test_selection_policy_or_winner_artifact_tamper_is_rejected(
    passed_quality_bundle: Mapping[str, Any], tmp_path: Path
) -> None:
    result = _freeze(passed_quality_bundle, "tamper-freeze")
    root = Path(result["receipt"]["freeze_root"])
    configuration_path = root / freeze.WINNER_CONFIGURATION_FILENAME
    configuration = json.loads(configuration_path.read_text())
    configuration["batch_size"] = 99
    configuration_path.write_text(
        json.dumps(configuration, sort_keys=True, separators=(",", ":"))
    )
    with pytest.raises(
        freeze.CanonicalV31DevelopmentFreezeError, match="configuration drifted"
    ):
        freeze.verify_development_winner_freeze(
            root, project_root=passed_quality_bundle["project"]
        )

    separate = fixtures._fixture(tmp_path / "policy")
    policy_path = (
        separate["controller_root"] / controller.SELECTION_POLICY_FILENAME
    )
    policy = json.loads(policy_path.read_text())
    policy["exact_metric_tie_policy"] = "variant_id"
    policy_path.write_text(json.dumps(policy, sort_keys=True, separators=(",", ":")))
    with pytest.raises(controller.Epoch7ControllerError, match="record drifted"):
        controller.load_epoch7_controller(
            separate["controller_root"], project_root=separate["project"]
        )


def test_freeze_cli_status_and_verify_add_no_semantic_calls(
    passed_quality_bundle: Mapping[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    root = passed_quality_bundle["project"] / "work" / "cli-freeze"
    monkeypatch.setattr(freeze, "PROJECT_ROOT", passed_quality_bundle["project"])
    code = freeze.main(
        [
            "freeze",
            "--controller-root",
            str(passed_quality_bundle["controller_root"]),
            "--quality-root",
            str(passed_quality_bundle["quality"]["quality_root"]),
            "--freeze-root",
            str(root),
            "--operator-authorization-id",
            "kolby-cli-freeze-authorization",
        ]
    )
    assert code == 0
    frozen = json.loads(capsys.readouterr().out)
    assert frozen["result"]["receipt"]["semantic_model_call_count_added"] == 0
    assert freeze.main(["status", "--freeze-root", str(root)]) == 0
    status = json.loads(capsys.readouterr().out)
    assert status["result"]["state"] == (
        "development_winner_frozen_holdout_and_production_closed"
    )
    assert freeze.main(["verify", "--freeze-root", str(root)]) == 0
    verified = json.loads(capsys.readouterr().out)
    assert verified["result"]["holdout_authorized"] is False
