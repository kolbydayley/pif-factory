from __future__ import annotations

import asyncio
import copy
import json
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping

import pytest

from research_factory import app_server_canonical_v31_development_matrix as matrix
from research_factory import app_server_canonical_v31_development_matrix_runtime as runtime
from research_factory import app_server_canonical_v31_episode_batch as adapter
from research_factory import app_server_thread_supervisor as supervisor
from tests import test_app_server_canonical_v31_development_matrix as matrix_fixtures
from tests import test_app_server_canonical_v31_episode_batch as adapter_fixtures


NOW = datetime(2026, 7, 18, 21, 0, tzinfo=timezone.utc)
PER_TURN_TOKENS = 2_000
PER_TURN_WALL = 10.0


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _sha(value: Any) -> str:
    import hashlib

    return hashlib.sha256(_canonical(value).encode("ascii")).hexdigest()


def _write_json(path: Path, value: Any) -> dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    return matrix._record(path)


class CapacityProbe:
    def __init__(
        self,
        *,
        available: bool = True,
        same_identity: bool = False,
        numeric_drift: bool = False,
    ) -> None:
        self.available = available
        self.same_identity = same_identity
        self.numeric_drift = numeric_drift
        self.calls: list[dict[str, Any]] = []

    def __call__(self, context: Mapping[str, Any]) -> dict[str, Any]:
        self.calls.append(copy.deepcopy(dict(context)))
        measurement_id = (
            "measurement-reused"
            if self.same_identity
            else f"measurement-{len(self.calls)}-{context['variant_id']}"
        )
        available_tokens: Any = (
            "not-numeric"
            if self.numeric_drift
            else int(context["required_remaining_total_token_ceiling"]) + 1_000
        )
        available_wall = float(context["required_remaining_wall_seconds_ceiling"]) + 60.0
        measurement = {
            "schema_version": runtime.CAPACITY_MEASUREMENT_VERSION,
            "measurement_id": measurement_id,
            "measured_at": (NOW - timedelta(seconds=1)).isoformat(),
            "expires_at": (NOW + timedelta(minutes=15)).isoformat(),
            "state": (
                "available_before_semantic_thread_or_turn"
                if self.available
                else "unavailable_before_semantic_thread_or_turn"
            ),
            **copy.deepcopy(dict(context)),
            "available_total_tokens": available_tokens,
            "available_wall_seconds": available_wall,
            "capacity_available": self.available,
            "capacity_unknown": False,
            "official_app_server_initialized_before_capacity": True,
            "semantic_thread_started": False,
            "semantic_thread_resumed": False,
            "semantic_turn_started": False,
            "managed_chatgpt_auth_only": True,
            "managed_chatgpt_plan_type": "pro",
            "extraction_only": True,
            "quality_evaluation_authorized": False,
            "holdout_authorized": False,
            "production_mutation_allowed": False,
            "offline_test_mode": True,
            "live_capacity_authority": False,
        }
        return measurement


class TrackingArmRunner:
    def __init__(self, mutate: str | None = None) -> None:
        self.calls: list[tuple[int, str]] = []
        self.mutate = mutate
        self.first_thread_id: str | None = None

    async def __call__(self, episodes, **kwargs):
        self.calls.append((kwargs["batch_size"], kwargs["thread_mode"]))
        report = await adapter.run_episode_batch_arm(episodes, **kwargs)
        arm_dir = Path(kwargs["output_dir"])
        if self.first_thread_id is None:
            self.first_thread_id = report["results"][0]["thread_id"]
        if self.mutate == "artifact" and len(self.calls) == 1:
            labels_path = Path(
                report["results"][0]["records"]["canonical_labels"]["path"]
            )
            labels_path.write_text(labels_path.read_text() + " ", encoding="utf-8")
        elif self.mutate == "opaque_order" and len(self.calls) == 1:
            labels_record = report["results"][0]["records"]["canonical_labels"]
            labels_path = Path(labels_record["path"])
            labels = json.loads(labels_path.read_text())
            labels[0], labels[1] = labels[1], labels[0]
            labels_path.write_text(json.dumps(labels, sort_keys=True) + "\n")
            labels_record.update(matrix._record(labels_path))
            (arm_dir / "report.json").write_text(
                json.dumps(report, sort_keys=True, indent=2) + "\n"
            )
        elif self.mutate == "cross_thread" and len(self.calls) == 2:
            report["results"][0]["thread_id"] = self.first_thread_id
            (arm_dir / "report.json").write_text(
                json.dumps(report, sort_keys=True, indent=2) + "\n"
            )
        elif self.mutate == "retry" and len(self.calls) == 1:
            result = report["results"][0]
            attempt_path = Path(result["records"]["attempt"]["path"])
            attempt = json.loads(attempt_path.read_text())
            attempt["retry_count"] = 1
            result["records"]["attempt"] = _write_json(attempt_path, attempt)
            _write_json(attempt_path.parent / "result.json", result)
            _write_json(arm_dir / "report.json", report)
        elif self.mutate == "duplicate_turn" and len(self.calls) == 1:
            first_turn = report["results"][0]["turn_id"]
            result = report["results"][1]
            sidecar_path = Path(result["records"]["sidecar"]["path"])
            sidecar = json.loads(sidecar_path.read_text())
            sidecar["turn_id"] = first_turn
            result["turn_id"] = first_turn
            result["records"]["sidecar"] = _write_json(sidecar_path, sidecar)
            _write_json(sidecar_path.parent / "result.json", result)
            _write_json(arm_dir / "report.json", report)
        elif self.mutate == "provenance_semantics" and len(self.calls) == 1:
            result = report["results"][0]
            provenance_path = Path(
                result["records"]["evidence_provenance"]["path"]
            )
            provenance = json.loads(provenance_path.read_text())
            provenance["exact_evidence_projected_from_private_source_text"] = False
            result["records"]["evidence_provenance"] = _write_json(
                provenance_path, provenance
            )
            _write_json(provenance_path.parent / "result.json", result)
            _write_json(arm_dir / "report.json", report)
        return report


class LiveFixtureClient(adapter_fixtures._FixtureClient):
    """Managed-client fixture with the exact official rate-limit RPC surface."""

    def __init__(self) -> None:
        super().__init__()
        self.rate_limit_reads = 0
        self.enter_count = 0
        self.exit_count = 0
        self.events: list[str] = []

    async def __aenter__(self):
        self.enter_count += 1
        self.events.append("official_initialize")
        return self

    async def __aexit__(self, *_args):
        self.exit_count += 1
        self.events.append("official_close")
        return None

    async def _request(self, method: str, params: Mapping[str, Any]):
        assert method == "account/rateLimits/read"
        assert params == {}
        assert self.enter_count > self.exit_count
        self.rate_limit_reads += 1
        self.events.append("capacity_read")
        reset_at = int(datetime.now(timezone.utc).timestamp()) + 3_600
        snapshot = {
            "limitId": "codex",
            "primary": {
                "usedPercent": 0,
                "resetsAt": reset_at,
                "windowDurationMins": 300,
            },
            "secondary": {
                "usedPercent": 0,
                "resetsAt": reset_at,
                "windowDurationMins": 10_080,
            },
            "rateLimitReachedType": None,
        }
        return {
            "rateLimits": copy.deepcopy(snapshot),
            "rateLimitsByLimitId": {"codex": snapshot},
            "rateLimitResetCredits": {"availableCount": 0},
        }

    async def start_thread(self, **kwargs):
        assert self.events[-1] == "capacity_read"
        self.events.append("thread_start")
        return await super().start_thread(**kwargs)

    async def run_structured_turn(self, **kwargs):
        assert self.events[-1] == "capacity_read"
        self.events.append("turn_start")
        return await super().run_structured_turn(**kwargs)


class LiveFixtureFactory:
    def __init__(self) -> None:
        self.client = LiveFixtureClient()

    def __call__(self):
        return self.client


def test_trusted_rate_limit_parser_requires_pinned_fallback_and_codex_bucket() -> None:
    client = LiveFixtureClient()
    client.enter_count = 1
    response = asyncio.run(client._request("account/rateLimits/read", {}))
    parsed = runtime.parse_trusted_rate_limit_capacity(response)
    assert parsed["limit_id"] == "codex"
    assert parsed["minimum_applicable_remaining_percent"] == 100
    assert parsed["capacity_is_reservation"] is False

    missing_fallback = copy.deepcopy(response)
    missing_fallback.pop("rateLimits")
    with pytest.raises(runtime.CanonicalV31CapacityUnavailable, match="fallback"):
        runtime.parse_trusted_rate_limit_capacity(missing_fallback)
    missing_codex = copy.deepcopy(response)
    missing_codex["rateLimitsByLimitId"] = {"other": response["rateLimits"]}
    with pytest.raises(runtime.CanonicalV31CapacityUnavailable, match="Codex"):
        runtime.parse_trusted_rate_limit_capacity(missing_codex)


def _runtime_fixture(
    tmp_path: Path,
    *,
    per_turn_wall: float = PER_TURN_WALL,
    now_value: datetime = NOW,
    offline_test_mode: bool = True,
    client_factory: Any | None = None,
) -> dict[str, Any]:
    fixture_root = tmp_path / "fixture"
    data = matrix_fixtures._fixture(fixture_root)
    capacity_path = Path(data["capacity_record"]["path"])
    capacity = json.loads(capacity_path.read_text())
    capacity["maximum_total_tokens_per_turn"] = PER_TURN_TOKENS
    capacity["maximum_wall_seconds_per_turn"] = per_turn_wall
    data["capacity_record"] = _write_json(capacity_path, capacity)

    preflight = matrix.build_matrix_dry_preflight(
        manifest_path=Path(data["manifest_record"]["path"]),
        manifest_sha256=data["manifest_record"]["sha256"],
        episodes=data["episodes"],
        capacity_policy_path=capacity_path,
        capacity_policy_sha256=data["capacity_record"]["sha256"],
        allowed_root=fixture_root,
    )
    precommit = matrix.build_matrix_precommit(preflight)
    project_root = tmp_path / "project"
    output_root = project_root / "work" / "canonical-runtime-dev"
    receipt_path = output_root / runtime.EXTRACTION_RECEIPT_FILENAME
    future_plan = matrix.build_future_plan_binding(precommit, output_root=output_root)
    directive_path = project_root / "automation" / "canonical-runtime-directive.json"
    semantic_plan_path = project_root / "config" / "semantic-plan.json"
    exact_calls = preflight["receipt"]["exact_request_count"]
    plan_payload = {
        "schema_version": supervisor.SEMANTIC_PLAN_SCHEMA_VERSION,
        "thread_id": supervisor.TARGET_THREAD_ID,
        "plan_epoch": 301,
        "state": "executable",
        "step": {
            "step_id": "canonical_v31_six_arm_extraction_runtime_v1",
            "state": "executable",
            "max_model_calls": exact_calls,
            "max_total_tokens": exact_calls * PER_TURN_TOKENS,
            "expected_receipt_path": str(receipt_path.resolve()),
            "accepted_receipt_states": ["passed", "rejected", "waiting"],
            "directive_path": str(directive_path.resolve()),
            "directive_sha256": "0" * 64,
        },
    }
    plan_contract = runtime.semantic_plan_contract(plan_payload)
    live_binding = runtime.build_runtime_dependency_binding()
    directive = matrix_fixtures._directive(future_plan, now_value)
    directive.update(
        {
            "authorized_by": "kolby",
            "thread_id": supervisor.TARGET_THREAD_ID,
            "plan_epoch": plan_payload["plan_epoch"],
            "step_id": plan_payload["step"]["step_id"],
            "expected_receipt_path": str(receipt_path.resolve()),
            "semantic_plan_path": str(semantic_plan_path.resolve()),
            "semantic_plan_contract_sha256": plan_contract["contract_sha256"],
            "max_model_calls": exact_calls,
            "max_total_tokens": exact_calls * PER_TURN_TOKENS,
            "maximum_total_tokens_per_turn": PER_TURN_TOKENS,
            "maximum_wall_seconds_per_turn": per_turn_wall,
            "minimum_remaining_reserve_percent": capacity[
                "minimum_remaining_reserve_percent"
            ],
            "capacity_safety_margin_percent": capacity[
                "capacity_safety_margin_percent"
            ],
            "quota_points_per_million_tokens": capacity[
                "quota_points_per_million_tokens"
            ],
            "quota_calibration_id": capacity["quota_calibration_id"],
            "maximum_rate_limit_snapshot_age_seconds": capacity[
                "maximum_rate_limit_snapshot_age_seconds"
            ],
            "operator_wall_deadline_safety_margin_seconds": capacity[
                "operator_wall_deadline_safety_margin_seconds"
            ],
            "token_capacity_is_estimate_not_reservation": True,
            "single_turn_token_cap_is_prospective_only": True,
            "preturn_reprobe_required": True,
            "postturn_measured_stop_required": True,
            "exact_request_count": exact_calls,
            "development_output_root": str(output_root.resolve()),
            "live_runtime_binding_sha256": live_binding["binding_sha256"],
            "runtime_module_sha256": live_binding["binding"]["runtime_module"][
                "sha256"
            ],
            "offline_matrix_module_sha256": live_binding["binding"][
                "exact_dependencies"
            ]["offline_matrix"]["sha256"],
            "canonical_adapter_module_sha256": live_binding["binding"][
                "exact_dependencies"
            ]["canonical_adapter"]["sha256"],
            "supervisor_module_sha256": live_binding["binding"][
                "exact_dependencies"
            ]["supervisor_semantic_plan"]["sha256"],
            "extraction_only": True,
            "quality_evaluation_authorized": False,
            "quality_selection_authorized": False,
            "api_key_auth_allowed": False,
            "raw_session_token_auth_allowed": False,
            "offline_test_mode_authorized": offline_test_mode,
            "live_pass_receipt_authorized": not offline_test_mode,
            "canonical_arm_runner_implementation_sha256": live_binding["binding"][
                "canonical_arm_runner_implementation_sha256"
            ],
            "official_managed_auth_client_implementation_sha256": live_binding[
                "binding"
            ]["official_managed_auth_client_implementation_sha256"],
            "trusted_capacity_implementation_state": live_binding["binding"][
                "trusted_capacity_implementation_state"
            ],
            "trusted_rate_limit_parser_implementation_sha256": live_binding[
                "binding"
            ]["trusted_rate_limit_parser_implementation_sha256"],
            "trusted_capacity_measurement_implementation_sha256": live_binding[
                "binding"
            ]["trusted_capacity_measurement_implementation_sha256"],
            "trusted_preturn_postturn_guard_implementation_sha256": live_binding[
                "binding"
            ]["trusted_preturn_postturn_guard_implementation_sha256"],
            "borrowed_official_client_context_implementation_sha256": live_binding[
                "binding"
            ]["borrowed_official_client_context_implementation_sha256"],
            "capacity_measurement_required_before_each_arm": True,
            "per_arm_capacity_admission_required": True,
            "new_empty_development_root_required": True,
        }
    )
    directive_path.parent.mkdir(parents=True, exist_ok=True)
    directive_path.write_bytes(_canonical(directive).encode("ascii"))
    import hashlib

    plan_payload["step"]["directive_sha256"] = hashlib.sha256(
        directive_path.read_bytes()
    ).hexdigest()
    _write_json(semantic_plan_path, plan_payload)
    selected_client_factory = client_factory or adapter_fixtures._FixtureFactory()
    args = {
        "output_root": output_root,
        "semantic_plan_path": semantic_plan_path,
        "thread_id": supervisor.TARGET_THREAD_ID,
        "project_root": project_root,
        "manifest_path": Path(data["manifest_record"]["path"]),
        "manifest_sha256": data["manifest_record"]["sha256"],
        "episodes": data["episodes"],
        "capacity_policy_path": capacity_path,
        "capacity_policy_sha256": data["capacity_record"]["sha256"],
        "future_plan_binding": future_plan,
        "allowed_root": fixture_root,
        "client_factory": selected_client_factory,
        "offline_test_mode": offline_test_mode,
    }
    if offline_test_mode:
        args.update({"environ": {}, "now": now_value})
    return {
        "args": args,
        "data": data,
        "preflight": preflight,
        "precommit": precommit,
        "future_plan": future_plan,
        "directive": directive,
        "directive_path": directive_path,
        "semantic_plan_path": semantic_plan_path,
        "output_root": output_root,
        "client_factory": selected_client_factory,
    }


def _rewrite_directive(bundle: dict[str, Any], mutate) -> None:
    directive = copy.deepcopy(bundle["directive"])
    mutate(directive)
    bundle["directive_path"].write_bytes(_canonical(directive).encode("ascii"))
    import hashlib

    plan = json.loads(bundle["semantic_plan_path"].read_text())
    plan["step"]["directive_sha256"] = hashlib.sha256(
        bundle["directive_path"].read_bytes()
    ).hexdigest()
    _write_json(bundle["semantic_plan_path"], plan)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda value: value.__setitem__("authorized_by", "not-kolby"),
        lambda value: value.__setitem__("precommit_sha256", "0" * 64),
        lambda value: value.__setitem__("quality_evaluation_authorized", True),
    ],
)
def test_epoch_authority_fails_before_capacity_probe(tmp_path: Path, mutate) -> None:
    bundle = _runtime_fixture(tmp_path)
    _rewrite_directive(bundle, mutate)
    probe = CapacityProbe()
    with pytest.raises((runtime.CanonicalV31DevelopmentRuntimeError, matrix.FutureExecutionDirectiveRequired)):
        asyncio.run(
            runtime.run_canonical_v31_development_matrix_runtime(
                **bundle["args"], capacity_probe=probe
            )
        )
    assert probe.calls == []
    assert bundle["client_factory"].client.model_calls == 0


@pytest.mark.parametrize(
    "auth_name",
    ["OPENAI_API_KEY", "OPENAI_SESSION_TOKEN", "CHATGPT_ACCESS_TOKEN"],
)
def test_api_key_and_raw_session_auth_are_rejected_before_probe(
    tmp_path: Path, auth_name: str
) -> None:
    bundle = _runtime_fixture(tmp_path)
    probe = CapacityProbe()
    args = {**bundle["args"], "environ": {auth_name: "forbidden"}}
    with pytest.raises(runtime.CanonicalV31DevelopmentRuntimeError, match="auth"):
        asyncio.run(
            runtime.run_canonical_v31_development_matrix_runtime(
                **args, capacity_probe=probe
            )
        )
    assert probe.calls == []
    assert bundle["client_factory"].client.model_calls == 0


def test_runtime_checksum_and_supervisor_caps_are_exact_before_probe(
    tmp_path: Path,
) -> None:
    bundle = _runtime_fixture(tmp_path)
    _rewrite_directive(
        bundle,
        lambda value: value.__setitem__("live_runtime_binding_sha256", "f" * 64),
    )
    probe = CapacityProbe()
    with pytest.raises(runtime.CanonicalV31DevelopmentRuntimeError, match="runtime|authority"):
        asyncio.run(
            runtime.run_canonical_v31_development_matrix_runtime(
                **bundle["args"], capacity_probe=probe
            )
        )
    assert probe.calls == []

    bundle = _runtime_fixture(tmp_path / "caps")
    plan = json.loads(bundle["semantic_plan_path"].read_text())
    plan["step"]["max_model_calls"] += 1
    _write_json(bundle["semantic_plan_path"], plan)
    with pytest.raises(runtime.CanonicalV31DevelopmentRuntimeError, match="plan|authority"):
        asyncio.run(
            runtime.run_canonical_v31_development_matrix_runtime(
                **bundle["args"], capacity_probe=probe
            )
        )
    assert probe.calls == []


def test_numeric_capacity_and_denial_stop_before_semantic_work(tmp_path: Path) -> None:
    bundle = _runtime_fixture(tmp_path / "numeric")
    numeric = CapacityProbe(numeric_drift=True)
    runner = TrackingArmRunner()
    with pytest.raises(runtime.CanonicalV31DevelopmentRuntimeError, match="numeric"):
        asyncio.run(
            runtime.run_canonical_v31_development_matrix_runtime(
                **bundle["args"], capacity_probe=numeric, arm_runner=runner
            )
        )
    assert runner.calls == []
    assert bundle["client_factory"].client.model_calls == 0

    bundle = _runtime_fixture(tmp_path / "denied")
    denied = CapacityProbe(available=False)
    runner = TrackingArmRunner()
    with pytest.raises(runtime.CanonicalV31CapacityUnavailable):
        asyncio.run(
            runtime.run_canonical_v31_development_matrix_runtime(
                **bundle["args"], capacity_probe=denied, arm_runner=runner
            )
        )
    assert runner.calls == []
    assert bundle["client_factory"].client.model_calls == 0


def test_exact_six_arms_artifacts_opaque_order_receipt_and_complete_adoption(
    tmp_path: Path,
) -> None:
    bundle = _runtime_fixture(tmp_path)
    probe = CapacityProbe()
    runner = TrackingArmRunner()
    first = asyncio.run(
        runtime.run_canonical_v31_development_matrix_runtime(
            **bundle["args"], capacity_probe=probe, arm_runner=runner
        )
    )
    expected = list(matrix.EXPECTED_ARMS)
    assert runner.calls == expected
    assert len(probe.calls) == 6
    assert first["fresh_arm_count"] == 6
    assert first["adopted_arm_count"] == 0
    receipt = first["receipt"]
    assert receipt["extraction_only"] is True
    assert receipt["quality_evaluation_performed"] is False
    assert receipt["holdout_inspected"] is False
    assert receipt["production_mutated"] is False
    assert receipt["exact_model_call_cap"] == sum(
        row["request_count"] for row in bundle["precommit"]["precommit"]["arms"]
    )
    assert receipt["state"] == "waiting"
    assert receipt["offline_test_mode"] is True
    assert receipt["promotable"] is False
    assert receipt["live_pass_authority"] is False
    assert receipt["measured_model_calls"] == 0
    assert receipt["verified_fixture_attempt_count"] == receipt[
        "exact_model_call_cap"
    ]
    assert receipt["verified_fixture_usage"]["total_tokens"] == (
        receipt["exact_model_call_cap"] * 1_200
    )
    assert receipt["full_output_validation"]["arm_count"] == 6
    assert Path(receipt["invocation_lock_record"]["path"]).is_file()
    assert [row["variant_id"] for row in receipt["arms"]] == [
        row["variant_id"] for row in bundle["precommit"]["precommit"]["arms"]
    ]
    assert all(Path(row["arm_envelope"]["path"]).is_file() for row in receipt["arms"])

    adoption_probe = CapacityProbe()
    adoption_runner = TrackingArmRunner()
    prior_model_calls = bundle["client_factory"].client.model_calls
    second = asyncio.run(
        runtime.run_canonical_v31_development_matrix_runtime(
            **bundle["args"],
            capacity_probe=adoption_probe,
            arm_runner=adoption_runner,
        )
    )
    assert second["receipt"] == receipt
    assert second["fresh_arm_count"] == 0
    assert second["adopted_arm_count"] == 6
    assert second["semantic_calls_started_by_this_invocation"] == 0
    assert second["fixture_attempts_started_by_this_invocation"] == 0
    assert adoption_probe.calls == []
    assert adoption_runner.calls == []
    assert bundle["client_factory"].client.model_calls == prior_model_calls


@pytest.mark.parametrize(
    "mutation,match",
    [
        ("artifact", "checksum|artifact"),
        ("opaque_order", "semantics|membership|order"),
        ("cross_thread", "semantics|replayed across arms"),
        ("retry", "retried|attempt"),
        ("duplicate_turn", "duplicated|unique|identity"),
        ("provenance_semantics", "provenance|semantics"),
    ],
)
def test_artifact_request_opaque_and_cross_arm_identity_fail_closed(
    tmp_path: Path, mutation: str, match: str
) -> None:
    bundle = _runtime_fixture(tmp_path)
    with pytest.raises(runtime.CanonicalV31DevelopmentRuntimeError, match=match):
        asyncio.run(
            runtime.run_canonical_v31_development_matrix_runtime(
                **bundle["args"],
                capacity_probe=CapacityProbe(),
                arm_runner=TrackingArmRunner(mutation),
            )
        )


def test_admission_identity_replay_and_partial_arm_never_replay(tmp_path: Path) -> None:
    bundle = _runtime_fixture(tmp_path / "admission")
    runner = TrackingArmRunner()
    with pytest.raises(runtime.CanonicalV31DevelopmentRuntimeError, match="measurement.*replayed"):
        asyncio.run(
            runtime.run_canonical_v31_development_matrix_runtime(
                **bundle["args"],
                capacity_probe=CapacityProbe(same_identity=True),
                arm_runner=runner,
            )
        )
    assert runner.calls == [matrix.EXPECTED_ARMS[0]]

    bundle = _runtime_fixture(tmp_path / "partial")
    first_id = bundle["precommit"]["precommit"]["arms"][0]["variant_id"]
    partial = bundle["output_root"] / "arms" / first_id
    partial.mkdir(parents=True)
    (partial / "sentinel").write_text("partial")
    probe = CapacityProbe()
    with pytest.raises(runtime.CanonicalV31RecoveryRequired, match="partial|control"):
        asyncio.run(
            runtime.run_canonical_v31_development_matrix_runtime(
                **bundle["args"], capacity_probe=probe
            )
        )
    assert probe.calls == []
    assert bundle["client_factory"].client.model_calls == 0


def test_second_concurrent_writer_fails_before_probe_or_client(tmp_path: Path) -> None:
    bundle = _runtime_fixture(tmp_path)
    first_probe = CapacityProbe()
    second_probe = CapacityProbe()
    second_errors: list[BaseException] = []

    class ContendedProbe:
        attempted = False

        def __call__(self, context: Mapping[str, Any]) -> dict[str, Any]:
            if not self.attempted:
                self.attempted = True

                def run_second() -> None:
                    try:
                        asyncio.run(
                            runtime.run_canonical_v31_development_matrix_runtime(
                                **bundle["args"],
                                capacity_probe=second_probe,
                                arm_runner=TrackingArmRunner(),
                            )
                        )
                    except BaseException as exc:  # pragma: no cover - asserted below
                        second_errors.append(exc)

                worker = threading.Thread(target=run_second, daemon=True)
                worker.start()
                worker.join(timeout=10)
                assert not worker.is_alive()
                assert len(second_errors) == 1
                assert isinstance(
                    second_errors[0], runtime.CanonicalV31RuntimeLockUnavailable
                )
                assert second_probe.calls == []
                assert bundle["client_factory"].client.model_calls == 0
            return first_probe(context)

    result = asyncio.run(
        runtime.run_canonical_v31_development_matrix_runtime(
            **bundle["args"],
            capacity_probe=ContendedProbe(),
            arm_runner=TrackingArmRunner(),
        )
    )
    assert result["receipt"]["state"] == "waiting"
    assert len(second_errors) == 1
    assert isinstance(second_errors[0], runtime.CanonicalV31RuntimeLockUnavailable)
    assert second_probe.calls == []


def test_symlinked_development_root_is_rejected_before_probe(tmp_path: Path) -> None:
    bundle = _runtime_fixture(tmp_path / "leaf")
    target = bundle["args"]["project_root"] / "work" / "symlink-target"
    target.mkdir(parents=True)
    bundle["output_root"].symlink_to(target, target_is_directory=True)
    probe = CapacityProbe()
    with pytest.raises(runtime.CanonicalV31DevelopmentRuntimeError, match="symlink"):
        asyncio.run(
            runtime.run_canonical_v31_development_matrix_runtime(
                **bundle["args"], capacity_probe=probe
            )
        )
    assert probe.calls == []
    assert bundle["client_factory"].client.model_calls == 0

    bundle = _runtime_fixture(tmp_path / "parent")
    alternate = bundle["args"]["project_root"] / "alternate-work"
    alternate.mkdir()
    (bundle["args"]["project_root"] / "work").symlink_to(
        alternate, target_is_directory=True
    )
    probe = CapacityProbe()
    with pytest.raises(runtime.CanonicalV31DevelopmentRuntimeError, match="symlink"):
        asyncio.run(
            runtime.run_canonical_v31_development_matrix_runtime(
                **bundle["args"], capacity_probe=probe
            )
        )
    assert probe.calls == []


def test_fake_live_path_uses_one_initialized_client_for_capacity_and_turns(
    tmp_path: Path, monkeypatch
) -> None:
    factory = LiveFixtureFactory()
    monkeypatch.setattr(adapter, "_client_factory", factory)
    bundle = _runtime_fixture(
        tmp_path,
        now_value=datetime.now(timezone.utc),
        offline_test_mode=False,
        client_factory=factory,
    )
    result = asyncio.run(
        runtime.run_canonical_v31_development_matrix_runtime(**bundle["args"])
    )
    receipt = result["receipt"]
    assert receipt["state"] == "passed"
    assert receipt["waiting_reason"] is None
    assert receipt["offline_test_mode"] is False
    assert receipt["promotable"] is True
    assert receipt["live_pass_authority"] is True
    assert receipt["live_dispatch_performed"] is True
    assert receipt["measured_model_calls"] == receipt["exact_model_call_cap"]
    assert receipt["verified_fixture_attempt_count"] == 0
    assert receipt["measured_usage"]["total_tokens"] == (
        receipt["exact_model_call_cap"] * 1_200
    )
    assert receipt["verified_fixture_usage"]["total_tokens"] == 0
    assert result["semantic_calls_started_by_this_invocation"] == receipt[
        "exact_model_call_cap"
    ]
    assert result["fixture_attempts_started_by_this_invocation"] == 0
    assert factory.client.model_calls == receipt["exact_model_call_cap"]
    expected_additional_thread_starts = sum(
        row["request_count"] - 1
        for row in bundle["precommit"]["precommit"]["arms"]
        if row["thread_mode"] == "new_thread"
    )
    assert factory.client.rate_limit_reads == (
        6 + receipt["exact_model_call_cap"] + expected_additional_thread_starts
    )
    assert factory.client.enter_count == 6
    assert factory.client.exit_count == 6
    assert all(row["capacity_reprobes"] for row in receipt["arms"])
    assert all(row["offline_test_mode"] is False for row in receipt["arms"])
    assert not list(bundle["output_root"].rglob("response.json"))

    prior_reads = factory.client.rate_limit_reads
    prior_calls = factory.client.model_calls
    adopted = asyncio.run(
        runtime.run_canonical_v31_development_matrix_runtime(**bundle["args"])
    )
    assert adopted["receipt"] == receipt
    assert adopted["fresh_arm_count"] == 0
    assert adopted["adopted_arm_count"] == 6
    assert adopted["semantic_calls_started_by_this_invocation"] == 0
    assert factory.client.rate_limit_reads == prior_reads
    assert factory.client.model_calls == prior_calls


def test_live_mode_rejects_every_injected_state_source(
    tmp_path: Path, monkeypatch
) -> None:
    factory = LiveFixtureFactory()
    monkeypatch.setattr(adapter, "_client_factory", factory)
    bundle = _runtime_fixture(
        tmp_path,
        now_value=datetime.now(timezone.utc),
        offline_test_mode=False,
        client_factory=factory,
    )
    live_args = bundle["args"]
    injected_probe = CapacityProbe()
    with pytest.raises(runtime.CanonicalV31DevelopmentRuntimeError, match="injected capacity"):
        asyncio.run(
            runtime.run_canonical_v31_development_matrix_runtime(
                **live_args, capacity_probe=injected_probe
            )
        )
    assert injected_probe.calls == []
    with pytest.raises(runtime.CanonicalV31DevelopmentRuntimeError, match="injected.*runner"):
        asyncio.run(
            runtime.run_canonical_v31_development_matrix_runtime(
                **live_args, arm_runner=TrackingArmRunner()
            )
        )
    with pytest.raises(runtime.CanonicalV31DevelopmentRuntimeError, match="client factory"):
        asyncio.run(
            runtime.run_canonical_v31_development_matrix_runtime(
                **{**live_args, "client_factory": adapter_fixtures._FixtureFactory()}
            )
        )
    with pytest.raises(runtime.CanonicalV31DevelopmentRuntimeError, match="environment|clock"):
        asyncio.run(
            runtime.run_canonical_v31_development_matrix_runtime(
                **live_args, environ={}
            )
        )


def test_orphan_capacity_admission_is_terminal_and_never_replayed(
    tmp_path: Path,
) -> None:
    bundle = _runtime_fixture(tmp_path)

    class CrashBeforeArm:
        async def __call__(self, *_args, **_kwargs):
            raise RuntimeError("fixture crash after admission")

    first_probe = CapacityProbe()
    with pytest.raises(RuntimeError, match="fixture crash"):
        asyncio.run(
            runtime.run_canonical_v31_development_matrix_runtime(
                **bundle["args"],
                capacity_probe=first_probe,
                arm_runner=CrashBeforeArm(),
            )
        )
    assert len(first_probe.calls) == 1
    first_variant = bundle["precommit"]["precommit"]["arms"][0]["variant_id"]
    assert not (bundle["output_root"] / "arms" / first_variant).exists()
    second_probe = CapacityProbe()
    with pytest.raises(runtime.CanonicalV31RecoveryRequired, match="orphan|admission"):
        asyncio.run(
            runtime.run_canonical_v31_development_matrix_runtime(
                **bundle["args"],
                capacity_probe=second_probe,
                arm_runner=CrashBeforeArm(),
            )
        )
    assert second_probe.calls == []
    assert bundle["client_factory"].client.model_calls == 0


def test_nested_symlink_and_foreign_capacity_tree_fail_before_probe(
    tmp_path: Path,
) -> None:
    bundle = _runtime_fixture(tmp_path / "symlink")
    with pytest.raises(runtime.CanonicalV31CapacityUnavailable):
        asyncio.run(
            runtime.run_canonical_v31_development_matrix_runtime(
                **bundle["args"], capacity_probe=CapacityProbe(available=False)
            )
        )
    arms_root = bundle["output_root"] / "arms"
    arms_root.rmdir()
    escaped = bundle["args"]["project_root"] / "work" / "escaped-arms"
    escaped.mkdir()
    arms_root.symlink_to(escaped, target_is_directory=True)
    probe = CapacityProbe()
    with pytest.raises(runtime.CanonicalV31DevelopmentRuntimeError, match="real directory|symlink"):
        asyncio.run(
            runtime.run_canonical_v31_development_matrix_runtime(
                **bundle["args"], capacity_probe=probe
            )
        )
    assert probe.calls == []

    bundle = _runtime_fixture(tmp_path / "foreign")
    with pytest.raises(runtime.CanonicalV31CapacityUnavailable):
        asyncio.run(
            runtime.run_canonical_v31_development_matrix_runtime(
                **bundle["args"], capacity_probe=CapacityProbe(available=False)
            )
        )
    (bundle["output_root"] / "capacity" / "foreign-variant").mkdir()
    probe = CapacityProbe()
    with pytest.raises(runtime.CanonicalV31RecoveryRequired, match="foreign"):
        asyncio.run(
            runtime.run_canonical_v31_development_matrix_runtime(
                **bundle["args"], capacity_probe=probe
            )
        )
    assert probe.calls == []


def test_directive_recheck_and_internal_capacity_hash_fail_before_runner(
    tmp_path: Path,
) -> None:
    bundle = _runtime_fixture(tmp_path / "directive")
    inner = CapacityProbe()

    class MutatingProbe:
        def __call__(self, context: Mapping[str, Any]) -> dict[str, Any]:
            measurement = inner(context)
            bundle["directive_path"].write_bytes(
                bundle["directive_path"].read_bytes() + b" "
            )
            return measurement

    runner = TrackingArmRunner()
    with pytest.raises(runtime.CanonicalV31RecoveryRequired, match="directive"):
        asyncio.run(
            runtime.run_canonical_v31_development_matrix_runtime(
                **bundle["args"],
                capacity_probe=MutatingProbe(),
                arm_runner=runner,
            )
        )
    assert runner.calls == []
    assert bundle["client_factory"].client.model_calls == 0

    bundle = _runtime_fixture(tmp_path / "self-attested")
    inner = CapacityProbe()

    class SelfAttestedProbe:
        def __call__(self, context: Mapping[str, Any]) -> dict[str, Any]:
            measurement = inner(context)
            return {
                "measurement": measurement,
                "measurement_sha256": _sha(measurement),
            }

    runner = TrackingArmRunner()
    with pytest.raises(runtime.CanonicalV31DevelopmentRuntimeError, match="capacity|measured"):
        asyncio.run(
            runtime.run_canonical_v31_development_matrix_runtime(
                **bundle["args"],
                capacity_probe=SelfAttestedProbe(),
                arm_runner=runner,
            )
        )
    assert runner.calls == []


def test_policy_wall_caps_apply_before_and_during_fixture_dispatch(
    tmp_path: Path,
) -> None:
    bundle = _runtime_fixture(tmp_path / "pre")
    probe = CapacityProbe()
    with pytest.raises(runtime.CanonicalV31DevelopmentRuntimeError, match="timeout.*cap"):
        asyncio.run(
            runtime.run_canonical_v31_development_matrix_runtime(
                **bundle["args"],
                capacity_probe=probe,
                timeout_seconds=PER_TURN_WALL + 1,
            )
        )
    assert probe.calls == []

    bundle = _runtime_fixture(tmp_path / "during", per_turn_wall=0.01)

    class SlowRunner:
        async def __call__(self, *_args, **kwargs):
            assert kwargs["timeout_seconds"] == 0.01
            await asyncio.sleep(1)

    with pytest.raises(runtime.CanonicalV31DevelopmentRuntimeError, match="wall ceiling"):
        asyncio.run(
            runtime.run_canonical_v31_development_matrix_runtime(
                **bundle["args"],
                capacity_probe=CapacityProbe(),
                arm_runner=SlowRunner(),
            )
        )
    assert bundle["client_factory"].client.model_calls == 0


def test_runtime_has_no_forbidden_live_lane_imports() -> None:
    source = Path(runtime.__file__).read_text()
    assert "app_server_expanded_cap_development_matrix" not in source
    assert "app_server_holdout" not in source
    assert "app_server_production" not in source
    assert "sqlite3" not in source
    assert "OPENAI_API_KEY" in source
    assert "run_episode_batch_arm" in source
