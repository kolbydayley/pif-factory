from __future__ import annotations

import hashlib
import io
import json
import tempfile
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

import pytest

from research_factory import pif_cli
from research_factory.kimi_code_runner import KimiCodeRunnerError
from research_factory.kimi_workhorse import (
    KimiWorkhorseError,
    freeze_manifest,
    load_manifest,
    main,
    profile_status,
    run_manifest,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TEST_LABEL_PACK = "tech_discourse_v1"
TEST_SCHEMA = PROJECT_ROOT / "label_packs" / TEST_LABEL_PACK / "schema.json"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _manifest(root: Path, **updates: object) -> Path:
    prompt = root / "prompt.md"
    prompt.write_text("Private podcast labeling prompt.", encoding="utf-8")
    segment = root / "segment.txt"
    segment.write_text("The private segment text to label.", encoding="utf-8")
    payload: dict[str, object] = {
        "schema_version": "pif_kimi_workhorse_manifest_v1",
        "job_id": "shadow-case-1",
        "privacy_tier": "full_text_allowed",
        "model": "k3",
        "prompt_path": prompt.name,
        "prompt_sha256": _sha256(prompt),
        "schema_path": str(TEST_SCHEMA),
        "schema_sha256": _sha256(TEST_SCHEMA),
        "output_path": "output.json",
        "label_pack": TEST_LABEL_PACK,
        "segment_text_path": segment.name,
        "segment_text_sha256": _sha256(segment),
    }
    payload.update(updates)
    path = root / "manifest.json"
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    return path


def test_dry_run_validates_manifest_without_dispatching_or_rendering_prompt() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        manifest = _manifest(root)

        def forbidden(**kwargs: object) -> dict[str, object]:
            raise AssertionError(f"runner must not be called: {kwargs}")

        result = run_manifest(manifest, execute=False, runner=forbidden)
        assert result["status"] == "planned"
        assert result["external_model_call"] is False
        assert result["canonical_db_opened"] is False
        assert "Private podcast" not in json.dumps(result)
        assert not (root / "output.json").exists()


def test_execute_passes_file_payload_then_publishes_only_validated_output() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        manifest = _manifest(root)
        captured: dict[str, object] = {}

        def fake_runner(**kwargs: object) -> dict[str, object]:
            captured.update(kwargs)
            validator = kwargs["schema_validator"]
            assert callable(validator)
            validator({"schema_version": TEST_LABEL_PACK})
            output_path = Path(str(kwargs["output_path"]))
            output_path.write_text(json.dumps({"schema_version": TEST_LABEL_PACK}) + "\n", encoding="utf-8")
            return {
                "schema_validation": "caller",
                "authorization": {"receipt_id": "test"},
                "diagnostics": {"stderr_chars": 0},
                "duration_ms": 123,
                "retry_count": 1,
                "actual_model_verified": False,
                "actual_model_id": None,
            }

        result = run_manifest(manifest, execute=True, runner=fake_runner)
        payload = captured["payload"]
        assert isinstance(payload, dict)
        assert "".join(payload["prompt_chunks"]) == "Private podcast labeling prompt."
        assert "".join(payload["segment_text_chunks"]) == "The private segment text to label."
        assert captured["model"] == "kimi-code/k3"
        assert result["status"] == "completed"
        assert result["schema_validation"] == "caller"
        assert result["duration_ms"] == 123
        assert result["retry_count"] == 1
        assert result["actual_model_verified"] is False
        assert "Private podcast" not in json.dumps(result)
        assert json.loads((root / "output.json").read_text(encoding="utf-8")) == {"schema_version": TEST_LABEL_PACK}
        attempt = json.loads((root / ".output.json.kimi-attempt.json").read_text(encoding="utf-8"))
        assert attempt["state"] == "completed"
        assert attempt["requested_model_id"] == "k3"


def test_manifest_rejects_remote_privacy_tier_hash_drift_and_existing_output() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        with pytest.raises(KimiWorkhorseError, match="privacy_tier"):
            load_manifest(_manifest(root, privacy_tier="metadata_only"))

    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        with pytest.raises(KimiWorkhorseError, match="content hash"):
            load_manifest(_manifest(root, prompt_sha256="0" * 64))

    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        manifest = _manifest(root)
        (root / "output.json").write_text('{"already": true}\n', encoding="utf-8")
        with pytest.raises(KimiWorkhorseError, match="already exists"):
            load_manifest(manifest)


def test_manifest_requires_hash_bound_segment_text_and_known_model_id() -> None:
    with tempfile.TemporaryDirectory() as temp:
        with pytest.raises(KimiWorkhorseError, match="requires hash-bound"):
            load_manifest(_manifest(Path(temp), segment_text_path=None, segment_text_sha256=None))
    with tempfile.TemporaryDirectory() as temp:
        with pytest.raises(KimiWorkhorseError, match="model"):
            load_manifest(_manifest(Path(temp), model="kimi-for-cdoing"))


def test_freeze_creates_hash_bound_manifest_without_model_call() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        source_manifest = _manifest(root)
        source = json.loads(source_manifest.read_text(encoding="utf-8"))
        source_manifest.unlink()
        target = root / "frozen.json"
        result = freeze_manifest(
            manifest_path=target,
            job_id="frozen-case",
            model="k3",
            prompt_path=root / str(source["prompt_path"]),
            schema_path=Path(str(source["schema_path"])),
            output_path=root / "frozen-output.json",
            label_pack=TEST_LABEL_PACK,
            segment_text_path=root / str(source["segment_text_path"]),
        )
        assert result["status"] == "frozen"
        assert result["model_call_made"] is False
        loaded = load_manifest(target)
        assert loaded.job_id == "frozen-case"
        assert loaded.prompt_sha256 == _sha256(root / "prompt.md")


def test_attempt_receipt_blocks_repeat_dispatch_after_failure() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        manifest = _manifest(root)
        calls = 0

        def failed_runner(**kwargs: object) -> dict[str, object]:
            nonlocal calls
            calls += 1
            from research_factory.kimi_code_runner import KimiCodeRunnerError

            raise KimiCodeRunnerError(
                "simulated post-dispatch failure",
                details={"model_call_started": True, "may_have_consumed_quota": True},
            )

        with pytest.raises(Exception, match="simulated post-dispatch"):
            run_manifest(manifest, execute=True, runner=failed_runner)
        assert calls == 1
        receipt = json.loads((root / ".output.json.kimi-attempt.json").read_text(encoding="utf-8"))
        assert receipt["state"] == "failed_after_dispatch"

        with pytest.raises(KimiWorkhorseError, match="attempt receipt"):
            run_manifest(manifest, execute=True, runner=failed_runner)
        assert calls == 1


def test_status_rejects_credential_left_by_failed_membership_provisioning() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        home = root / "kimi-home"
        credential = home / "credentials" / "kimi-code.json"
        credential.parent.mkdir(parents=True)
        credential.write_text("not inspected", encoding="utf-8")
        receipt = root / "approval.json"
        binary = Path("/opt/homebrew/bin/kimi")
        with patch(
            "research_factory.kimi_workhorse.prepare_kimi_code_home",
            return_value={"ok": True},
        ), patch(
            "research_factory.kimi_workhorse.load_authorization_receipt"
        ) as authorization, patch(
            "research_factory.kimi_workhorse.inspect_kimi_code_provider_profile",
            return_value={
                "provider_probe_ok": True,
                "managed_oauth_provider_provisioned": False,
                "managed_model_count": 0,
                "login_provisioning_complete": False,
                "network_call_made": False,
                "model_call_made": False,
            },
        ):
            authorization.return_value.sanitized_metadata.return_value = {"provider": "Kimi"}
            result = profile_status(binary=binary, kimi_home=home, expected_version="0.29.0", receipt=receipt)
        assert result["credential_files_present"] is True
        assert result["expected_credential_file_present"] is True
        assert result["login_provisioning_complete"] is False
        assert result["ready_for_live_call"] is False
        assert result["ok"] is False
        assert result["status"] == "membership_or_login_provisioning_required"
        assert result["model_call_made"] is False
        assert credential.read_text(encoding="utf-8") == "not inspected"


def test_status_requires_exact_kimi_credential_even_with_completed_provider_profile() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        home = root / "kimi-home"
        credentials = home / "credentials"
        credentials.mkdir(parents=True)
        (credentials / "other.json").write_text("unrelated", encoding="utf-8")
        with patch("research_factory.kimi_workhorse.prepare_kimi_code_home", return_value={"ok": True}), patch(
            "research_factory.kimi_workhorse.load_authorization_receipt"
        ) as authorization, patch(
            "research_factory.kimi_workhorse.inspect_kimi_code_provider_profile",
            return_value={
                "provider_probe_ok": True,
                "managed_oauth_provider_provisioned": True,
                "managed_model_count": 3,
                "login_provisioning_complete": True,
                "network_call_made": False,
                "model_call_made": False,
            },
        ):
            authorization.return_value.sanitized_metadata.return_value = {"provider": "Kimi"}
            result = profile_status(
                binary=Path("/opt/homebrew/bin/kimi"),
                kimi_home=home,
                expected_version="0.29.0",
                receipt=root / "approval.json",
            )
        assert result["credential_files_present"] is True
        assert result["expected_credential_file_present"] is False
        assert result["ready_for_live_call"] is False
        assert result["status"] == "login_required"


def test_status_classifies_local_preflight_timeout_as_inconclusive_probe_failure() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        failure = KimiCodeRunnerError(
            "Kimi configuration check process exceeded its 15-second hard deadline",
            details={
                "error_class": "preflight_timeout",
                "preflight_stage": "configuration check",
                "timeout_seconds": 15,
                "duration_ms": 15_001,
                "model_call_started": False,
                "may_have_consumed_quota": False,
                "network_call_made": False,
            },
        )
        with patch("research_factory.kimi_workhorse.prepare_kimi_code_home", side_effect=failure), patch(
            "research_factory.kimi_workhorse.load_authorization_receipt"
        ):
            result = profile_status(
                binary=Path("/opt/homebrew/bin/kimi"),
                kimi_home=root / "kimi-home",
                expected_version="0.29.0",
                receipt=root / "approval.json",
            )
        assert result["status"] == "profile_probe_failed"
        assert result["profile_valid"] is None
        assert result["profile_error_class"] == "preflight_timeout"
        assert result["profile_error_stage"] == "configuration check"
        assert result["profile_error_timeout_seconds"] == 15
        assert result["status_check_completed"] is False
        assert result["ready_for_live_call"] is False


def test_real_execute_path_rejects_incomplete_login_before_claiming_attempt() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        manifest = _manifest(root)
        credential = root / "kimi-home" / "credentials" / "kimi-code.json"
        credential.parent.mkdir(parents=True)
        credential.write_text("not inspected", encoding="utf-8")
        with patch("research_factory.kimi_workhorse.load_authorization_receipt"), patch(
            "research_factory.kimi_workhorse.prepare_kimi_code_home",
            return_value={"ok": True},
        ), patch(
            "research_factory.kimi_workhorse.inspect_kimi_code_provider_profile",
            return_value={
                "provider_probe_ok": True,
                "managed_oauth_provider_provisioned": False,
                "managed_model_count": 0,
                "login_provisioning_complete": False,
                "network_call_made": False,
                "model_call_made": False,
            },
        ), patch("research_factory.kimi_workhorse.run_kimi_code_job") as dispatch:
            with pytest.raises(KimiWorkhorseError, match="did not complete") as error:
                run_manifest(manifest, execute=True, kimi_home=root / "kimi-home")
        assert error.value.details["model_call_started"] is False
        assert error.value.details["may_have_consumed_quota"] is False
        assert dispatch.call_count == 0
        assert not (root / ".output.json.kimi-attempt.json").exists()


def test_real_execute_path_timeout_never_claims_attempt_or_dispatches() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        manifest = _manifest(root)
        failure = KimiCodeRunnerError(
            "Kimi version check process exceeded its 15-second hard deadline",
            details={
                "error_class": "preflight_timeout",
                "preflight_stage": "version check",
                "timeout_seconds": 15,
                "model_call_started": False,
                "may_have_consumed_quota": False,
                "network_call_made": False,
            },
        )
        with patch("research_factory.kimi_workhorse.load_authorization_receipt"), patch(
            "research_factory.kimi_workhorse.prepare_kimi_code_home",
            side_effect=failure,
        ), patch("research_factory.kimi_workhorse.run_kimi_code_job") as dispatch:
            with pytest.raises(KimiCodeRunnerError, match="version check") as error:
                run_manifest(manifest, execute=True)
        assert error.value.details["model_call_started"] is False
        assert error.value.details["may_have_consumed_quota"] is False
        assert dispatch.call_count == 0
        assert not (root / ".output.json.kimi-attempt.json").exists()
        assert not (root / "output.json").exists()


def test_attempt_update_failure_still_reports_that_quota_may_have_been_consumed() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        manifest = _manifest(root)

        def successful_runner(**kwargs: object) -> dict[str, object]:
            validator = kwargs["schema_validator"]
            assert callable(validator)
            validator({"schema_version": TEST_LABEL_PACK})
            output_path = Path(str(kwargs["output_path"]))
            output_path.write_text(json.dumps({"schema_version": TEST_LABEL_PACK}) + "\n", encoding="utf-8")
            return {"schema_validation": "caller", "duration_ms": 10, "retry_count": 0}

        with patch("research_factory.kimi_workhorse.os.replace", side_effect=PermissionError("simulated")):
            with pytest.raises(KimiWorkhorseError, match="receipt could not be updated") as error:
                run_manifest(manifest, execute=True, runner=successful_runner)
        assert error.value.details["model_call_started"] is True
        assert error.value.details["may_have_consumed_quota"] is True
        assert error.value.details["external_model_call_completed"] is True
        assert error.value.details["output_published"] is True
        assert error.value.details["output_path"] == str((root / "output.json").resolve())
        assert error.value.details["output_sha256"] == hashlib.sha256((root / "output.json").read_bytes()).hexdigest()
        assert (root / "output.json").is_file()


def test_compact_cli_dispatches_both_kimi_lab_names_before_legacy_lab() -> None:
    for name in ("kimi-workhorse", "kimi-code"):
        with patch("research_factory.kimi_workhorse.main", return_value=0) as dispatched, patch.object(
            pif_cli.legacy_cli, "main", side_effect=AssertionError("legacy lab must not receive Kimi command")
        ):
            assert pif_cli.main(["lab", name, "status"]) == 0
        dispatched.assert_called_once_with(["status"])


def test_main_dry_run_prints_sanitized_json() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        manifest = _manifest(root)
        output = io.StringIO()
        with redirect_stdout(output):
            assert main(["run", "--manifest", str(manifest)]) == 0
        rendered = output.getvalue()
        assert '"status": "planned"' in rendered
        assert "Private podcast labeling prompt" not in rendered


def test_main_surfaces_published_output_when_receipt_update_failed() -> None:
    failure = KimiWorkhorseError(
        "receipt update failed",
        details={
            "model_call_started": True,
            "may_have_consumed_quota": True,
            "external_model_call_completed": True,
            "output_published": True,
            "output_path": "/private/shadow/output.json",
            "output_sha256": "a" * 64,
            "attempt_receipt_path": "/private/shadow/.output.json.kimi-attempt.json",
        },
    )
    error_output = io.StringIO()
    with patch("research_factory.kimi_workhorse.run_manifest", side_effect=failure), redirect_stderr(error_output):
        assert main(["run", "--manifest", "/private/shadow/job.json", "--execute"]) == 2
    rendered = json.loads(error_output.getvalue())
    assert rendered["external_model_call_completed"] is True
    assert rendered["external_model_call_started"] is True
    assert rendered["may_have_consumed_quota"] is True
    assert rendered["output_published"] is True
    assert rendered["output_path"] == "/private/shadow/output.json"
    assert rendered["output_sha256"] == "a" * 64
