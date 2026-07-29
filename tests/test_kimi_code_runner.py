from __future__ import annotations

import json
import os
import signal
import sys
import tempfile
from pathlib import Path
from unittest import mock

import pytest

from research_factory.kimi_code_runner import (
    AUTHORIZATION_SCHEMA_VERSION,
    AUTHORIZATION_SCOPE,
    KimiAuthorizationError,
    KimiCodeRunnerError,
    inspect_kimi_code_provider_profile,
    prepare_kimi_code_home,
    run_kimi_code_job,
)


FAKE_KIMI = '''
import json
import os
import subprocess
import sys
import time
from pathlib import Path

capture = os.environ.get("PIF_KIMI_TEST_CAPTURE_ARGV")
if capture:
    Path(capture).write_text(json.dumps(sys.argv), encoding="utf-8")
if "--version" in sys.argv:
    print("0.29.0")
    raise SystemExit(0)
if len(sys.argv) >= 3 and sys.argv[-2:] == ["doctor", "config"]:
    raise SystemExit(0)
if "doctor" in sys.argv and "config" in sys.argv:
    raise SystemExit(0)
mode = os.environ.get("PIF_KIMI_TEST_MODE", "success")
if sys.argv[-2:] == ["provider", "list"]:
    if mode == "provider-timeout":
        time.sleep(60)
    if mode == "provider-unprovisioned":
        print("No providers configured.")
    elif mode == "provider-zero-models":
        print("managed:kimi-code  type=kimi  models=0  source=oauth")
    else:
        print("managed:kimi-code  type=kimi  models=3  source=oauth")
    raise SystemExit(0)
if mode == "timeout":
    time.sleep(60)
if mode == "nonzero":
    print("simulated failure", file=sys.stderr)
    raise SystemExit(23)
if mode == "invalid-jsonl":
    print("not json")
    raise SystemExit(0)
if mode == "missing-final":
    print(json.dumps({"type": "system", "message": "done"}))
    raise SystemExit(0)
if mode == "invalid-output":
    print(json.dumps({"type": "message", "role": "assistant", "content": "not-json"}))
    raise SystemExit(0)
if mode == "stale-valid-then-invalid":
    print(json.dumps({
        "type": "message.completed",
        "role": "assistant",
        "content": "{\\\"answer\\\": 7}",
        "tool_calls": [{"name": "Read", "arguments": {"path": "job.json"}}],
    }))
    print(json.dumps({"type": "tool", "role": "tool", "content": "job contents"}))
    print(json.dumps({"type": "message.completed", "role": "assistant", "content": "not-json"}))
    raise SystemExit(0)
if mode == "final-tool-call":
    print(json.dumps({
        "type": "message.completed",
        "role": "assistant",
        "content": "{\\\"answer\\\": 7}",
        "tool_calls": [{"name": "Read", "arguments": {"path": "job.json"}}],
    }))
    raise SystemExit(0)
if mode == "retry-success":
    print(json.dumps({"role": "meta", "type": "turn.step.retrying"}))
print(json.dumps({"type": "message.delta", "role": "assistant", "delta": "{\\\"answer\\\":"}))
print(json.dumps({"type": "message.delta", "role": "assistant", "delta": " 7}"}))
print(json.dumps({"type": "message.completed", "role": "assistant", "content": "{\\\"answer\\\": 7}"}))
'''


def _receipt(path: Path) -> Path:
    path.write_text(
        json.dumps(
            {
                "schema_version": AUTHORIZATION_SCHEMA_VERSION,
                "approved": True,
                "receipt_id": "kimi-support-approval-2026-07-22",
                "provider": "Kimi",
                "scope": AUTHORIZATION_SCOPE,
                "approved_at": "2026-07-22T12:00:00-04:00",
                "authorization_basis": "moonshot_written_exception",
                "evidence": "Intentionally not returned by runner diagnostics.",
            }
        ),
        encoding="utf-8",
    )
    return path


def _fake_command(root: Path) -> tuple[str, str]:
    executable = root / "fake_kimi.py"
    executable.write_text(FAKE_KIMI, encoding="utf-8")
    return (sys.executable, str(executable))


def _run(root: Path, **overrides: object) -> dict[str, object]:
    receipt = _receipt(root / "approval.json")
    arguments: dict[str, object] = {
        "payload": {"secret_transcript": "private text never belongs on argv"},
        "output_path": root / "result.json",
        "authorization_receipt_path": receipt,
        "job_root": root / "jobs",
        "kimi_home": root / "kimi-home",
        "command": _fake_command(root),
        "timeout_seconds": 3,
        "retain_job_directory": True,
    }
    arguments.update(overrides)
    return run_kimi_code_job(**arguments)  # type: ignore[arg-type]


def test_missing_authorization_never_spawns_a_process() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        marker = root / "spawned"
        fake = root / "fake_kimi.py"
        fake.write_text(f"from pathlib import Path; Path({str(marker)!r}).write_text('spawned')", encoding="utf-8")
        with pytest.raises(KimiAuthorizationError):
            run_kimi_code_job(
                payload={"private": "text"},
                output_path=root / "output.json",
                authorization_receipt_path=root / "missing.json",
                job_root=root / "jobs",
                command=(sys.executable, str(fake)),
            )
        assert not marker.exists()
        assert not (root / "jobs").exists()


def test_prepare_home_validates_only_the_local_cli_profile() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        prepared = prepare_kimi_code_home(root / "kimi-home", command=_fake_command(root))
        assert prepared["version"] == "0.29.0"
        assert prepared["config_valid"] is True
        assert Path(str(prepared["skills_dir"])).is_dir()
        assert Path(str(prepared["config_path"])).is_file()
        assert json.loads(Path(str(prepared["mcp_path"])).read_text(encoding="utf-8")) == {"mcpServers": {}}


def test_provider_profile_probe_recognizes_completed_managed_login_without_model_call() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        result = inspect_kimi_code_provider_profile(root, command=_fake_command(root))
        assert result == {
            "provider_probe_ok": True,
            "managed_oauth_provider_provisioned": True,
            "managed_model_count": 3,
            "login_provisioning_complete": True,
            "network_call_made": False,
            "model_call_made": False,
        }


@pytest.mark.parametrize("mode", ["provider-unprovisioned", "provider-zero-models"])
def test_provider_profile_probe_rejects_incomplete_local_provisioning(mode: str) -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        result = inspect_kimi_code_provider_profile(
            root,
            command=_fake_command(root),
            extra_env={"PIF_KIMI_TEST_MODE": mode},
        )
        assert result["login_provisioning_complete"] is False
        assert result["model_call_made"] is False
        assert result["network_call_made"] is False


def test_provider_profile_probe_allows_cold_start_budget_above_five_seconds_without_waiting() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        process = mock.Mock()
        process.returncode = 0

        def communicate(*, timeout: float) -> tuple[str, str]:
            assert timeout > 5
            return "managed:kimi-code  type=kimi  models=3  source=oauth\n", ""

        process.communicate.side_effect = communicate
        with mock.patch("research_factory.kimi_code_runner.subprocess.Popen", return_value=process):
            result = inspect_kimi_code_provider_profile(root, command=("fake-kimi",))
        assert result["login_provisioning_complete"] is True


def test_provider_profile_timeout_identifies_the_failed_local_stage() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        with pytest.raises(KimiCodeRunnerError, match="provider profile check.*0.1-second") as error:
            inspect_kimi_code_provider_profile(
                root,
                command=_fake_command(root),
                extra_env={"PIF_KIMI_TEST_MODE": "provider-timeout"},
                timeout_seconds=0.1,
            )
        assert error.value.details["error_class"] == "preflight_timeout"
        assert error.value.details["preflight_stage"] == "provider profile check"
        assert error.value.details["timeout_seconds"] == 0.1
        assert error.value.details["model_call_started"] is False
        assert error.value.details["may_have_consumed_quota"] is False
        assert error.value.details["network_call_made"] is False


def test_cli_profile_preflights_use_named_stages() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        with mock.patch(
            "research_factory.kimi_code_runner._run_preflight",
            side_effect=[(0, "0.29.0\n", ""), (0, "", "")],
        ) as preflight:
            prepare_kimi_code_home(root / "kimi-home", command=("fake-kimi",))
        assert preflight.call_args_list[0].kwargs["stage"] == "version check"
        assert preflight.call_args_list[1].kwargs["stage"] == "configuration check"

        with mock.patch(
            "research_factory.kimi_code_runner._run_preflight",
            return_value=(0, "managed:kimi-code  type=kimi  models=3  source=oauth\n", ""),
        ) as provider_preflight:
            inspect_kimi_code_provider_profile(root / "kimi-home", command=("fake-kimi",))
        assert provider_preflight.call_args.kwargs["stage"] == "provider profile check"


def test_success_parses_final_json_validates_and_writes_atomically_without_payload_in_argv() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        argv_path = root / "argv.json"
        payload = {"secret_transcript": "private text never belongs on argv"}
        result = _run(
            root,
            payload=payload,
            extra_env={"PIF_KIMI_TEST_CAPTURE_ARGV": str(argv_path)},
            schema_validator=lambda value: value["answer"] == 7 or (_ for _ in ()).throw(ValueError("bad answer")),
        )
        output_path = root / "result.json"
        assert json.loads(output_path.read_text(encoding="utf-8")) == {"answer": 7}
        assert result["schema_validation"] == "caller"
        assert isinstance(result["duration_ms"], int)
        assert result["retry_count"] == 0
        assert result["actual_model_verified"] is False
        assert result["actual_model_id"] is None
        assert result["authorization"] == {
            "receipt_id": "kimi-support-approval-2026-07-22",
            "provider": "Kimi",
            "scope": AUTHORIZATION_SCOPE,
            "approved_at": "2026-07-22T12:00:00-04:00",
            "authorization_basis": "moonshot_written_exception",
        }
        argv = json.loads(argv_path.read_text(encoding="utf-8"))
        assert json.dumps(payload) not in json.dumps(argv)
        assert "private text never belongs on argv" not in json.dumps(argv)
        job_path = Path(str(result["job_dir"])) / "job.json"
        assert json.loads(job_path.read_text(encoding="utf-8"))["payload"] == payload
        config = (root / "kimi-home" / "config.toml").read_text(encoding="utf-8")
        assert 'pattern = "Read(!job.json)"' in config
        assert 'pattern = "mcp__*"' in config
        assert '[tools]\nenabled = ["Read"]' in config
        assert "--auto" not in argv
        assert "--skills-dir" in argv


def test_retry_event_is_counted_without_changing_validated_output() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        result = _run(root, extra_env={"PIF_KIMI_TEST_MODE": "retry-success"})
        assert result["retry_count"] == 1
        assert json.loads((root / "result.json").read_text(encoding="utf-8")) == {"answer": 7}


def test_job_directory_is_removed_by_default_after_valid_output() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        result = _run(root, retain_job_directory=False)
        assert result["job_dir"] is None
        assert result["job_dir_retained"] is False
        assert not list((root / "jobs").glob("kimi-code-job-*"))


@pytest.mark.parametrize(
    "mode",
    ["invalid-jsonl", "invalid-output", "missing-final", "stale-valid-then-invalid", "final-tool-call"],
)
def test_invalid_or_missing_assistant_json_never_creates_output(mode: str) -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        output = root / "result.json"
        with pytest.raises(KimiCodeRunnerError):
            _run(root, output_path=output, extra_env={"PIF_KIMI_TEST_MODE": mode})
        assert not output.exists()


def test_existing_output_fails_before_cli_preflight_or_dispatch() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        marker = root / "spawned"
        fake = root / "fake_kimi.py"
        fake.write_text(f"from pathlib import Path; Path({str(marker)!r}).write_text('spawned')", encoding="utf-8")
        output = root / "result.json"
        output.write_text('{"existing": true}\n', encoding="utf-8")
        with pytest.raises(KimiCodeRunnerError, match="already exists"):
            _run(root, output_path=output, command=(sys.executable, str(fake)))
        assert json.loads(output.read_text(encoding="utf-8")) == {"existing": True}
        assert not marker.exists()


def test_preexisting_or_broadened_security_policy_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        home = root / "kimi-home"
        home.mkdir()
        (home / "config.toml").write_text('[tools]\nenabled = ["Read", "Bash"]\n', encoding="utf-8")
        with pytest.raises(KimiCodeRunnerError, match="restricted policy|Read-only"):
            prepare_kimi_code_home(home, command=_fake_command(root))

        (home / "config.toml").write_text(
            '# Managed\nmerge_all_available_skills = false\ntelemetry = false\n\n'
            '[tools]\nenabled = ["Read"]\n\n'
            '[loop_control]\nmax_steps_per_turn = 3\nmax_retries_per_step = 2\n\n'
            '[background]\nprint_background_mode = "exit"\nprint_max_turns = 1\n\n'
            '[permission]\n[[permission.rules]]\ndecision = "allow"\npattern = "Read"\n',
            encoding="utf-8",
        )
        with pytest.raises(KimiCodeRunnerError, match="permission rules"):
            prepare_kimi_code_home(home, command=_fake_command(root))


def test_read_unsafe_single_line_payload_fails_before_cli_dispatch() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        marker = root / "spawned"
        fake = root / "fake_kimi.py"
        fake.write_text(f"from pathlib import Path; Path({str(marker)!r}).write_text('spawned')", encoding="utf-8")
        with pytest.raises(KimiCodeRunnerError, match="Read tool would truncate"):
            _run(root, payload={"long_unwrapped_text": "x" * 2_000}, command=(sys.executable, str(fake)))
        assert not marker.exists()


def test_unicode_line_separator_cannot_bypass_physical_line_limit() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        marker = root / "spawned"
        fake = root / "fake_kimi.py"
        fake.write_text(f"from pathlib import Path; Path({str(marker)!r}).write_text('spawned')", encoding="utf-8")
        with pytest.raises(KimiCodeRunnerError, match="Read tool would truncate"):
            _run(
                root,
                payload={"long_unwrapped_text": "x" * 900 + "\u2028" + "y" * 900},
                command=(sys.executable, str(fake)),
            )
        assert not marker.exists()


def test_nonzero_exit_never_creates_output_and_reports_bounded_diagnostic() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        output = root / "result.json"
        with pytest.raises(KimiCodeRunnerError) as error:
            _run(root, output_path=output, extra_env={"PIF_KIMI_TEST_MODE": "nonzero"})
        assert error.value.details["error_class"] == "nonzero_exit"
        assert error.value.details["exit_code"] == 23
        diagnostics = error.value.details["diagnostics"]
        assert diagnostics["stderr_chars"] == len("simulated failure\n")
        assert len(diagnostics["stderr_sha256"]) == 64
        assert not output.exists()


def test_timeout_kills_the_process_group_and_never_creates_output() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        output = root / "result.json"
        real_killpg = os.killpg
        with mock.patch("research_factory.kimi_code_runner.os.killpg", side_effect=real_killpg) as killpg:
            with pytest.raises(KimiCodeRunnerError) as error:
                _run(
                    root,
                    output_path=output,
                    timeout_seconds=0.1,
                    extra_env={"PIF_KIMI_TEST_MODE": "timeout"},
                )
        assert error.value.details["error_class"] == "process_timeout"
        assert killpg.call_count == 1
        assert killpg.call_args.args[1] == signal.SIGKILL
        assert not output.exists()
