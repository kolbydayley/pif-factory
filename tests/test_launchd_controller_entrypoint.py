from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path


LAUNCHER = (
    Path(__file__).resolve().parents[1]
    / "automation"
    / "pif_sdk_pipeline_controller_launcher.py"
)


def _module():
    spec = importlib.util.spec_from_file_location("pif_launchd_bootstrap", LAUNCHER)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_preflight_runs_from_root_without_inherited_environment(tmp_path, monkeypatch) -> None:
    launcher = _module()
    project = tmp_path / "project"
    for path in (
        project / "research_factory" / "sdk_pipeline_controller.py",
        project / "data" / "factory.sqlite",
        project / "config" / "sources.yaml",
        project / "label_packs" / "ai_discourse_v3_1" / "schema.json",
    ):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("x", encoding="utf-8")
    (project / "corpus" / "segments").mkdir(parents=True)
    prior = Path.cwd()
    try:
        os.chdir("/")
        result = launcher._bounded_access_check(project, timeout_seconds=1)
    finally:
        os.chdir(prior)
    assert result["startup_cwd"] == "/"
    assert result["project_root"] == str(project.resolve())


def test_missing_project_fails_loudly_and_writes_durable_status(tmp_path) -> None:
    launcher = _module()
    status = tmp_path / "status.json"
    code = launcher.main(
        [
            "--project-root",
            str(tmp_path / "missing"),
            "--status-path",
            str(status),
            "--preflight-only",
        ]
    )
    receipt = json.loads(status.read_text(encoding="utf-8"))
    assert code == launcher.EX_CONFIG
    assert receipt["status"] == "preflight_failed"
    assert receipt["error_class"] == "LaunchPreflightError"
    assert "project root is not readable" in receipt["message"]


def test_plist_template_uses_external_launcher_and_cannot_crash_loop() -> None:
    plist = (
        LAUNCHER.parent / "com.kolby.pif.sdk-pipeline-controller.plist.template"
    ).read_text(encoding="utf-8")
    assert "/Users/kolbydayley/.local/libexec/pif-sdk-pipeline-controller" in plist
    assert "<string>/</string>" in plist
    assert "PYTHONPATH" not in plist
    assert "<key>KeepAlive</key>" not in plist
