#!/opt/homebrew/bin/python3.12
"""Build the checksum-bound home-scoped native goal supervisor runtime."""

from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import tempfile
import zipapp
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CODEX_HOME = Path.home() / ".codex"
OUTPUT = CODEX_HOME / "lib" / "pif-native-goal-supervisor.pyz"
OUTPUT_CHECKSUM = OUTPUT.with_suffix(OUTPUT.suffix + ".sha256")
CANONICAL_AUTOMATION = (
    CODEX_HOME
    / "automations"
    / "pif-evaluation-goal-resumer-v2"
    / "automation.toml"
)
CANONICAL_OUTPUT = CODEX_HOME / "lib" / "pif-evaluation-goal-resumer-v2.canonical.toml"
CANONICAL_CHECKSUM = CANONICAL_OUTPUT.with_suffix(CANONICAL_OUTPUT.suffix + ".sha256")
SEMANTIC_PLAN_DIRECTORY = PROJECT_ROOT / "automation"
SEMANTIC_PLAN_GLOB = "pif-evaluation-semantic-plan-*.json"
SEMANTIC_PLAN_OUTPUT = CODEX_HOME / "lib" / "pif-evaluation-semantic-plan-v1.json"
SEMANTIC_PLAN_CHECKSUM = SEMANTIC_PLAN_OUTPUT.with_suffix(
    SEMANTIC_PLAN_OUTPUT.suffix + ".sha256"
)
EXPECTED_PROMPT_SHA256 = "942b7b04d7bc710725abc65542c95c0c4ee7987417e5dd202ca94f98e837d3f4"

FILES = {
    PROJECT_ROOT / "automation" / "pif-native-goal-supervisor-runtime" / "__main__.py": Path("__main__.py"),
    PROJECT_ROOT / "research_factory" / "__init__.py": Path("research_factory/__init__.py"),
    PROJECT_ROOT / "research_factory" / "app_server_thread_supervisor.py": Path(
        "research_factory/app_server_thread_supervisor.py"
    ),
    PROJECT_ROOT / "research_factory" / "app_server_goal_control.py": Path(
        "research_factory/app_server_goal_control.py"
    ),
    PROJECT_ROOT / "research_factory" / "codex_app_server.py": Path(
        "research_factory/codex_app_server.py"
    ),
    PROJECT_ROOT / "research_factory" / "labels.py": Path(
        "research_factory/labels.py"
    ),
    PROJECT_ROOT / "research_factory" / "paths.py": Path(
        "research_factory/paths.py"
    ),
    PROJECT_ROOT / "research_factory" / "util.py": Path("research_factory/util.py"),
    PROJECT_ROOT
    / "research_factory"
    / "protocol"
    / "codex_app_server_0_144_1"
    / "codex_app_server_protocol.v2.schemas.json": Path(
        "research_factory/protocol/codex_app_server_0_144_1/codex_app_server_protocol.v2.schemas.json"
    ),
}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_checksum(path: Path, digest: str) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(digest + "\n", encoding="ascii")
    temporary.replace(path)


def verify_canonical_source() -> None:
    if not CANONICAL_AUTOMATION.is_file():
        raise SystemExit("operator-owned native automation is unavailable")
    text = CANONICAL_AUTOMATION.read_text(encoding="utf-8")
    marker = 'prompt = """'
    if marker not in text:
        raise SystemExit("operator-owned native automation prompt is malformed")
    prompt = text.split(marker, 1)[1].split('"""', 1)[0]
    if hashlib.sha256(prompt.encode("utf-8")).hexdigest() != EXPECTED_PROMPT_SHA256:
        raise SystemExit("refusing to package a drifted operator prompt")
    required = (
        'id = "pif-evaluation-goal-resumer-v2"',
        'status = "ACTIVE"',
        'rrule = "RRULE:FREQ=HOURLY;INTERVAL=4"',
        'target_thread_id = "019f4cf1-c46e-7db3-acd2-bf03c4459a10"',
    )
    if any(item not in text for item in required):
        raise SystemExit("refusing to package a drifted operator automation")


def _load_semantic_plan(path: Path) -> dict:
    if not path.is_file():
        raise SystemExit("semantic plan source is unavailable")
    try:
        plan = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SystemExit("semantic plan source is invalid") from exc
    if not isinstance(plan, dict):
        raise SystemExit("semantic plan source is invalid")
    return plan


def select_current_semantic_plan_source(
    directory: Path = SEMANTIC_PLAN_DIRECTORY,
) -> Path:
    candidates = []
    for path in sorted(directory.glob(SEMANTIC_PLAN_GLOB)):
        plan = _load_semantic_plan(path)
        epoch = plan.get("plan_epoch")
        if isinstance(epoch, bool) or not isinstance(epoch, int) or epoch < 1:
            raise SystemExit("semantic plan epoch is invalid")
        candidates.append((epoch, path.resolve()))
    if not candidates:
        raise SystemExit("semantic plan source is unavailable")
    highest_epoch = max(epoch for epoch, _path in candidates)
    selected = [path for epoch, path in candidates if epoch == highest_epoch]
    if len(selected) != 1:
        raise SystemExit("multiple semantic plans claim the current epoch")
    return selected[0]


def verify_semantic_plan_source(path: Path | None = None) -> Path:
    source = (path or select_current_semantic_plan_source()).resolve()
    plan = _load_semantic_plan(source)
    step = plan.get("step") if isinstance(plan, dict) else None
    expected_plan_keys = {"schema_version", "thread_id", "plan_epoch", "state", "step"}
    expected_step_keys = {
        "step_id",
        "state",
        "max_model_calls",
        "max_total_tokens",
        "expected_receipt_path",
        "accepted_receipt_states",
        "directive_path",
        "directive_sha256",
    }
    if (
        set(plan) != expected_plan_keys
        or not isinstance(step, dict)
        or set(step) != expected_step_keys
        or plan.get("schema_version") != "pif_evaluation_semantic_plan_v1"
        or plan.get("thread_id") != "019f4cf1-c46e-7db3-acd2-bf03c4459a10"
        or isinstance(plan.get("plan_epoch"), bool)
        or not isinstance(plan.get("plan_epoch"), int)
        or plan.get("plan_epoch") < 1
        or plan.get("state") != "executable"
        or not isinstance(step.get("step_id"), str)
        or not step.get("step_id").strip()
        or step.get("state") != "executable"
        or isinstance(step.get("max_model_calls"), bool)
        or not isinstance(step.get("max_model_calls"), int)
        or step.get("max_model_calls") < 0
        or isinstance(step.get("max_total_tokens"), bool)
        or not isinstance(step.get("max_total_tokens"), int)
        or step.get("max_total_tokens") < 0
    ):
        raise SystemExit("refusing to package a drifted semantic plan")
    directive_path = Path(str(step.get("directive_path") or "")).expanduser().resolve()
    directive_sha256 = step.get("directive_sha256")
    if (
        not directive_path.is_file()
        or not isinstance(directive_sha256, str)
        or sha256_file(directive_path) != directive_sha256
    ):
        raise SystemExit("semantic plan directive is unavailable or drifted")
    if source == SEMANTIC_PLAN_OUTPUT.resolve():
        raise SystemExit("installed semantic plan cannot be its own immutable source")
    return source


def verify_archive(path: Path) -> None:
    probe = (
        "import sys; "
        f"sys.path.insert(0, {str(path)!r}); "
        "from research_factory.app_server_goal_control import "
        "inspect_thread_goal, resume_blocked_thread_goal, resume_paused_thread_goal; "
        "from research_factory.app_server_thread_supervisor import "
        "OPERATOR_HOLD_SCHEMA_VERSION, read_operator_hold_marker; "
        "from research_factory.codex_app_server import verify_protocol_schema; "
        "assert verify_protocol_schema() == "
        "'312b90372fd7a03423df7f46c60d623ada3ed066abcc5af2e6144bfa83b62026'"
    )
    completed = subprocess.run(
        ["/opt/homebrew/bin/python3.12", "-c", probe],
        cwd=Path.home(),
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    if completed.returncode != 0:
        raise SystemExit("packaged native supervisor self-test failed")


def main() -> int:
    verify_canonical_source()
    semantic_plan_source = verify_semantic_plan_source()
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="pif-native-supervisor-build-") as raw_stage:
        stage = Path(raw_stage)
        for source, relative in FILES.items():
            if not source.is_file():
                raise SystemExit(f"required supervisor source missing: {source}")
            destination = stage / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, destination)
        temporary_output = OUTPUT.with_suffix(OUTPUT.suffix + ".tmp")
        temporary_output.unlink(missing_ok=True)
        zipapp.create_archive(
            stage,
            target=temporary_output,
            interpreter="/opt/homebrew/bin/python3.12",
            compressed=True,
        )
        temporary_output.chmod(0o755)
        verify_archive(temporary_output)
        temporary_output.replace(OUTPUT)

    canonical_temporary = CANONICAL_OUTPUT.with_suffix(CANONICAL_OUTPUT.suffix + ".tmp")
    shutil.copy2(CANONICAL_AUTOMATION, canonical_temporary)
    canonical_temporary.chmod(0o600)
    canonical_temporary.replace(CANONICAL_OUTPUT)
    semantic_plan_temporary = SEMANTIC_PLAN_OUTPUT.with_suffix(
        SEMANTIC_PLAN_OUTPUT.suffix + ".tmp"
    )
    shutil.copy2(semantic_plan_source, semantic_plan_temporary)
    semantic_plan_temporary.chmod(0o600)
    semantic_plan_temporary.replace(SEMANTIC_PLAN_OUTPUT)
    write_checksum(OUTPUT_CHECKSUM, sha256_file(OUTPUT))
    write_checksum(CANONICAL_CHECKSUM, sha256_file(CANONICAL_OUTPUT))
    write_checksum(SEMANTIC_PLAN_CHECKSUM, sha256_file(SEMANTIC_PLAN_OUTPUT))
    print(f"{OUTPUT} {sha256_file(OUTPUT)}")
    print(f"{CANONICAL_OUTPUT} {sha256_file(CANONICAL_OUTPUT)}")
    print(f"{SEMANTIC_PLAN_OUTPUT} {sha256_file(SEMANTIC_PLAN_OUTPUT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
