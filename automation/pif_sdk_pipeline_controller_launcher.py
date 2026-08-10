#!/Users/kolbydayley/.codex/venvs/pif-sdk-pipeline-controller/bin/python
"""Launchd-safe bootstrap for the PIF daily controller.

This file is installed outside the TCC-managed Documents tree.  Python can
therefore initialize without scanning the project root.  The project is added
to sys.path only after a bounded, explicit prerequisite check has succeeded.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import pwd
import signal
import sys
from pathlib import Path
from typing import Any, Callable


DEFAULT_PROJECT_ROOT = Path(
    "/Users/kolbydayley/pif-factory"
)
DEFAULT_STATUS_PATH = Path(
    "/Users/kolbydayley/.codex/pif-controller/launch-status.json"
)
DEFAULT_CODEX_BIN = Path("/Users/kolbydayley/.local/bin/codex")
PREFLIGHT_TIMEOUT_SECONDS = 10
EX_CONFIG = 78


class LaunchPreflightError(RuntimeError):
    pass


def _now() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def _write_status(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    os.replace(temporary, path)


def _bounded_access_check(
    project_root: Path,
    *,
    timeout_seconds: int = PREFLIGHT_TIMEOUT_SECONDS,
    alarm: Callable[[int], Any] = signal.alarm,
) -> dict[str, Any]:
    required = (
        project_root / "research_factory" / "sdk_pipeline_controller.py",
        project_root / "data" / "factory.sqlite",
        project_root / "config" / "sources.yaml",
        project_root / "label_packs" / "ai_discourse_v3_1" / "schema.json",
        project_root / "corpus" / "segments",
    )

    def timed_out(_signum: int, _frame: Any) -> None:
        raise TimeoutError(
            f"project prerequisite probe exceeded {timeout_seconds}s"
        )

    previous = signal.signal(signal.SIGALRM, timed_out)
    alarm(timeout_seconds)
    try:
        if not project_root.is_dir():
            raise LaunchPreflightError(f"project root is not readable: {project_root}")
        # Force directory enumeration; exists()/is_dir() alone can be satisfied
        # by cached metadata while launchd still cannot traverse Documents.
        with os.scandir(project_root) as entries:
            next(iter(entries), None)
        missing = [str(path) for path in required if not path.exists()]
        unreadable = [
            str(path)
            for path in required
            if path.exists() and not os.access(path, os.R_OK)
        ]
        if missing or unreadable:
            raise LaunchPreflightError(
                f"controller prerequisites unavailable; missing={missing}; "
                f"unreadable={unreadable}"
            )
        return {
            "project_root": str(project_root.resolve()),
            "required_paths": [str(path) for path in required],
            "startup_cwd": os.getcwd(),
        }
    except (InterruptedError, OSError, TimeoutError) as exc:
        raise LaunchPreflightError(
            f"cannot traverse project root from clean launch context: "
            f"{type(exc).__name__}: {exc}"
        ) from exc
    finally:
        alarm(0)
        signal.signal(signal.SIGALRM, previous)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", type=Path, default=DEFAULT_PROJECT_ROOT)
    parser.add_argument("--status-path", type=Path, default=DEFAULT_STATUS_PATH)
    parser.add_argument("--preflight-only", action="store_true")
    parser.add_argument("controller_args", nargs=argparse.REMAINDER)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    project_root = args.project_root.expanduser()
    status_path = args.status_path.expanduser()
    started_at = _now()
    base = {
        "schema_version": "pif_launchd_controller_bootstrap_v1",
        "started_at": started_at,
        "pid": os.getpid(),
        "startup_cwd": os.getcwd(),
        "python": sys.executable,
        "project_root": str(project_root),
    }
    try:
        home = Path(pwd.getpwuid(os.getuid()).pw_dir)
        os.environ["HOME"] = str(home)
        os.environ["PATH"] = "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"
        os.environ.pop("PYTHONPATH", None)
        if DEFAULT_CODEX_BIN.is_file() and os.access(DEFAULT_CODEX_BIN, os.X_OK):
            os.environ["PIF_CODEX_BIN"] = str(DEFAULT_CODEX_BIN)
        probe = _bounded_access_check(project_root)
        resolved_root = Path(probe["project_root"])
        sys.path.insert(0, str(resolved_root))
        from research_factory import sdk_pipeline_controller as controller

        module_path = Path(controller.__file__).resolve()
        if resolved_root not in module_path.parents:
            raise LaunchPreflightError(
                f"controller imported from unexpected root: {module_path}"
            )
        _write_status(
            status_path,
            {
                **base,
                "status": "preflight_passed",
                "checked_at": _now(),
                "probe": probe,
                "controller_module": str(module_path),
            },
        )
        if args.preflight_only:
            return 0
        controller_args = list(args.controller_args) or ["serve"]
        exit_code = int(controller.main(controller_args))
        _write_status(
            status_path,
            {
                **base,
                "status": "controller_exited",
                "completed_at": _now(),
                "exit_code": exit_code,
                "probe": probe,
            },
        )
        return exit_code
    except BaseException as exc:
        failure = {
            **base,
            "status": "preflight_failed",
            "failed_at": _now(),
            "error_class": type(exc).__name__,
            "message": str(exc)[:1000],
            "exit_code": EX_CONFIG,
        }
        _write_status(status_path, failure)
        print(
            "PIF_CONTROLLER_PREFLIGHT_FAILED "
            + json.dumps(failure, sort_keys=True),
            file=sys.stderr,
            flush=True,
        )
        return EX_CONFIG


if __name__ == "__main__":
    raise SystemExit(main())
