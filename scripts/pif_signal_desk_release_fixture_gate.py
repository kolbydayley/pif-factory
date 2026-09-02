#!/usr/bin/env python3
"""Bind rendered-DOM regression-fixture results to one built Signal Desk site.

The browser harness that creates ``--execution`` keeps screenshots and source
context in private build storage. This gate verifies that every named fixture
ran against the exact current static assets, then writes a compact immutable
receipt suitable for the clean-release gate.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PIF_ROOT = Path(__file__).resolve().parents[1]
if str(PIF_ROOT) not in sys.path:
    sys.path.insert(0, str(PIF_ROOT))

from research_factory.signal_desk_rebuild_release_fixtures import (  # noqa: E402
    ReleaseFixtureError,
    built_site_sha256,
    evaluate_rendered_fixture_execution,
)
from research_factory.util import dumps_json  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--site-root", type=Path, default=PIF_ROOT / "site")
    parser.add_argument("--execution", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    if args.out.exists():
        raise ReleaseFixtureError("fixture gate output is immutable")
    try:
        execution = json.loads(args.execution.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ReleaseFixtureError("browser fixture execution is unreadable") from exc
    actual_site_sha = built_site_sha256(args.site_root)
    if execution.get("site_build_sha256") != actual_site_sha:
        raise ReleaseFixtureError("browser fixture execution was not run on these built assets")
    receipt = evaluate_rendered_fixture_execution(execution)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(dumps_json(receipt) + "\n", encoding="utf-8")
    print(dumps_json({
        "passed": receipt["passed"],
        "fixture_count": receipt["fixture_count"],
        "site_build_sha256": receipt["site_build_sha256"],
        "out": str(args.out),
    }))
    return 0 if receipt["passed"] else 2


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ReleaseFixtureError as exc:
        print(f"release fixture gate blocked: {exc}", file=sys.stderr)
        raise SystemExit(2)
