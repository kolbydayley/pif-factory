"""Compact production entry point for Podcast Intelligence Factory.

The historical ``research-factory`` parser remains available to repository
tests and recovery tooling.  The installed ``pif`` command intentionally
exposes only the bounded production surface approved for the rebuild.  Legacy
evaluator arms can be inspected through ``pif lab`` but cannot be executed by
this entry point; epochs 6-44 are frozen research evidence.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from . import cli as legacy_cli
from .paths import db_path


PRODUCTION_COMMANDS = (
    "radar",
    "status",
    "release",
    "run",
    "reconcile",
    "outcomes",
    "publish-ops",
    "ui",
    "lab",
)
FROZEN_EVALUATION_EPOCHS = "6-44"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pif",
        description="Local/private Podcast Intelligence Factory production control.",
    )
    parser.add_argument("--db", default=str(db_path()), help="Authoritative local SQLite path.")
    parser.add_argument("command", nargs="?", choices=PRODUCTION_COMMANDS)
    parser.add_argument("arguments", nargs=argparse.REMAINDER)
    return parser


def _print(value: dict[str, object], *, stream: object = sys.stdout) -> None:
    print(json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True), file=stream)


def main(argv: Sequence[str] | None = None) -> int:
    values = list(argv if argv is not None else sys.argv[1:])
    args = build_parser().parse_args(values)
    if args.command is None:
        build_parser().print_help()
        return 0

    if args.command == "radar":
        # Research Radar owns a separate hot database outside the repository by
        # default.  Only forward the legacy global --db value when the operator
        # explicitly supplied it before the `radar` command; `pif radar --db …`
        # is already preserved in ``args.arguments``.
        from . import radar_cli

        radar_arguments = list(args.arguments)
        try:
            command_index = values.index("radar")
        except ValueError:
            command_index = 0
        explicit_global_db = any(
            value == "--db" or value.startswith("--db=")
            for value in values[:command_index]
        )
        if explicit_global_db and not any(
            value == "--db" or value.startswith("--db=")
            for value in radar_arguments
        ):
            radar_arguments = ["--db", str(Path(args.db).expanduser()), *radar_arguments]
        return int(radar_cli.main(radar_arguments))

    if args.command == "run":
        if not args.arguments or args.arguments[0] != "daily":
            _print(
                {
                    "ok": False,
                    "error": "only_bounded_daily_run_is_production_authorized",
                    "allowed": "pif run daily",
                },
                stream=sys.stderr,
            )
            return 2

    if args.command == "lab":
        requested = list(args.arguments)
        if requested and requested[0] == "sol-final-gate":
            from .extractor_gate import main as extractor_gate_main

            return extractor_gate_main(requested[1:])
        if requested and requested[0] in {"kimi-code", "kimi-workhorse"}:
            from .kimi_workhorse import main as kimi_workhorse_main

            return int(kimi_workhorse_main(requested[1:]))
        if requested and requested[0] in {"glm", "glm-workhorse"}:
            from .glm_workhorse import main as glm_workhorse_main

            return int(glm_workhorse_main(requested[1:]))
        if requested and requested[0] in {"true-north", "true-north-benchmark"}:
            from .true_north import main as true_north_main

            forwarded = list(requested[1:])
            if not any(
                value == "--source-db" or value.startswith("--source-db=")
                for value in forwarded
            ):
                forwarded = ["--source-db", str(Path(args.db).expanduser()), *forwarded]
            return int(true_north_main(forwarded))
        if "--allow-write" in requested:
            _print(
                {
                    "ok": False,
                    "error": "frozen_evaluator_execution_blocked",
                    "frozen_epochs": FROZEN_EVALUATION_EPOCHS,
                    "canonical_mutation": False,
                    "message": (
                        "Legacy evaluator arms are immutable research evidence. "
                        "The bounded final Sol gate is a separate shadow-only project."
                    ),
                },
                stream=sys.stderr,
            )
            return 2

    forwarded = ["--db", str(Path(args.db).expanduser()), args.command, *args.arguments]
    return int(legacy_cli.main(forwarded))


if __name__ == "__main__":
    raise SystemExit(main())
