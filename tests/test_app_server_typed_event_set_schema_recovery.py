from __future__ import annotations

import json
from pathlib import Path

import pytest

from research_factory import app_server_typed_event_set_schema_recovery as recovery


def test_schema_recovery_removes_only_min_length() -> None:
    schema = {
        "type": "object",
        "additionalProperties": False,
        "required": ["value", "items"],
        "properties": {
            "value": {"type": "string", "minLength": 1, "maxLength": 10},
            "items": {
                "type": "array",
                "minItems": 0,
                "maxItems": 2,
                "uniqueItems": True,
                "items": {"type": "string", "minLength": 2},
            },
        },
    }

    compatible = recovery.build_provider_compatible_schema(schema)

    assert recovery._count_key(schema, "minLength") == 2  # noqa: SLF001
    assert recovery._count_key(compatible, "minLength") == 0  # noqa: SLF001
    assert compatible["properties"]["value"]["maxLength"] == 10
    assert recovery._count_key(compatible, "uniqueItems") == 0  # noqa: SLF001
    assert compatible["properties"]["items"]["maxItems"] == 2


def test_predecessor_schema_profile_is_below_documented_global_limits() -> None:
    schema = recovery._load_json(  # noqa: SLF001
        recovery._predecessor_paths()["predecessor_schema"], "predecessor schema"  # noqa: SLF001
    )
    profile = recovery._schema_profile(schema)  # noqa: SLF001

    assert profile["min_length_keyword_count"] == 25
    assert profile["unique_items_keyword_count"] == 8
    assert profile["object_property_count"] == 299
    assert profile["object_property_count"] < 5000
    assert profile["enum_value_count"] == 493
    assert profile["enum_value_count"] < 1000
    assert (
        profile["enum_string_char_count"] + profile["property_name_char_count"]
        < 120000
    )


def test_frozen_semantic_request_bytes_match_predecessor(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    predecessor = tmp_path / "predecessor"
    predecessor_turn = predecessor / "turns" / "typed-event-set-extraction"
    predecessor_turn.mkdir(parents=True)
    root = tmp_path / "recovery"
    for name, content in (
        ("input.private.json", "{}\n"),
        ("prompt.private.md", "prompt\n"),
        ("base-instructions.private.md", "base\n"),
        ("projection-schema.json", "{}\n"),
        (
            "schema.json",
            json.dumps(
                {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["value"],
                    "properties": {"value": {"type": "string", "minLength": 1}},
                }
            )
            + "\n",
        ),
    ):
        (predecessor_turn / name).write_text(content, encoding="utf-8")

    monkeypatch.setattr(recovery, "PREDECESSOR_ROOT", predecessor)
    monkeypatch.setattr(recovery, "_validate_predecessor", lambda _paths: {
        "terminal": {},
        "sidecar": {},
        "capacity": {},
        "schema": json.loads((predecessor_turn / "schema.json").read_text()),
    })
    monkeypatch.setattr(
        recovery,
        "_predecessor_paths",
        lambda: {
            "predecessor_terminal": predecessor / "terminal.json",
            "predecessor_runtime_lock": predecessor / "runtime-lock.json",
            "predecessor_runtime_closure": predecessor / "runtime-lock-closure.json",
            "predecessor_capacity": predecessor_turn / "capacity.json",
            "predecessor_sidecar": predecessor_turn / "sidecar.json",
            "predecessor_config": predecessor / "experiment-config.json",
            "predecessor_schema": predecessor_turn / "schema.json",
            "architecture_decision": predecessor / "architecture-decision.json",
            "source_input": predecessor_turn / "input.private.json",
            "source_prompt": predecessor_turn / "prompt.private.md",
            "source_base": predecessor_turn / "base-instructions.private.md",
            "projection_schema": predecessor_turn / "projection-schema.json",
        },
    )
    for path in recovery._predecessor_paths().values():  # noqa: SLF001
        if not path.exists():
            path.write_text("{}\n", encoding="utf-8")

    config_path = recovery.prepare_recovery(root)
    config = json.loads(config_path.read_text())
    turn = recovery._turn_paths(root)  # noqa: SLF001
    for name, key in (
        ("input", "source_input"),
        ("prompt", "source_prompt"),
        ("base", "source_base"),
        ("projection_schema", "projection_schema"),
    ):
        source = Path(config[key]["path"])
        if name == "projection_schema":
            value = json.loads(source.read_text())
            recovery._write_immutable(turn[name], value)  # noqa: SLF001
        else:
            recovery._write_private_text(turn[name], source.read_text())  # noqa: SLF001
        assert recovery._record(turn[name])["sha256"] == config[key]["sha256"]  # noqa: SLF001
    compatible = recovery.build_provider_compatible_schema(
        json.loads(Path(config["predecessor_schema"]["path"]).read_text())
    )
    assert recovery._count_key(compatible, "minLength") == 0  # noqa: SLF001


def test_existing_terminal_prevents_replay(tmp_path: Path) -> None:
    root = tmp_path / "recovery"
    root.mkdir()
    terminal = {
        "terminal_reason": "infrastructure_or_extraction_attempt_failed",
        "support_alignment_authorized": False,
    }
    (root / "terminal.json").write_text(json.dumps(terminal), encoding="utf-8")

    returned = __import__("asyncio").run(recovery.run_recovery(root / "experiment-config.json"))

    assert returned == terminal
