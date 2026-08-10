from __future__ import annotations

import ast
from pathlib import Path


SOURCE = Path(__file__).parents[1] / "research_factory" / "headless_codex.py"


def test_every_one_shot_codex_exec_is_ephemeral() -> None:
    tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
    commands: list[list[str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.List):
            continue
        values = [item.value for item in node.elts if isinstance(item, ast.Constant) and isinstance(item.value, str)]
        if "exec" in values and "--ephemeral" in values:
            commands.append(values)

    assert commands, "Expected at least one one-shot Codex command"
    assert all("--ephemeral" in command for command in commands)
