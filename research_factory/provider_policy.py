"""Fail-closed stage -> (provider, model) routing policy.

Before this module, production could not route: ``worker.py`` raised unless
the model was ``gpt-5.5`` at four separate pins, ~35 CLI defaults repeated the
constant, and the only executor was ``codex exec``. The policy table replaces
scattered constants with one reviewed, committed source of truth
(``config/provider_policy.json``).

Fail closed: an unknown stage, an unknown provider, or a missing model is an
error — never a silent fallback to Codex. Flipping a stage to the GLM lane is
a one-line policy edit, gated by the Phase-3 provider-agreement loop, reviewed
in git; never an environment variable.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from .paths import root

__all__ = [
    "ProviderPolicyError",
    "StagePolicy",
    "load_policy",
    "stage_policy",
    "assert_stage_model",
    "default_policy_path",
]


class ProviderPolicyError(RuntimeError):
    """Raised when the routing policy cannot answer fail-closed."""


@dataclass(frozen=True)
class StagePolicy:
    stage: str
    provider: str
    model: str
    transport: str
    billing: str


def default_policy_path() -> Path:
    return root() / "config" / "provider_policy.json"


def load_policy(path: Path | None = None) -> dict[str, Any]:
    policy_path = path if path is not None else default_policy_path()
    try:
        payload = json.loads(policy_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ProviderPolicyError(
            f"provider policy missing: {policy_path}"
        ) from exc
    except json.JSONDecodeError as exc:
        raise ProviderPolicyError(
            f"provider policy is not valid JSON: {policy_path}: {exc}"
        ) from exc
    if payload.get("schema_version") != "pif_provider_policy_v1":
        raise ProviderPolicyError(
            "provider policy schema_version must be pif_provider_policy_v1"
        )
    if not isinstance(payload.get("stages"), Mapping) or not isinstance(
        payload.get("providers"), Mapping
    ):
        raise ProviderPolicyError(
            "provider policy requires 'stages' and 'providers' mappings"
        )
    return payload


def stage_policy(
    stage: str,
    *,
    policy: Mapping[str, Any] | None = None,
) -> StagePolicy:
    resolved = policy if policy is not None else load_policy()
    stages = resolved["stages"]
    entry = stages.get(stage)
    if not isinstance(entry, Mapping):
        raise ProviderPolicyError(
            f"unknown stage {stage!r}: not present in provider policy"
        )
    provider = str(entry.get("provider") or "")
    providers = resolved["providers"]
    provider_entry = providers.get(provider)
    if not isinstance(provider_entry, Mapping):
        raise ProviderPolicyError(
            f"stage {stage!r} names unknown provider {provider!r}"
        )
    model = entry.get("model")
    if not isinstance(model, str) or not model.strip():
        raise ProviderPolicyError(
            f"stage {stage!r} has no model in provider policy"
        )
    return StagePolicy(
        stage=stage,
        provider=provider,
        model=model,
        transport=str(provider_entry.get("transport") or ""),
        billing=str(provider_entry.get("billing") or ""),
    )


def assert_stage_model(
    stage: str,
    model: str,
    *,
    policy: Mapping[str, Any] | None = None,
) -> StagePolicy:
    """The worker's old hard pins, expressed against the policy table."""

    resolved = stage_policy(stage, policy=policy)
    if model != resolved.model:
        raise ProviderPolicyError(
            f"model {model!r} for stage {stage!r} does not match policy "
            f"model {resolved.model!r}"
        )
    return resolved
