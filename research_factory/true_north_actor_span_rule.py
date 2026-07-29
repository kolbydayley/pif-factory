"""Deterministic construction rule for `reported_actor`.

`docs/TRUE_NORTH_ACTOR_CONTRACT.md` already states two mechanical rules as
normative:

    "A non-null value that is not a case-insensitive literal span of the
    evidence is mechanically converted to `null`."

    "A direct speaker discussing their own action in their own voice is not a
    `reported_actor`."

Both were enforced only as *rejection* — a provider answer violating the span
invariant was refused and the call charged — never as *construction*. This
module applies them deterministically to composed atomics, with no model call.

Measured effect on the frozen candidate priors (see
`docs/TRUE_NORTH_ACTOR_SPAN_RULE_RESULTS.md`): field agreement with repaired
gold rises from a raw-prior baseline to a span-enforced one at zero paid cost,
because most prior actors are not literal evidence spans and the contract
requires those to be null.

This module makes no gate change and no gold change. It is a composition
helper; wiring it into a scored lane is a separate, explicit step.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence

__all__ = [
    "ActorSpanRuleError",
    "ActorSpanRuleResult",
    "ABSENT_TEXT",
    "span_enforced_actor",
    "apply_actor_span_rule",
]


class ActorSpanRuleError(ValueError):
    """Raised when an atomic cannot be evaluated against the contract."""


# Sentinels the upstream extractors use for "no value"; mirrors the frozen
# prior-adoption contract in true_north_prior_adoption._ABSENT_TEXT.
ABSENT_TEXT = frozenset(
    {"", "none", "null", "n/a", "na", "unknown", "unspecified", "-"}
)


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() in ABSENT_TEXT:
        return None
    return text


def _normalized(text: str) -> str:
    return " ".join(text.lower().split())


def span_enforced_actor(
    reported_actor: Any,
    evidence_text: Any,
    raw_speaker: Any = None,
    *,
    exclude_speaker_self: bool = True,
) -> str | None:
    """Return the actor prior if the contract admits it, else ``None``.

    The prior's own surface form is preserved when kept: the span test is
    case-insensitive, but the stored value stays verbatim so downstream exact
    comparisons see the extractor's wording, not a normalized rewrite.
    """

    actor = _clean(reported_actor)
    if actor is None:
        return None

    evidence = _clean(evidence_text)
    if evidence is None:
        return None

    if _normalized(actor) not in _normalized(evidence):
        return None

    if exclude_speaker_self:
        speaker = _clean(raw_speaker)
        if speaker is not None and _normalized(actor) == _normalized(speaker):
            return None

    return actor


@dataclass(frozen=True)
class ActorSpanRuleResult:
    """Rewritten atomics plus a per-reason accounting of every change."""

    atomics: tuple[dict[str, Any], ...]
    report: dict[str, int]


def apply_actor_span_rule(
    atomics: Iterable[Mapping[str, Any]],
    *,
    exclude_speaker_self: bool = True,
) -> ActorSpanRuleResult:
    """Apply the contract's mechanical rules across a sequence of atomics.

    Inputs are never mutated. Every atomic is copied; only ``reported_actor``
    can differ, and each change is counted under the reason that caused it.
    """

    rows: Sequence[Mapping[str, Any]] = list(atomics)
    out: list[dict[str, Any]] = []
    report = {
        "total": len(rows),
        "kept": 0,
        "already_null": 0,
        "nulled_absent_span": 0,
        "nulled_speaker_self": 0,
    }

    for index, row in enumerate(rows):
        if "evidence_text" not in row:
            raise ActorSpanRuleError(
                f"atomic at index {index} lacks evidence_text"
            )
        prior = _clean(row.get("reported_actor"))
        resolved = span_enforced_actor(
            row.get("reported_actor"),
            row.get("evidence_text"),
            row.get("raw_speaker"),
            exclude_speaker_self=exclude_speaker_self,
        )

        if prior is None:
            report["already_null"] += 1
        elif resolved is not None:
            report["kept"] += 1
        elif span_enforced_actor(
            row.get("reported_actor"),
            row.get("evidence_text"),
            row.get("raw_speaker"),
            exclude_speaker_self=False,
        ) is not None:
            # Survived the span test; only the speaker-self rule removed it.
            report["nulled_speaker_self"] += 1
        else:
            report["nulled_absent_span"] += 1

        updated = dict(row)
        updated["reported_actor"] = resolved
        out.append(updated)

    return ActorSpanRuleResult(atomics=tuple(out), report=report)
