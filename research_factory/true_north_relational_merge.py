"""Certification-time verifier for relational-junk merge closure.

True-North plan Task 4d option 2 proposes binding the Phase C disposition gate
to *intrinsic* junk only (chrome, bare mention, fragment) and moving *relational*
junk -- the ``non_useful_repetition*`` and ``nonasserted_question_frame`` classes
-- downstream, where canonicalization is supposed to fold the duplicate into its
original's canonical group (ledger category ``merged_duplicate_retained``).

That reclassification is only safe if the downstream merge actually happened.
This module supplies the missing measurement: given the atomic claims produced
for each candidate, the canonical groups produced by the canonicalization stage,
and the list of relational-junk escapes, it asserts that every escape landed in
an identifying canonical group that already contains another candidate's atomic
claim.  Anything else is corpus contamination and is named explicitly.

Design notes
------------

* :func:`verify_relational_merges` is pure and deterministic.  It performs no
  model calls, no network access, and no disk access, so it can be unit tested
  without any stored campaign artifact.
* :func:`load_relational_merge_inputs` is the only disk-aware surface.  It reads
  a completed true-north run's shadow database read-only and the development
  gold file, exactly as the existing certification scorer does.  It refuses to
  touch sealed-holdout material.
* Nothing here is wired into the pipeline.  Another task can call
  :func:`verify_run` from the certification stage when Kolby rules on 4d.
* No candidate identifier or answer key is embedded in this module.  Junk
  reason codes are read from the gold file passed in at call time.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .true_north import DEFAULT_PRIVATE_ROOT, SUITE_ID


SCHEMA_VERSION = "pif_true_north_relational_merge_v1"

#: Relational junk reason-code families.  ``non_useful_repetition`` is a prefix
#: family because the gold adjudication qualifies it (for example
#: ``non_useful_repetition_of_model_definition``).
RELATIONAL_JUNK_REASON_PREFIXES: tuple[str, ...] = ("non_useful_repetition",)
RELATIONAL_JUNK_REASONS: tuple[str, ...] = ("nonasserted_question_frame",)

#: Ledger categories that mean the pipeline let a candidate into the corpus.
#: A gold-junk candidate carrying one of these is an escape.
VALUE_LEDGER_CATEGORIES: frozenset[str] = frozenset(
    {
        "retained_supported_singleton",
        "retained_canonical_member",
        "merged_duplicate_retained",
        "revised_to_valid",
    }
)

#: Canonical keys that carry no identity.  A producer that emits one of these
#: has declined to reconcile the variant, so co-membership under such a key is
#: not evidence of a merge and must never certify a relational escape.
NON_IDENTIFYING_CANONICAL_KEYS: frozenset[str] = frozenset(
    {
        "",
        "unmapped",
        "unassigned",
        "unresolved",
        "unknown",
        "none",
        "null",
        "n/a",
        "na",
        "other",
        "misc",
        "miscellaneous",
    }
)

GROUP_KEY_SEPARATOR = "::"

_MERGED = "merged_into_duplicate_canonical_group"
_NO_ATOMICS = "no_atomic_claims"
_NO_GROUP = "no_canonical_group"
_NON_IDENTIFYING = "non_identifying_canonical_group"
_SINGLETON = "singleton_canonical_group"
_ONLY_JUNK_PEERS = "only_junk_peers_in_canonical_group"
_DECLARED_MISMATCH = "declared_duplicate_not_in_canonical_group"


class RelationalMergeError(Exception):
    """Raised when verifier inputs are malformed or out of contract."""


def is_relational_junk_reason(reason: Any) -> bool:
    """Return ``True`` for the relational junk reason-code families."""

    if not isinstance(reason, str):
        return False
    normalized = reason.strip().lower()
    if not normalized:
        return False
    if normalized in RELATIONAL_JUNK_REASONS:
        return True
    return any(
        normalized.startswith(prefix) for prefix in RELATIONAL_JUNK_REASON_PREFIXES
    )


def is_identifying_canonical_key(key: Any) -> bool:
    """Return ``True`` when a canonical key actually names a group."""

    if not isinstance(key, str):
        return False
    return key.strip().lower() not in NON_IDENTIFYING_CANONICAL_KEYS


@dataclass(frozen=True)
class EscapeMergeStatus:
    """Per-escape merge outcome."""

    candidate_id: str
    junk_reason: str
    merged: bool
    canonical_group_id: str | None
    duplicate_of: str | None
    duplicate_candidate_ids: tuple[str, ...]
    excluded_peer_candidate_ids: tuple[str, ...]
    atomic_claim_ids: tuple[str, ...]
    ledger_category: str | None
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "junk_reason": self.junk_reason,
            "merged": self.merged,
            "canonical_group_id": self.canonical_group_id,
            "duplicate_of": self.duplicate_of,
            "duplicate_candidate_ids": list(self.duplicate_candidate_ids),
            "excluded_peer_candidate_ids": list(self.excluded_peer_candidate_ids),
            "atomic_claim_ids": list(self.atomic_claim_ids),
            "ledger_category": self.ledger_category,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class RelationalMergeReport:
    """Aggregate certification verdict for the relational-junk escapes."""

    schema_version: str
    escapes: tuple[EscapeMergeStatus, ...]
    relational_escapes: int
    merged_count: int
    unmerged: tuple[str, ...]
    contamination_zero: bool
    non_identifying_groups: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "escapes": [entry.to_dict() for entry in self.escapes],
            "relational_escapes": self.relational_escapes,
            "merged_count": self.merged_count,
            "unmerged": list(self.unmerged),
            "contamination_zero": self.contamination_zero,
            "non_identifying_groups": list(self.non_identifying_groups),
        }


@dataclass(frozen=True)
class RelationalMergeInputs:
    """Verifier inputs recovered from stored campaign artifacts."""

    suite_id: str
    run_id: str
    partition: str
    shadow_path: str
    gold_path: str
    atomics: tuple[dict[str, Any], ...]
    canonical_groups: tuple[dict[str, Any], ...]
    escapes: tuple[dict[str, Any], ...]
    diagnostics: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "suite_id": self.suite_id,
            "run_id": self.run_id,
            "partition": self.partition,
            "shadow_path": self.shadow_path,
            "gold_path": self.gold_path,
            "atomic_record_count": len(self.atomics),
            "canonical_group_count": len(self.canonical_groups),
            "escape_count": len(self.escapes),
            "diagnostics": _jsonable(self.diagnostics),
        }


def _require_str(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RelationalMergeError(f"{label} must be a non-empty string")
    return value


def _atomic_ids_of(record: Mapping[str, Any]) -> list[str]:
    if "atomic_claim_ids" in record:
        raw = record["atomic_claim_ids"]
        if isinstance(raw, str) or not isinstance(raw, Iterable):
            raise RelationalMergeError("atomic_claim_ids must be a sequence")
        return [_require_str(value, "atomic_claim_id") for value in raw]
    if "atomic_claim_id" in record:
        return [_require_str(record["atomic_claim_id"], "atomic_claim_id")]
    return []


def _index_atomics(
    atomics: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, str], dict[str, list[str]], dict[str, str | None]]:
    owner_of: dict[str, str] = {}
    by_candidate: dict[str, list[str]] = {}
    ledger_category: dict[str, str | None] = {}
    for record in atomics:
        if not isinstance(record, Mapping):
            raise RelationalMergeError("each atomics entry must be a mapping")
        candidate_id = _require_str(record.get("candidate_id"), "candidate_id")
        bucket = by_candidate.setdefault(candidate_id, [])
        category = record.get("ledger_category")
        if category is not None and not isinstance(category, str):
            raise RelationalMergeError("ledger_category must be a string or null")
        if candidate_id in ledger_category and ledger_category[candidate_id] != category:
            raise RelationalMergeError(
                f"conflicting ledger_category for candidate {candidate_id}"
            )
        ledger_category[candidate_id] = category
        for atomic_id in _atomic_ids_of(record):
            existing = owner_of.get(atomic_id)
            if existing is not None and existing != candidate_id:
                raise RelationalMergeError(
                    f"atomic claim {atomic_id} is claimed by both {existing} "
                    f"and {candidate_id}"
                )
            owner_of[atomic_id] = candidate_id
            if atomic_id not in bucket:
                bucket.append(atomic_id)
    return owner_of, by_candidate, ledger_category


def _index_groups(
    canonical_groups: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, list[str]], dict[str, bool]]:
    members: dict[str, list[str]] = {}
    identifying: dict[str, bool] = {}
    for group in canonical_groups:
        if not isinstance(group, Mapping):
            raise RelationalMergeError("each canonical group must be a mapping")
        group_id = _require_str(group.get("canonical_group_id"), "canonical_group_id")
        flag = group.get("identifying", True)
        if not isinstance(flag, bool):
            raise RelationalMergeError("identifying must be a boolean")
        if group_id in identifying and identifying[group_id] != flag:
            raise RelationalMergeError(
                f"conflicting identifying flag for canonical group {group_id}"
            )
        identifying[group_id] = flag
        bucket = members.setdefault(group_id, [])
        for atomic_id in _atomic_ids_of(group):
            if atomic_id not in bucket:
                bucket.append(atomic_id)
    return members, identifying


def _validated_escapes(
    escapes: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    seen: set[str] = set()
    rows: list[dict[str, Any]] = []
    for escape in escapes:
        if not isinstance(escape, Mapping):
            raise RelationalMergeError("each escape must be a mapping")
        candidate_id = _require_str(escape.get("candidate_id"), "candidate_id")
        junk_reason = _require_str(escape.get("junk_reason"), "junk_reason")
        if not is_relational_junk_reason(junk_reason):
            raise RelationalMergeError(
                "escapes accepts relational junk reason codes only; "
                f"{candidate_id} carries {junk_reason!r}"
            )
        if candidate_id in seen:
            raise RelationalMergeError(f"duplicate escape candidate_id: {candidate_id}")
        seen.add(candidate_id)
        declared = escape.get("duplicate_of")
        if declared is not None:
            declared = _require_str(declared, "duplicate_of")
        rows.append(
            {
                "candidate_id": candidate_id,
                "junk_reason": junk_reason,
                "duplicate_of": declared,
                "ledger_category": escape.get("ledger_category"),
            }
        )
    rows.sort(key=lambda row: row["candidate_id"])
    return rows


def verify_relational_merges(
    atomics: Sequence[Mapping[str, Any]],
    canonical_groups: Sequence[Mapping[str, Any]],
    escapes: Sequence[Mapping[str, Any]],
) -> RelationalMergeReport:
    """Assert every relational-junk escape was merged downstream.

    ``atomics`` entries are ``{"candidate_id", "atomic_claim_ids"|"atomic_claim_id",
    "ledger_category"?}``.  ``canonical_groups`` entries are
    ``{"canonical_group_id", "atomic_claim_ids", "identifying"?}``.  ``escapes``
    entries are ``{"candidate_id", "junk_reason", "duplicate_of"?}`` and must
    carry a relational reason code; anything else raises.

    An escape counts as merged only when one of its atomic claims sits in an
    *identifying* canonical group that also contains an atomic claim belonging
    to a **corroborating** candidate: one that is not itself a relational-junk
    escape, and whose ledger category (when known) admitted it to the corpus.
    Two co-grouped escapes cannot certify each other.  An empty escape list is
    vacuously clean.
    """

    owner_of, atomics_by_candidate, ledger_by_candidate = _index_atomics(atomics)
    group_members, group_identifying = _index_groups(canonical_groups)
    rows = _validated_escapes(escapes)

    escape_ids = {row["candidate_id"] for row in rows}
    declared_category = {
        row["candidate_id"]: row["ledger_category"]
        for row in rows
        if row["ledger_category"] is not None
    }

    def _is_corroborating_peer(peer_id: str) -> bool:
        # A relational-junk escape is junk still in the corpus.  It can never be
        # the duplicate an escape was "merged into", or a duplicate pair of
        # repetition junk would certify itself and report zero contamination.
        if peer_id in escape_ids:
            return False
        category = declared_category.get(peer_id, ledger_by_candidate.get(peer_id))
        if category is not None and category not in VALUE_LEDGER_CATEGORIES:
            return False
        return True

    groups_of_atomic: dict[str, list[str]] = {}
    for group_id in sorted(group_members):
        for atomic_id in group_members[group_id]:
            groups_of_atomic.setdefault(atomic_id, []).append(group_id)

    statuses: list[EscapeMergeStatus] = []
    for row in rows:
        candidate_id = row["candidate_id"]
        declared = row["duplicate_of"]
        own_atomics = tuple(sorted(atomics_by_candidate.get(candidate_id, ())))
        ledger_category = row["ledger_category"]
        if ledger_category is None:
            ledger_category = ledger_by_candidate.get(candidate_id)

        candidate_groups: list[str] = []
        for atomic_id in own_atomics:
            for group_id in groups_of_atomic.get(atomic_id, ()):
                if group_id not in candidate_groups:
                    candidate_groups.append(group_id)
        candidate_groups.sort()

        identifying_groups = [
            group_id
            for group_id in candidate_groups
            if group_identifying.get(group_id, True)
        ]

        merged_group: str | None = None
        merged_peers: tuple[str, ...] = ()
        fallback_group: str | None = None
        fallback_peers: tuple[str, ...] = ()
        excluded_group: str | None = None
        excluded_peers: tuple[str, ...] = ()
        saw_corroborating_peer = False
        declared_mismatch = False
        for group_id in identifying_groups:
            all_peers = sorted(
                {
                    owner_of[atomic_id]
                    for atomic_id in group_members[group_id]
                    if atomic_id in owner_of and owner_of[atomic_id] != candidate_id
                }
            )
            peers = tuple(peer for peer in all_peers if _is_corroborating_peer(peer))
            rejected = tuple(peer for peer in all_peers if peer not in peers)
            if rejected and excluded_group is None:
                excluded_group = group_id
                excluded_peers = rejected
            if fallback_group is None:
                fallback_group = group_id
                fallback_peers = peers
            if not peers:
                continue
            saw_corroborating_peer = True
            if declared is not None and declared not in peers:
                declared_mismatch = True
                if not fallback_peers:
                    fallback_group = group_id
                    fallback_peers = peers
                continue
            merged_group = group_id
            merged_peers = peers
            break

        if merged_group is not None:
            duplicate_of = declared if declared is not None else merged_peers[0]
            statuses.append(
                EscapeMergeStatus(
                    candidate_id=candidate_id,
                    junk_reason=row["junk_reason"],
                    merged=True,
                    canonical_group_id=merged_group,
                    duplicate_of=duplicate_of,
                    duplicate_candidate_ids=merged_peers,
                    excluded_peer_candidate_ids=excluded_peers,
                    atomic_claim_ids=own_atomics,
                    ledger_category=ledger_category,
                    reason=_MERGED,
                )
            )
            continue

        if not own_atomics:
            reason = _NO_ATOMICS
        elif not candidate_groups:
            reason = _NO_GROUP
        elif not identifying_groups:
            reason = _NON_IDENTIFYING
        elif not saw_corroborating_peer and excluded_peers:
            reason = _ONLY_JUNK_PEERS
        elif declared_mismatch:
            reason = _DECLARED_MISMATCH
        else:
            reason = _SINGLETON
        if reason in {_SINGLETON, _DECLARED_MISMATCH}:
            group_id_for_reason = fallback_group
        elif reason == _ONLY_JUNK_PEERS:
            group_id_for_reason = excluded_group
        elif reason == _NON_IDENTIFYING:
            group_id_for_reason = candidate_groups[0]
        else:
            group_id_for_reason = None
        statuses.append(
            EscapeMergeStatus(
                candidate_id=candidate_id,
                junk_reason=row["junk_reason"],
                merged=False,
                canonical_group_id=group_id_for_reason,
                duplicate_of=None,
                duplicate_candidate_ids=fallback_peers,
                excluded_peer_candidate_ids=excluded_peers,
                atomic_claim_ids=own_atomics,
                ledger_category=ledger_category,
                reason=reason,
            )
        )

    merged_count = sum(1 for status in statuses if status.merged)
    unmerged = tuple(
        status.candidate_id for status in statuses if not status.merged
    )
    non_identifying = tuple(
        sorted(
            group_id
            for group_id, flag in group_identifying.items()
            if not flag
        )
    )
    return RelationalMergeReport(
        schema_version=SCHEMA_VERSION,
        escapes=tuple(statuses),
        relational_escapes=len(statuses),
        merged_count=merged_count,
        unmerged=unmerged,
        contamination_zero=not unmerged,
        non_identifying_groups=non_identifying,
    )


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------


def _jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_jsonable(item) for item in value]
    return value


def suite_root(output_root: str | Path | None = None, suite: str = SUITE_ID) -> Path:
    base = (
        Path(output_root).expanduser().resolve()
        if output_root
        else DEFAULT_PRIVATE_ROOT
    )
    return base / suite


def _guard_sealed(path: Path, label: str) -> Path:
    parts = {part.lower() for part in path.parts}
    if "sealed-holdout" in parts or "holdout" in parts:
        raise RelationalMergeError(
            f"refusing to read sealed holdout material via {label}: {path}"
        )
    return path


def _readonly(path: Path) -> sqlite3.Connection:
    if not path.is_file():
        raise RelationalMergeError(f"missing shadow database: {path}")
    conn = sqlite3.connect(
        f"file:{path.expanduser().resolve()}?mode=ro", uri=True, timeout=30.0
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = ON")
    return conn


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE name = ? AND type IN ('table','view')",
        (name,),
    ).fetchone()
    return row is not None


def _gold_reject_reasons(gold_path: Path) -> dict[str, str]:
    if not gold_path.is_file():
        raise RelationalMergeError(f"missing gold file: {gold_path}")
    payload = json.loads(gold_path.read_text(encoding="utf-8"))
    items = payload.get("items")
    if not isinstance(items, list):
        raise RelationalMergeError(f"gold file has no items list: {gold_path}")
    reasons: dict[str, str] = {}
    for item in items:
        if not isinstance(item, Mapping):
            continue
        if str(item.get("disposition")) != "reject":
            continue
        candidate_id = str(item.get("candidate_id") or "")
        if not candidate_id:
            continue
        reasons[candidate_id] = str(item.get("reason_code") or "")
    return reasons


def _positions_relation(conn: sqlite3.Connection) -> str:
    for name in (
        "current_accepted_position_observations",
        "accepted_position_observations",
    ):
        if _table_exists(conn, name):
            return name
    raise RelationalMergeError("shadow database exposes no position observations")


def _load_canonical_subject_map(
    run_root: Path,
    run_id: str,
) -> tuple[dict[str, str], Path]:
    path = run_root / "canonical-map" / "final.private.json"
    if not path.is_file():
        raise RelationalMergeError(
            f"missing canonical-map artifact for run {run_id}: {path}"
        )
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RelationalMergeError(
            f"cannot read canonical-map artifact: {path}"
        ) from exc
    if payload.get("schema_version") != "pif_true_north_canonical_map_v1":
        raise RelationalMergeError(
            "canonical-map artifact has the wrong schema_version"
        )
    if payload.get("run_id") != run_id:
        raise RelationalMergeError(
            "canonical-map artifact run_id does not match canonical run"
        )
    rows = payload.get("subjects")
    if not isinstance(rows, list) or not rows:
        raise RelationalMergeError(
            "canonical-map artifact has no subject mappings"
        )
    resolved: dict[str, str] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            raise RelationalMergeError(
                "canonical-map subject entry must be a mapping"
            )
        key = _require_str(
            row.get("canonical_subject_key"),
            "canonical_subject_key",
        )
        subject_id = _require_str(row.get("subject_id"), "subject_id")
        existing = resolved.get(key)
        if existing is not None and existing != subject_id:
            raise RelationalMergeError(
                f"canonical subject key {key} maps to multiple subject ids"
            )
        resolved[key] = subject_id
    return resolved, path


def load_relational_merge_inputs(
    *,
    run_id: str,
    output_root: str | Path | None = None,
    suite: str = SUITE_ID,
    partition: str = "development",
    gold_path: str | Path | None = None,
    escape_candidate_ids: Sequence[str] | None = None,
) -> RelationalMergeInputs:
    """Recover verifier inputs from a stored true-north run.

    Reads the run's shadow database read-only and the partition gold file.
    Sealed-holdout material is refused outright.

    By default the escape set is derived from the run's own ledger: a gold-junk
    candidate whose ledger category admitted it to the corpus.  Pass
    ``escape_candidate_ids`` to score a different stage's escape set instead
    (for example the frozen Phase C composed disposition), in which case only
    the ids carrying a relational gold reason code become escapes and the rest
    are reported as diagnostics.
    """

    if partition != "development":
        raise RelationalMergeError(
            "relational-merge verification is authorised for the development "
            f"partition only; refused: {partition}"
        )
    root = suite_root(output_root, suite)
    run_root = root / "runs" / _require_str(run_id, "run_id")
    shadow_path = _guard_sealed(run_root / "shadow.sqlite", "shadow database")
    subject_id_by_key, canonical_map_path = (
        _load_canonical_subject_map(run_root, run_id)
    )
    resolved_gold = (
        Path(gold_path).expanduser()
        if gold_path is not None
        else root / "gold" / partition / "final" / "gold.private.json"
    )
    _guard_sealed(resolved_gold, "gold file")

    reject_reasons = _gold_reject_reasons(resolved_gold)

    conn = _readonly(shadow_path)
    try:
        if not _table_exists(conn, "true_north_stage_ledger"):
            raise RelationalMergeError(
                f"shadow database has no stage ledger: {shadow_path}"
            )
        ledger_rows = conn.execute(
            """
            SELECT candidate_id, category, atomic_claim_ids_json
            FROM true_north_stage_ledger
            WHERE run_id = ? AND stage = 'atomic'
            ORDER BY candidate_id
            """,
            (run_id,),
        ).fetchall()
        canonical_rows: list[sqlite3.Row] = []
        if _table_exists(conn, "true_north_variant_canonical_map"):
            positions = _positions_relation(conn)
            canonical_rows = conn.execute(
                f"""
                SELECT map.subject_id AS subject_id,
                       map.canonical_subject_key AS subject_key,
                       map.canonical_proposition_key AS proposition_key,
                       positions.atomic_claim_id AS atomic_claim_id
                FROM true_north_variant_canonical_map AS map
                JOIN {positions} AS positions
                  ON positions.variant_id = map.variant_id
                WHERE map.run_id = ?
                ORDER BY map.canonical_subject_key,
                         map.canonical_proposition_key,
                         positions.atomic_claim_id
                """,
                (run_id,),
            ).fetchall()
    finally:
        conn.close()

    if not ledger_rows:
        raise RelationalMergeError(
            f"run {run_id} has no atomic-stage ledger rows in {shadow_path}"
        )

    atomics: list[dict[str, Any]] = []
    category_by_candidate: dict[str, str] = {}
    atomic_ids_by_candidate: dict[str, tuple[str, ...]] = {}
    category_counts: dict[str, int] = {}
    for row in ledger_rows:
        candidate_id = str(row["candidate_id"])
        category = str(row["category"])
        category_by_candidate[candidate_id] = category
        category_counts[category] = category_counts.get(category, 0) + 1
        raw_ids = json.loads(row["atomic_claim_ids_json"] or "[]")
        atomic_ids_by_candidate[candidate_id] = tuple(
            str(value) for value in raw_ids
        )
        atomics.append(
            {
                "candidate_id": candidate_id,
                "atomic_claim_ids": list(
                    atomic_ids_by_candidate[candidate_id]
                ),
                "ledger_category": category,
            }
        )

    grouped: dict[str, list[str]] = {}
    key_by_group: dict[str, str] = {}
    for row in canonical_rows:
        subject_key = str(row["subject_key"])
        resolved_subject_id = subject_id_by_key.get(subject_key)
        if resolved_subject_id is None:
            raise RelationalMergeError(
                "canonical subject key is absent from the final map: "
                f"{subject_key}"
            )
        if str(row["subject_id"]) != resolved_subject_id:
            raise RelationalMergeError(
                "canonical subject id disagrees with the final map for key: "
                f"{subject_key}"
            )
        group_id = resolved_subject_id
        key_by_group[group_id] = subject_key
        bucket = grouped.setdefault(group_id, [])
        atomic_id = str(row["atomic_claim_id"])
        if atomic_id not in bucket:
            bucket.append(atomic_id)
    canonical_groups: list[dict[str, Any]] = []
    for group_id in sorted(grouped):
        subject_key, _, proposition_key = group_id.partition(GROUP_KEY_SEPARATOR)
        canonical_groups.append(
            {
                "canonical_group_id": group_id,
                "atomic_claim_ids": sorted(grouped[group_id]),
                "canonical_subject_key": key_by_group[group_id],
                "identifying": True,
            }
        )

    declared_escapes: set[str] | None = None
    unknown_declared: list[str] = []
    if escape_candidate_ids is not None:
        declared_escapes = {
            _require_str(value, "escape candidate_id") for value in escape_candidate_ids
        }
        unknown_declared = sorted(
            candidate_id
            for candidate_id in declared_escapes
            if candidate_id not in reject_reasons
        )

    escapes: list[dict[str, Any]] = []
    intrinsic_escapes: list[str] = []
    relational_non_escapes: list[str] = []
    held_relational_candidates: list[str] = []
    for candidate_id, reason in sorted(reject_reasons.items()):
        category = category_by_candidate.get(candidate_id)
        if declared_escapes is None:
            escaped = category in VALUE_LEDGER_CATEGORIES
        else:
            escaped = candidate_id in declared_escapes
        held_without_claim = (
            category == "held_needs_review"
            and not atomic_ids_by_candidate.get(candidate_id)
        )
        if is_relational_junk_reason(reason):
            if escaped and held_without_claim:
                held_relational_candidates.append(candidate_id)
            elif escaped:
                escapes.append(
                    {
                        "candidate_id": candidate_id,
                        "junk_reason": reason,
                        "ledger_category": category,
                    }
                )
            else:
                relational_non_escapes.append(candidate_id)
        elif escaped:
            intrinsic_escapes.append(candidate_id)

    diagnostics = {
        "escape_source": (
            "run_ledger" if declared_escapes is None else "declared_candidate_ids"
        ),
        "declared_escape_count": (
            0 if declared_escapes is None else len(declared_escapes)
        ),
        "declared_escapes_absent_from_gold_rejects": tuple(unknown_declared),
        "gold_reject_count": len(reject_reasons),
        "ledger_row_count": len(ledger_rows),
        "ledger_category_counts": dict(sorted(category_counts.items())),
        "canonical_map_row_count": len(canonical_rows),
        "canonical_group_count": len(canonical_groups),
        "canonical_subject_map_path": str(canonical_map_path),
        "canonical_subject_map_count": len(subject_id_by_key),
        "non_identifying_group_count": sum(
            1 for group in canonical_groups if not group["identifying"]
        ),
        "canonicalization_ran": bool(canonical_rows),
        "relational_escapes": tuple(row["candidate_id"] for row in escapes),
        "held_candidate_ids": tuple(
            sorted(
                candidate_id
                for candidate_id, category in category_by_candidate.items()
                if category == "held_needs_review"
                and not atomic_ids_by_candidate.get(candidate_id)
            )
        ),
        "held_relational_candidates": tuple(
            held_relational_candidates
        ),
        "relational_gold_rejects_not_escaped": tuple(relational_non_escapes),
        "intrinsic_junk_escapes": tuple(intrinsic_escapes),
        "merged_duplicate_retained_ledger_rows": category_counts.get(
            "merged_duplicate_retained", 0
        ),
    }
    return RelationalMergeInputs(
        suite_id=suite,
        run_id=run_id,
        partition=partition,
        shadow_path=str(shadow_path),
        gold_path=str(resolved_gold),
        atomics=tuple(atomics),
        canonical_groups=tuple(canonical_groups),
        escapes=tuple(escapes),
        diagnostics=diagnostics,
    )


def discover_canonicalized_runs(
    *,
    output_root: str | Path | None = None,
    suite: str = SUITE_ID,
) -> tuple[str, ...]:
    """Return stored run ids whose shadow database holds a canonical map."""

    runs_root = suite_root(output_root, suite) / "runs"
    if not runs_root.is_dir():
        return ()
    found: list[str] = []
    for run_dir in sorted(runs_root.iterdir()):
        shadow = run_dir / "shadow.sqlite"
        if not shadow.is_file():
            continue
        try:
            conn = _readonly(shadow)
        except RelationalMergeError:
            continue
        try:
            if not _table_exists(conn, "true_north_variant_canonical_map"):
                continue
            count = conn.execute(
                "SELECT COUNT(*) FROM true_north_variant_canonical_map"
            ).fetchone()[0]
        except sqlite3.DatabaseError:
            continue
        finally:
            conn.close()
        if int(count or 0) > 0:
            found.append(run_dir.name)
    return tuple(found)


def verify_run(
    *,
    run_id: str,
    output_root: str | Path | None = None,
    suite: str = SUITE_ID,
    partition: str = "development",
    gold_path: str | Path | None = None,
    escape_candidate_ids: Sequence[str] | None = None,
) -> tuple[RelationalMergeReport, RelationalMergeInputs]:
    """Load stored artifacts for ``run_id`` and verify relational-merge closure."""

    inputs = load_relational_merge_inputs(
        run_id=run_id,
        output_root=output_root,
        suite=suite,
        partition=partition,
        gold_path=gold_path,
        escape_candidate_ids=escape_candidate_ids,
    )
    report = verify_relational_merges(
        inputs.atomics, inputs.canonical_groups, inputs.escapes
    )
    return report, inputs


def main(argv: Sequence[str] | None = None) -> int:
    """Offline dry-run entry point.  Reads artifacts, prints JSON, writes nothing."""

    parser = argparse.ArgumentParser(
        prog="true-north-relational-merge",
        description=(
            "Verify that relational-junk escapes were merged into their "
            "duplicate's canonical group (Task 4d option 2 prerequisite)."
        ),
    )
    parser.add_argument("--run-id", help="stored true-north run id")
    parser.add_argument("--output-root", default=None)
    parser.add_argument("--suite", default=SUITE_ID)
    parser.add_argument("--partition", default="development")
    parser.add_argument("--gold-path", default=None)
    parser.add_argument(
        "--escape-id",
        action="append",
        dest="escape_ids",
        default=None,
        help=(
            "score this candidate id as an escape instead of deriving the "
            "escape set from the run ledger; repeatable"
        ),
    )
    parser.add_argument(
        "--list-runs",
        action="store_true",
        help="list stored runs that carry a canonical map and exit",
    )
    args = parser.parse_args(argv)

    if args.list_runs:
        runs = discover_canonicalized_runs(
            output_root=args.output_root, suite=args.suite
        )
        print(json.dumps({"canonicalized_runs": list(runs)}, indent=2, sort_keys=True))
        return 0
    if not args.run_id:
        parser.error("--run-id is required unless --list-runs is given")
    try:
        report, inputs = verify_run(
            run_id=args.run_id,
            output_root=args.output_root,
            suite=args.suite,
            partition=args.partition,
            gold_path=args.gold_path,
            escape_candidate_ids=args.escape_ids,
        )
    except RelationalMergeError as error:
        print(json.dumps({"error": str(error)}, indent=2, sort_keys=True))
        return 1
    print(
        json.dumps(
            {"inputs": inputs.to_dict(), "report": report.to_dict()},
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if report.contamination_zero else 2


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
