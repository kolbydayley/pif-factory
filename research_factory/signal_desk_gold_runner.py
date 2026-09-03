"""Resumable dev-first Gold A/B/C/audit runner for Signal Desk."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import time
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any, Mapping, Sequence

from .codex_app_server import CodexAppServerClient
from .signal_desk_gold_capacity import (
    admit_gold_call,
    capacity_backend_message_from_sidecar,
    capacity_error_from_sidecar,
    capacity_status,
    is_model_capacity_error,
    record_capacity_failure,
    record_gold_admission_success,
    release_gold_admission,
)
from .signal_desk_adaptive_concurrency import (
    GOLD_BOUNDS, admission_limit, initialize_lane, record_outcome,
)
from .signal_desk_background_admission import BackgroundAdmission
from .signal_desk_gold_atomicity import ATOMICITY_REVIEW_WINDOW_IDS
from .signal_desk_gold_audit import select_dev_audit_windows
from .signal_desk_gold_budget import (
    mark_provider_started,
    release_unstarted_reservation,
    reserve_gold_call,
    settle_gold_call,
)
from .signal_desk_gold_measurement import (
    A_SYSTEM_PROMPT, AUDIT_SYSTEM_PROMPT, B_SYSTEM_PROMPT, C_SYSTEM_PROMPT,
)
from .signal_desk_rebuild_contracts import validate_output
from .signal_desk_rebuild_dispatch import (
    acquire_lease, complete_attempt, enqueue_task, fail_attempt_semantically,
    initialize_dispatch_schema, release_attempt_for_retry, resurrect_task,
)
from .signal_desk_rebuild_gold import (
    build_gold_packets, select_blind_gold_audit_windows, verify_frozen_manifest,
)
from .signal_desk_rebuild_gold_canary import _prompt
from .util import now_iso


RESERVE_TOKENS = {"A": 48_000, "B": 49_000, "C": 57_000, "AUDIT": 48_000}
SYSTEM_PROMPTS = {"A": A_SYSTEM_PROMPT, "B": B_SYSTEM_PROMPT,
                  "C": C_SYSTEM_PROMPT, "AUDIT": AUDIT_SYSTEM_PROMPT}
GOLD_SPLITS = ("development", "validation", "sealed_holdout")
GOLD_TURN_TYPES = ("A", "B", "C", "AUDIT")
COMPLETE_BENCHMARK_WINDOWS = 804
DEVELOPMENT_AUDIT_INITIAL_WINDOWS = 19


class GoldRunnerError(RuntimeError):
    """A fail-closed Gold authoring runner error."""


class GoldResumePlanError(GoldRunnerError):
    """The staged resume plan violates a frozen-corpus invariant."""


class GoldForegroundDeferred(GoldRunnerError):
    """A foreground Codex session owns the shared model lane for now."""

    def __init__(self, admission: Any) -> None:
        self.admission = admission
        super().__init__(
            f"Gold provider work deferred: {admission.reason}; "
            f"retry after {admission.retry_after_seconds}s"
        )


class GoldCapacityDeferred(GoldRunnerError):
    """The durable Gold capacity circuit has not admitted another call yet."""

    def __init__(self, capacity: Mapping[str, Any]) -> None:
        self.capacity = dict(capacity)
        super().__init__(
            f"Gold provider work deferred: {capacity['reason']}; "
            f"retry after {capacity['retry_after_seconds']}s"
        )


def gold_model_admission(*, configured_concurrency: int) -> BackgroundAdmission:
    """Gold uses provider health, not local foreground state, for admission."""

    return BackgroundAdmission(
        allowed=True,
        reason="gold_provider_capacity_governed",
        retry_after_seconds=0,
        provider_concurrency_cap=int(configured_concurrency),
        input_idle_seconds=None,
    )


def repair_unique_evidence_offsets(
    output: Mapping[str, Any], *, transcript_window: str
) -> tuple[dict[str, Any], int]:
    """Rebind offset-only slips when a verbatim excerpt occurs exactly once."""

    repaired = json.loads(json.dumps(output))
    count = 0
    for event in repaired.get("events") or []:
        evidence = str(event.get("evidence_text") or "")
        declared_start = int(event.get("evidence_start") or 0)
        declared_end = int(event.get("evidence_end") or 0)
        if (
            evidence
            and declared_start >= 0
            and declared_end == declared_start + len(evidence)
            and transcript_window[declared_start:declared_end] == evidence
        ):
            continue
        first = transcript_window.find(evidence) if evidence else -1
        if first >= 0 and transcript_window.find(evidence, first + 1) < 0:
            event["evidence_start"] = first
            event["evidence_end"] = first + len(evidence)
            count += 1
            continue
        # Flattened captions occasionally omit or add a space while copying.
        # Restore only source whitespace at the declared span; any non-space
        # character disagreement remains a semantic contract failure.
        if 0 <= declared_start < declared_end <= len(transcript_window):
            declared_source = transcript_window[declared_start:declared_end]
            if evidence and "".join(evidence.split()) == "".join(declared_source.split()):
                event["evidence_text"] = declared_source
                count += 1
    return repaired, count


def compact_adjudication_output(output: Mapping[str, Any]) -> dict[str, Any]:
    """Keep Gold-C semantic inputs while dropping reconstructable bulk."""

    keep = (
        "claim_text", "speech_act", "evidence_text", "speaker_id",
        "quoted_person_id", "mentioned_person_ids", "attribution_type",
        "attribution_confidence", "issue_label", "stance",
        "publishability_state",
    )
    return {
        "window_disposition": output.get("window_disposition"),
        "events": [
            {key: event.get(key) for key in keep}
            for event in (output.get("events") or [])
        ],
    }


def acquire_pipeline_lease(
    dispatch: sqlite3.Connection,
    *,
    task_namespace: str,
    lease_owner: str,
    lease_seconds: int,
) -> Mapping[str, Any] | None:
    """Prefer finishing a window before admitting more first-pass work."""

    for turn_type in ("C", "B", "A"):
        lease = acquire_lease(
            dispatch,
            lease_owner=lease_owner,
            lease_seconds=lease_seconds,
            task_key_prefix=f"{task_namespace}:{turn_type}:",
        )
        if lease is not None:
            return lease
    return None


def _notify_stall(kind: str, detail: str, next_step: str) -> None:
    subprocess.run(
        ["codex-ops", "notify", "--source", "signal-desk-gold-authoring",
         "--summary", f"Signal Desk gold authoring stalled: {kind}",
         "--severity", "high", "--details", detail, "--next-step", next_step,
         "--dedupe-key", f"signal-desk-gold-stall:{kind}", "--telegram-mode", "prefer", "--json"],
        capture_output=True, text=True, timeout=30, check=False,
    )


# A contract failure is the model returning an excerpt that is not exact at
# its declared offsets.  It is rare (~1% of windows) and usually a one-off,
# so one fresh attempt is worth one call; the rejected output is never
# reused.  A second failure on the same window is quarantined.
MAX_GOLD_CONTRACT_ATTEMPTS = 2


PARSE_SCHEMA_TOKENS = ("schema", "parse", "json", "validation")


def classify_adaptive_outcome(exc: BaseException, *, provider_capacity: bool) -> str:
    """Map a failed Gold call to the adaptive limiter's outcome vocabulary.

    The limiter trips (-2 slots, 10-minute cooldown, slow climb-back) when
    ``parse_schema`` exceeds 2% of a 600s window - about one event in ~13
    calls at full concurrency.  An ``EvidenceContractError`` is a content
    fault in one window (an excerpt that is not exact, chrome text), not the
    provider degrading under load, and it is retried on its own; recording
    it as ``parse_schema`` cost 25-50% of throughput for 20-70 minutes after
    every such failure on 2026-09-03.  It is ``failure``: counted, neutral.
    Genuine malformed-output failures (schema, parse, JSON, validation)
    still count as ``parse_schema``.
    """

    if provider_capacity:
        return "rate_limit"
    name = type(exc).__name__
    if "EvidenceContract" in name:
        return "failure"
    detail_text = f"{name}: {exc}".casefold()
    if "timeout" in detail_text:
        return "timeout"
    if any(token in detail_text for token in PARSE_SCHEMA_TOKENS):
        return "parse_schema"
    return "failure"


def contract_failure_action(attempt_number: int) -> str:
    """retry | quarantine for a contract failure on the given attempt."""

    return "retry" if int(attempt_number) < MAX_GOLD_CONTRACT_ATTEMPTS else "quarantine"


def archive_semantic_rejected_sidecar(
    *, sidecar_path: Path, recovery_root: Path, attempt_id: int,
) -> Path | None:
    """Preserve the completed transport record of a contract-failed call.

    ``archive_retryable_sidecar_for_retry`` deliberately leaves completed
    sidecars alone; a resurrected attempt would otherwise overwrite the only
    telemetry of the call that failed the contract.
    """

    if not sidecar_path.exists():
        return None
    recovery_root.mkdir(parents=True, exist_ok=True)
    target = recovery_root / f"{sidecar_path.stem}.attempt-{attempt_id}.semantic-rejected.json"
    if target.exists():
        return None
    sidecar_path.replace(target)
    return target


def quarantined_window_ids(
    dispatch: sqlite3.Connection, *, task_namespace: str
) -> dict[str, dict[str, str]]:
    """Windows whose Gold task was terminalized by a semantic failure.

    A contract failure is terminal in dispatch (only explicit resurrection can
    retry it), so it is the durable quarantine record: a resumed runner must
    exclude these windows instead of spinning forever waiting for outputs
    that no lease will ever produce.  Nothing here touches item content.
    """

    rows = dispatch.execute(
        """
        SELECT t.task_key, t.payload_json, a.semantic_failure_code, a.attempt_number
        FROM signal_desk_rebuild_tasks t
        JOIN signal_desk_rebuild_attempts a ON a.id = t.current_attempt_id
        WHERE t.status = 'terminal_failed'
          AND substr(t.task_key, 1, length(?)) = ?
        ORDER BY t.task_key
        """,
        (f"{task_namespace}:", f"{task_namespace}:"),
    ).fetchall()
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        payload = json.loads(row["payload_json"])
        window_id = str(payload["window_id"])
        # A window is quarantined at its earliest failed turn; later turns
        # are never enqueued for it, so first-seen is the authoritative one.
        result.setdefault(window_id, {
            "turn_type": str(payload["turn_type"]),
            "failure_code": str(row["semantic_failure_code"] or ""),
            "attempt_number": int(row["attempt_number"] or 1),
            "task_key": str(row["task_key"]),
        })
    return result


def resurrect_retryable_quarantine(
    dispatch: sqlite3.Connection, *, task_namespace: str
) -> dict[str, dict[str, Any]]:
    """Give contract-failed windows their remaining bounded attempts at startup.

    A runner that quarantined before this policy existed, or that stopped
    between the failure and the retry, leaves windows terminal with attempts
    to spare.  Resurrect those; return only the windows that stay quarantined.
    """

    remaining: dict[str, dict[str, Any]] = {}
    for window_id, info in quarantined_window_ids(dispatch, task_namespace=task_namespace).items():
        if (
            info["failure_code"] == "gold_contract_failure"
            and contract_failure_action(info["attempt_number"]) == "retry"
        ):
            resurrect_task(
                dispatch,
                task_key=info["task_key"],
                resurrected_by="gold-runner",
                reason=f"bounded contract retry {info['attempt_number'] + 1}/{MAX_GOLD_CONTRACT_ATTEMPTS} at phase start",
            )
            continue
        remaining[window_id] = info
    return remaining


def _phase_required_ids(
    target_ids: Sequence[str], quarantined: Mapping[str, Any]
) -> list[str]:
    """Windows a phase must complete: every target not under quarantine."""

    return [window_id for window_id in target_ids if window_id not in quarantined]


def _load_outputs(root: Path, turn_type: str) -> dict[str, Mapping[str, Any]]:
    output_dir = root / turn_type
    result = {}
    if output_dir.exists():
        for path in output_dir.glob("*.json"):
            value = json.loads(path.read_text(encoding="utf-8"))
            result[str(value["window_id"])] = value
    return result


RETRYABLE_TURN_ERROR_INFO = {
    "serverOverloaded",
    "rateLimitExceeded",
    "serviceUnavailable",
}


def archive_retryable_sidecar_for_retry(
    *, sidecar_path: Path, output_path: Path, recovery_root: Path,
    attempt_id: int, lease_generation: int,
) -> Path | None:
    """Archive an explicitly cancelled transport record before a safe retry.

    A cancelled sidecar, a failed sidecar carrying an allowlisted provider
    transport error, or a turn that timed out (state ``interrupted``, status
    ``timeout``) all prove that no completed structured output was accepted:
    the runner only writes the output artifact after the turn completes and
    validates.  It is therefore safe to preserve the transport receipt and
    retry the same semantic attempt lineage.  Without this, a timed-out turn's
    ``interrupted`` sidecar trips ``_assert_new_sidecar`` on every relaunch
    (``AppServerRecoveryRequired``), which the runner reports as an
    infrastructure failure and which stops the whole swarm on each pass -
    a single slow window wedges the campaign.  Completed, semantic, or unknown
    failures remain fail-closed and require operator adjudication.
    """

    if not sidecar_path.exists():
        return None
    payload = json.loads(sidecar_path.read_text(encoding="utf-8"))
    state = payload.get("state")
    retryable_failure = (
        state == "failed"
        and payload.get("error_class") == "turn_failed"
        and (payload.get("turn_error") or {}).get("codex_error_info")
        in RETRYABLE_TURN_ERROR_INFO
    )
    retryable_timeout = state == "interrupted" and payload.get("status") == "timeout"
    if state != "cancelled" and not retryable_failure and not retryable_timeout:
        return None
    if output_path.exists():
        raise RuntimeError("cancelled sidecar unexpectedly has a completed output artifact")
    recovery_root.mkdir(parents=True, exist_ok=True)
    target = recovery_root / (
        f"{sidecar_path.stem}.attempt-{attempt_id}.generation-{lease_generation}.json"
    )
    if target.exists():
        raise RuntimeError(f"cancelled sidecar recovery target already exists: {target}")
    sidecar_path.replace(target)
    return target


def _import_seed_outputs(
    seed_roots: Mapping[str, Path], result_root: Path,
    allowed_ids: Mapping[str, set[str]],
) -> dict[str, int]:
    counts = {}
    for turn_type, source_root in seed_roots.items():
        destination = result_root / turn_type
        destination.mkdir(parents=True, exist_ok=True)
        count = 0
        for source in sorted(source_root.glob("*.output.json")):
            value = json.loads(source.read_text(encoding="utf-8"))
            if str(value["window_id"]) not in allowed_ids[turn_type]:
                continue
            target = destination / f"{value['window_id']}.json"
            if not target.exists():
                shutil.copyfile(source, target)
            count += 1
        counts[turn_type] = count
    return counts


def _require_complete_frozen_manifest(manifest: Mapping[str, Any]) -> None:
    """Reject partial or mutable benchmarks before a paid Gold call can start."""

    verify_frozen_manifest(manifest)
    counts = manifest.get("counts") or {}
    by_split = counts.get("by_split") or {}
    if (
        manifest.get("complete_benchmark") is not True
        or manifest.get("frozen") is not True
        or len(manifest.get("windows") or ()) != COMPLETE_BENCHMARK_WINDOWS
        or int(counts.get("windows") or 0) != COMPLETE_BENCHMARK_WINDOWS
        or set(by_split) != set(GOLD_SPLITS)
    ):
        raise GoldResumePlanError(
            "Gold authoring requires the complete, frozen 804-window benchmark manifest"
        )


def _secure_result_root(
    result_root: Path,
    *,
    split: str,
    sealed_output_root: Path | None,
) -> None:
    """Create only owner-readable output roots for sealed item-level answers."""

    result_root = result_root.expanduser()
    if result_root.is_symlink():
        raise GoldResumePlanError("Gold result roots must not be symbolic links")
    if split in {"validation", "sealed_holdout"}:
        if sealed_output_root is None:
            raise GoldResumePlanError(
                f"{split} Gold outputs require an explicit sealed 0700 output root"
            )
        sealed_output_root = sealed_output_root.expanduser()
        if sealed_output_root.is_symlink():
            raise GoldResumePlanError("sealed Gold output root must not be a symbolic link")
        sealed_root = sealed_output_root.resolve()
        target = result_root.resolve()
        try:
            target.relative_to(sealed_root)
        except ValueError as exc:
            raise GoldResumePlanError(
                f"{split} Gold result root must be inside the sealed output root"
            ) from exc
        sealed_output_root.mkdir(parents=True, exist_ok=True)
        os.chmod(sealed_output_root, 0o700)
        result_root.mkdir(parents=True, exist_ok=True)
        os.chmod(result_root, 0o700)
        if (result_root.stat().st_mode & 0o777) != 0o700:
            raise GoldResumePlanError("sealed Gold output root permissions are not 0700")
        return
    result_root.mkdir(parents=True, exist_ok=True)


def _write_split_status(
    *,
    result_root: Path,
    split: str,
    turn_type: str,
    status: str,
    reason: str,
    retry_after_seconds: int,
    manifest_sha256: str | None = None,
) -> dict[str, Any]:
    """Checkpoint a zero-call pause without including any item-level content."""

    result_root.mkdir(parents=True, exist_ok=True)
    receipt: dict[str, Any] = {
        "schema_version": "pif_signal_desk_gold_split_status_v1",
        "created_at": now_iso(),
        "status": status,
        "complete": False,
        "split": split,
        "turn_type": turn_type,
        "reason": reason,
        "retry_after_seconds": max(0, int(retry_after_seconds)),
        "provider_calls_started": 0,
        "manifest_sha256": manifest_sha256,
        "contains_a1_or_a2": False,
    }
    receipt["receipt_sha256"] = hashlib.sha256(
        json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    target = result_root / f"gold-{split}-status.json"
    target.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if split in {"validation", "sealed_holdout"}:
        os.chmod(target, 0o600)
    return receipt


def _capacity_preflight(
    budget: sqlite3.Connection,
    *,
    configured_concurrency: int,
) -> dict[str, Any]:
    """Refuse an open/backed-up circuit before creating app-server clients."""

    adaptive = admission_limit(budget, lane="gold")
    capacity = capacity_status(budget)
    effective_limit = min(
        int(configured_concurrency), int(adaptive["effective_limit"])
    )
    state = str(capacity["state"])
    active = int(capacity["active_admissions"])
    if state == "open" and int(capacity["retry_after_seconds"]) > 0:
        return {
            "allowed": False,
            "reason": "gold_model_capacity_backoff",
            "retry_after_seconds": int(capacity["retry_after_seconds"]),
            "capacity_state": state,
            "active_admissions": active,
            "effective_limit": effective_limit,
        }
    if state == "half_open" and active:
        return {
            "allowed": False,
            "reason": "gold_model_capacity_probe_in_progress",
            "retry_after_seconds": max(1, int(capacity["retry_after_seconds"]) or 60),
            "capacity_state": state,
            "active_admissions": active,
            "effective_limit": 1,
        }
    if state == "closed" and active >= effective_limit:
        return {
            "allowed": False,
            "reason": "gold_model_capacity_slots_full",
            "retry_after_seconds": 60,
            "capacity_state": state,
            "active_admissions": active,
            "effective_limit": effective_limit,
        }
    return {
        "allowed": True,
        "reason": None,
        "retry_after_seconds": 0,
        "capacity_state": state,
        "active_admissions": active,
        # An open circuit whose deadline has arrived admits precisely one
        # serialized recovery probe; the first worker transitions it to
        # half-open atomically inside admit_gold_call.
        "effective_limit": 1 if state in {"open", "half_open"} else effective_limit,
    }


def _validate_phase_outputs(
    *,
    result_root: Path,
    turn_type: str,
    by_window: Mapping[str, Mapping[str, Any]],
    required_ids: Sequence[str],
) -> dict[str, Mapping[str, Any]]:
    """Read and contract-check a complete prerequisite phase before reuse."""

    outputs = _load_outputs(result_root, turn_type)
    expected = set(required_ids)
    observed = set(outputs)
    if observed != expected:
        raise GoldRunnerError(
            f"Gold {turn_type} is incomplete for the current split: "
            f"{len(observed & expected)}/{len(expected)} windows"
        )
    for window_id in sorted(expected):
        transcript_window = str(by_window[window_id]["input"]["window_text"])
        repaired, repair_count = repair_unique_evidence_offsets(
            outputs[window_id], transcript_window=transcript_window
        )
        validate_output(
            repaired,
            transcript_window=transcript_window,
            expected_window_id=window_id,
        )
        if repair_count:
            target = result_root / turn_type / f"{window_id}.json"
            target.write_text(
                json.dumps(repaired, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            if target.parent.exists() and (target.parent.stat().st_mode & 0o777) == 0o700:
                os.chmod(target, 0o600)
            outputs[window_id] = repaired
    return outputs


def _audit_target_ids(
    *,
    manifest: Mapping[str, Any],
    split: str,
    by_window: Mapping[str, Mapping[str, Any]],
    result_root: Path,
) -> tuple[list[str], dict[str, Any] | None, tuple[str, ...], tuple[str, ...]]:
    """Resolve audit members without opening a non-current split's outputs."""

    frozen_slice = tuple(
        window_id
        for window_id in select_blind_gold_audit_windows(manifest)
        if window_id in by_window
    )
    if not frozen_slice:
        raise GoldResumePlanError(f"{split} has no frozen blind-audit slice")
    all_ids = sorted(by_window)
    # The independent audit always follows a complete Gold C for the exact
    # current split.  It is not merely a supervisor ordering convention.
    c_outputs = _validate_phase_outputs(
        result_root=result_root,
        turn_type="C",
        by_window=by_window,
        required_ids=all_ids,
    )
    if split != "development":
        return list(frozen_slice), None, frozen_slice, ()
    if len(frozen_slice) != DEVELOPMENT_AUDIT_INITIAL_WINDOWS:
        raise GoldResumePlanError(
            "development blind-audit seed must contain exactly 19 deterministic windows"
        )
    audit_plan = select_dev_audit_windows(
        manifest,
        {window_id: len(output["events"]) for window_id, output in c_outputs.items()},
        initial_window_ids=frozen_slice,
    )
    if not audit_plan["decision_ready"]:
        raise GoldRunnerError("development audit cannot reach the 1,000-event floor")
    atomicity_review_ids = tuple(
        sorted(set(ATOMICITY_REVIEW_WINDOW_IDS) & set(by_window))
    )
    target_ids = sorted(set(audit_plan["window_ids"]) | set(atomicity_review_ids))
    return target_ids, audit_plan, frozen_slice, atomicity_review_ids


def build_gold_resume_plan(
    *,
    manifest_path: Path,
    gold_root: Path,
    allow_sealed_holdout: bool,
) -> dict[str, Any]:
    """Return the only authorized stage ordering; this never starts a model call."""

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    _require_complete_frozen_manifest(manifest)
    if not allow_sealed_holdout:
        # Refuse before Dev C so a caller cannot accidentally complete only a
        # prefix and then discover that the holdout authorization was absent.
        raise GoldResumePlanError("sealed-holdout Gold requires explicit authorization")
    sealed_root = gold_root / "sealed-gold-results"
    dispatch_by_split = {
        "development": gold_root / "dispatch-dev.sqlite",
        "validation": gold_root / "dispatch-validation.sqlite",
        "sealed_holdout": gold_root / "dispatch-sealed-holdout.sqlite",
    }
    if len({path.resolve() for path in dispatch_by_split.values()}) != len(dispatch_by_split):
        raise GoldResumePlanError("Gold resume stages require split-specific dispatch databases")
    result_by_split = {
        "development": gold_root / "results" / "development",
        "validation": sealed_root / "validation",
        "sealed_holdout": sealed_root / "sealed_holdout",
    }
    stages = (
        ("development", "C"),
        ("validation", "PIPELINE"),
        ("sealed_holdout", "PIPELINE"),
        ("development", "AUDIT"),
        ("validation", "AUDIT"),
        ("sealed_holdout", "AUDIT"),
    )
    return {
        "schema_version": "pif_signal_desk_gold_resume_plan_v1",
        "manifest_sha256": manifest["manifest_sha256"],
        "contains_a1_or_a2": False,
        "configured_max_concurrency": 8,
        "initial_adaptive_concurrency": GOLD_BOUNDS.minimum,
        "lease_seconds": 1800,
        "deadline_seconds": 900,
        "stages": tuple(
            {
                "split": split,
                "turn_type": turn_type,
                "dispatch_database": dispatch_by_split[split],
                "result_root": result_by_split[split],
                "sealed_output_root": sealed_root if split != "development" else None,
                "task_namespace": "dev" if split == "development" else split,
                "allow_sealed_holdout": split == "sealed_holdout",
            }
            for split, turn_type in stages
        ),
    }


async def _run_gold_split_phases(
    *, manifest_path: Path, project_root: Path, result_root: Path,
    dispatch_database: Path, budget_database: Path, grant_path: Path,
    session_root: Path, budget_dir: Path, seed_roots: Mapping[str, Path],
    split: str, phase_order: Sequence[str], task_namespace: str,
    allow_sealed_holdout: bool = False,
    sealed_output_root: Path | None = None,
    concurrency: int = 8, binary: str = "codex",
    foreground_admission: Callable[..., Any] = gold_model_admission,
) -> dict[str, Any]:
    if not 2 <= concurrency <= 8:
        raise ValueError("gold concurrency must be 2-8")
    if split not in GOLD_SPLITS:
        raise GoldResumePlanError(f"unknown Gold split: {split}")
    if not phase_order or any(turn_type not in GOLD_TURN_TYPES for turn_type in phase_order):
        raise GoldResumePlanError("Gold runner phase order contains an unsupported phase")
    if len(set(phase_order)) != len(phase_order):
        raise GoldResumePlanError("Gold runner phase order must not repeat a phase")
    if split == "sealed_holdout" and not allow_sealed_holdout:
        raise GoldResumePlanError("sealed-holdout Gold requires explicit authorization")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    _require_complete_frozen_manifest(manifest)
    _secure_result_root(
        result_root,
        split=split,
        sealed_output_root=sealed_output_root,
    )
    packets = build_gold_packets(
        manifest,
        project_root=project_root,
        gold_pass="A",
        splits=(split,),
        allow_sealed=split == "sealed_holdout",
    )
    by_window = {str(packet["input"]["window_id"]): packet for packet in packets}
    if not by_window:
        raise GoldResumePlanError(f"Gold split has no frozen packets: {split}")
    blind_audit_ids = tuple(
        window_id
        for window_id in select_blind_gold_audit_windows(manifest)
        if window_id in by_window
    )
    atomicity_review_ids: tuple[str, ...] = ()
    audit_ids: set[str] = set(blind_audit_ids)
    audit_plan: dict[str, Any] | None = None
    imported = _import_seed_outputs(
        seed_roots, result_root,
        {"A": set(by_window), "B": set(by_window), "C": set(by_window),
         "AUDIT": set(by_window)},
    )
    dispatch_database.parent.mkdir(parents=True, exist_ok=True)
    dispatch = sqlite3.connect(dispatch_database)
    dispatch.row_factory = sqlite3.Row
    initialize_dispatch_schema(dispatch)
    budget = sqlite3.connect(budget_database)
    budget.row_factory = sqlite3.Row
    initialize_lane(
        # A configured 8 is a ceiling, never the fresh-lane starting load.
        # The durable controller grows from two only after three healthy
        # windows; an existing learned limit is preserved by initialize_lane.
        budget, lane="gold", bounds=GOLD_BOUNDS, initial_limit=GOLD_BOUNDS.minimum,
    )
    phase_receipts = []
    run_started = time.monotonic()
    stop = asyncio.Event()
    foreground_stop: Any | None = None
    capacity_stop: dict[str, Any] | None = None
    pipeline_mode = tuple(phase_order) == ("A", "B", "C")

    try:
        for turn_type in (("A",) if pipeline_mode else phase_order):
            all_ids = sorted(by_window)
            if turn_type == "B" and not pipeline_mode:
                # The stage contract is public-entrypoint enforced as well as
                # supervisor enforced: B cannot bypass a complete Gold A.
                _validate_phase_outputs(
                    result_root=result_root,
                    turn_type="A",
                    by_window=by_window,
                    required_ids=all_ids,
                )
            if turn_type == "C" and not pipeline_mode:
                # No C task may be enqueued before both independent authoring
                # passes are complete and contract-valid for this exact split.
                _validate_phase_outputs(
                    result_root=result_root,
                    turn_type="A",
                    by_window=by_window,
                    required_ids=all_ids,
                )
                _validate_phase_outputs(
                    result_root=result_root,
                    turn_type="B",
                    by_window=by_window,
                    required_ids=all_ids,
                )
            if turn_type == "AUDIT":
                # ``_audit_target_ids`` validates C before selecting either
                # the expanded Dev slice or the frozen validation/holdout
                # slice.  This must happen before dispatch enqueue.
                target_ids, audit_plan, blind_audit_ids, atomicity_review_ids = _audit_target_ids(
                    manifest=manifest,
                    split=split,
                    by_window=by_window,
                    result_root=result_root,
                )
                audit_ids = set(target_ids)
            elif pipeline_mode:
                target_ids = all_ids
            else:
                target_ids = all_ids
            phase_types = ("A", "B", "C") if pipeline_mode else (turn_type,)
            existing_by_turn: dict[str, dict[str, Mapping[str, Any]]] = {}
            for phase_type in phase_types:
                phase_existing = _load_outputs(result_root, phase_type)
                for window_id in set(phase_existing) & set(target_ids):
                    transcript_window = str(by_window[window_id]["input"]["window_text"])
                    repaired, repair_count = repair_unique_evidence_offsets(
                        phase_existing[window_id], transcript_window=transcript_window
                    )
                    validate_output(
                        repaired,
                        transcript_window=transcript_window,
                        expected_window_id=window_id,
                    )
                    if repair_count:
                        (result_root / phase_type / f"{window_id}.json").write_text(
                            json.dumps(repaired, indent=2, sort_keys=True) + "\n", encoding="utf-8"
                        )
                        phase_existing[window_id] = repaired
                existing_by_turn[phase_type] = phase_existing
            existing = existing_by_turn[turn_type]
            missing = (
                [window_id for window_id in target_ids if window_id not in existing_by_turn["C"]]
                if pipeline_mode
                else [window_id for window_id in target_ids if window_id not in existing]
            )
            worker_concurrency = 0
            if missing:
                capacity = _capacity_preflight(
                    budget,
                    configured_concurrency=concurrency,
                )
                if not capacity["allowed"]:
                    _write_split_status(
                        result_root=result_root,
                        split=split,
                        turn_type=turn_type,
                        status="deferred",
                        reason=str(capacity["reason"]),
                        retry_after_seconds=int(capacity["retry_after_seconds"]),
                        manifest_sha256=str(manifest["manifest_sha256"]),
                    )
                    raise GoldCapacityDeferred(capacity)
                # Keep the configured worker pool alive. Each worker reads the
                # durable limit before leasing work, so the pool can ramp or
                # back off in place without restarting the supervisor.
                worker_concurrency = concurrency
                if worker_concurrency < 1:
                    raise GoldRunnerError("Gold worker pool is empty")
            for window_id in missing:
                packet = by_window[window_id]
                enqueue_turn = turn_type
                if pipeline_mode:
                    enqueue_turn = (
                        "A" if window_id not in existing_by_turn["A"]
                        else "B" if window_id not in existing_by_turn["B"]
                        else "C"
                    )
                enqueue_task(
                    dispatch,
                    task_key=f"{task_namespace}:{enqueue_turn}:{window_id}",
                    task_type="gold_window",
                    payload={"window_id": window_id, "turn_type": enqueue_turn,
                             "text_sha256": hashlib.sha256(packet["input"]["window_text"].encode()).hexdigest()},
                )
            # Durable across resumes: a terminal_failed task is never revived
            # by re-enqueue, so quarantine is read back from dispatch rather
            # than remembered only in this process.
            quarantined = resurrect_retryable_quarantine(dispatch, task_namespace=task_namespace)
            contract_retries: list[str] = []
            phase_start = time.monotonic()
            completed_rows: list[dict[str, Any]] = []

            async def process_one(client: CodexAppServerClient, lease: Mapping[str, Any], worker_id: int) -> None:
                nonlocal foreground_stop, capacity_stop
                payload = lease["payload"]
                window_id = str(payload["window_id"])
                task_turn_type = str(payload["turn_type"])
                if task_turn_type not in GOLD_TURN_TYPES:
                    raise GoldRunnerError("leased Gold task has an invalid turn type")
                packet = by_window[window_id]
                capacity_task_key = (
                    f"{task_namespace}:{task_turn_type}:{window_id}:attempt:{lease['current_attempt_id']}:"
                    f"generation:{lease['lease_generation']}"
                )
                adaptive = admission_limit(budget, lane="gold")
                admission = admit_gold_call(
                    budget,
                    task_key=capacity_task_key,
                    lease_owner=str(lease["lease_owner"]),
                    configured_concurrency=int(adaptive["effective_limit"]),
                    lane="gpt_5_6_sol_gold_authoring",
                )
                if not admission["allowed"]:
                    release_attempt_for_retry(
                        dispatch, attempt_id=int(lease["current_attempt_id"]),
                        lease_owner=str(lease["lease_owner"]), lease_generation=int(lease["lease_generation"]),
                        failure_code=str(admission["reason"]),
                        failure_detail=(
                            "provider capacity circuit is protecting the shared GPT-5.6-sol lane; "
                            f"retry after {int(admission.get('retry_after_seconds') or 0)} seconds"
                        ),
                    )
                    # A lower adaptive ceiling is normal while calls already
                    # in flight drain.  Requeue quietly; the existing capacity
                    # circuit still stops the swarm for a provider outage.
                    if admission["reason"] == "gold_model_capacity_slots_full":
                        await asyncio.sleep(1)
                        return
                    capacity_stop = {
                        "reason": str(admission["reason"]),
                        "retry_after_seconds": int(admission.get("retry_after_seconds") or 60),
                        "capacity_state": str(admission.get("state") or "open"),
                    }
                    stop.set()
                    return
                admission_id = str(admission["admission_id"])
                try:
                    live_snapshot = await client.read_weekly_rate_limit()
                    reservation = reserve_gold_call(
                        budget, grant_path=grant_path, session_root=session_root,
                        budget_dir=budget_dir,
                        task_key=capacity_task_key,
                        turn_type=task_turn_type,
                        reserve_tokens=RESERVE_TOKENS[task_turn_type],
                        live_snapshot=live_snapshot,
                    )
                except Exception:
                    release_gold_admission(budget, admission_id=admission_id)
                    raise
                if not reservation.get("allowed"):
                    release_gold_admission(budget, admission_id=admission_id)
                    release_attempt_for_retry(
                        dispatch, attempt_id=int(lease["current_attempt_id"]),
                        lease_owner=str(lease["lease_owner"]), lease_generation=int(lease["lease_generation"]),
                        failure_code=str(reservation.get("reason")), failure_detail="weekly health gate stalled",
                    )
                    stop.set()
                    _notify_stall(str(reservation.get("reason")), "Gold checkpoint is clean; no new call started.",
                                  "Restore/reset the weekly subscription window; the runner can resume from leases.")
                    return
                reservation_id = str(reservation["reservation_id"])
                prompt = _prompt(packet)
                if task_turn_type == "C":
                    a = json.loads((result_root / "A" / f"{window_id}.json").read_text())
                    b = json.loads((result_root / "B" / f"{window_id}.json").read_text())
                    prompt += "\n\nGOLD A OUTPUT\n" + json.dumps(
                        compact_adjudication_output(a), separators=(",", ":"), sort_keys=True
                    )
                    prompt += "\n\nGOLD B OUTPUT\n" + json.dumps(
                        compact_adjudication_output(b), separators=(",", ":"), sort_keys=True
                    )
                output_path = result_root / task_turn_type / f"{window_id}.json"
                sidecar_path = result_root / "sidecars" / task_turn_type / f"{window_id}.json"
                output_path.parent.mkdir(parents=True, exist_ok=True)
                sidecar_path.parent.mkdir(parents=True, exist_ok=True)
                if split in {"validation", "sealed_holdout"}:
                    os.chmod(output_path.parent, 0o700)
                    os.chmod(sidecar_path.parent, 0o700)
                archive_retryable_sidecar_for_retry(
                    sidecar_path=sidecar_path,
                    output_path=output_path,
                    recovery_root=result_root / "recovery-sidecars" / task_turn_type,
                    attempt_id=int(lease["current_attempt_id"]),
                    lease_generation=int(lease["lease_generation"]),
                )
                started = time.monotonic()
                reservation_settled = False
                provider_started = False
                result = None
                try:
                    async def invoke_provider_turn() -> Any:
                        nonlocal provider_started
                        mark_provider_started(budget, reservation_id)
                        provider_started = True
                        return await client.run_ephemeral_structured_turn(
                            model="gpt-5.6-sol", effort="medium",
                            base_instructions=SYSTEM_PROMPTS[task_turn_type], prompt=prompt,
                            output_schema=packet["output_schema"], cwd=project_root,
                            sidecar_path=sidecar_path, output_path=output_path,
                            timeout_seconds=900,
                        )

                    result = await invoke_provider_turn()
                    usage = int(result.usage.total_tokens) if result.usage else 0
                    settle_gold_call(budget, reservation_id=reservation_id,
                                     actual_tokens=usage, provider_calls=1)
                    reservation_settled = True
                    if not result.status_ok or result.output is None:
                        raise RuntimeError(result.error_class or result.status)
                    repaired, repair_count = repair_unique_evidence_offsets(
                        result.output,
                        transcript_window=str(packet["input"]["window_text"]),
                    )
                    validated = validate_output(
                        repaired, transcript_window=str(packet["input"]["window_text"]),
                        expected_window_id=window_id,
                    )
                    if repair_count:
                        output_path.write_text(
                            json.dumps(repaired, indent=2, sort_keys=True) + "\n", encoding="utf-8"
                        )
                    if split in {"validation", "sealed_holdout"} and output_path.exists():
                        os.chmod(output_path, 0o600)
                    record_outcome(
                        budget, lane="gold", outcome="success",
                        latency_seconds=time.monotonic() - started,
                    )
                    record_gold_admission_success(budget, admission_id=admission_id)
                except Exception as exc:  # noqa: BLE001
                    foreground_yield = False
                    capacity_error_code = capacity_error_from_sidecar(str(sidecar_path))
                    provider_capacity = not foreground_yield and is_model_capacity_error(
                        error_code=capacity_error_code,
                        detail=str(exc),
                    )
                    detail_text = f"{type(exc).__name__}: {str(exc)}".casefold()
                    adaptive_outcome = classify_adaptive_outcome(
                        exc, provider_capacity=provider_capacity
                    )
                    if not foreground_yield:
                        record_outcome(
                            budget, lane="gold", outcome=adaptive_outcome,
                            latency_seconds=time.monotonic() - started,
                        )
                    if not reservation_settled:
                        if not provider_started:
                            release_unstarted_reservation(budget, reservation_id)
                        else:
                            # The provider call started, but an exception denied
                            # us authoritative usage. Charge the full reservation
                            # so an infrastructure failure can never become
                            # unmetered.
                            settle_gold_call(
                                budget,
                                reservation_id=reservation_id,
                                actual_tokens=RESERVE_TOKENS[task_turn_type],
                                provider_calls=1,
                            )
                    if output_path.exists():
                        rejected = result_root / "rejected" / task_turn_type / output_path.name
                        rejected.parent.mkdir(parents=True, exist_ok=True)
                        output_path.replace(rejected)
                    detail = f"{type(exc).__name__}: {str(exc)[:300]}"
                    if foreground_yield:
                        foreground_stop = exc.admission
                        release_gold_admission(budget, admission_id=admission_id)
                        failure_code = "gold_foreground_reserved"
                        detail = (
                            "foreground Codex resumed; cancelled the background Gold turn "
                            f"and preserved its sidecar for retry ({exc.admission.reason})"
                        )
                        release_attempt_for_retry(
                            dispatch, attempt_id=int(lease["current_attempt_id"]),
                            lease_owner=str(lease["lease_owner"]),
                            lease_generation=int(lease["lease_generation"]),
                            failure_code=failure_code, failure_detail=detail,
                        )
                    elif "EvidenceContract" in type(exc).__name__:
                        # One malformed answer is a data-quality fault in a
                        # single window, not a lane-wide problem.  Quarantine
                        # that window (terminal in dispatch, never retried,
                        # never accepted) and keep the other workers going
                        # instead of idling the whole campaign.
                        release_gold_admission(budget, admission_id=admission_id)
                        fail_attempt_semantically(
                            dispatch, attempt_id=int(lease["current_attempt_id"]),
                            lease_owner=str(lease["lease_owner"]), lease_generation=int(lease["lease_generation"]),
                            failure_code="gold_contract_failure", failure_detail=detail,
                        )
                        archive_semantic_rejected_sidecar(
                            sidecar_path=sidecar_path,
                            recovery_root=result_root / "recovery-sidecars" / task_turn_type,
                            attempt_id=int(lease["current_attempt_id"]),
                        )
                        if contract_failure_action(int(lease["attempt_number"])) == "retry":
                            # Fresh attempt lineage, audited in dispatch; the
                            # rejected output stays under rejected/ untouched.
                            resurrect_task(
                                dispatch,
                                task_key=f"{task_namespace}:{task_turn_type}:{window_id}",
                                resurrected_by="gold-runner",
                                reason=f"bounded contract retry {int(lease['attempt_number']) + 1}/{MAX_GOLD_CONTRACT_ATTEMPTS}: {detail[:160]}",
                            )
                            contract_retries.append(window_id)
                            return
                        quarantined[window_id] = {
                            "turn_type": task_turn_type,
                            "failure_code": "gold_contract_failure",
                        }
                        window_hash = hashlib.sha256(window_id.encode()).hexdigest()[:16]
                        _notify_stall(
                            "gold_contract_quarantine",
                            f"{split} Gold {task_turn_type} window {window_hash} quarantined: {detail}; "
                            f"{len(quarantined)} window(s) quarantined, other windows continue",
                            "Inspect the rejected output under rejected/; resurrect the task explicitly "
                            "to retry, otherwise the split completes without this window.",
                        )
                        return
                    else:
                        if provider_capacity:
                            backend_message = capacity_backend_message_from_sidecar(
                                str(sidecar_path)
                            )
                            capacity = record_capacity_failure(
                                budget, admission_id=admission_id,
                                error_code=str(capacity_error_code or "serverOverloaded"),
                                backend_message=backend_message,
                            )
                            failure_code = "gold_model_capacity_backoff"
                            detail = (
                                f"{detail}; capacity circuit opened for "
                                f"{int(capacity['backoff_seconds'])} seconds"
                            )
                            capacity_stop = {
                                "reason": "gold_model_capacity_backoff",
                                "retry_after_seconds": int(capacity["backoff_seconds"]),
                                "capacity_state": "open",
                            }
                        else:
                            release_gold_admission(budget, admission_id=admission_id)
                            failure_code = "gold_infrastructure_failure"
                        release_attempt_for_retry(
                            dispatch, attempt_id=int(lease["current_attempt_id"]),
                            lease_owner=str(lease["lease_owner"]), lease_generation=int(lease["lease_generation"]),
                            failure_code=failure_code, failure_detail=detail,
                        )
                    stop.set()
                    if foreground_yield:
                        # Do not page or open a provider circuit for the
                        # normal act of returning control to the user.
                        return
                    if provider_capacity:
                        _notify_stall(
                            "model_capacity",
                            detail,
                            "Gold is checkpointed. Wait for the capacity circuit's single-call probe "
                            "instead of restarting or increasing concurrency.",
                        )
                    else:
                        _notify_stall(type(exc).__name__, detail,
                                      "Inspect the failed lease; resume uses the same semantic task lineage.")
                    return
                complete_attempt(
                    dispatch, attempt_id=int(lease["current_attempt_id"]),
                    lease_owner=str(lease["lease_owner"]), lease_generation=int(lease["lease_generation"]),
                    output={"window_id": window_id, "turn_type": task_turn_type,
                            "events": len(validated["events"]), "tokens": usage,
                            "deterministic_offset_repairs": repair_count,
                            "output_sha256": hashlib.sha256(output_path.read_bytes()).hexdigest()},
                )
                completed_rows.append({"window_id": window_id, "tokens": usage,
                                       "wall_seconds": time.monotonic() - started,
                                       "events": len(validated["events"]), "worker": worker_id,
                                       "turn_type": task_turn_type})
                if pipeline_mode and task_turn_type in {"A", "B"}:
                    successor = "B" if task_turn_type == "A" else "C"
                    enqueue_task(
                        dispatch,
                        task_key=f"{task_namespace}:{successor}:{window_id}",
                        task_type="gold_window",
                        payload={
                            "window_id": window_id,
                            "turn_type": successor,
                            "text_sha256": hashlib.sha256(
                                packet["input"]["window_text"].encode()
                            ).hexdigest(),
                        },
                    )

            lock = asyncio.Lock()

            async def worker(worker_id: int) -> None:
                async with CodexAppServerClient(
                    command=[binary, "app-server", "--stdio", "--strict-config"],
                    expected_cli_version="0.147.0",
                ) as client:
                    while not stop.is_set():
                        adaptive = admission_limit(budget, lane="gold")
                        if worker_id >= int(adaptive["effective_limit"]):
                            await asyncio.sleep(10)
                            continue
                        async with lock:
                            owner = f"{task_namespace}-gold-{turn_type}-{worker_id}"
                            lease = (
                                acquire_pipeline_lease(
                                    dispatch,
                                    task_namespace=task_namespace,
                                    lease_owner=owner,
                                    lease_seconds=1800,
                                )
                                if pipeline_mode
                                else acquire_lease(
                                    dispatch,
                                    lease_owner=owner,
                                    lease_seconds=1800,
                                    task_key_prefix=f"{task_namespace}:{turn_type}:",
                                )
                            )
                        if lease is None:
                            if pipeline_mode and any(
                                not (result_root / "C" / f"{window_id}.json").exists()
                                for window_id in _phase_required_ids(target_ids, quarantined)
                            ):
                                await asyncio.sleep(1)
                                continue
                            return
                        await process_one(client, lease, worker_id)

            if missing:
                await asyncio.gather(*(worker(i) for i in range(worker_concurrency)))
            if stop.is_set():
                if foreground_stop is not None:
                    _write_split_status(
                        result_root=result_root,
                        split=split,
                        turn_type=turn_type,
                        status="deferred",
                        reason=foreground_stop.reason,
                        retry_after_seconds=foreground_stop.retry_after_seconds,
                        manifest_sha256=str(manifest["manifest_sha256"]),
                    )
                    raise GoldForegroundDeferred(foreground_stop)
                if capacity_stop is not None:
                    _write_split_status(
                        result_root=result_root,
                        split=split,
                        turn_type=turn_type,
                        status="deferred",
                        reason=str(capacity_stop["reason"]),
                        retry_after_seconds=int(capacity_stop["retry_after_seconds"]),
                        manifest_sha256=str(manifest["manifest_sha256"]),
                    )
                    raise GoldCapacityDeferred(capacity_stop)
                raise GoldRunnerError(
                    f"{split} Gold stopped during {turn_type}; checkpoint preserved"
                )
            completed_phase_types = ("A", "B", "C") if pipeline_mode else (turn_type,)
            required_ids = _phase_required_ids(target_ids, quarantined)
            for completed_type in completed_phase_types:
                final_outputs = _load_outputs(result_root, completed_type)
                if any(window_id not in final_outputs for window_id in required_ids):
                    raise GoldRunnerError(
                        f"{split} Gold phase {completed_type} is incomplete"
                    )
                rows = [
                    row for row in completed_rows
                    if row["turn_type"] == completed_type
                ]
                phase_receipts.append({
                    "turn_type": completed_type,
                    "target_windows": len(target_ids),
                    "quarantined_windows": len(quarantined),
                    "contract_retries": len(contract_retries),
                    "worker_concurrency": worker_concurrency,
                    "reused_outputs": len(existing_by_turn[completed_type]),
                    "new_outputs": len(rows),
                    "new_tokens": sum(row["tokens"] for row in rows),
                    "wall_seconds": time.monotonic() - phase_start,
                    "mean_call_wall_seconds": (
                        sum(row["wall_seconds"] for row in rows) / len(rows)
                        if rows else 0.0
                    ),
                })

        receipt = {
            "schema_version": "pif_signal_desk_gold_split_run_v2",
            "created_at": now_iso(),
            "complete": True,
            "manifest_sha256": manifest["manifest_sha256"],
            "split": split,
            "turn_types": list(phase_order),
            "split_windows": len(by_window),
            "audit_windows": len(audit_ids),
            "blind_audit_windows": len(blind_audit_ids),
            "atomicity_review_windows": len(atomicity_review_ids),
            "audit_power_plan": audit_plan,
            "configured_max_concurrency": concurrency,
            "initial_adaptive_concurrency": GOLD_BOUNDS.minimum,
            "lease_seconds": 1800,
            "deadline_seconds": 900,
            "task_namespace": task_namespace,
            "dispatch_database": dispatch_database.name,
            "imported_seed_outputs": imported,
            "quarantined_windows": len(quarantined),
            "quarantined_window_id_sha256": sorted(
                hashlib.sha256(window_id.encode()).hexdigest() for window_id in quarantined
            ),
            "phases": phase_receipts,
            "wall_seconds": time.monotonic() - run_started,
            "contains_a1_or_a2": False,
        }
        receipt["receipt_sha256"] = hashlib.sha256(
            json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        receipt_path = result_root / f"gold-{split}-receipt.json"
        receipt_path.write_text(
            json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        if split in {"validation", "sealed_holdout"}:
            os.chmod(receipt_path, 0o600)
        return receipt
    finally:
        dispatch.close(); budget.close()


async def run_gold_split_phase(
    *,
    manifest_path: Path,
    project_root: Path,
    result_root: Path,
    dispatch_database: Path,
    budget_database: Path,
    grant_path: Path,
    session_root: Path,
    budget_dir: Path,
    split: str,
    turn_type: str,
    task_namespace: str | None = None,
    allow_sealed_holdout: bool = False,
    sealed_output_root: Path | None = None,
    seed_roots: Mapping[str, Path] | None = None,
    concurrency: int = 8,
    binary: str = "codex",
    foreground_admission: Callable[..., Any] = gold_model_admission,
) -> dict[str, Any]:
    """Run one resumable Gold phase for one frozen benchmark split.

    The one-phase entrypoint is what prevents a resumed Dev C call from
    jumping into an audit or an unrelated validation phase.  It retains the
    old runner's leased-task, budget, capacity, adaptive, and foreground
    protections; no caller can use it to initiate A1 or A2.
    """

    if turn_type not in (*GOLD_TURN_TYPES, "PIPELINE"):
        raise GoldResumePlanError(
            "Gold split runner accepts A, B, C, AUDIT, or PIPELINE"
        )
    return await _run_gold_split_phases(
        manifest_path=manifest_path,
        project_root=project_root,
        result_root=result_root,
        dispatch_database=dispatch_database,
        budget_database=budget_database,
        grant_path=grant_path,
        session_root=session_root,
        budget_dir=budget_dir,
        seed_roots=seed_roots or {},
        split=split,
        phase_order=("A", "B", "C") if turn_type == "PIPELINE" else (turn_type,),
        task_namespace=task_namespace or split,
        allow_sealed_holdout=allow_sealed_holdout,
        sealed_output_root=sealed_output_root,
        concurrency=concurrency,
        binary=binary,
        foreground_admission=foreground_admission,
    )


async def run_dev_gold(
    *, manifest_path: Path, project_root: Path, result_root: Path,
    dispatch_database: Path, budget_database: Path, grant_path: Path,
    session_root: Path, budget_dir: Path, seed_roots: Mapping[str, Path],
    concurrency: int = 4, binary: str = "codex",
    foreground_admission: Callable[..., Any] = gold_model_admission,
) -> dict[str, Any]:
    """Backward-compatible full development A/B/C/audit runner.

    New resume work should use :func:`run_gold_split_phase` or
    :func:`run_gold_resume_supervisor`, but existing invocations retain their
    original full-development contract and receipt name.
    """

    receipt = await _run_gold_split_phases(
        manifest_path=manifest_path,
        project_root=project_root,
        result_root=result_root,
        dispatch_database=dispatch_database,
        budget_database=budget_database,
        grant_path=grant_path,
        session_root=session_root,
        budget_dir=budget_dir,
        seed_roots=seed_roots,
        split="development",
        phase_order=GOLD_TURN_TYPES,
        task_namespace="dev",
        concurrency=concurrency,
        binary=binary,
        foreground_admission=foreground_admission,
    )
    legacy = {
        "schema_version": "pif_signal_desk_dev_gold_run_v1",
        "created_at": receipt["created_at"],
        "complete": receipt["complete"],
        "manifest_sha256": receipt["manifest_sha256"],
        "development_windows": receipt["split_windows"],
        "development_audit_windows": receipt["audit_windows"],
        "development_blind_audit_windows": receipt["blind_audit_windows"],
        "development_atomicity_review_windows": receipt["atomicity_review_windows"],
        "development_audit_power_plan": receipt["audit_power_plan"],
        "concurrency": concurrency,
        "imported_seed_outputs": receipt["imported_seed_outputs"],
        "phases": receipt["phases"],
        "wall_seconds": receipt["wall_seconds"],
    }
    legacy["receipt_sha256"] = hashlib.sha256(
        json.dumps(legacy, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    (result_root / "dev-gold-receipt.json").write_text(
        json.dumps(legacy, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return legacy


def _write_resume_supervision_checkpoint(
    *, gold_root: Path, receipt: Mapping[str, Any]
) -> Path:
    """Atomically persist aggregate resume progress after every transition."""

    artifact_dir = gold_root / "artifacts"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    target = artifact_dir / "gold-resume-supervisor.json"
    temporary = target.with_suffix(".json.tmp")
    temporary.write_text(
        json.dumps(dict(receipt), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, target)
    return target


async def run_gold_resume_supervisor(
    *,
    manifest_path: Path,
    project_root: Path,
    gold_root: Path,
    budget_database: Path,
    grant_path: Path,
    session_root: Path,
    budget_dir: Path,
    allow_sealed_holdout: bool,
    seed_roots_by_split: Mapping[str, Mapping[str, Path]] | None = None,
    concurrency: int = 8,
    binary: str = "codex",
    phase_runner: Callable[..., Awaitable[dict[str, Any]]] = run_gold_split_phase,
    foreground_admission: Callable[..., Any] = gold_model_admission,
) -> dict[str, Any]:
    """Resume exactly the authorized Gold phases and nothing downstream.

    This supervisor intentionally has no dependency on the A1 calibration or
    A2 scorer machinery. Gold uses the provider-capacity circuit and weekly
    budget directly; local foreground application state is not a model-pool
    capacity signal.
    """

    if not 2 <= concurrency <= 8:
        raise ValueError("gold concurrency must be 2-8")
    plan = build_gold_resume_plan(
        manifest_path=manifest_path,
        gold_root=gold_root,
        allow_sealed_holdout=allow_sealed_holdout,
    )
    completed: list[dict[str, Any]] = []
    roots = seed_roots_by_split or {}
    for stage in plan["stages"]:
        _write_resume_supervision_checkpoint(
            gold_root=gold_root,
            receipt={
                "schema_version": "pif_signal_desk_gold_resume_supervision_v1",
                "status": "running_stage",
                "manifest_sha256": plan["manifest_sha256"],
                "completed_stages": completed,
                "next_stage": {
                    "split": stage["split"],
                    "turn_type": stage["turn_type"],
                },
                "contains_a1_or_a2": False,
            },
        )
        try:
            receipt = await phase_runner(
                manifest_path=manifest_path,
                project_root=project_root,
                result_root=stage["result_root"],
                dispatch_database=stage["dispatch_database"],
                budget_database=budget_database,
                grant_path=grant_path,
                session_root=session_root,
                budget_dir=budget_dir,
                split=stage["split"],
                turn_type=stage["turn_type"],
                task_namespace=stage["task_namespace"],
                allow_sealed_holdout=bool(stage["allow_sealed_holdout"]),
                sealed_output_root=stage["sealed_output_root"],
                seed_roots=roots.get(str(stage["split"]), {}),
                concurrency=concurrency,
                binary=binary,
                foreground_admission=foreground_admission,
            )
        except GoldCapacityDeferred as exc:
            deferred = {
                "schema_version": "pif_signal_desk_gold_resume_supervision_v1",
                "status": "deferred",
                "manifest_sha256": plan["manifest_sha256"],
                "reason": exc.capacity["reason"],
                "retry_after_seconds": exc.capacity["retry_after_seconds"],
                "completed_stages": completed,
                "next_stage": {
                    "split": stage["split"],
                    "turn_type": stage["turn_type"],
                },
                "provider_calls_started_during_deferred_check": 0,
                "contains_a1_or_a2": False,
                "capacity_state": exc.capacity["capacity_state"],
            }
            _write_resume_supervision_checkpoint(gold_root=gold_root, receipt=deferred)
            return deferred
        if not receipt.get("complete"):
            raise GoldRunnerError("Gold stage returned a non-complete receipt")
        completed.append({
            "split": stage["split"],
            "turn_type": stage["turn_type"],
            "receipt_sha256": receipt.get("receipt_sha256"),
            "target_windows": (receipt.get("phases") or [{}])[-1].get("target_windows"),
            "quarantined_windows": int(receipt.get("quarantined_windows") or 0),
        })
        _write_resume_supervision_checkpoint(
            gold_root=gold_root,
            receipt={
                "schema_version": "pif_signal_desk_gold_resume_supervision_v1",
                "status": "stage_complete",
                "manifest_sha256": plan["manifest_sha256"],
                "completed_stages": completed,
                "last_completed_stage": completed[-1],
                "contains_a1_or_a2": False,
            },
        )
    receipt = {
        "schema_version": "pif_signal_desk_gold_resume_supervision_v1",
        "status": "complete",
        "manifest_sha256": plan["manifest_sha256"],
        "completed_stages": completed,
        "contains_a1_or_a2": False,
        "configured_max_concurrency": concurrency,
        "initial_adaptive_concurrency": GOLD_BOUNDS.minimum,
        "lease_seconds": 1800,
        "deadline_seconds": 900,
    }
    receipt["receipt_sha256"] = hashlib.sha256(
        json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    _write_resume_supervision_checkpoint(gold_root=gold_root, receipt=receipt)
    return receipt
