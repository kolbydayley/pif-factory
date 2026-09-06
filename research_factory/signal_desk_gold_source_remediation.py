"""Content-free source-remediation plan for residual development speakers.

The plan never edits the frozen benchmark and never calls a provider.  It
distinguishes invalid source bindings from valid but identity-poor transcripts,
so unsafe claims are replaced/re-authored rather than recovered by inference.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Mapping

from .signal_desk_gold_repair_v4 import _sha_json


SCHEMA_VERSION = "pif_signal_desk_gold_source_remediation_v1"
XAI_CONTRACT_SHA256 = "92893c6a67d487a57b6fa2d322be377d94f4993f391f4a40920914aaec8a10f3"
XAI_RATE_USD_PER_HOUR = 0.10
SYNTAX_REPLACEMENT_EPISODE_ID = "ep_c8128735533bf43edb41b910"


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def diagnose_source(
    *, show_id: str, source_kind: str | None, source_url: str | None,
    text: str, duration_seconds: int | None,
) -> str:
    words = len(text.split())
    duration = int(duration_seconds or 0)
    wpm = words * 60 / duration if duration else None
    lowered = text[:1000].casefold()
    if wpm is not None and wpm > 300:
        return "episode_transcript_mismatch"
    if "github.com" in str(source_url or "") and ("navigation menu" in lowered or "skip to content" in lowered):
        return "non_transcript_source_document"
    if duration >= 600 and wpm is not None and wpm < 60:
        return "incomplete_transcript_or_page_extract"
    labels = re.findall(r"(?m)^\s*([^\n:]{1,80})\s*:\s*", text)
    canonical = {" ".join(label.casefold().split()) for label in labels}
    if canonical and all(re.fullmatch(r"speaker\s*\d+", label) for label in canonical):
        return "collapsed_generic_speaker_labels"
    if labels:
        return "unlabeled_narration_before_named_turns"
    return "no_explicit_identity_provenance_after_wide_context"


def build_plan(
    *, manifest_path: Path, v6_root: Path, database_path: Path,
    project_root: Path, xai_contract_path: Path,
) -> dict[str, Any]:
    manifest = json.loads(manifest_path.read_text())
    contract = json.loads(xai_contract_path.read_text())
    if contract["asr_contract"]["contract_sha256"] != XAI_CONTRACT_SHA256:
        raise RuntimeError("pinned xAI ASR contract changed")
    run = json.loads((v6_root / "variant-run-receipt.json").read_text())
    if int(run["totals"]["residual"]) != 103:
        raise RuntimeError("V6 residual inventory is no longer the frozen 103-claim input")
    metadata = {str(row["window_id"]): row for row in manifest["windows"]}
    episode_windows: dict[str, list[str]] = defaultdict(list)
    for row in manifest["windows"]:
        if row["split"] == "development":
            episode_windows[str(row["episode_id"])].append(str(row["window_id"]))
    connection = sqlite3.connect(database_path); connection.row_factory = sqlite3.Row
    inventory = []
    invalid_episodes: dict[str, dict[str, Any]] = {}
    asr_episode_ids: set[str] = set()
    predicted = Counter()
    try:
        for residual in run["residual_windows"]:
            window_id = str(residual["window_id"]); row = metadata[window_id]
            path = Path(str(row["transcript_path"])); path = path if path.is_absolute() else project_root / path
            text = path.read_text(encoding="utf-8")
            episode = connection.execute(
                "SELECT id,source_id,duration_seconds,audio_url FROM episodes WHERE id=?",
                (str(row["episode_id"]),),
            ).fetchone()
            transcript = connection.execute(
                "SELECT source_kind,source_url FROM transcripts WHERE id=?",
                (str(row["transcript_id"]),),
            ).fetchone()
            source_kind = str(transcript["source_kind"]) if transcript else (
                "official_external_benchmark_fixture" if str(row["show_id"]).startswith("ood-") or str(row["transcript_id"]).startswith("benchmark_external_") else None
            )
            source_url = str(transcript["source_url"]) if transcript and transcript["source_url"] else None
            diagnosis = diagnose_source(
                show_id=str(row["show_id"]), source_kind=source_kind,
                source_url=source_url, text=text,
                duration_seconds=int(episode["duration_seconds"]) if episode and episode["duration_seconds"] else None,
            )
            if str(row["show_id"]).startswith("ood-"):
                diagnosis = "ood_indeterminable_voice_after_official_wide_context"
            count = int(residual["residual_count"])
            invalid = diagnosis in {
                "episode_transcript_mismatch", "non_transcript_source_document",
                "incomplete_transcript_or_page_extract",
            }
            if invalid:
                predicted["invalidated_legacy_residual_claims"] += count
                eid = str(row["episode_id"])
                invalid_episodes[eid] = {
                    "episode_id": eid, "show_id": str(row["show_id"]),
                    "diagnosis": diagnosis,
                    "all_development_window_ids": sorted(episode_windows[eid]),
                    "all_development_window_ids_sha256": _sha_json(sorted(episode_windows[eid])),
                    "replacement_required": True,
                }
                asr_episode_ids.add(SYNTAX_REPLACEMENT_EPISODE_ID if diagnosis == "episode_transcript_mismatch" else eid)
                evidence_path = "replace_source_and_reauthor_all_episode_windows"
                recoverable = 0
            elif diagnosis in {"collapsed_generic_speaker_labels", "unlabeled_narration_before_named_turns"}:
                predicted["diarization_alignment_recoverable_claims"] += count
                asr_episode_ids.add(str(row["episode_id"])); evidence_path = "pinned_asr_diarization_plus_name_resolution_and_text_alignment"; recoverable = count
            else:
                predicted["preserve_indeterminable_claims"] += count
                evidence_path = "preserve_quarantine_no_safe_identity_evidence"; recoverable = 0
            inventory.append({
                "window_id": window_id, "episode_id": str(row["episode_id"]),
                "show_id": str(row["show_id"]), "transcript_structure": str(row["transcript_structure"]),
                "residual_claim_count": count, "diagnosis": diagnosis,
                "least_risky_evidence_path": evidence_path,
                "predicted_legacy_claims_recoverable": recoverable,
                "transcript_sha256": str(row["transcript_sha256"]),
                "window_text_sha256": str(row["text_sha256"]),
                "source_kind": source_kind,
                "source_url_sha256": _sha(source_url) if source_url else None,
                "audio_url_sha256": _sha(str(episode["audio_url"])) if episode and episode["audio_url"] else None,
            })
        durations = []
        for episode_id in sorted(asr_episode_ids):
            row = connection.execute(
                "SELECT id,source_id,duration_seconds,audio_url FROM episodes WHERE id=?", (episode_id,)
            ).fetchone()
            if not row or not row["audio_url"] or not row["duration_seconds"]:
                raise RuntimeError(f"planned remediation episode lacks audio: {episode_id}")
            durations.append({
                "episode_id": episode_id, "show_id": str(row["source_id"]),
                "duration_seconds": int(row["duration_seconds"]),
                "audio_url_sha256": _sha(str(row["audio_url"])),
            })
    finally:
        connection.close()
    invalid_rows = sorted(invalid_episodes.values(), key=lambda value: value["episode_id"])
    invalid_window_count = sum(len(row["all_development_window_ids"]) for row in invalid_rows)
    hours = sum(row["duration_seconds"] for row in durations) / 3600
    plan = {
        "schema_version": SCHEMA_VERSION,
        "scope": "development_only",
        "sealed_or_validation_items_opened": False,
        "source_v6_run_receipt_sha256": run["receipt_sha256"],
        "residual_claim_count": sum(row["residual_claim_count"] for row in inventory),
        "residual_window_count": len(inventory),
        "inventory": sorted(inventory, key=lambda value: value["window_id"]),
        "inventory_digest": _sha_json(sorted(inventory, key=lambda value: value["window_id"])),
        "invalid_source_episodes": invalid_rows,
        "invalid_source_episode_count": len(invalid_rows),
        "development_windows_requiring_refreeze_and_new_gold": invalid_window_count,
        "safe_local_deterministic_source_upgrade_count": 0,
        "predicted_counts": dict(predicted),
        "paid_network_preflight": {
            "authorized_to_launch": False,
            "provider_calls_started": 0,
            "xai_asr_contract_sha256": XAI_CONTRACT_SHA256,
            "xai_asr_calls": len(durations),
            "xai_asr_episodes": durations,
            "estimated_audio_hours": round(hours, 4),
            "estimated_cost_usd": round(hours * XAI_RATE_USD_PER_HOUR, 4),
            "gpt_5_5_name_resolution_calls_maximum": len(durations),
            "gpt_5_5_model": "gpt-5.5",
            "gpt_5_5_input_cap_per_call": 12000,
            "gold_reauthor_calls_after_refreeze": invalid_window_count * 3,
            "gold_reauthor_model": "gpt-5.6-sol",
            "maximum_concurrency": 2,
        },
        "acceptance": {
            "source_plausibility_passes": True,
            "all_replacement_windows_digest_frozen_before_gold": True,
            "speaker_mapping_requires_diarized_label_plus_explicit_name_evidence": True,
            "no_third_party_as_own": True,
            "evidence_grounding": 1.0,
            "semantic_edits_to_existing_claims": 0,
            "residual_current_valid_claims_maximum": 1,
            "rerun_dev_audit_a1_a2_after_refreeze": True,
        },
        "receipt_contains_transcript_or_claim_text": False,
    }
    plan["plan_sha256"] = _sha_json(plan)
    return plan
