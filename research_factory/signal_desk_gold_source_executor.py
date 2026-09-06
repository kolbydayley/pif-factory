"""Leased xAI acquisition and development-only refreeze for source remediation."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import tempfile
import uuid
from pathlib import Path
from typing import Any, Mapping, Sequence
from urllib.request import Request, urlopen

from .signal_desk_gold_repair_v4 import _sha_json
from .signal_desk_rebuild_asr_sanity import evaluate_episode
from .signal_desk_rebuild_dispatch import (
    acquire_lease, complete_attempt, enqueue_task, fail_attempt_semantically,
    initialize_dispatch_schema, release_attempt_for_retry,
)
from .signal_desk_rebuild_gold import turn_aligned_windows
from .signal_desk_rebuild_xai_asr import (
    XAI_STT_RATE_USD_PER_HOUR, _transcribe, asr_contract,
)
from .util import now_iso, write_text_atomic


TASK_PREFIX = "gold-source-remediation-asr-v1:"
LEASE_SECONDS = 1800
MAX_CONCURRENCY = 2
EXPECTED_EPISODES = 5
INVALID_REPLACEMENTS = {
    "ep_b040f341e9545494a92d5f1b": "ep_c8128735533bf43edb41b910",
    "ep_0d0610e66a47dfe39e6db916": "ep_0d0610e66a47dfe39e6db916",
    "ep_6233461f1464120dc8e6e5d0": "ep_6233461f1464120dc8e6e5d0",
}
EXPECTED_NAME_TERMS = {
    "ep_c8128735533bf43edb41b910": ("Wes Bos", "Scott Tolinski"),
    "ep_0d0610e66a47dfe39e6db916": ("Dan",),
    "ep_6233461f1464120dc8e6e5d0": ("Marc Andreessen", "Ben Horowitz", "Erik Torenberg"),
    "ep_d0109952dfdda761558801bf": ("Lisa Mae Brunson", "Meghan McCarty Carino"),
    "ep_e82b7d4e566373e4494ff635": ("Joe Weisenthal", "Tracy Alloway", "Tom Keene"),
}


class SourceExecutionError(RuntimeError):
    pass


def load_api_key() -> str:
    value = os.environ.get("XAI_API_KEY", "").strip()
    if not value and os.environ.get("XAI_API_KEY_FILE"):
        path = Path(os.environ["XAI_API_KEY_FILE"]).expanduser()
        value = path.read_text(encoding="utf-8").strip()
    if not value:
        raise SourceExecutionError("XAI_API_KEY or XAI_API_KEY_FILE is required")
    return value


def _sha_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _episode_rows(database_path: Path, plan: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    connection = sqlite3.connect(database_path); connection.row_factory = sqlite3.Row
    result = {}
    try:
        for planned in plan["paid_network_preflight"]["xai_asr_episodes"]:
            row = connection.execute(
                "SELECT id,source_id,title,published_at,duration_seconds,audio_url FROM episodes WHERE id=?",
                (planned["episode_id"],),
            ).fetchone()
            if not row or not row["audio_url"]:
                raise SourceExecutionError("planned episode/audio disappeared")
            item = dict(row)
            item["episode_id"] = str(row["id"])
            if hashlib.sha256(str(item["audio_url"]).encode()).hexdigest() != planned["audio_url_sha256"]:
                raise SourceExecutionError("audio URL changed after preflight")
            result[str(row["id"])] = item
    finally:
        connection.close()
    if len(result) != EXPECTED_EPISODES:
        raise SourceExecutionError("remediation lane must remain exactly five episodes")
    return result


def prepare_dispatch(*, dispatch_path: Path, plan: Mapping[str, Any]) -> dict[str, int]:
    connection = sqlite3.connect(dispatch_path); connection.row_factory = sqlite3.Row
    counts: dict[str, int] = {}
    try:
        initialize_dispatch_schema(connection)
        for row in plan["paid_network_preflight"]["xai_asr_episodes"]:
            outcome = enqueue_task(
                connection, task_key=TASK_PREFIX + str(row["episode_id"]), task_type="xai_asr_source_remediation",
                payload={"episode_id": row["episode_id"], "audio_url_sha256": row["audio_url_sha256"],
                         "asr_contract_sha256": plan["paid_network_preflight"]["xai_asr_contract_sha256"]},
            )["enqueue_outcome"]
            counts[outcome] = counts.get(outcome, 0) + 1
    finally:
        connection.close()
    return counts


def download_audio(row: Mapping[str, Any], path: Path) -> dict[str, Any]:
    if path.exists():
        data = path.read_bytes()
        return {"audio_sha256": _sha_bytes(data), "audio_bytes": len(data), "cache_hit": True}
    request = Request(str(row["audio_url"]), headers={"User-Agent": "pif-signal-desk-remediation/1"})
    with urlopen(request, timeout=300) as response:  # noqa: S310 - catalog-pinned podcast audio
        content_type = str(response.headers.get("Content-Type") or "").casefold()
        data = response.read(1_000_000_001)
    if len(data) > 1_000_000_000 or len(data) < max(10_000, int(row["duration_seconds"] or 0) * 500):
        raise SourceExecutionError("downloaded audio size is implausible")
    if "html" in content_type or data[:256].lstrip().lower().startswith((b"<!doctype", b"<html")):
        raise SourceExecutionError("audio URL resolved to HTML")
    path.parent.mkdir(parents=True, exist_ok=True); path.parent.chmod(0o700)
    fd, temporary = tempfile.mkstemp(dir=path.parent, prefix=path.name + ".", suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as handle: handle.write(data)
        os.chmod(temporary, 0o600); os.replace(temporary, path)
    finally:
        if os.path.exists(temporary): os.unlink(temporary)
    return {"audio_sha256": _sha_bytes(data), "audio_bytes": len(data), "cache_hit": False}


def _name_sanity(episode_id: str, text: str) -> dict[str, Any]:
    terms = EXPECTED_NAME_TERMS[episode_id]
    normalized = " ".join(text.casefold().split())
    matched = [term for term in terms if " ".join(term.casefold().split()) in normalized]
    return {
        "expected_count": len(terms), "matched_count": len(matched),
        "expected_terms_sha256": _sha_json(list(terms)),
        "matched_terms_sha256": _sha_json(matched),
        # Missing names are reported for later name resolution, not grounds to
        # alter ASR text or infer identity.
        "passed_for_source_readiness": True,
    }


def persist_result(
    *, row: Mapping[str, Any], result: Mapping[str, Any], audio: Mapping[str, Any], output_root: Path,
) -> dict[str, Any]:
    episode_id = str(row["id"]); root = output_root / str(row["source_id"]); root.mkdir(parents=True, exist_ok=True); root.chmod(0o700)
    text_path=root/f"{episode_id}.txt";raw_path=root/f"{episode_id}.xai-stt.json";meta_path=root/f"{episode_id}.meta.json";sidecar_path=root/f"{episode_id}.sidecar.json"
    text_value=str(result["text"]).strip()+"\n"
    if text_path.exists() or raw_path.exists() or meta_path.exists() or sidecar_path.exists():
        raise SourceExecutionError("partial or immutable output already exists")
    write_text_atomic(text_path,text_value);write_text_atomic(raw_path,json.dumps(result["response"],sort_keys=True)+"\n")
    for path in (text_path,raw_path):path.chmod(0o600)
    selected={"id":episode_id,"source_id":row["source_id"],"path":str(text_path.resolve()),"response_path":str(raw_path.resolve()),"sha256":_sha_bytes(text_value.encode())}
    sanity=evaluate_episode(selected)
    name_sanity=_name_sanity(episode_id,text_value)
    metadata={"schema_version":"pif_signal_desk_source_remediation_episode_v1","episode_id":episode_id,"source_id":row["source_id"],"asr_contract_sha256":asr_contract()["contract_sha256"],"audio_url_sha256":hashlib.sha256(str(row["audio_url"]).encode()).hexdigest(),"audio_sha256":audio["audio_sha256"],"audio_bytes":audio["audio_bytes"],"text_sha256":selected["sha256"],"response_sha256":_sha_bytes(raw_path.read_bytes()),"observed_duration_seconds":sanity["duration_seconds"],"estimated_cost_usd":round(sanity["duration_seconds"]/3600*XAI_STT_RATE_USD_PER_HOUR,4),"sanity":sanity,"name_sanity":name_sanity,"ready_for_refreeze":bool(sanity["passed"]),"created_at":now_iso(),"private_analysis_only":True}
    write_text_atomic(meta_path,json.dumps(metadata,indent=2,sort_keys=True)+"\n");meta_path.chmod(0o600)
    sidecar={k:metadata[k] for k in ("schema_version","episode_id","source_id","asr_contract_sha256","audio_url_sha256","audio_sha256","audio_bytes","text_sha256","response_sha256","observed_duration_seconds","estimated_cost_usd","sanity","name_sanity","ready_for_refreeze","created_at")};sidecar["contains_transcript_or_audio_content"]=False;sidecar["sidecar_sha256"]=_sha_json(sidecar)
    write_text_atomic(sidecar_path,json.dumps(sidecar,indent=2,sort_keys=True)+"\n");sidecar_path.chmod(0o600)
    if not sanity["passed"]: raise SourceExecutionError("ASR WPM/gap/diarization sanity failed")
    return sidecar


def execute(*, plan: Mapping[str, Any], database_path: Path, dispatch_path: Path, output_root: Path, concurrency: int) -> dict[str, Any]:
    if not 1 <= concurrency <= MAX_CONCURRENCY: raise SourceExecutionError("concurrency must be 1 or 2")
    if plan["paid_network_preflight"]["xai_asr_contract_sha256"] != asr_contract()["contract_sha256"]: raise SourceExecutionError("ASR contract drift")
    rows=_episode_rows(database_path,plan);prepare_dispatch(dispatch_path=dispatch_path,plan=plan);api_key=load_api_key()
    # Deliberately simple bounded workers; dispatch leases make restarts safe.
    import concurrent.futures
    def worker(index:int)->None:
        connection=sqlite3.connect(dispatch_path);connection.row_factory=sqlite3.Row;owner=f"source-remediation-{index}-{uuid.uuid4().hex[:8]}"
        try:
            while True:
                lease=acquire_lease(connection,lease_owner=owner,lease_seconds=LEASE_SECONDS,task_key_prefix=TASK_PREFIX)
                if lease is None:return
                episode_id=str(lease["payload"]["episode_id"]);row=rows[episode_id];root=output_root/str(row["source_id"]);sidecar=root/f"{episode_id}.sidecar.json"
                try:
                    if sidecar.exists():
                        saved=json.loads(sidecar.read_text())
                        if not saved.get("ready_for_refreeze"):raise SourceExecutionError("immutable ASR artifact failed sanity")
                        complete_attempt(connection,attempt_id=lease["current_attempt_id"],lease_owner=owner,lease_generation=lease["lease_generation"],output={"episode_id":episode_id,"sidecar_sha256":saved["sidecar_sha256"],"recovered_from_immutable_artifact":True});continue
                    audio=download_audio(row,output_root/'audio'/f"{episode_id}.audio")
                    result=_transcribe(row,api_key=api_key,attempts=1)
                    if result.get("validation_error"):raise SourceExecutionError(str(result["validation_error"]))
                    receipt=persist_result(row=row,result=result,audio=audio,output_root=output_root)
                    complete_attempt(connection,attempt_id=lease["current_attempt_id"],lease_owner=owner,lease_generation=lease["lease_generation"],output={"episode_id":episode_id,"sidecar_sha256":receipt["sidecar_sha256"]})
                except SourceExecutionError as exc:
                    fail_attempt_semantically(connection,attempt_id=lease["current_attempt_id"],lease_owner=owner,lease_generation=lease["lease_generation"],failure_code="source_or_sanity_failure",failure_detail=str(exc)[:250])
                except Exception as exc:
                    release_attempt_for_retry(connection,attempt_id=lease["current_attempt_id"],lease_owner=owner,lease_generation=lease["lease_generation"],failure_code="transport_failure",failure_detail=f"{type(exc).__name__}: {str(exc)[:200]}")
                    return
        finally:connection.close()
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:list(pool.map(worker,range(concurrency)))
    connection=sqlite3.connect(dispatch_path);connection.row_factory=sqlite3.Row
    states=dict(connection.execute("select status,count(*) from signal_desk_rebuild_tasks group by status").fetchall());connection.close()
    sidecars=[json.loads(path.read_text()) for path in output_root.glob("*/*.sidecar.json")]
    ready=sum(bool(row.get("ready_for_refreeze")) for row in sidecars)
    receipt={"schema_version":"pif_signal_desk_source_remediation_run_v1","task_states":states,"episode_count":len(sidecars),"ready_episode_count":ready,"complete":len(sidecars)==EXPECTED_EPISODES and ready==EXPECTED_EPISODES and states.get("succeeded",0)==EXPECTED_EPISODES,"total_estimated_cost_usd":round(sum(float(row["estimated_cost_usd"]) for row in sidecars),4),"sidecars_digest":_sha_json(sorted((row["episode_id"],row["sidecar_sha256"]) for row in sidecars)),"provider_calls_started":True,"receipt_contains_transcript_audio_or_credentials":False}
    receipt["receipt_sha256"]=_sha_json(receipt);return receipt


def build_refreeze(*, base_manifest_path: Path, source_plan: Mapping[str, Any], run_receipt: Mapping[str, Any], database_path: Path, output_root: Path, project_root: Path) -> dict[str, Any]:
    if not run_receipt.get("complete"):raise SourceExecutionError("all five source episodes must pass before refreeze")
    manifest=json.loads(base_manifest_path.read_text());removed=[];replacements=[]
    connection=sqlite3.connect(database_path);connection.row_factory=sqlite3.Row
    try:
        for old_episode,new_episode in INVALID_REPLACEMENTS.items():
            old_rows=[row for row in manifest["windows"] if row["split"]=="development" and row["episode_id"]==old_episode]
            if len(old_rows)!=3:raise SourceExecutionError("invalid episode does not own exactly three development windows")
            removed.extend(str(row["window_id"]) for row in old_rows)
            episode=connection.execute("select id,source_id,title from episodes where id=?",(new_episode,)).fetchone();text_path=next(output_root.glob(f"*/{new_episode}.txt"));text=text_path.read_text();transcript_sha=_sha_bytes(text.encode())
            for index,window in enumerate(turn_aligned_windows(text,count=3)):
                identity=f"in_domain|{episode['source_id']}|{new_episode}|{index}|{window['text_sha256']}"
                replacements.append({"window_id":"sdw_"+hashlib.sha256(identity.encode()).hexdigest()[:20],"corpus":"in_domain","split":"development","show_id":episode["source_id"],"show_name":old_rows[0]["show_name"],"canonical_feed":old_rows[0]["canonical_feed"],"source_shape":old_rows[0].get("source_shape"),"episode_id":new_episode,"episode_title":episode["title"],"transcript_id":"remediation_xai_"+new_episode.removeprefix("ep_"),"transcript_path":str(text_path.relative_to(project_root)),"transcript_sha256":transcript_sha,"transcript_revision":{"source":"xai_asr_source_remediation","asr_contract_sha256":asr_contract()["contract_sha256"]},"window_index":index,**window,"transcript_structure":"asr_diarized"})
    finally:connection.close()
    protected=[row for row in manifest["windows"] if row["split"] in {"validation","sealed_holdout"}]
    receipt={"schema_version":"pif_signal_desk_development_source_refreeze_v1","base_manifest_sha256":manifest["manifest_sha256"],"source_plan_sha256":source_plan["plan_sha256"],"source_run_receipt_sha256":run_receipt["receipt_sha256"],"removed_development_window_ids":sorted(removed),"removed_development_window_ids_sha256":_sha_json(sorted(removed)),"replacement_windows":sorted(replacements,key=lambda row:row["window_id"]),"replacement_windows_digest":_sha_json(sorted(replacements,key=lambda row:row["window_id"])),"replacement_window_count":len(replacements),"validation_and_holdout_window_count":len(protected),"validation_and_holdout_metadata_digest":_sha_json(protected),"base_manifest_mutated":False,"validation_or_holdout_content_opened":False,"ready_for_development_gold_authoring":len(replacements)==9,"provider_calls_started_for_gold_authoring":0,"receipt_contains_transcript_text":False}
    receipt["receipt_sha256"]=_sha_json(receipt);return receipt
