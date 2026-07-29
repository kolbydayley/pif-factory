from __future__ import annotations

import json
import os
import urllib.parse
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from .mcp_broker import (
    ALL_SCOPES,
    DEFAULT_OBSERVER_URL,
    DEFAULT_SCOPES,
    FUTURE_SCOPES,
    BrokerStore,
    broker_store_from_env,
    make_access_token,
    queue_summary_from_snapshot,
    status_from_snapshot,
    verify_access_token,
    verify_pkce,
    worker_policy,
)


SERVER_NAME = "podcast-intelligence-factory"
MCP_PROTOCOL_VERSION = "2025-06-18"
BASE_URL = os.environ.get("PIF_MCP_PUBLIC_URL", "").rstrip("/")
AUTH_ISSUER = os.environ.get("PIF_MCP_AUTH_ISSUER", BASE_URL).rstrip("/")
AUTH_SECRET = os.environ.get("PIF_MCP_AUTH_SECRET", os.environ.get("PIF_MCP_BROKER_TOKEN", "dev-pif-mcp-secret"))
INGEST_TOKEN = os.environ.get("PIF_MCP_INGEST_TOKEN", os.environ.get("RAILWAY_UI_INGEST_TOKEN", ""))
REQUIRE_AUTH = os.environ.get("PIF_MCP_REQUIRE_AUTH", "1") != "0"
MAX_BODY_BYTES = int(os.environ.get("PIF_MCP_MAX_BODY_BYTES", "1000000"))
PHASE2_ENABLED = os.environ.get("PIF_MCP_PHASE2_ENABLED", "0") == "1"
PHASE2_SMOKE_ALLOW_CONTROL_SCOPE = os.environ.get("PIF_MCP_PHASE2_SMOKE_ALLOW_CONTROL_SCOPE", "0") == "1"
PHASE2_SMOKE_ALLOW_STATUS_SCOPE = os.environ.get("PIF_MCP_PHASE2_SMOKE_ALLOW_STATUS_SCOPE", "0") == "1"

GENERIC_OBJECT_SCHEMA = {"type": "object", "additionalProperties": True}
OK_SCHEMA = {"type": "object", "properties": {"ok": {"type": "boolean"}}, "additionalProperties": True}

TOOL_SCOPES_BY_NAME = {
    "get_status": ["factory.status"],
    "get_queue_summary": ["factory.status"],
    "get_worker_policy": ["factory.status"],
    "get_observer_url": ["factory.status"],
    "request_local_cycle": ["factory.control"],
    "claim_work": ["factory.claim"],
    "reserve_evidence_task": ["factory.claim"],
    "reserve_source_card_task": ["factory.claim"],
    "reserve_source_card_batch": ["factory.claim"],
    "get_work_context": ["factory.claim"],
    "get_work_chunk": ["factory.claim"],
    "get_evidence_manifest": ["factory.claim"],
    "get_evidence_packet": ["factory.claim"],
    "get_source_card": ["factory.claim"],
    "get_source_card_batch": ["factory.claim"],
    "submit_work_notes": ["factory.claim"],
    "get_work_notes": ["factory.claim"],
    "heartbeat_work": ["factory.claim"],
    "release_work": ["factory.claim"],
    "skip_source_card_task": ["factory.claim"],
    "submit_work_output": ["factory.submit"],
    "submit_work_outputs": ["factory.submit"],
}


PHASE1_TOOLS = [
    {
        "name": "get_status",
        "description": "Return sanitized Podcast Intelligence Factory MCP broker and observer status.",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
        "outputSchema": GENERIC_OBJECT_SCHEMA,
        "annotations": {"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": False},
    },
    {
        "name": "get_queue_summary",
        "description": "Return sanitized queue depth, worker state, and remote-claimable counts.",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
        "outputSchema": GENERIC_OBJECT_SCHEMA,
        "annotations": {"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": False},
    },
    {
        "name": "get_worker_policy",
        "description": "Return the local-first worker, Railway, privacy, and phase-gate policy.",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
        "outputSchema": GENERIC_OBJECT_SCHEMA,
        "annotations": {"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": False},
    },
    {
        "name": "request_local_cycle",
        "description": "Record a request for a local Codex-controlled cycle. This does not run compute on Railway.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "cycle_type": {"type": "string", "enum": ["observer_publish", "bridge_sync", "bounded_extraction", "reviewer_audit"]},
                "reason": {"type": "string"},
            },
            "required": ["cycle_type"],
            "additionalProperties": False,
        },
        "outputSchema": OK_SCHEMA,
        "annotations": {"readOnlyHint": False, "destructiveHint": False, "idempotentHint": False, "openWorldHint": False},
    },
    {
        "name": "get_observer_url",
        "description": "Return the public sanitized observer URL.",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
        "outputSchema": OK_SCHEMA,
        "annotations": {"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": False},
    },
]

PHASE2_TOOLS = [
    {
        "name": "claim_work",
        "description": "Claim one Phase 2 remote extraction package that was explicitly marked full_text_allowed by the local bridge.",
        "inputSchema": {
            "type": "object",
            "properties": {"worker_id": {"type": "string"}},
            "additionalProperties": False,
        },
        "outputSchema": GENERIC_OBJECT_SCHEMA,
        "annotations": {"readOnlyHint": False, "destructiveHint": False, "idempotentHint": False, "openWorldHint": False},
    },
    {
        "name": "reserve_evidence_task",
        "description": "Reserve one pending derived-evidence analysis task. Returns only task metadata and no source text.",
        "inputSchema": {
            "type": "object",
            "properties": {"worker_id": {"type": "string"}},
            "additionalProperties": False,
        },
        "outputSchema": GENERIC_OBJECT_SCHEMA,
        "annotations": {"readOnlyHint": True, "destructiveHint": False, "idempotentHint": False, "openWorldHint": False},
    },
    {
        "name": "reserve_source_card_task",
        "description": "Reserve one pending bounded source-card extraction task. Returns task metadata and no full transcript chunks.",
        "inputSchema": {
            "type": "object",
            "properties": {"worker_id": {"type": "string"}},
            "additionalProperties": False,
        },
        "outputSchema": GENERIC_OBJECT_SCHEMA,
        "annotations": {"readOnlyHint": True, "destructiveHint": False, "idempotentHint": False, "openWorldHint": False},
    },
    {
        "name": "reserve_source_card_batch",
        "description": "Reserve up to three pending bounded source-card extraction tasks in one turn. Returns task metadata and no full transcript chunks.",
        "inputSchema": {
            "type": "object",
            "properties": {"worker_id": {"type": "string"}, "max_tasks": {"type": "integer", "minimum": 1, "maximum": 3}},
            "additionalProperties": False,
        },
        "outputSchema": GENERIC_OBJECT_SCHEMA,
        "annotations": {"readOnlyHint": True, "destructiveHint": False, "idempotentHint": False, "openWorldHint": False},
    },
    {
        "name": "get_work_context",
        "description": "Return a compact manifest, instructions, output schema, and first chunk id for a claimed full_text_allowed remote extraction package. This does not return transcript text.",
        "inputSchema": {
            "type": "object",
            "properties": {"work_id": {"type": "string"}, "worker_id": {"type": "string"}},
            "required": ["work_id"],
            "additionalProperties": False,
        },
        "outputSchema": GENERIC_OBJECT_SCHEMA,
        "annotations": {"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": False},
    },
    {
        "name": "get_work_chunk",
        "description": "Return one bounded remote-safe evidence packet for a claimed full_text_allowed remote extraction package. The default ChatGPT path does not return raw transcript text.",
        "inputSchema": {
            "type": "object",
            "properties": {"work_id": {"type": "string"}, "worker_id": {"type": "string"}, "chunk_id": {"type": "string"}},
            "required": ["work_id", "chunk_id"],
            "additionalProperties": False,
        },
        "outputSchema": GENERIC_OBJECT_SCHEMA,
        "annotations": {"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": False},
    },
    {
        "name": "get_evidence_manifest",
        "description": "Return a compact manifest for derived evidence packets. This tool does not return source text.",
        "inputSchema": {
            "type": "object",
            "properties": {"work_id": {"type": "string"}, "worker_id": {"type": "string"}},
            "required": ["work_id"],
            "additionalProperties": False,
        },
        "outputSchema": GENERIC_OBJECT_SCHEMA,
        "annotations": {"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": False},
    },
    {
        "name": "get_evidence_packet",
        "description": "Return one derived evidence packet for analysis. This tool does not return source text.",
        "inputSchema": {
            "type": "object",
            "properties": {"work_id": {"type": "string"}, "worker_id": {"type": "string"}, "chunk_id": {"type": "string"}},
            "required": ["work_id", "chunk_id"],
            "additionalProperties": False,
        },
        "outputSchema": GENERIC_OBJECT_SCHEMA,
        "annotations": {"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": False},
    },
    {
        "name": "get_source_card",
        "description": "Return one bounded transcript-derived source card. This tool never returns full transcript chunks.",
        "inputSchema": {
            "type": "object",
            "properties": {"work_id": {"type": "string"}, "worker_id": {"type": "string"}, "card_id": {"type": "string"}},
            "required": ["work_id", "card_id"],
            "additionalProperties": False,
        },
        "outputSchema": GENERIC_OBJECT_SCHEMA,
        "annotations": {"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": False},
    },
    {
        "name": "get_source_card_batch",
        "description": "Return up to three bounded transcript-derived source cards in one call. This tool never returns full transcript chunks.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "worker_id": {"type": "string"},
                "items": {
                    "type": "array",
                    "minItems": 1,
                    "maxItems": 3,
                    "items": {
                        "type": "object",
                        "properties": {"work_id": {"type": "string"}, "card_id": {"type": "string"}},
                        "required": ["work_id", "card_id"],
                        "additionalProperties": False,
                    },
                },
            },
            "required": ["items"],
            "additionalProperties": False,
        },
        "outputSchema": GENERIC_OBJECT_SCHEMA,
        "annotations": {"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": False},
    },
    {
        "name": "submit_work_notes",
        "description": "Submit compact per-chunk notes for a claimed remote package. Notes are intermediate and do not mutate canonical SQLite.",
        "inputSchema": {
            "type": "object",
            "properties": {"work_id": {"type": "string"}, "worker_id": {"type": "string"}, "chunk_id": {"type": "string"}, "notes": {"type": "object"}},
            "required": ["work_id", "chunk_id", "notes"],
            "additionalProperties": False,
        },
        "outputSchema": OK_SCHEMA,
        "annotations": {"readOnlyHint": False, "destructiveHint": False, "idempotentHint": True, "openWorldHint": False},
    },
    {
        "name": "get_work_notes",
        "description": "Return compact notes accumulated for a claimed remote package, without transcript text.",
        "inputSchema": {
            "type": "object",
            "properties": {"work_id": {"type": "string"}, "worker_id": {"type": "string"}},
            "required": ["work_id"],
            "additionalProperties": False,
        },
        "outputSchema": GENERIC_OBJECT_SCHEMA,
        "annotations": {"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": False},
    },
    {
        "name": "submit_work_output",
        "description": "Submit structured JSON output for a claimed remote package. Canonical SQLite mutation is performed only by the local importer after validation.",
        "inputSchema": {
            "type": "object",
            "properties": {"work_id": {"type": "string"}, "worker_id": {"type": "string"}, "output": {"type": "object"}},
            "required": ["work_id", "output"],
            "additionalProperties": False,
        },
        "outputSchema": OK_SCHEMA,
        "annotations": {"readOnlyHint": False, "destructiveHint": False, "idempotentHint": True, "openWorldHint": False},
    },
    {
        "name": "submit_work_outputs",
        "description": "Submit up to three structured JSON outputs for claimed remote packages. Canonical SQLite mutation is performed only by the local importer after validation.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "worker_id": {"type": "string"},
                "outputs": {
                    "type": "array",
                    "minItems": 1,
                    "maxItems": 3,
                    "items": {
                        "type": "object",
                        "properties": {"work_id": {"type": "string"}, "output": {"type": "object"}},
                        "required": ["work_id", "output"],
                        "additionalProperties": False,
                    },
                },
            },
            "required": ["outputs"],
            "additionalProperties": False,
        },
        "outputSchema": OK_SCHEMA,
        "annotations": {"readOnlyHint": False, "destructiveHint": False, "idempotentHint": True, "openWorldHint": False},
    },
    {
        "name": "skip_source_card_task",
        "description": "Mark one claimed source-card task as skipped after a blocked or invalid attempt. This records sanitized audit only and does not mutate canonical SQLite.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "work_id": {"type": "string"},
                "worker_id": {"type": "string"},
                "card_id": {"type": "string"},
                "stage": {"type": "string"},
                "reason_code": {
                    "type": "string",
                    "enum": ["tool_safety_block", "validation_failed", "insufficient_features", "worker_timeout", "other"],
                },
            },
            "required": ["work_id", "stage", "reason_code"],
            "additionalProperties": False,
        },
        "outputSchema": OK_SCHEMA,
        "annotations": {"readOnlyHint": False, "destructiveHint": False, "idempotentHint": True, "openWorldHint": False},
    },
    {
        "name": "heartbeat_work",
        "description": "Extend the lease for a claimed remote extraction package.",
        "inputSchema": {
            "type": "object",
            "properties": {"work_id": {"type": "string"}, "worker_id": {"type": "string"}},
            "required": ["work_id"],
            "additionalProperties": False,
        },
        "outputSchema": OK_SCHEMA,
        "annotations": {"readOnlyHint": False, "destructiveHint": False, "idempotentHint": False, "openWorldHint": False},
    },
    {
        "name": "release_work",
        "description": "Release a claimed remote extraction package without submitting output.",
        "inputSchema": {
            "type": "object",
            "properties": {"work_id": {"type": "string"}, "worker_id": {"type": "string"}, "reason": {"type": "string"}},
            "required": ["work_id"],
            "additionalProperties": False,
        },
        "outputSchema": OK_SCHEMA,
        "annotations": {"readOnlyHint": False, "destructiveHint": False, "idempotentHint": False, "openWorldHint": False},
    },
]


class Handler(BaseHTTPRequestHandler):
    store: BrokerStore

    def do_GET(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/healthz":
            self._json(
                HTTPStatus.OK,
                {
                    "ok": True,
                    "service": SERVER_NAME,
                    "mcp_phase": "remote_extraction_bridge" if PHASE2_ENABLED else "control_status_only",
                    "broker_storage": self.store.storage_info(),
                },
            )
        elif parsed.path == "/readyz":
            self._json(HTTPStatus.OK, {"ok": True})
        elif parsed.path == "/.well-known/oauth-protected-resource":
            base = _base_url(self)
            self._json(
                HTTPStatus.OK,
                {
                    "resource": f"{base}/mcp",
                    "authorization_servers": [AUTH_ISSUER or base],
                    "scopes_supported": sorted(ALL_SCOPES),
                    "bearer_methods_supported": ["header"],
                },
            )
        elif parsed.path == "/.well-known/oauth-authorization-server":
            base = _base_url(self)
            issuer = AUTH_ISSUER or base
            self._json(
                HTTPStatus.OK,
                {
                    "issuer": issuer,
                    "authorization_endpoint": f"{base}/oauth/authorize",
                    "token_endpoint": f"{base}/oauth/token",
                    "registration_endpoint": f"{base}/oauth/register",
                    "response_types_supported": ["code"],
                    "grant_types_supported": ["authorization_code", "refresh_token"],
                    "code_challenge_methods_supported": ["S256", "plain"],
                    "scopes_supported": sorted(ALL_SCOPES),
                    "token_endpoint_auth_methods_supported": ["none"],
                },
            )
        elif parsed.path == "/oauth/authorize":
            self._authorize(parsed)
        else:
            self._json(HTTPStatus.NOT_FOUND, {"ok": False, "error": "not_found"})

    def do_POST(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/oauth/register":
            metadata = self._read_json()
            self._json(HTTPStatus.CREATED, self.store.register_client(metadata))
        elif parsed.path == "/oauth/token":
            self._token()
        elif parsed.path == "/ingest-snapshot":
            self._ingest_snapshot()
        elif parsed.path == "/ingest-smoke-work":
            self._ingest_smoke_work()
        elif parsed.path == "/ingest-work-packages":
            self._ingest_work_packages()
        elif parsed.path == "/export-submissions":
            self._export_submissions()
        elif parsed.path == "/export-skips":
            self._export_skips()
        elif parsed.path == "/mark-submission-imported":
            self._mark_submission_imported()
        elif parsed.path == "/audit-events":
            self._audit_events()
        elif parsed.path == "/mcp":
            self._mcp()
        else:
            self._json(HTTPStatus.NOT_FOUND, {"ok": False, "error": "not_found"})

    def _authorize(self, parsed) -> None:
        query = urllib.parse.parse_qs(parsed.query)
        redirect_uri = _one(query, "redirect_uri")
        client_id = _one(query, "client_id")
        state = _one(query, "state")
        scope = _normalize_scope(_one(query, "scope") or " ".join(sorted(DEFAULT_SCOPES)))
        if not redirect_uri or not client_id:
            self._json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "missing redirect_uri or client_id"})
            return
        code = self.store.create_oauth_code(
            client_id=client_id,
            redirect_uri=redirect_uri,
            scope=scope,
            code_challenge=_one(query, "code_challenge"),
            code_challenge_method=_one(query, "code_challenge_method"),
        )
        location = f"{redirect_uri}?{urllib.parse.urlencode({'code': code, 'state': state or ''})}"
        self.send_response(HTTPStatus.FOUND)
        self.send_header("Location", location)
        self.send_header("Content-Length", "0")
        self.send_header("Connection", "close")
        self.end_headers()

    def _token(self) -> None:
        params = self._read_form()
        if params.get("grant_type") == "refresh_token":
            self._refresh_token(params)
            return
        if params.get("grant_type") != "authorization_code":
            self._json(HTTPStatus.BAD_REQUEST, {"error": "unsupported_grant_type"})
            return
        row = self.store.pop_oauth_code(params.get("code") or "")
        if not row:
            self._json(HTTPStatus.BAD_REQUEST, {"error": "invalid_grant"})
            return
        if params.get("redirect_uri") != row["redirect_uri"]:
            self._json(HTTPStatus.BAD_REQUEST, {"error": "invalid_grant", "error_description": "redirect_uri mismatch"})
            return
        if not verify_pkce(
            verifier=params.get("code_verifier"),
            challenge=row["code_challenge"],
            method=row["code_challenge_method"],
        ):
            self._json(HTTPStatus.BAD_REQUEST, {"error": "invalid_grant", "error_description": "pkce verification failed"})
            return
        base = _base_url(self)
        token = make_access_token(
            secret=AUTH_SECRET,
            issuer=AUTH_ISSUER or base,
            audience=f"{base}/mcp",
            subject=row["client_id"],
            scope=row["scope"],
        )
        refresh_token = self.store.create_refresh_token(client_id=row["client_id"], scope=row["scope"])
        self._json(
            HTTPStatus.OK,
            {
                "access_token": token,
                "refresh_token": refresh_token,
                "token_type": "Bearer",
                "expires_in": 3600,
                "scope": row["scope"],
            },
        )

    def _refresh_token(self, params: dict[str, str]) -> None:
        refreshed = self.store.rotate_refresh_token(params.get("refresh_token") or "")
        if not refreshed:
            self._json(HTTPStatus.BAD_REQUEST, {"error": "invalid_grant"})
            return
        base = _base_url(self)
        token = make_access_token(
            secret=AUTH_SECRET,
            issuer=AUTH_ISSUER or base,
            audience=f"{base}/mcp",
            subject=refreshed["client_id"],
            scope=refreshed["scope"],
        )
        self._json(
            HTTPStatus.OK,
            {
                "access_token": token,
                "refresh_token": refreshed["refresh_token"],
                "token_type": "Bearer",
                "expires_in": 3600,
                "scope": refreshed["scope"],
            },
        )

    def _ingest_snapshot(self) -> None:
        if not INGEST_TOKEN:
            self._json(HTTPStatus.SERVICE_UNAVAILABLE, {"ok": False, "error": "ingest token not configured"})
            return
        if self.headers.get("Authorization", "") != f"Bearer {INGEST_TOKEN}":
            self._json(HTTPStatus.UNAUTHORIZED, {"ok": False, "error": "unauthorized"})
            return
        payload = self._read_json()
        self._json(HTTPStatus.OK, self.store.write_snapshot(payload))

    def _ingest_smoke_work(self) -> None:
        if not PHASE2_ENABLED:
            self._json(HTTPStatus.FORBIDDEN, {"ok": False, "error": "phase2 smoke disabled"})
            return
        if not INGEST_TOKEN:
            self._json(HTTPStatus.SERVICE_UNAVAILABLE, {"ok": False, "error": "ingest token not configured"})
            return
        if self.headers.get("Authorization", "") != f"Bearer {INGEST_TOKEN}":
            self._json(HTTPStatus.UNAUTHORIZED, {"ok": False, "error": "unauthorized"})
            return
        self._json(HTTPStatus.OK, self.store.seed_smoke_work_package())

    def _ingest_work_packages(self) -> None:
        if not PHASE2_ENABLED:
            self._json(HTTPStatus.FORBIDDEN, {"ok": False, "error": "phase2 disabled"})
            return
        if not self._verify_ingest_token():
            return
        payload = self._read_json()
        packages = payload.get("packages")
        if not isinstance(packages, list):
            self._json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "packages must be an array"})
            return
        self._json(HTTPStatus.OK, self.store.upsert_work_packages(packages, replace=bool(payload.get("replace"))))

    def _export_submissions(self) -> None:
        if not PHASE2_ENABLED:
            self._json(HTTPStatus.FORBIDDEN, {"ok": False, "error": "phase2 disabled"})
            return
        if not self._verify_ingest_token():
            return
        payload = self._read_json()
        self._json(HTTPStatus.OK, self.store.pending_submissions(limit=int(payload.get("limit") or 20)))

    def _export_skips(self) -> None:
        if not PHASE2_ENABLED:
            self._json(HTTPStatus.FORBIDDEN, {"ok": False, "error": "phase2 disabled"})
            return
        if not self._verify_ingest_token():
            return
        payload = self._read_json()
        self._json(HTTPStatus.OK, self.store.pending_skips(limit=int(payload.get("limit") or 20)))

    def _mark_submission_imported(self) -> None:
        if not PHASE2_ENABLED:
            self._json(HTTPStatus.FORBIDDEN, {"ok": False, "error": "phase2 disabled"})
            return
        if not self._verify_ingest_token():
            return
        payload = self._read_json()
        self._json(
            HTTPStatus.OK,
            self.store.mark_submission_imported(
                work_id=str(payload.get("work_id") or ""),
                status=str(payload.get("status") or ""),
                validation=payload.get("validation") if isinstance(payload.get("validation"), dict) else {},
            ),
        )

    def _audit_events(self) -> None:
        if not self._verify_ingest_token():
            return
        payload = self._read_json()
        self._json(HTTPStatus.OK, self.store.audit_events(limit=int(payload.get("limit") or 100)))

    def _verify_ingest_token(self) -> bool:
        if not INGEST_TOKEN:
            self._json(HTTPStatus.SERVICE_UNAVAILABLE, {"ok": False, "error": "ingest token not configured"})
            return False
        if self.headers.get("Authorization", "") != f"Bearer {INGEST_TOKEN}":
            self._json(HTTPStatus.UNAUTHORIZED, {"ok": False, "error": "unauthorized"})
            return False
        return True

    def _mcp(self) -> None:
        request = self._read_json()
        method = request.get("method")
        request_id = request.get("id")
        if method in {"initialize", "notifications/initialized"}:
            self._mcp_result(
                request_id,
                {
                    "protocolVersion": MCP_PROTOCOL_VERSION,
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": SERVER_NAME, "version": "0.1.0"},
                },
            )
            return
        required = _required_scopes_for_method(method, request)
        if required:
            try:
                self._verify_request(required)
            except PermissionError as exc:
                message = str(exc)
                if message == "missing required scope":
                    self._mcp_error(
                        request_id,
                        -32001,
                        message,
                        status=HTTPStatus.FORBIDDEN,
                        headers={"WWW-Authenticate": _scope_challenge_header(_base_url(self), required)},
                    )
                else:
                    self._mcp_error(
                        request_id,
                        -32001,
                        message,
                        status=HTTPStatus.UNAUTHORIZED,
                        headers={"WWW-Authenticate": _auth_challenge_header(_base_url(self))},
                    )
                return
        if method == "tools/list":
            self._mcp_result(request_id, {"tools": _tools()})
        elif method == "tools/call":
            params = request.get("params") or {}
            try:
                self._mcp_result(
                    request_id,
                    _tool_response(_call_tool(self.store, params.get("name"), params.get("arguments") or {}, self._subject())),
                )
            except Exception as exc:
                self._mcp_error(request_id, -32602, str(exc))
        else:
            self._mcp_error(request_id, -32601, "method not found")

    def _verify_request(self, required_scopes: set[str]) -> dict[str, Any]:
        if not REQUIRE_AUTH:
            return {"sub": "dev-no-auth", "scope": " ".join(sorted(ALL_SCOPES))}
        auth = self.headers.get("Authorization", "")
        if not auth.startswith("Bearer "):
            raise PermissionError("missing bearer token")
        return verify_access_token(
            auth.removeprefix("Bearer ").strip(),
            secret=AUTH_SECRET,
            issuer=AUTH_ISSUER or _base_url(self),
            audience=f"{_base_url(self)}/mcp",
            required_scopes=required_scopes,
        )

    def _subject(self) -> str:
        if not REQUIRE_AUTH:
            return "dev-no-auth"
        auth = self.headers.get("Authorization", "")
        if not auth.startswith("Bearer "):
            return "unknown"
        try:
            claims = verify_access_token(
                auth.removeprefix("Bearer ").strip(),
                secret=AUTH_SECRET,
                issuer=AUTH_ISSUER or _base_url(self),
                audience=f"{_base_url(self)}/mcp",
                required_scopes=set(),
            )
            return str(claims.get("sub") or "unknown")
        except PermissionError:
            return "unknown"

    def _read_json(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0") or "0")
        if length > MAX_BODY_BYTES:
            raise ValueError("request body too large")
        if length == 0:
            return {}
        return json.loads(self.rfile.read(length).decode("utf-8"))

    def _read_form(self) -> dict[str, str]:
        length = int(self.headers.get("Content-Length", "0") or "0")
        body = self.rfile.read(length).decode("utf-8")
        parsed = urllib.parse.parse_qs(body)
        return {key: values[-1] for key, values in parsed.items() if values}

    def _mcp_result(self, request_id: Any, result: dict[str, Any]) -> None:
        self._json(HTTPStatus.OK, {"jsonrpc": "2.0", "id": request_id, "result": result})

    def _mcp_error(
        self,
        request_id: Any,
        code: int,
        message: str,
        *,
        status: HTTPStatus = HTTPStatus.OK,
        headers: dict[str, str] | None = None,
    ) -> None:
        self._json(status, {"jsonrpc": "2.0", "id": request_id, "error": {"code": code, "message": message}}, headers=headers)

    def _json(self, status: HTTPStatus, payload: dict[str, Any], *, headers: dict[str, str] | None = None) -> None:
        body = json.dumps(payload, ensure_ascii=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args) -> None:
        return


def main() -> int:
    Handler.store = broker_store_from_env()
    port = int(os.environ.get("PORT", "8080"))
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    server.serve_forever()
    return 0


def _call_tool(store: BrokerStore, name: str | None, arguments: dict[str, Any], subject: str) -> dict[str, Any]:
    snapshot = store.latest_snapshot()
    if name == "get_status":
        status = status_from_snapshot(snapshot, store.control_request_counts())
        status["mcp_phase"] = "remote_extraction_bridge" if PHASE2_ENABLED else status["mcp_phase"]
        status["phase2_tools_enabled"] = PHASE2_ENABLED
        status["broker_storage"] = store.storage_info()
        if PHASE2_ENABLED:
            remote_work = store.remote_work_summary()
            status["remote_work"] = remote_work
            status["remote_work_smoke"] = remote_work
            if PHASE2_SMOKE_ALLOW_STATUS_SCOPE:
                status["phase2_scope_mode"] = "factory.status smoke compatibility"
            elif PHASE2_SMOKE_ALLOW_CONTROL_SCOPE:
                status["phase2_scope_mode"] = "factory.control smoke compatibility"
            else:
                status["phase2_scope_mode"] = "factory.claim factory.submit"
        return status
    if name == "get_queue_summary":
        summary = queue_summary_from_snapshot(snapshot)
        if PHASE2_ENABLED:
            remote_work = store.remote_work_summary()
            summary["remote_work"] = remote_work
            summary["remote_work_smoke"] = remote_work
            summary["remote_contract"] = "remote_extraction_bridge_full_text_allowed_only"
        return summary
    if name == "get_worker_policy":
        return worker_policy()
    if name == "request_local_cycle":
        cycle_type = arguments.get("cycle_type")
        if cycle_type not in {"observer_publish", "bridge_sync", "bounded_extraction", "reviewer_audit"}:
            raise ValueError("cycle_type must be observer_publish, bridge_sync, bounded_extraction, or reviewer_audit")
        return store.create_control_request(
            request_type=cycle_type,
            requested_by=subject,
            payload={"reason": str(arguments.get("reason") or "")[:500]},
        )
    if name == "get_observer_url":
        return {"ok": True, "observer_url": snapshot.get("observer_url", DEFAULT_OBSERVER_URL)}
    if name == "claim_work":
        _require_phase2()
        return store.claim_work(worker_id=_worker_id(arguments, subject))
    if name == "reserve_evidence_task":
        _require_phase2()
        return store.reserve_evidence_task(worker_id=_worker_id(arguments, subject))
    if name == "reserve_source_card_task":
        _require_phase2()
        return store.reserve_source_card_task(worker_id=_worker_id(arguments, subject))
    if name == "reserve_source_card_batch":
        _require_phase2()
        return store.reserve_source_card_batch(
            worker_id=_worker_id(arguments, subject),
            max_tasks=int(arguments.get("max_tasks") or 3),
        )
    if name == "get_work_context":
        _require_phase2()
        work_id = str(arguments.get("work_id") or "")
        if not work_id:
            raise ValueError("work_id is required")
        return store.get_work_context(work_id=work_id, worker_id=_worker_id(arguments, subject))
    if name == "get_work_chunk":
        _require_phase2()
        work_id = str(arguments.get("work_id") or "")
        chunk_id = str(arguments.get("chunk_id") or "")
        if not work_id or not chunk_id:
            raise ValueError("work_id and chunk_id are required")
        return store.get_work_chunk(work_id=work_id, worker_id=_worker_id(arguments, subject), chunk_id=chunk_id)
    if name == "get_evidence_manifest":
        _require_phase2()
        work_id = str(arguments.get("work_id") or "")
        if not work_id:
            raise ValueError("work_id is required")
        return store.get_evidence_manifest(work_id=work_id, worker_id=_worker_id(arguments, subject))
    if name == "get_evidence_packet":
        _require_phase2()
        work_id = str(arguments.get("work_id") or "")
        chunk_id = str(arguments.get("chunk_id") or "")
        if not work_id or not chunk_id:
            raise ValueError("work_id and chunk_id are required")
        return store.get_work_chunk(work_id=work_id, worker_id=_worker_id(arguments, subject), chunk_id=chunk_id)
    if name == "get_source_card":
        _require_phase2()
        work_id = str(arguments.get("work_id") or "")
        card_id = str(arguments.get("card_id") or "")
        if not work_id or not card_id:
            raise ValueError("work_id and card_id are required")
        return store.get_source_card(work_id=work_id, worker_id=_worker_id(arguments, subject), card_id=card_id)
    if name == "get_source_card_batch":
        _require_phase2()
        items = arguments.get("items")
        if not isinstance(items, list):
            raise ValueError("items array is required")
        return store.get_source_card_batch(worker_id=_worker_id(arguments, subject), items=items)
    if name == "submit_work_notes":
        _require_phase2()
        work_id = str(arguments.get("work_id") or "")
        chunk_id = str(arguments.get("chunk_id") or "")
        notes = arguments.get("notes")
        if not work_id or not chunk_id or not isinstance(notes, dict):
            raise ValueError("work_id, chunk_id, and object notes are required")
        return store.submit_work_notes(work_id=work_id, worker_id=_worker_id(arguments, subject), chunk_id=chunk_id, notes=notes)
    if name == "get_work_notes":
        _require_phase2()
        work_id = str(arguments.get("work_id") or "")
        if not work_id:
            raise ValueError("work_id is required")
        return store.get_work_notes(work_id=work_id, worker_id=_worker_id(arguments, subject))
    if name == "submit_work_output":
        _require_phase2()
        work_id = str(arguments.get("work_id") or "")
        output = arguments.get("output")
        if not work_id or not isinstance(output, dict):
            raise ValueError("work_id and object output are required")
        return store.submit_work_output(work_id=work_id, worker_id=_worker_id(arguments, subject), output=output)
    if name == "submit_work_outputs":
        _require_phase2()
        outputs = arguments.get("outputs")
        if not isinstance(outputs, list):
            raise ValueError("outputs array is required")
        return store.submit_work_outputs(worker_id=_worker_id(arguments, subject), outputs=outputs)
    if name == "skip_source_card_task":
        _require_phase2()
        work_id = str(arguments.get("work_id") or "")
        if not work_id:
            raise ValueError("work_id is required")
        return store.skip_source_card_task(
            work_id=work_id,
            worker_id=_worker_id(arguments, subject),
            card_id=str(arguments.get("card_id") or "") or None,
            stage=str(arguments.get("stage") or "unknown"),
            reason_code=str(arguments.get("reason_code") or "other"),
        )
    if name == "heartbeat_work":
        _require_phase2()
        work_id = str(arguments.get("work_id") or "")
        if not work_id:
            raise ValueError("work_id is required")
        return store.heartbeat_work(work_id=work_id, worker_id=_worker_id(arguments, subject))
    if name == "release_work":
        _require_phase2()
        work_id = str(arguments.get("work_id") or "")
        if not work_id:
            raise ValueError("work_id is required")
        return store.release_work(work_id=work_id, worker_id=_worker_id(arguments, subject), reason=str(arguments.get("reason") or ""))
    raise ValueError(f"Unknown tool: {name}")


def _tool_response(payload: dict[str, Any]) -> dict[str, Any]:
    return {"content": [{"type": "text", "text": tool_result_summary(payload)}], "structuredContent": payload}


def _required_scopes_for_method(method: str | None, request: dict[str, Any]) -> set[str]:
    if method == "tools/call":
        params = request.get("params") or {}
        name = params.get("name")
        if name == "request_local_cycle":
            return {"factory.control"}
        if name in {
            "claim_work",
            "reserve_evidence_task",
            "reserve_source_card_task",
            "reserve_source_card_batch",
            "get_work_context",
            "get_work_chunk",
            "get_evidence_manifest",
            "get_evidence_packet",
            "get_source_card",
            "get_source_card_batch",
            "submit_work_notes",
            "get_work_notes",
            "heartbeat_work",
            "release_work",
            "skip_source_card_task",
        }:
            if PHASE2_SMOKE_ALLOW_STATUS_SCOPE:
                return {"factory.status"}
            if PHASE2_SMOKE_ALLOW_CONTROL_SCOPE:
                return {"factory.control"}
            return {"factory.claim"}
        if name in {"submit_work_output", "submit_work_outputs"}:
            if PHASE2_SMOKE_ALLOW_STATUS_SCOPE:
                return {"factory.status"}
            if PHASE2_SMOKE_ALLOW_CONTROL_SCOPE:
                return {"factory.control"}
            return {"factory.submit"}
        return {"factory.status"}
    return set()


def _base_url(handler: BaseHTTPRequestHandler) -> str:
    if BASE_URL:
        return BASE_URL
    host = handler.headers.get("Host") or f"localhost:{os.environ.get('PORT', '8080')}"
    proto = handler.headers.get("X-Forwarded-Proto") or "http"
    return f"{proto}://{host}".rstrip("/")


def _one(query: dict[str, list[str]], key: str) -> str | None:
    values = query.get(key)
    return values[-1] if values else None


def _normalize_scope(scope: str) -> str:
    requested = set(scope.split()) & ALL_SCOPES
    if not requested:
        requested = ALL_SCOPES if PHASE2_ENABLED else DEFAULT_SCOPES
    elif PHASE2_ENABLED and requested.issubset(DEFAULT_SCOPES):
        requested = requested | FUTURE_SCOPES
    if FUTURE_SCOPES & requested and not PHASE2_ENABLED:
        requested = (requested - FUTURE_SCOPES) | DEFAULT_SCOPES
    return " ".join(sorted(requested))


def _auth_challenge_header(base_url: str) -> str:
    return f'Bearer error="invalid_token", resource_metadata="{base_url}/.well-known/oauth-protected-resource"'


def _scope_challenge_header(base_url: str, required: set[str]) -> str:
    scope = " ".join(sorted(required))
    return (
        'Bearer error="insufficient_scope", '
        f'scope="{scope}", '
        f'resource_metadata="{base_url}/.well-known/oauth-protected-resource"'
    )


def _tools() -> list[dict[str, Any]]:
    tools = PHASE1_TOOLS + (PHASE2_TOOLS if PHASE2_ENABLED else [])
    return [with_security_metadata(tool) for tool in tools]


def _require_phase2() -> None:
    if not PHASE2_ENABLED:
        raise ValueError("Phase 2 extraction tools are disabled")


def _worker_id(arguments: dict[str, Any], subject: str) -> str:
    value = str(arguments.get("worker_id") or "").strip()
    if value:
        return value[:120]
    return f"mcp-{subject}"[:120]


def with_security_metadata(tool: dict[str, Any]) -> dict[str, Any]:
    scopes = TOOL_SCOPES_BY_NAME.get(str(tool.get("name") or ""), ["factory.status"])
    scheme = {"type": "oauth2", "scopes": scopes}
    enriched = dict(tool)
    enriched["securitySchemes"] = [scheme]
    meta = dict(enriched.get("_meta") or {})
    meta["securitySchemes"] = [scheme]
    enriched["_meta"] = meta
    return enriched


def tool_result_summary(payload: dict[str, Any]) -> str:
    if not payload.get("ok"):
        return "Podcast Intelligence Factory tool returned an error."
    if payload.get("work_id") and payload.get("card_id"):
        return f"Returned bounded source card {payload.get('card_index')} of {payload.get('card_count')} for {payload.get('work_id')}."
    if payload.get("work_id") and payload.get("chunk_id"):
        if "chunk_text" in payload:
            return f"Returned transcript chunk {payload.get('chunk_index')} of {payload.get('chunk_count')} for {payload.get('work_id')}."
        if payload.get("delivery_mode") == "remote_safe_evidence":
            return f"Returned remote-safe evidence packet {payload.get('chunk_index')} of {payload.get('chunk_count')} for {payload.get('work_id')}."
        return f"Recorded notes for {payload.get('chunk_id')} on {payload.get('work_id')}."
    if payload.get("work_id"):
        return f"Podcast Intelligence Factory work item {payload.get('work_id')} updated."
    if payload.get("observer_url"):
        return "Returned sanitized Podcast Intelligence Factory observer status."
    return "Podcast Intelligence Factory tool completed successfully."


if __name__ == "__main__":
    raise SystemExit(main())
