"""Private loopback dashboard and JSON API for Research Radar.

The Radar interface is deliberately separate from the Railway observer.  It
serves private briefings and their evidence only from a loopback socket and
never exposes transcript bodies.  The HTML is progressively rendered without
JavaScript so the same small :class:`radar.ResearchRadar` query surface powers
both the dashboard and the JSON API.
"""

from __future__ import annotations

import argparse
import html
import ipaddress
import json
import os
import socket
from contextlib import contextmanager
from dataclasses import asdict, is_dataclass
from datetime import date, datetime
from enum import Enum
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, Protocol, Sequence
from urllib.parse import parse_qs, quote, unquote, urlencode, urlsplit

from .radar import RadarNotFoundError, ResearchRadar, default_radar_db_path


DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8767
DEFAULT_LIMIT = 50
MAX_LIMIT = 200
MAX_PARAM_CHARS = 500
MAX_REQUEST_TARGET_BYTES = 8_192
MAX_FEEDBACK_BODY_BYTES = 16_384
FEEDBACK_RATINGS = frozenset({"useful", "irrelevant", "wrong", "duplicate", "follow"})

# Exact evidence excerpts are product data.  Full source/transcript bodies are
# not, and are removed even if a future service accidentally includes them.
_PRIVATE_BODY_FIELDS = frozenset(
    {
        "transcript",
        "transcript_text",
        "raw_transcript",
        "raw_transcript_text",
        "raw_content",
        "raw_content_text",
        "normalized_text",
        "document_text",
        "full_text",
        "content_blob",
    }
)


class RadarSurface(Protocol):
    def list_workspaces(self, limit: int = DEFAULT_LIMIT) -> Any: ...

    def list_briefings(
        self,
        workspace_id: str | None = None,
        status: str | None = None,
        unread_only: bool = False,
        limit: int = DEFAULT_LIMIT,
    ) -> Any: ...

    def get_briefing(self, briefing_id: str) -> Any: ...

    def workspace_timeline(
        self,
        workspace_id: str,
        status: str | None = None,
        limit: int = DEFAULT_LIMIT,
    ) -> Any: ...

    def entity_positions(self, entity_id: str, limit: int = DEFAULT_LIMIT) -> Any: ...

    def list_sources(
        self,
        workspace_id: str | None = None,
        status: str | None = None,
        limit: int = DEFAULT_LIMIT,
    ) -> Any: ...

    def operations_summary(self) -> Any: ...

    def record_feedback(self, briefing_id: str, rating: str) -> Any: ...

    def set_source_status(self, source_id: str, status: str, *, reason: str) -> Any: ...


ServiceFactory = Callable[[], Any]


def validate_loopback_host(host: str) -> str:
    """Return a normalized loopback host or reject it before binding."""

    value = str(host or "").strip()
    if value.lower().rstrip(".") == "localhost":
        return "localhost"
    unbracketed = value[1:-1] if value.startswith("[") and value.endswith("]") else value
    try:
        address = ipaddress.ip_address(unbracketed)
    except ValueError as exc:
        raise ValueError("Research Radar host must be localhost or a loopback IP address") from exc
    if not address.is_loopback:
        raise ValueError("Research Radar refuses non-loopback binds")
    return str(address)


def _is_loopback(value: str) -> bool:
    try:
        return ipaddress.ip_address(str(value).split("%", 1)[0]).is_loopback
    except ValueError:
        return str(value).lower().rstrip(".") == "localhost"


def _allowed_host_header(value: str) -> bool:
    if not value or any(character in value for character in "\r\n"):
        return False
    try:
        parsed = urlsplit(f"//{value}")
        hostname = parsed.hostname
        if parsed.port is not None and not 0 <= parsed.port <= 65_535:
            return False
    except ValueError:
        return False
    return bool(hostname) and _is_loopback(str(hostname))


@contextmanager
def radar_service(database_path: str | Path) -> Iterator[ResearchRadar]:
    """Create a short-lived Radar service for one HTTP request."""

    service = ResearchRadar(Path(database_path).expanduser().resolve())
    try:
        yield service
    finally:
        close = getattr(service, "close", None)
        if callable(close):
            close()


def _contextualize(factory: ServiceFactory) -> Iterator[RadarSurface]:
    produced = factory()
    if hasattr(produced, "__enter__") and hasattr(produced, "__exit__"):
        with produced as service:
            yield service
        return
    try:
        yield produced
    finally:
        close = getattr(produced, "close", None)
        if callable(close):
            close()


class RadarHTTPServer(ThreadingHTTPServer):
    """Threaded server that binds to and accepts requests from loopback only."""

    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, server_address: tuple[str, int], service_factory: ServiceFactory):
        host = validate_loopback_host(server_address[0])
        port = int(server_address[1])
        if not 0 <= port <= 65_535:
            raise ValueError("port must be between 0 and 65535")
        if ":" in host:
            self.address_family = socket.AF_INET6
        self.service_factory = service_factory
        super().__init__((host, port), RadarHandler)

    @contextmanager
    def service(self) -> Iterator[RadarSurface]:
        yield from _contextualize(self.service_factory)

    def verify_request(self, request: Any, client_address: tuple[Any, ...]) -> bool:
        return bool(client_address) and _is_loopback(str(client_address[0]))


def create_server(
    database_path: str | Path | None = None,
    *,
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    service_factory: ServiceFactory | None = None,
) -> RadarHTTPServer:
    """Create, but do not start, the private Radar server."""

    normalized = validate_loopback_host(host)
    if service_factory is None:
        resolved = Path(database_path or default_radar_db_path()).expanduser().resolve()
        service_factory = lambda: radar_service(resolved)
    return RadarHTTPServer((normalized, int(port)), service_factory)


class RadarHandler(BaseHTTPRequestHandler):
    server: RadarHTTPServer

    def do_GET(self) -> None:  # noqa: N802 - stdlib handler API
        self._handle_get(include_body=True)

    def do_HEAD(self) -> None:  # noqa: N802 - stdlib handler API
        self._handle_get(include_body=False)

    def do_POST(self) -> None:  # noqa: N802 - stdlib handler API
        self._handle_post()

    def do_PUT(self) -> None:  # noqa: N802 - stdlib handler API
        self._method_not_allowed("GET, HEAD, POST")

    def do_PATCH(self) -> None:  # noqa: N802 - stdlib handler API
        self._method_not_allowed("GET, HEAD, POST")

    def do_DELETE(self) -> None:  # noqa: N802 - stdlib handler API
        self._method_not_allowed("GET, HEAD, POST")

    def log_message(self, format: str, *args: Any) -> None:
        # Briefing identifiers and workspace filters are private research state.
        return

    def _request_is_allowed(self, *, include_body: bool = True) -> bool:
        if len(self.path.encode("utf-8", errors="ignore")) > MAX_REQUEST_TARGET_BYTES:
            self._error(
                HTTPStatus.REQUEST_URI_TOO_LONG,
                "Request too long",
                "Research Radar rejected an oversized request.",
                include_body=include_body,
            )
            return False
        if not _allowed_host_header(self.headers.get("Host", "")):
            self._error(
                HTTPStatus.BAD_REQUEST,
                "Invalid host",
                "Research Radar accepts loopback Host headers only.",
                include_body=include_body,
            )
            return False
        return True

    def _handle_get(self, *, include_body: bool) -> None:
        if not self._request_is_allowed(include_body=include_body):
            return
        parsed = urlsplit(self.path)
        try:
            params = parse_qs(parsed.query, keep_blank_values=True, max_num_fields=24)
            if parsed.path == "/":
                self._redirect("/inbox")
                return
            if parsed.path == "/healthz":
                with self.server.service() as service:
                    service.operations_summary()
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "ok": True,
                        "privacy": "local_private",
                        "loopback_only": True,
                        "railway_content_used": False,
                        "transcripts_exposed": False,
                    },
                    include_body=include_body,
                )
                return
            if parsed.path.startswith("/api/"):
                self._handle_api_get(parsed.path, params, include_body=include_body)
                return
            if parsed.path not in _HTML_ROUTES:
                self._error(
                    HTTPStatus.NOT_FOUND,
                    "Not found",
                    "That private Radar page does not exist.",
                    include_body=include_body,
                )
                return
            with self.server.service() as service:
                document = render_route(service, parsed.path, params)
            self._send_html(HTTPStatus.OK, document, include_body=include_body)
        except RadarNotFoundError as exc:
            self._error(
                HTTPStatus.NOT_FOUND,
                "Record not found",
                str(exc),
                include_body=include_body,
            )
        except (ValueError, KeyError) as exc:
            self._error(
                HTTPStatus.BAD_REQUEST,
                "Invalid request",
                str(exc),
                include_body=include_body,
            )
        except Exception as exc:  # UI boundary must not leak database or model details.
            self._error(
                HTTPStatus.SERVICE_UNAVAILABLE,
                "Radar unavailable",
                f"The local Radar query could not be completed ({type(exc).__name__}).",
                include_body=include_body,
            )

    def _handle_api_get(
        self,
        path: str,
        params: Mapping[str, Sequence[str]],
        *,
        include_body: bool,
    ) -> None:
        limit = _limit(params)
        with self.server.service() as service:
            if path == "/api/workspaces":
                data = service.list_workspaces(limit=limit)
            elif path in {"/api/briefs", "/api/briefings"}:
                data = service.list_briefings(
                    workspace_id=_optional_param(params, "workspace_id"),
                    status=_optional_param(params, "status"),
                    unread_only=_bool_param(params, "unread_only", default=False),
                    limit=limit,
                )
            elif path.startswith("/api/briefs/") or path.startswith("/api/briefings/"):
                identifier = _path_identifier(path)
                data = service.get_briefing(identifier)
                if data is None:
                    self._error(
                        HTTPStatus.NOT_FOUND,
                        "Briefing not found",
                        "No briefing has that identifier.",
                        include_body=include_body,
                    )
                    return
            elif path == "/api/timeline":
                workspace_id = _required_param(params, "workspace_id")
                data = service.workspace_timeline(
                    workspace_id,
                    status=_optional_param(params, "status"),
                    limit=limit,
                )
            elif path == "/api/entity-positions":
                data = service.entity_positions(_required_param(params, "entity_id"), limit=limit)
            elif path.startswith("/api/entities/") and path.endswith("/positions"):
                entity_id = _entity_path_identifier(path)
                data = service.entity_positions(entity_id, limit=limit)
            elif path == "/api/sources":
                data = service.list_sources(
                    workspace_id=_optional_param(params, "workspace_id"),
                    status=_optional_param(params, "status"),
                    limit=limit,
                )
            elif path == "/api/operations":
                data = service.operations_summary()
            else:
                self._error(
                    HTTPStatus.NOT_FOUND,
                    "API route not found",
                    "That private Radar API route does not exist.",
                    include_body=include_body,
                )
                return
        self._send_json(
            HTTPStatus.OK,
            {"ok": True, "privacy": "local_private", "data": data},
            include_body=include_body,
        )

    def _handle_post(self) -> None:
        if not self._request_is_allowed():
            return
        parsed = urlsplit(self.path)
        if parsed.path not in {"/api/feedback", "/feedback", "/api/source-status", "/source-status"}:
            self._method_not_allowed("GET, HEAD")
            return
        if not self._origin_is_allowed():
            self._error(
                HTTPStatus.FORBIDDEN,
                "Cross-origin request rejected",
                "Feedback must originate from this private loopback interface.",
            )
            return
        try:
            values = self._post_values(api=parsed.path.startswith("/api/"))
            if parsed.path in {"/api/source-status", "/source-status"}:
                source_id = _bounded_value(values.get("source_id"), "source_id")
                status = _bounded_value(values.get("status"), "status").lower()
                if status not in {"paused", "blocked"}:
                    raise ValueError("source status control permits paused or blocked only")
                reason = _bounded_value(values.get("reason") or "private_source_ledger", "reason")
                with self.server.service() as service:
                    result = service.set_source_status(source_id, status, reason=reason)
                if parsed.path == "/source-status":
                    self._redirect(_url("/sources", source_update="recorded"))
                    return
                self._send_json(
                    HTTPStatus.OK,
                    {"ok": True, "privacy": "local_private", "data": result},
                    include_body=True,
                )
                return
            briefing_id = _bounded_value(values.get("briefing_id"), "briefing_id")
            rating = _bounded_value(values.get("rating"), "rating").lower()
            if rating not in FEEDBACK_RATINGS:
                raise ValueError("rating must be useful, irrelevant, wrong, duplicate, or follow")
            with self.server.service() as service:
                result = service.record_feedback(briefing_id, rating)
            if parsed.path == "/feedback":
                self._redirect(_url("/brief", briefing_id=briefing_id, feedback="recorded"))
                return
            self._send_json(
                HTTPStatus.CREATED,
                {"ok": True, "privacy": "local_private", "data": result},
                include_body=True,
            )
        except (ValueError, json.JSONDecodeError) as exc:
            title = "Invalid source update" if "source-status" in parsed.path else "Invalid feedback"
            self._error(HTTPStatus.BAD_REQUEST, title, str(exc))
        except Exception as exc:
            title = "Source update unavailable" if "source-status" in parsed.path else "Feedback unavailable"
            self._error(
                HTTPStatus.SERVICE_UNAVAILABLE,
                title,
                f"The update could not be stored locally ({type(exc).__name__}).",
            )

    def _origin_is_allowed(self) -> bool:
        origin = self.headers.get("Origin")
        if not origin:
            return True
        if origin == "null":
            return False
        parsed = urlsplit(origin)
        return parsed.scheme in {"http", "https"} and bool(parsed.hostname) and _is_loopback(str(parsed.hostname))

    def _post_values(self, *, api: bool) -> Mapping[str, Any]:
        raw_length = self.headers.get("Content-Length")
        try:
            length = int(raw_length or "0")
        except ValueError as exc:
            raise ValueError("Content-Length must be an integer") from exc
        if not 0 < length <= MAX_FEEDBACK_BODY_BYTES:
            raise ValueError(f"request body must be between 1 and {MAX_FEEDBACK_BODY_BYTES} bytes")
        body = self.rfile.read(length)
        media_type = self.headers.get_content_type().lower()
        if api:
            if media_type != "application/json":
                raise ValueError("the private mutation API requires application/json")
            decoded = json.loads(body.decode("utf-8"))
            if not isinstance(decoded, Mapping):
                raise ValueError("request JSON must be an object")
            return decoded
        if media_type != "application/x-www-form-urlencoded":
            raise ValueError("the private form requires application/x-www-form-urlencoded")
        parsed = parse_qs(body.decode("utf-8"), keep_blank_values=True, max_num_fields=4)
        return {key: values[0] if values else "" for key, values in parsed.items()}

    def _method_not_allowed(self, allowed: str) -> None:
        payload = render_error_page("Method not allowed", "That method is not available on this Radar route.")
        body = payload.encode("utf-8")
        self.send_response(HTTPStatus.METHOD_NOT_ALLOWED)
        self.send_header("Allow", allowed)
        self._security_headers()
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _redirect(self, location: str) -> None:
        self.send_response(HTTPStatus.SEE_OTHER)
        self.send_header("Location", location)
        self._security_headers()
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _error(
        self,
        status: HTTPStatus,
        title: str,
        message: str,
        *,
        include_body: bool = True,
    ) -> None:
        if urlsplit(self.path).path.startswith("/api/"):
            self._send_json(
                status,
                {"ok": False, "error": {"code": status.name.lower(), "message": message}},
                include_body=include_body,
            )
        else:
            self._send_html(status, render_error_page(title, message), include_body=include_body)

    def _send_html(self, status: HTTPStatus, document: str, *, include_body: bool) -> None:
        payload = document.encode("utf-8")
        self.send_response(status)
        self._security_headers()
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        if include_body:
            self.wfile.write(payload)

    def _send_json(self, status: HTTPStatus, value: Any, *, include_body: bool) -> None:
        public = _public_value(value)
        payload = (json.dumps(public, ensure_ascii=True, sort_keys=True) + "\n").encode("utf-8")
        self.send_response(status)
        self._security_headers()
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        if include_body:
            self.wfile.write(payload)

    def _security_headers(self) -> None:
        self.send_header("Cache-Control", "no-store")
        self.send_header(
            "Content-Security-Policy",
            "default-src 'none'; style-src 'unsafe-inline'; form-action 'self'; "
            "base-uri 'none'; frame-ancestors 'none'",
        )
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Cross-Origin-Resource-Policy", "same-origin")
        self.send_header("Permissions-Policy", "camera=(), microphone=(), geolocation=()")


_HTML_ROUTES = frozenset({"/inbox", "/timeline", "/brief", "/positions", "/sources", "/operations"})


def render_route(
    service: RadarSurface,
    route: str,
    params: Mapping[str, Sequence[str]],
) -> str:
    """Render one dashboard route through the bounded ResearchRadar surface."""

    limit = _limit(params)
    if route == "/inbox":
        unread_only = _bool_param(params, "unread_only", default=True)
        status = _optional_param(params, "status")
        records = service.list_briefings(
            workspace_id=_optional_param(params, "workspace_id"),
            status=status,
            unread_only=unread_only,
            limit=limit,
        )
        return _document(
            "Inbox",
            route,
            _heading("Radar inbox", "Meaningful developments, with evidence and uncertainty attached.")
            + _inbox_filters(params, unread_only=unread_only)
            + _briefing_cards(records),
        )
    if route == "/timeline":
        workspaces = service.list_workspaces(limit=MAX_LIMIT)
        workspace_id = _optional_param(params, "workspace_id") or _first_identifier(workspaces)
        status = _optional_param(params, "status")
        records = service.workspace_timeline(workspace_id, status=status, limit=limit) if workspace_id else []
        return _document(
            "Timeline",
            route,
            _heading("Workspace timeline", "A chronological record of developments, amendments, and retractions.")
            + _workspace_filters("/timeline", params, workspaces, workspace_id=workspace_id, include_status=True)
            + _timeline(records),
        )
    if route == "/brief":
        briefing_id = _required_param(params, "briefing_id")
        briefing = service.get_briefing(briefing_id)
        if briefing is None:
            return render_error_page("Briefing not found", "No briefing has that identifier.")
        return _briefing_document(briefing, feedback_recorded=_param(params, "feedback") == "recorded")
    if route == "/positions":
        entity_id = _optional_param(params, "entity_id")
        records = service.entity_positions(entity_id, limit=limit) if entity_id else []
        return _document(
            "Positions",
            route,
            _heading("Entity positions", "Evidence-backed positions and changes over time.")
            + _simple_filter("/positions", "entity_id", "Entity ID", entity_id or "", "entity_…", limit)
            + _positions(records, entity_id=entity_id),
        )
    if route == "/sources":
        workspaces = service.list_workspaces(limit=MAX_LIMIT)
        workspace_id = _optional_param(params, "workspace_id") or _first_identifier(workspaces)
        status = _optional_param(params, "status")
        sources = service.list_sources(workspace_id=workspace_id, status=status, limit=limit)
        notice = (
            '<p class="notice" role="status">Source status updated locally. The source was not deleted.</p>'
            if _param(params, "source_update") == "recorded"
            else ""
        )
        return _document(
            "Sources",
            route,
            _heading("Source ledger", "Trust, probation, contribution history, and failure state in one place.")
            + _workspace_filters("/sources", params, workspaces, workspace_id=workspace_id, include_status=True)
            + notice
            + _source_cards(sources),
        )
    if route == "/operations":
        summary = service.operations_summary()
        return _document(
            "Operations",
            route,
            _heading("Operations", "Bounded queue health, dead letters, budgets, and the last successful cycle.")
            + _operations(summary),
        )
    raise ValueError("unknown Radar route")


def render_error_page(title: str, message: str) -> str:
    return _document(title, "", _heading(title, message, error=True))


def _heading(title: str, description: str, *, error: bool = False) -> str:
    role = ' role="alert"' if error else ""
    return (
        '<header class="page-heading">'
        '<p class="eyebrow">AI &amp; Technology Radar</p>'
        f"<h1>{_escape(title)}</h1><p{role}>{_escape(description)}</p>"
        "</header>"
    )


def _document(title: str, active_path: str, main: str) -> str:
    nav_items = (
        ("/inbox", "Inbox"),
        ("/timeline", "Timeline"),
        ("/positions", "Positions"),
        ("/sources", "Sources"),
        ("/operations", "Operations"),
    )
    nav = "".join(
        f'<a href="{path}"'
        + (' aria-current="page"' if path == active_path else "")
        + f">{label}</a>"
        for path, label in nav_items
    )
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{_escape(title)} · Research Radar</title>
  <style>{_STYLES}</style>
</head>
<body>
  <a class="skip-link" href="#main">Skip to content</a>
  <header class="site-header">
    <a class="brand" href="/inbox" aria-label="Research Radar home"><span class="brand-mark" aria-hidden="true">R</span><span><strong>Research Radar</strong><small>Private evidence intelligence</small></span></a>
    <div class="trust"><span aria-hidden="true"></span> Local · Private · Evidence-linked</div>
  </header>
  <nav class="primary-nav" aria-label="Primary">{nav}</nav>
  <main id="main" tabindex="-1">{main}</main>
  <footer>Local SQLite authority · No raw transcripts · No Railway research content</footer>
</body>
</html>"""


def _inbox_filters(params: Mapping[str, Sequence[str]], *, unread_only: bool) -> str:
    status = _optional_param(params, "status") or ""
    workspace_id = _optional_param(params, "workspace_id") or ""
    return (
        '<form class="filters" method="get" action="/inbox" aria-label="Inbox filters">'
        + _input("workspace_id", "Workspace ID", workspace_id, "Optional workspace")
        + _select(
            "status",
            "Status",
            status,
            (("", "All states"), ("provisional", "Provisional"), ("verified", "Verified"), ("amended", "Amended"), ("retracted", "Retracted")),
        )
        + _select("unread_only", "Visibility", "1" if unread_only else "0", (("1", "Unread only"), ("0", "All briefings")))
        + _limit_input(_limit(params))
        + '<button type="submit">Apply</button></form>'
    )


def _workspace_filters(
    action: str,
    params: Mapping[str, Sequence[str]],
    workspaces: Any,
    *,
    workspace_id: str | None,
    include_status: bool,
) -> str:
    options = []
    for workspace in _records(workspaces, "workspaces", "data"):
        identifier = str(_get(workspace, "id", "workspace_id") or "")
        label = str(_get(workspace, "name", "title", "topic") or identifier)
        if identifier:
            options.append((identifier, label))
    if not options:
        options.append(("", "No workspace configured"))
    controls = _select("workspace_id", "Workspace", workspace_id or "", tuple(options))
    if include_status:
        status_options = (
            (("", "All states"), ("probation", "Probation"), ("active", "Active"), ("paused", "Paused"), ("blocked", "Blocked"))
            if action == "/sources"
            else (("", "All states"), ("provisional", "Provisional"), ("verified", "Verified"), ("amended", "Amended"), ("retracted", "Retracted"))
        )
        controls += _select(
            "status",
            "Status",
            _optional_param(params, "status") or "",
            status_options,
        )
    return (
        f'<form class="filters" method="get" action="{_escape(action)}" aria-label="Workspace filters">'
        + controls
        + _limit_input(_limit(params))
        + '<button type="submit">Apply</button></form>'
    )


def _simple_filter(
    action: str,
    name: str,
    label: str,
    value: str,
    placeholder: str,
    limit: int,
) -> str:
    return (
        f'<form class="filters" method="get" action="{_escape(action)}">'
        + _input(name, label, value, placeholder, required=True)
        + _limit_input(limit)
        + '<button type="submit">Look up</button></form>'
    )


def _input(name: str, label: str, value: str, placeholder: str, *, required: bool = False) -> str:
    return (
        '<div class="field">'
        f'<label for="field-{_escape(name)}">{_escape(label)}</label>'
        f'<input id="field-{_escape(name)}" name="{_escape(name)}" value="{_escape(value)}" '
        f'placeholder="{_escape(placeholder)}" autocomplete="off"{(" required" if required else "")}></div>'
    )


def _select(name: str, label: str, selected: str, options: Sequence[tuple[str, str]]) -> str:
    rendered = "".join(
        f'<option value="{_escape(value)}"{(" selected" if value == selected else "")}>{_escape(text)}</option>'
        for value, text in options
    )
    return (
        '<div class="field">'
        f'<label for="field-{_escape(name)}">{_escape(label)}</label>'
        f'<select id="field-{_escape(name)}" name="{_escape(name)}">{rendered}</select></div>'
    )


def _limit_input(value: int) -> str:
    return (
        '<div class="field field-limit"><label for="field-limit">Limit</label>'
        f'<input id="field-limit" name="limit" type="number" min="1" max="{MAX_LIMIT}" value="{value}"></div>'
    )


def _briefing_cards(value: Any) -> str:
    records = _records(value, "briefings", "data", "items")
    if not records:
        return _empty("Inbox clear", "No briefings match this view.")
    return '<div class="brief-grid">' + "".join(_brief_card(record) for record in records) + "</div>"


def _brief_card(record: Mapping[str, Any]) -> str:
    identifier = str(_get(record, "id", "briefing_id") or "")
    title = _get(record, "title", "headline", "development_title") or "Untitled development"
    summary = _get(record, "what_changed", "summary", "synthesis") or "Open the briefing for evidence and context."
    status = str(_get(record, "status") or "provisional")
    time = _get(record, "event_time", "occurred_at", "published_at", "updated_at")
    unread_value = _get(record, "unread", "is_unread")
    unread = bool(unread_value) if unread_value is not None else record.get("read_at") in (None, "")
    meta = " · ".join(part for part in (str(time or ""), "Unread" if unread else "") if part)
    return (
        '<article class="brief-card">'
        f'<div class="card-top">{_status_badge(status)}<span class="meta">{_escape(meta)}</span></div>'
        f'<h2><a href="{_escape(_url("/brief", briefing_id=identifier))}">{_escape(title)}</a></h2>'
        f'<p>{_escape(summary)}</p>'
        f'<a class="text-link" href="{_escape(_url("/brief", briefing_id=identifier))}">Open evidence brief <span aria-hidden="true">→</span></a>'
        "</article>"
    )


def _timeline(value: Any) -> str:
    records = _records(value, "timeline", "developments", "briefings", "data", "items")
    if not records:
        return _empty("No timeline entries", "This workspace has no developments matching the filters.")
    items = []
    for record in records:
        identifier = str(_get(record, "briefing_id", "id") or "")
        title = _get(record, "title", "headline", "development_title") or "Development"
        summary = _get(record, "what_changed", "summary", "description") or ""
        status = str(_get(record, "status") or "provisional")
        occurred = _get(record, "event_time", "occurred_at", "published_at", "updated_at") or "Time unknown"
        items.append(
            '<li><div class="timeline-time">'
            f"{_escape(occurred)}</div><article>{_status_badge(status)}"
            f'<h2><a href="{_escape(_url("/brief", briefing_id=identifier))}">{_escape(title)}</a></h2>'
            f"<p>{_escape(summary)}</p></article></li>"
        )
    return '<ol class="timeline">' + "".join(items) + "</ol>"


def _briefing_document(value: Any, *, feedback_recorded: bool) -> str:
    public = _public_value(value)
    record = _mapping(public)
    if "briefing" in record and isinstance(record["briefing"], Mapping):
        briefing = _mapping(record["briefing"])
        merged = dict(briefing)
        for key in ("evidence", "feedback", "status_history", "interpretations", "unknowns", "positions_changed", "related_developments"):
            if key in record and key not in merged:
                merged[key] = record[key]
        record = merged
    identifier = str(_get(record, "id", "briefing_id") or "")
    title = _get(record, "title", "headline", "development_title") or "Evidence briefing"
    status = str(_get(record, "status") or "provisional")
    main = (
        '<div class="brief-heading">'
        f'<div>{_status_badge(status)}<p class="eyebrow">Evidence briefing</p><h1>{_escape(title)}</h1></div>'
        f'<p class="brief-time">{_escape(_get(record, "event_time", "occurred_at", "published_at", "updated_at") or "")}</p>'
        "</div>"
    )
    if feedback_recorded:
        main += '<p class="notice" role="status">Feedback recorded locally. Evidence and history were not rewritten.</p>'
    main += '<div class="brief-layout"><div class="brief-main">'
    main += _narrative_section("What changed", _get(record, "what_changed", "summary", "synthesis"))
    main += _narrative_section("Why it may matter", _get(record, "why_it_matters", "significance"))
    main += _structured_section("Competing interpretations", _get(record, "competing_interpretations", "interpretations"))
    main += _structured_section("Positions changed", _get(record, "positions_changed", "position_changes"))
    main += _structured_section("Unresolved questions", _get(record, "unresolved_questions", "unknowns"))
    main += _structured_section("What to watch", _get(record, "watch_next", "watch_items"))
    main += _evidence_section(_get(record, "evidence", "evidence_spans", "supporting_evidence"))
    main += "</div><aside>"
    main += _feedback_form(identifier)
    main += _status_history(_get(record, "status_history", "history"))
    workspace_id = _get(record, "workspace_id")
    related = _get(record, "related_developments", "related_briefings")
    if workspace_id:
        main += (
            '<section class="side-panel"><h2>Workspace</h2>'
            f'<a class="text-link" href="{_escape(_url("/timeline", workspace_id=str(workspace_id)))}">Open timeline</a></section>'
        )
    main += _structured_section("Related developments", related, side=True)
    main += "</aside></div>"
    return _document(str(title), "/inbox", main)


def _narrative_section(title: str, value: Any) -> str:
    if value in (None, "", [], {}):
        return ""
    return f'<section class="brief-section"><h2>{_escape(title)}</h2><p>{_escape(value)}</p></section>'


def _structured_section(title: str, value: Any, *, side: bool = False) -> str:
    if value in (None, "", [], {}):
        return ""
    class_name = "side-panel" if side else "brief-section"
    return f'<section class="{class_name}"><h2>{_escape(title)}</h2>{_structured(value)}</section>'


def _structured(value: Any) -> str:
    public = _public_value(value)
    if isinstance(public, list):
        return '<ul class="clean-list">' + "".join(f"<li>{_structured(item)}</li>" for item in public) + "</ul>"
    if isinstance(public, Mapping):
        return '<dl class="detail-list">' + "".join(
            f"<div><dt>{_escape(_humanize(str(key)))}</dt><dd>{_structured(item)}</dd></div>"
            for key, item in public.items()
        ) + "</dl>"
    return _escape(public)


def _evidence_section(value: Any) -> str:
    evidence = _records(value, "evidence", "data", "items")
    if not evidence:
        return _structured_section("Supporting evidence", value)
    cards = []
    for item in evidence:
        excerpt = _get(item, "excerpt", "quote", "quote_text", "evidence_text", "text") or "Evidence span"
        source = _get(item, "source_name", "source_title", "publisher", "content_title") or "Source"
        support_kind = _get(item, "support_kind", "evidence_kind") or "attributed_claim"
        tier = _get(item, "source_tier", "trust_tier", "tier") or "Unclassified source"
        locator_value = _get(item, "locator", "timestamp", "page")
        if not locator_value and item.get("start_char") is not None and item.get("end_char") is not None:
            locator_value = f"characters {item['start_char']}–{item['end_char']}"
        locator_parts = [
            _get(item, "occurred_at", "published_at", "observed_at"),
            locator_value,
        ]
        locator = " · ".join(str(part) for part in locator_parts if part not in (None, ""))
        source_url = _get(item, "source_url", "url", "canonical_url")
        source_html = _escape(source)
        if _safe_web_url(source_url):
            source_html = f'<a href="{_escape(source_url)}" rel="noreferrer">{source_html}</a>'
        cards.append(
            '<article class="evidence-card">'
            f'<div class="evidence-meta"><span class="tier">{_escape(_humanize(str(support_kind)))}</span><span>{_escape(locator)}</span></div>'
            f"<blockquote>{_escape(excerpt)}</blockquote><p>{source_html} · {_escape(_humanize(str(tier)))}</p>"
            "</article>"
        )
    return '<section class="brief-section"><h2>Supporting evidence</h2><div class="evidence-list">' + "".join(cards) + "</div></section>"


def _feedback_form(briefing_id: str) -> str:
    options = "".join(
        f'<option value="{rating}">{_escape(_humanize(rating))}</option>' for rating in sorted(FEEDBACK_RATINGS)
    )
    return (
        '<section class="side-panel"><h2>Feedback</h2><p>Tune ranking without rewriting evidence.</p>'
        '<form class="feedback" method="post" action="/feedback">'
        f'<input type="hidden" name="briefing_id" value="{_escape(briefing_id)}">'
        f'<label for="feedback-rating">Assessment</label><select id="feedback-rating" name="rating">{options}</select>'
        '<button type="submit">Record feedback</button></form></section>'
    )


def _status_history(value: Any) -> str:
    records = _records(value, "status_history", "history", "data", "items")
    if not records:
        return ""
    items = "".join(
        f'<li>{_status_badge(str(_get(record, "status", "to_status") or "unknown"))}'
        f'<span>{_escape(_get(record, "changed_at", "created_at", "recorded_at") or "")}</span></li>'
        for record in records
    )
    return f'<section class="side-panel"><h2>Status history</h2><ol class="history">{items}</ol></section>'


def _positions(value: Any, *, entity_id: str | None) -> str:
    records = _records(value, "positions", "observations", "data", "items")
    if not entity_id:
        return _empty("Choose an entity", "Enter an entity identifier to inspect its evidence-backed positions.")
    if not records:
        return _empty("No positions", "No position observations were found for this entity.")
    cards = []
    for record in records:
        statement = _get(record, "position", "claim", "statement", "position_text") or "Position observation"
        stance = _get(record, "stance", "position_type") or "Observed"
        observed = _get(record, "observed_at", "event_time", "published_at") or "Time unknown"
        change = _get(record, "change_type", "position_change")
        if not change and record.get("changed_position"):
            change = "Position changed"
        evidence = _get(record, "evidence", "evidence_span", "excerpt")
        evidence_id = _get(record, "evidence_span_id")
        cards.append(
            '<article class="position-card">'
            f'<div class="card-top"><span class="tier">{_escape(_humanize(str(stance)))}</span><span class="meta">{_escape(observed)}</span></div>'
            f"<h2>{_escape(statement)}</h2>"
            + (f'<p class="change">{_escape(change)}</p>' if change else "")
            + (f'<blockquote>{_escape(evidence)}</blockquote>' if isinstance(evidence, str) else _structured(evidence) if evidence else "")
            + (f'<p class="meta">Evidence span: <code>{_escape(evidence_id)}</code></p>' if evidence_id else "")
            + "</article>"
        )
    return '<div class="position-list">' + "".join(cards) + "</div>"


def _source_cards(value: Any) -> str:
    records = _records(value, "sources", "data", "items")
    if not records:
        return _empty("No sources", "No sources match this workspace and status.")
    cards = []
    for record in records:
        name = _get(record, "name", "title", "source_name") or "Unnamed source"
        status = str(_get(record, "status") or "probation")
        tier = _get(record, "trust_tier", "source_tier", "tier") or "Unclassified"
        reason = _get(record, "discovery_reason", "reason") or ""
        contributions = _get(record, "contribution_count", "relevant_contributions", "content_count", "probation_sample_count", "sample_count")
        failures = _get(record, "consecutive_failures", "fetch_failures")
        last = _get(record, "last_contribution_at", "last_relevant_at", "last_success_at", "updated_at")
        url = _get(record, "url", "canonical_url", "feed_url")
        name_html = _escape(name)
        if _safe_web_url(url):
            name_html = f'<a href="{_escape(url)}" rel="noreferrer">{name_html}</a>'
        facts = {
            "Trust tier": _humanize(str(tier)),
            "Contributions": contributions,
            "Consecutive failures": failures,
            "Last contribution": last,
        }
        cards.append(
            '<article class="source-card">'
            f'<div class="card-top">{_status_badge(status)}<span class="tier">{_escape(_humanize(str(tier)))}</span></div>'
            f"<h2>{name_html}</h2><p>{_escape(reason)}</p>{_fact_list(facts)}"
            + _source_control(str(_get(record, "id", "source_id") or ""))
            + "</article>"
        )
    return '<div class="source-grid">' + "".join(cards) + "</div>"


def _operations(value: Any) -> str:
    public = _public_value(value)
    if public in (None, {}, []):
        return _empty("No operating state", "The local scheduler has not recorded a cycle yet.")
    if isinstance(public, Mapping):
        scalars = {str(key): item for key, item in public.items() if not isinstance(item, (Mapping, list))}
        nested = {str(key): item for key, item in public.items() if isinstance(item, (Mapping, list))}
        cards = "".join(
            f'<article class="metric"><span>{_escape(_humanize(key))}</span><strong>{_escape(item if item is not None else "—")}</strong></article>'
            for key, item in scalars.items()
        )
        sections = "".join(
            f'<section class="ops-section"><h2>{_escape(_humanize(key))}</h2>{_structured(item)}</section>'
            for key, item in nested.items()
        )
        return f'<div class="metrics">{cards}</div>{sections}'
    return _structured(public)


def _fact_list(values: Mapping[str, Any]) -> str:
    return '<dl class="facts">' + "".join(
        f'<div><dt>{_escape(key)}</dt><dd>{_escape(value if value not in (None, "") else "—")}</dd></div>'
        for key, value in values.items()
    ) + "</dl>"


def _source_control(source_id: str) -> str:
    if not source_id:
        return ""
    return (
        '<form class="source-control" method="post" action="/source-status">'
        f'<input type="hidden" name="source_id" value="{_escape(source_id)}">'
        '<input type="hidden" name="reason" value="private_source_ledger">'
        f'{_select("status", "Source control", "paused", (("paused", "Pause"), ("blocked", "Block")))}'
        '<button type="submit">Update source</button></form>'
    )


def _empty(title: str, message: str) -> str:
    return f'<div class="empty" role="status"><strong>{_escape(title)}</strong><span>{_escape(message)}</span></div>'


def _status_badge(status: str) -> str:
    normalized = status.lower().strip().replace("_", "-")
    css = normalized if normalized in {"provisional", "verified", "amended", "retracted", "active", "probation", "paused"} else "neutral"
    return f'<span class="status status-{css}">{_escape(_humanize(status))}</span>'


def _public_value(value: Any) -> Any:
    """Convert service results to JSON-safe values and drop private bodies."""

    if is_dataclass(value) and not isinstance(value, type):
        value = asdict(value)
    if isinstance(value, Mapping):
        return {
            str(key): _public_value(item)
            for key, item in value.items()
            if str(key).lower() not in _PRIVATE_BODY_FIELDS
        }
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_public_value(item) for item in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Enum):
        return _public_value(value.value)
    if isinstance(value, Path):
        return str(value)
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _mapping(value: Any) -> dict[str, Any]:
    public = _public_value(value)
    return dict(public) if isinstance(public, Mapping) else {}


def _records(value: Any, *container_keys: str) -> list[Mapping[str, Any]]:
    public = _public_value(value)
    if isinstance(public, list):
        return [item for item in public if isinstance(item, Mapping)]
    if isinstance(public, Mapping):
        for key in container_keys:
            nested = public.get(key)
            if isinstance(nested, list):
                return [item for item in nested if isinstance(item, Mapping)]
        if public and all(not isinstance(item, (Mapping, list)) for item in public.values()):
            return [public]
    return []


def _get(record: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in record and record[key] not in (None, ""):
            return record[key]
    return None


def _first_identifier(value: Any) -> str | None:
    records = _records(value, "workspaces", "data", "items")
    if not records:
        return None
    identifier = _get(records[0], "id", "workspace_id")
    return str(identifier) if identifier not in (None, "") else None


def _param(params: Mapping[str, Sequence[str]], name: str) -> str:
    raw = params.get(name, ())
    value = str(raw[0]) if raw else ""
    if len(value) > MAX_PARAM_CHARS:
        raise ValueError(f"{name} exceeds the {MAX_PARAM_CHARS}-character limit")
    return value.strip()


def _optional_param(params: Mapping[str, Sequence[str]], name: str) -> str | None:
    return _param(params, name) or None


def _required_param(params: Mapping[str, Sequence[str]], name: str) -> str:
    value = _param(params, name)
    if not value:
        raise ValueError(f"{name} is required")
    return value


def _bounded_value(value: Any, name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{name} is required")
    if len(text) > MAX_PARAM_CHARS:
        raise ValueError(f"{name} exceeds the {MAX_PARAM_CHARS}-character limit")
    return text


def _limit(params: Mapping[str, Sequence[str]]) -> int:
    raw = _param(params, "limit")
    if not raw:
        return DEFAULT_LIMIT
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError("limit must be an integer") from exc
    if not 1 <= value <= MAX_LIMIT:
        raise ValueError(f"limit must be between 1 and {MAX_LIMIT}")
    return value


def _bool_param(params: Mapping[str, Sequence[str]], name: str, *, default: bool) -> bool:
    raw = _param(params, name).lower()
    if not raw:
        return default
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be true or false")


def _path_identifier(path: str) -> str:
    parts = path.strip("/").split("/")
    if len(parts) != 3 or parts[0] != "api" or parts[1] not in {"briefs", "briefings"}:
        raise ValueError("invalid briefing route")
    return _bounded_value(unquote(parts[2]), "briefing_id")


def _entity_path_identifier(path: str) -> str:
    parts = path.strip("/").split("/")
    if len(parts) != 4 or parts[0:2] != ["api", "entities"] or parts[3] != "positions":
        raise ValueError("invalid entity positions route")
    return _bounded_value(unquote(parts[2]), "entity_id")


def _safe_web_url(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    parsed = urlsplit(value)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _url(path: str, **params: str) -> str:
    return path + "?" + urlencode(params, quote_via=quote)


def _humanize(value: str) -> str:
    return value.replace("_", " ").replace("-", " ").strip().title()


def _escape(value: Any) -> str:
    return html.escape(str(value), quote=True)


_STYLES = """
:root{color-scheme:light;--ink:#17201e;--muted:#68736f;--paper:#f4f4ed;--panel:#fff;--line:#d9ded8;--forest:#163d37;--accent:#1e665b;--accent-soft:#e5f1ed;--warm:#a85c32;--danger:#a03f3f;--shadow:0 14px 38px rgba(26,48,43,.08);font-family:ui-sans-serif,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}*{box-sizing:border-box}body{margin:0;background:var(--paper);color:var(--ink);line-height:1.55}.skip-link{position:absolute;left:-9999px;top:8px;background:var(--ink);color:#fff;padding:10px 14px;z-index:20}.skip-link:focus{left:8px}.site-header{display:flex;align-items:center;justify-content:space-between;gap:18px;padding:18px clamp(18px,4vw,54px);background:var(--forest);color:#fff}.brand{display:flex;align-items:center;gap:12px;color:#fff;text-decoration:none}.brand-mark{display:grid;place-items:center;width:42px;height:42px;border:1px solid #8db6ad;border-radius:50%;color:#daf1eb;font-family:ui-serif,Georgia,serif;font-size:22px}.brand strong,.brand small{display:block}.brand small{color:#bcd3ce}.trust{display:flex;align-items:center;gap:8px;color:#d9eee9;font-size:.88rem}.trust span{width:9px;height:9px;border-radius:50%;background:#82d3ad;box-shadow:0 0 0 4px rgba(130,211,173,.14)}.primary-nav{display:flex;gap:5px;overflow-x:auto;padding:9px clamp(14px,4vw,50px);background:#fff;border-bottom:1px solid var(--line)}.primary-nav a{display:flex;align-items:center;min-height:44px;padding:8px 14px;border-radius:9px;color:#43504c;text-decoration:none;white-space:nowrap;font-size:.92rem}.primary-nav a:hover,.primary-nav a:focus-visible{background:var(--accent-soft);color:var(--accent)}.primary-nav a[aria-current=page]{background:var(--accent);color:#fff}main{width:min(1260px,100%);min-height:72vh;margin:0 auto;padding:clamp(26px,5vw,60px)}.page-heading{max-width:780px;margin-bottom:26px}.eyebrow{margin:0 0 7px;color:var(--warm);font-size:.76rem;font-weight:760;letter-spacing:.13em;text-transform:uppercase}h1{margin:0;font-family:ui-serif,Georgia,serif;font-size:clamp(2.2rem,5vw,4rem);font-weight:520;line-height:1.05;letter-spacing:-.025em}h2{font-family:ui-serif,Georgia,serif;font-weight:570;line-height:1.18}.page-heading>p:last-child{color:var(--muted);font-size:1.05rem}.filters{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));align-items:end;gap:12px;margin:22px 0 28px;padding:17px;background:var(--panel);border:1px solid var(--line);border-radius:15px;box-shadow:var(--shadow)}.field{display:grid;gap:5px}.field-limit{max-width:130px}.field label,.feedback label,.source-control label{font-size:.78rem;font-weight:750;color:#4a5753}input,select,button{min-height:44px;border:1px solid #b9c3bf;border-radius:9px;font:inherit}input,select{width:100%;padding:9px 11px;background:#fff;color:var(--ink)}input:focus,select:focus,button:focus-visible,a:focus-visible{outline:3px solid rgba(30,102,91,.28);outline-offset:2px}button{padding:9px 18px;background:var(--accent);color:#fff;border-color:var(--accent);font-weight:720;cursor:pointer}.brief-grid,.source-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,330px),1fr));gap:16px}.brief-card,.source-card,.position-card,.metric,.side-panel,.brief-section,.ops-section{background:var(--panel);border:1px solid var(--line);border-radius:15px;box-shadow:var(--shadow)}.brief-card,.source-card,.position-card{padding:21px}.brief-card h2,.source-card h2,.position-card h2{margin:14px 0 10px;font-size:1.3rem}.brief-card h2 a,.source-card h2 a,.timeline h2 a{color:var(--ink);text-decoration:none}.brief-card h2 a:hover,.timeline h2 a:hover{color:var(--accent);text-decoration:underline}.brief-card>p,.source-card>p{color:#4e5b57}.card-top,.evidence-meta{display:flex;align-items:center;justify-content:space-between;gap:12px}.meta{color:var(--muted);font-size:.78rem}.status,.tier{display:inline-flex;align-items:center;width:max-content;padding:4px 9px;border-radius:999px;background:#edf0ed;color:#54605c;font-size:.7rem;font-weight:790;letter-spacing:.055em;text-transform:uppercase}.status-provisional,.status-probation{background:#fff0db;color:#8b531d}.status-verified,.status-active{background:#def2e8;color:#24614c}.status-amended{background:#e7ebf7;color:#465b8a}.status-retracted{background:#f7dddd;color:#8d3434}.status-paused{background:#ecebe8;color:#5d5a53}.text-link{color:var(--accent);font-weight:730;text-decoration:none}.text-link:hover{text-decoration:underline}.empty,.notice{padding:20px;border:1px solid #cbd8d3;border-radius:13px;background:var(--accent-soft)}.empty{display:grid;gap:4px;color:var(--muted)}.empty strong{color:var(--ink)}.timeline{position:relative;display:grid;gap:0;margin:30px 0;padding:0;list-style:none}.timeline:before{content:"";position:absolute;left:150px;top:5px;bottom:5px;width:1px;background:#c5cdc9}.timeline li{display:grid;grid-template-columns:130px 1fr;gap:42px;position:relative;padding:0 0 28px}.timeline li:before{content:"";position:absolute;left:144px;top:7px;width:13px;height:13px;border:3px solid var(--paper);border-radius:50%;background:var(--accent)}.timeline-time{color:var(--muted);font-size:.8rem;text-align:right}.timeline article{padding:0 0 16px}.timeline h2{margin:9px 0 5px;font-size:1.35rem}.timeline p{margin:0;color:#4f5b57}.brief-heading{display:flex;align-items:flex-end;justify-content:space-between;gap:30px;margin-bottom:28px}.brief-heading .status{margin-bottom:14px}.brief-time{color:var(--muted);white-space:nowrap}.brief-layout{display:grid;grid-template-columns:minmax(0,2fr) minmax(260px,1fr);align-items:start;gap:22px}.brief-main{display:grid;gap:18px}.brief-section,.side-panel,.ops-section{padding:clamp(18px,3vw,30px)}.brief-section h2,.side-panel h2,.ops-section h2{margin:0 0 13px;font-size:1.3rem}.brief-section>p{font-size:1.05rem}.brief-layout aside{display:grid;gap:16px;position:sticky;top:18px}.clean-list{display:grid;gap:10px;margin:0;padding-left:20px}.detail-list{display:grid;gap:10px;margin:0}.detail-list>div{display:grid;gap:3px}.detail-list dt,.facts dt{color:var(--muted);font-size:.72rem;font-weight:760;text-transform:uppercase}.detail-list dd,.facts dd{margin:0}.evidence-list{display:grid;gap:12px}.evidence-card{padding:17px;border-left:4px solid var(--accent);border-radius:4px 11px 11px 4px;background:#f7faf8}.evidence-card blockquote,.position-card blockquote{margin:14px 0;padding:0;color:#26332f;font-family:ui-serif,Georgia,serif;font-size:1.06rem}.evidence-card>p{margin:5px 0 0;color:var(--muted);font-size:.83rem}.evidence-card a,.source-card a{color:var(--accent)}.feedback{display:grid;gap:8px}.feedback button{margin-top:5px}.history{display:grid;gap:11px;margin:0;padding:0;list-style:none}.history li{display:flex;align-items:center;justify-content:space-between;gap:8px}.history li>span:last-child{color:var(--muted);font-size:.75rem}.position-list{display:grid;gap:14px}.position-card{max-width:850px}.position-card .change{color:var(--warm);font-weight:700}.facts{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin:18px 0 0}.facts div{min-width:0}.facts dd{overflow-wrap:anywhere}.source-control{display:grid;grid-template-columns:1fr auto;align-items:end;gap:10px;margin-top:18px;padding-top:16px;border-top:1px solid var(--line)}.source-control .field{min-width:0}.metrics{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:13px;margin-bottom:24px}.metric{display:grid;gap:9px;padding:19px}.metric span{color:var(--muted);font-size:.75rem;font-weight:760;text-transform:uppercase}.metric strong{font-family:ui-serif,Georgia,serif;font-size:1.7rem}.ops-section{margin-top:16px}.ops-section .detail-list{grid-template-columns:repeat(auto-fit,minmax(190px,1fr))}footer{padding:26px clamp(18px,4vw,54px);border-top:1px solid var(--line);color:var(--muted);font-size:.82rem}@media(max-width:760px){.site-header{align-items:flex-start;flex-direction:column}.trust{font-size:.8rem}main{padding:30px 14px}.filters{grid-template-columns:1fr}.field-limit{max-width:none}.brief-heading{align-items:flex-start;flex-direction:column}.brief-layout{grid-template-columns:1fr}.brief-layout aside{position:static}.timeline:before{left:6px}.timeline li{grid-template-columns:1fr;gap:6px;padding-left:30px}.timeline li:before{left:0}.timeline-time{text-align:left}.facts,.source-control{grid-template-columns:1fr}}
"""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Serve Research Radar privately on loopback.")
    parser.add_argument("--db", default=str(default_radar_db_path()), help="Canonical Research Radar SQLite path.")
    parser.add_argument("--host", default=DEFAULT_HOST, help="Loopback host only (default: 127.0.0.1).")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if os.environ.get("RAILWAY_ENVIRONMENT"):
        raise RuntimeError("Research Radar private content must not run on Railway")
    host = validate_loopback_host(args.host)
    database = Path(args.db).expanduser().resolve()
    # ResearchRadar owns idempotent initialization of a new canonical store.
    with radar_service(database) as service:
        service.operations_summary()
    server = create_server(database, host=host, port=args.port)
    address = server.server_address
    display_host = f"[{address[0]}]" if ":" in str(address[0]) else address[0]
    print(f"Research Radar: http://{display_host}:{address[1]}/inbox")
    try:
        server.serve_forever(poll_interval=0.25)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
