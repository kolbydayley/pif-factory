"""Loopback-only HTML explorer for private, accepted PIF intelligence.

This module is intentionally separate from the Railway observer.  Every page
opens the canonical SQLite database in read-only/query-only mode and delegates
all data selection to :class:`query_api.LocalQueryService`.
"""

from __future__ import annotations

import argparse
import html
import ipaddress
import json
import os
import socket
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence
from urllib.parse import parse_qs, urlencode, urlsplit

from .paths import db_path
from .query_api import LocalQueryService, open_read_only


DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8766
MAX_REQUEST_TARGET_BYTES = 8_192
MAX_PARAM_CHARS = 500
DEFAULT_LIMIT = 50
MAX_LIMIT = 200


@dataclass(frozen=True)
class Page:
    path: str
    key: str
    nav_label: str
    title: str
    description: str


PAGES = (
    Page("/search", "search", "Search", "Search", "Search current accepted claims, subjects, and people."),
    Page(
        "/subjects",
        "subjects",
        "Subjects / Propositions",
        "Subjects / Propositions",
        "Browse accepted subjects and follow their proposition and position lineage.",
    ),
    Page("/people", "people", "People", "People", "Browse accepted identities referenced by accepted intelligence."),
    Page(
        "/network",
        "networks",
        "Show / Network graph",
        "Show / Network graph",
        "Inspect people, shows, affiliations, positions, and accepted claim relations.",
    ),
    Page(
        "/consensus",
        "consensus",
        "Consensus / Contrarian",
        "Consensus / Contrarian timeline",
        "Review accepted consensus, contrarian, and relation records over time.",
    ),
    Page("/outcomes", "outcomes", "Outcomes", "Outcomes", "Review accepted forecast resolutions and scoring provenance."),
    Page("/quality", "quality", "Quality", "Quality", "Inspect accepted extraction, judgment, and pipeline quality summaries."),
    Page("/lineage", "lineage", "Lineage", "Lineage", "Trace a claim, subject, pipeline run, or corpus release."),
)
PAGE_BY_PATH = {page.path: page for page in PAGES}


def validate_loopback_host(host: str) -> str:
    """Return a normalized loopback host or reject the bind before opening it."""

    value = str(host or "").strip()
    if value.lower().rstrip(".") == "localhost":
        return "localhost"
    unbracketed = value[1:-1] if value.startswith("[") and value.endswith("]") else value
    try:
        address = ipaddress.ip_address(unbracketed)
    except ValueError as exc:
        raise ValueError("private UI host must be localhost or a loopback IP address") from exc
    if not address.is_loopback:
        raise ValueError("private UI refuses non-loopback binds")
    return str(address)


def _is_loopback_address(value: str) -> bool:
    try:
        return ipaddress.ip_address(str(value).split("%", 1)[0]).is_loopback
    except ValueError:
        return False


@contextmanager
def private_query_service(database_path: str | Path) -> Iterator[LocalQueryService]:
    """Yield the strict current-accepted query service over a read-only DB."""

    with open_read_only(database_path) as conn:
        service = LocalQueryService(conn, enforce_query_only=True, allow_legacy=False)
        yield service


class PrivateUIHTTPServer(ThreadingHTTPServer):
    """HTTP server that can only bind to and accept loopback addresses."""

    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, server_address: tuple[str, int], database_path: str | Path):
        host = validate_loopback_host(server_address[0])
        port = int(server_address[1])
        if not 0 <= port <= 65_535:
            raise ValueError("port must be between 0 and 65535")
        if ":" in host:
            self.address_family = socket.AF_INET6
        self.database_path = Path(database_path).expanduser().resolve()
        super().__init__((host, port), PrivateUIHandler)

    def verify_request(self, request: Any, client_address: tuple[Any, ...]) -> bool:
        return bool(client_address) and _is_loopback_address(str(client_address[0]))


def create_server(
    database_path: str | Path,
    *,
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
) -> PrivateUIHTTPServer:
    """Create, but do not start, a loopback-only private UI server."""

    normalized = validate_loopback_host(host)
    return PrivateUIHTTPServer((normalized, int(port)), database_path)


class PrivateUIHandler(BaseHTTPRequestHandler):
    server: PrivateUIHTTPServer

    def do_GET(self) -> None:  # noqa: N802 - stdlib handler API
        self._handle(include_body=True)

    def do_HEAD(self) -> None:  # noqa: N802 - stdlib handler API
        self._handle(include_body=False)

    def do_POST(self) -> None:  # noqa: N802 - stdlib handler API
        self._method_not_allowed()

    def do_PUT(self) -> None:  # noqa: N802 - stdlib handler API
        self._method_not_allowed()

    def do_PATCH(self) -> None:  # noqa: N802 - stdlib handler API
        self._method_not_allowed()

    def do_DELETE(self) -> None:  # noqa: N802 - stdlib handler API
        self._method_not_allowed()

    def log_message(self, format: str, *args: Any) -> None:
        # Request targets can contain private search terms; do not copy them to logs.
        return

    def _handle(self, *, include_body: bool) -> None:
        if len(self.path.encode("utf-8", errors="ignore")) > MAX_REQUEST_TARGET_BYTES:
            self._send_html(
                HTTPStatus.REQUEST_URI_TOO_LONG,
                render_error_page("Request too long", "The private UI rejected an oversized request."),
                include_body=include_body,
            )
            return
        parsed = urlsplit(self.path)
        if parsed.path == "/":
            self.send_response(HTTPStatus.SEE_OTHER)
            self.send_header("Location", "/search")
            self._security_headers()
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if parsed.path == "/healthz":
            try:
                with private_query_service(self.server.database_path) as service:
                    payload = {
                        "ok": True,
                        "privacy": "local_private_read_only",
                        "railway_observer_data_used": False,
                        "capabilities": service.capabilities(),
                    }
                self._send_json(HTTPStatus.OK, payload, include_body=include_body)
            except (OSError, sqlite3.Error) as exc:
                self._send_json(
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    {"ok": False, "error": type(exc).__name__},
                    include_body=include_body,
                )
            return
        if parsed.path not in PAGE_BY_PATH:
            self._send_html(
                HTTPStatus.NOT_FOUND,
                render_error_page("Not found", "That private intelligence page does not exist."),
                include_body=include_body,
            )
            return
        try:
            params = parse_qs(parsed.query, keep_blank_values=True, max_num_fields=20)
            with private_query_service(self.server.database_path) as service:
                document = render_route(service, parsed.path, params)
            self._send_html(HTTPStatus.OK, document, include_body=include_body)
        except ValueError as exc:
            self._send_html(
                HTTPStatus.BAD_REQUEST,
                render_error_page("Invalid request", str(exc)),
                include_body=include_body,
            )
        except (OSError, sqlite3.Error) as exc:
            self._send_html(
                HTTPStatus.SERVICE_UNAVAILABLE,
                render_error_page(
                    "Private database unavailable",
                    f"The read-only query could not be completed ({type(exc).__name__}).",
                ),
                include_body=include_body,
            )

    def _method_not_allowed(self) -> None:
        body = render_error_page("Read only", "This local interface accepts GET and HEAD requests only.")
        payload = body.encode("utf-8")
        self.send_response(HTTPStatus.METHOD_NOT_ALLOWED)
        self.send_header("Allow", "GET, HEAD")
        self._security_headers()
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _send_html(self, status: HTTPStatus, document: str, *, include_body: bool) -> None:
        payload = document.encode("utf-8")
        self.send_response(status)
        self._security_headers()
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        if include_body:
            self.wfile.write(payload)

    def _send_json(self, status: HTTPStatus, value: Mapping[str, Any], *, include_body: bool) -> None:
        payload = (json.dumps(value, ensure_ascii=True, sort_keys=True) + "\n").encode("utf-8")
        self.send_response(status)
        self._security_headers()
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        if include_body:
            self.wfile.write(payload)

    def _security_headers(self) -> None:
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Security-Policy", "default-src 'none'; style-src 'unsafe-inline'; form-action 'self'; base-uri 'none'; frame-ancestors 'none'")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")


def render_route(
    service: LocalQueryService,
    route: str,
    params: Mapping[str, Sequence[str]],
) -> str:
    """Render one known route using only the strict QueryService surface."""

    page = PAGE_BY_PATH.get(route)
    if page is None:
        raise ValueError("unknown private UI route")
    limit = _param_limit(params)
    if page.key == "search":
        query = _param(params, "q")
        result = service.search(query, limit=limit)
        form = _form(route, params, (("q", "Search", "Claims, subjects, or people"),))
    elif page.key == "subjects":
        query = _param(params, "q")
        result = service.subjects(query=query or None, limit=limit)
        form = _form(route, params, (("q", "Filter", "Subject or domain"),))
    elif page.key == "people":
        query = _param(params, "q")
        as_of = _optional_param(params, "as_of")
        observed_from = _optional_param(params, "observed_from")
        observed_through = _optional_param(params, "observed_through")
        minimum = _minimum_resolved_claims(params)
        people = service.people(query=query or None, limit=limit)
        rankings = service.accuracy_rankings(
            as_of=as_of,
            observed_from=observed_from,
            observed_through=observed_through,
            minimum_resolved_claims=minimum,
            limit=limit,
        )
        result = {
            "ok": True,
            "surface": "people",
            "privacy": "local_private_read_only",
            "data": {
                "people": people.get("data"),
                "accuracy_rankings": rankings.get("data"),
            },
            "unavailable": rankings.get("unavailable") or people.get("unavailable"),
        }
        form = _form(
            route,
            params,
            (
                ("q", "Filter", "Person name"),
                ("as_of", "As of", "ISO-8601 knowledge cutoff"),
                ("observed_from", "Claims from", "ISO-8601 timestamp"),
                ("observed_through", "Claims through", "ISO-8601 timestamp"),
                ("minimum_resolved_claims", "Minimum resolved", "10"),
            ),
        )
    elif page.key == "networks":
        person_id = _optional_param(params, "person_id")
        as_of = _optional_param(params, "as_of")
        observed_from = _optional_param(params, "observed_from")
        observed_through = _optional_param(params, "observed_through")
        detail = service.networks(
            person_id=person_id,
            source_id=_optional_param(params, "source_id"),
            claim_id=_optional_param(params, "claim_id"),
            limit=limit,
        )
        graphs = service.graphs(as_of=as_of, limit=limit)
        data: dict[str, Any] = {
            "network_detail": detail.get("data"),
            "accepted_graphs": graphs.get("data"),
        }
        unavailable = detail.get("unavailable") or graphs.get("unavailable")
        if person_id:
            history = service.accuracy_history(
                person_id,
                as_of=as_of,
                observed_from=observed_from,
                observed_through=observed_through,
            )
            contrarian = service.contrarian_success(
                person_id,
                as_of=as_of,
                observed_from=observed_from,
                observed_through=observed_through,
            )
            data["person_accuracy_history"] = history.get("data")
            data["person_contrarian_success"] = contrarian.get("data")
            unavailable = unavailable or history.get("unavailable") or contrarian.get("unavailable")
        result = {
            "ok": True,
            "surface": "networks",
            "privacy": "local_private_read_only",
            "data": data,
            "unavailable": unavailable,
        }
        form = _form(
            route,
            params,
            (
                ("person_id", "Person ID", "person_…"),
                ("source_id", "Show / source ID", "source_…"),
                ("claim_id", "Claim ID", "claim_…"),
                ("as_of", "As of", "ISO-8601 knowledge cutoff"),
                ("observed_from", "Claims from", "ISO-8601 timestamp"),
                ("observed_through", "Claims through", "ISO-8601 timestamp"),
            ),
        )
    elif page.key == "consensus":
        result = service.consensus(claim_id=_optional_param(params, "claim_id"), limit=limit)
        form = _form(route, params, (("claim_id", "Claim ID", "Optional claim filter"),))
    elif page.key == "outcomes":
        result = service.outcomes(
            claim_id=_optional_param(params, "claim_id"),
            person_id=_optional_param(params, "person_id"),
            limit=limit,
        )
        form = _form(
            route,
            params,
            (("claim_id", "Claim ID", "Optional claim filter"), ("person_id", "Person ID", "Optional person filter")),
        )
    elif page.key == "quality":
        result = service.quality(limit=limit)
        form = _form(route, params, ())
    else:
        identifier = _param(params, "id")
        kind = _param(params, "kind") or "claim"
        allowed_kinds = {"claim", "atomic_claim", "subject", "pipeline_run", "corpus_release"}
        if kind not in allowed_kinds:
            raise ValueError("lineage kind is not supported")
        result = (
            service.lineage(identifier, kind=kind, limit=limit)
            if identifier
            else {
                "ok": True,
                "surface": "lineage",
                "privacy": "local_private_read_only",
                "data": {},
            }
        )
        form = _lineage_form(route, params, kind)
    return render_document(page, result, form=form)


def render_document(page: Page, result: Mapping[str, Any], *, form: str = "") -> str:
    unavailable = result.get("unavailable")
    notice = (
        f'<p class="notice" role="status">{_escape(unavailable)}</p>'
        if unavailable
        else ""
    )
    content = _render_data(result.get("data"), surface=page.key, section=page.key)
    return _document(
        title=page.title,
        active_path=page.path,
        main=(
            '<div class="page-heading">'
            f'<p class="eyebrow">Private intelligence</p><h1>{_escape(page.title)}</h1>'
            f'<p>{_escape(page.description)}</p></div>'
            f"{form}{notice}{content}"
        ),
    )


def render_error_page(title: str, message: str) -> str:
    return _document(
        title=title,
        active_path="",
        main=(
            '<div class="page-heading"><p class="eyebrow">Private intelligence</p>'
            f'<h1>{_escape(title)}</h1><p role="alert">{_escape(message)}</p></div>'
        ),
    )


def _document(*, title: str, active_path: str, main: str) -> str:
    nav = "".join(
        (
            f'<a href="{page.path}" aria-current="page">{_escape(page.nav_label)}</a>'
            if page.path == active_path
            else f'<a href="{page.path}">{_escape(page.nav_label)}</a>'
        )
        for page in PAGES
    )
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{_escape(title)} · PIF Private</title>
  <style>{_STYLES}</style>
</head>
<body>
  <a class="skip-link" href="#main">Skip to content</a>
  <header class="site-header">
    <div class="brand"><span class="brand-mark" aria-hidden="true">P</span><span><strong>Podcast Intelligence</strong><small>Local private explorer</small></span></div>
    <div class="trust" aria-label="Privacy status"><span aria-hidden="true"></span> Loopback · Read only</div>
  </header>
  <nav class="primary-nav" aria-label="Primary">{nav}</nav>
  <main id="main" tabindex="-1">{main}</main>
  <footer>Current accepted intelligence only · SQLite remains authoritative · No Railway observer data</footer>
</body>
</html>"""


def _form(
    action: str,
    params: Mapping[str, Sequence[str]],
    fields: Sequence[tuple[str, str, str]],
) -> str:
    controls = "".join(
        '<div class="field">'
        f'<label for="filter-{_escape(name)}">{_escape(label)}</label>'
        f'<input id="filter-{_escape(name)}" name="{_escape(name)}" value="{_escape(_param(params, name))}" '
        f'placeholder="{_escape(placeholder)}" autocomplete="off"></div>'
        for name, label, placeholder in fields
    )
    limit = _param(params, "limit") or str(DEFAULT_LIMIT)
    controls += (
        '<div class="field field-limit"><label for="filter-limit">Limit</label>'
        f'<input id="filter-limit" name="limit" type="number" min="1" max="{MAX_LIMIT}" value="{_escape(limit)}"></div>'
    )
    return (
        f'<form class="filters" method="get" action="{_escape(action)}" aria-label="Page filters">'
        f'{controls}<button type="submit">Apply</button></form>'
    )


def _lineage_form(
    action: str,
    params: Mapping[str, Sequence[str]],
    selected_kind: str,
) -> str:
    options = "".join(
        f'<option value="{kind}"{(" selected" if kind == selected_kind else "")}>{_escape(label)}</option>'
        for kind, label in (
            ("claim", "Claim"),
            ("subject", "Subject"),
            ("pipeline_run", "Pipeline run"),
            ("corpus_release", "Corpus release"),
        )
    )
    return (
        f'<form class="filters" method="get" action="{_escape(action)}" aria-label="Lineage lookup">'
        '<div class="field"><label for="lineage-id">Identifier</label>'
        f'<input id="lineage-id" name="id" value="{_escape(_param(params, "id"))}" required autocomplete="off"></div>'
        '<div class="field"><label for="lineage-kind">Kind</label>'
        f'<select id="lineage-kind" name="kind">{options}</select></div>'
        '<div class="field field-limit"><label for="lineage-limit">Limit</label>'
        f'<input id="lineage-limit" name="limit" type="number" min="1" max="{MAX_LIMIT}" value="{_escape(_param(params, "limit") or str(DEFAULT_LIMIT))}"></div>'
        '<button type="submit">Trace lineage</button></form>'
    )


def _render_data(value: Any, *, surface: str, section: str) -> str:
    if value is None or value == [] or value == {}:
        return '<div class="empty" role="status"><strong>No accepted records</strong><span>Adjust the filters or check release and pipeline lineage.</span></div>'
    if isinstance(value, list):
        if all(isinstance(item, Mapping) for item in value):
            return _render_table(value, surface=surface, section=section)
        return '<ul class="value-list">' + "".join(
            f'<li>{_render_value("value", item, surface=surface, section=section, row={{}})}</li>'
            for item in value
        ) + "</ul>"
    if isinstance(value, Mapping):
        scalar_items = {key: item for key, item in value.items() if not isinstance(item, (Mapping, list))}
        nested_items = {key: item for key, item in value.items() if isinstance(item, (Mapping, list))}
        parts: list[str] = []
        if scalar_items:
            parts.append(_render_definition_list(scalar_items, surface=surface, section=section))
        for key, item in nested_items.items():
            parts.append(
                f'<section class="result-section"><h2>{_escape(_humanize(key))}</h2>'
                f'{_render_data(item, surface=surface, section=str(key))}</section>'
            )
        return "".join(parts) or '<div class="empty" role="status">No accepted records</div>'
    return f'<p class="scalar">{_render_value("value", value, surface=surface, section=section, row={{}})}</p>'


def _render_table(rows: Sequence[Mapping[str, Any]], *, surface: str, section: str) -> str:
    columns: list[str] = []
    for row in rows:
        for key in row:
            if key not in columns:
                columns.append(str(key))
    headings = "".join(f'<th scope="col">{_escape(_humanize(column))}</th>' for column in columns)
    body = "".join(
        "<tr>"
        + "".join(
            f'<td data-label="{_escape(_humanize(column))}">'
            f'{_render_value(column, row.get(column), surface=surface, section=section, row=row)}</td>'
            for column in columns
        )
        + "</tr>"
        for row in rows
    )
    caption = f"{_humanize(section)} — {len(rows)} record{'s' if len(rows) != 1 else ''}"
    return (
        '<div class="table-scroll" tabindex="0" role="region" aria-label="Scrollable results">'
        f'<table><caption>{_escape(caption)}</caption><thead><tr>{headings}</tr></thead><tbody>{body}</tbody></table></div>'
    )


def _render_definition_list(
    values: Mapping[str, Any],
    *,
    surface: str,
    section: str,
) -> str:
    return '<dl class="record">' + "".join(
        f'<div><dt>{_escape(_humanize(str(key)))}</dt><dd>'
        f'{_render_value(str(key), value, surface=surface, section=section, row=values)}</dd></div>'
        for key, value in values.items()
    ) + "</dl>"


def _render_value(
    key: str,
    value: Any,
    *,
    surface: str,
    section: str,
    row: Mapping[str, Any],
) -> str:
    if value is None or value == "":
        return '<span class="muted">—</span>'
    if isinstance(value, bool):
        return "Yes" if value else "No"
    if isinstance(value, (Mapping, list)):
        return _render_data(value, surface=surface, section=key)
    if isinstance(value, str) and key.endswith("_json"):
        try:
            parsed = json.loads(value)
        except (TypeError, json.JSONDecodeError):
            parsed = None
        if isinstance(parsed, (Mapping, list)):
            return (
                '<details><summary>Structured data</summary>'
                f'{_render_data(parsed, surface=surface, section=key)}</details>'
            )
    text = str(value)
    if _is_web_url(key, text):
        safe = _escape(text)
        return f'<a href="{safe}" rel="noreferrer">{safe}</a>'
    href = _identifier_href(key, text, surface=surface, section=section, row=row)
    escaped = _escape(text)
    if href:
        return f'<a class="identifier" href="{_escape(href)}"><code>{escaped}</code></a>'
    if key == "confidence" or key.endswith("_confidence") or key.endswith("_score"):
        try:
            return f"{float(value):.3f}"
        except (TypeError, ValueError):
            pass
    return escaped


def _identifier_href(
    key: str,
    value: str,
    *,
    surface: str,
    section: str,
    row: Mapping[str, Any],
) -> str | None:
    lowered = key.lower()
    if lowered in {"pipeline_run_id", "promotion_pipeline_run_id", "parent_run_id"}:
        return _url("/lineage", kind="pipeline_run", id=value)
    if lowered in {"corpus_release_id", "release_id", "parent_release_id"}:
        return _url("/lineage", kind="corpus_release", id=value)
    if lowered in {
        "claim_id",
        "atomic_claim_id",
        "source_claim_id",
        "target_claim_id",
        "focal_claim_id",
        "supersedes_claim_id",
    }:
        return _url("/lineage", kind="claim", id=value)
    if lowered in {"subject_id", "supersedes_subject_id"}:
        return _url("/lineage", kind="subject", id=value)
    if lowered in {"canonical_person_id", "person_id"}:
        return _url("/network", person_id=value)
    if lowered == "source_id":
        return _url("/network", source_id=value)
    if lowered == "id":
        result_type = str(row.get("result_type") or "")
        if surface == "search" and result_type in {"atomic_claim", "claim", "legacy_claim"}:
            return _url("/lineage", kind="claim", id=value)
        if surface == "search" and result_type == "subject" or surface == "subjects" or section == "subject":
            return _url("/lineage", kind="subject", id=value)
        if surface == "people":
            return _url("/network", person_id=value)
        if section in {"claim", "revisions"}:
            return _url("/lineage", kind="claim", id=value)
        if section in {"pipeline_run", "pipeline_runs"}:
            return _url("/lineage", kind="pipeline_run", id=value)
        if section in {"corpus_release", "corpus_releases"}:
            return _url("/lineage", kind="corpus_release", id=value)
    if lowered in {
        "evidence_unit_id",
        "discourse_event_id",
        "segment_id",
        "label_id",
        "episode_id",
        "transcript_id",
        "source_event_id",
        "raw_mention_id",
    }:
        claim_context = row.get("claim_id") or row.get("atomic_claim_id")
        if not claim_context and surface == "search" and row.get("result_type") == "atomic_claim":
            claim_context = row.get("id")
        if claim_context:
            return _url("/lineage", kind="claim", id=str(claim_context))
        return _url("/search", q=value)
    return None


def _is_web_url(key: str, value: str) -> bool:
    if key.lower() not in {"url", "source_url", "evidence_url", "canonical_url"}:
        return False
    parsed = urlsplit(value)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _url(path: str, **params: str) -> str:
    return path + "?" + urlencode(params)


def _param(params: Mapping[str, Sequence[str]], name: str) -> str:
    raw = params.get(name, ())
    value = str(raw[0]) if raw else ""
    if len(value) > MAX_PARAM_CHARS:
        raise ValueError(f"{name} exceeds the {MAX_PARAM_CHARS}-character limit")
    return value.strip()


def _optional_param(params: Mapping[str, Sequence[str]], name: str) -> str | None:
    return _param(params, name) or None


def _param_limit(params: Mapping[str, Sequence[str]]) -> int:
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


def _minimum_resolved_claims(params: Mapping[str, Sequence[str]]) -> int:
    raw = _param(params, "minimum_resolved_claims")
    if not raw:
        return 10
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError("minimum_resolved_claims must be an integer") from exc
    if value < 1:
        raise ValueError("minimum_resolved_claims must be positive")
    return value


def _humanize(value: str) -> str:
    return value.replace("_", " ").strip().title()


def _escape(value: Any) -> str:
    return html.escape(str(value), quote=True)


_STYLES = """
:root{color-scheme:light;--ink:#172022;--muted:#657276;--paper:#f7f5ef;--panel:#fff;--line:#d8ddd9;--accent:#165c54;--accent-soft:#e2f1ed;--warm:#bd6a32;--shadow:0 12px 36px rgba(24,39,40,.08);font-family:ui-sans-serif,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}*{box-sizing:border-box}body{margin:0;background:var(--paper);color:var(--ink);line-height:1.5}.skip-link{position:absolute;left:-9999px;top:8px;background:var(--ink);color:#fff;padding:10px 14px;z-index:20}.skip-link:focus{left:8px}.site-header{display:flex;align-items:center;justify-content:space-between;gap:18px;padding:18px clamp(18px,4vw,52px);background:#102d2b;color:#fff}.brand{display:flex;align-items:center;gap:12px}.brand-mark{display:grid;place-items:center;width:40px;height:40px;border:1px solid #8eb4ac;border-radius:12px;color:#d9eee9;font-family:ui-serif,Georgia,serif;font-size:22px}.brand strong,.brand small{display:block}.brand small{color:#b9cfca}.trust{display:flex;align-items:center;gap:8px;color:#d9eee9;font-size:.9rem}.trust span{width:9px;height:9px;border-radius:50%;background:#7dd3a9;box-shadow:0 0 0 4px rgba(125,211,169,.14)}.primary-nav{display:flex;gap:4px;overflow-x:auto;padding:10px clamp(14px,4vw,48px);background:#fff;border-bottom:1px solid var(--line);scrollbar-width:thin}.primary-nav a{display:flex;align-items:center;min-height:44px;padding:8px 13px;border-radius:9px;color:#435154;text-decoration:none;white-space:nowrap;font-size:.92rem}.primary-nav a:hover,.primary-nav a:focus-visible{background:var(--accent-soft);color:var(--accent)}.primary-nav a[aria-current=page]{background:var(--accent);color:#fff}main{width:min(1480px,100%);margin:0 auto;padding:clamp(24px,5vw,56px)}.page-heading{max-width:790px;margin-bottom:24px}.eyebrow{margin:0 0 6px;color:var(--warm);font-size:.78rem;font-weight:750;letter-spacing:.12em;text-transform:uppercase}h1{margin:0;font-family:ui-serif,Georgia,serif;font-size:clamp(2rem,5vw,3.8rem);font-weight:520;line-height:1.05;letter-spacing:-.025em}h2{margin:32px 0 12px;font-family:ui-serif,Georgia,serif;font-weight:550}.page-heading>p:last-child{color:var(--muted);font-size:1.05rem}.filters{display:grid;grid-template-columns:repeat(auto-fit,minmax(190px,1fr));align-items:end;gap:12px;margin:22px 0 26px;padding:18px;background:var(--panel);border:1px solid var(--line);border-radius:14px;box-shadow:var(--shadow)}.field{display:grid;gap:5px}.field-limit{max-width:140px}.field label{font-size:.8rem;font-weight:700;color:#465458}input,select,button{min-height:44px;border-radius:9px;border:1px solid #b9c2c0;font:inherit}input,select{width:100%;padding:9px 11px;background:#fff;color:var(--ink)}input:focus,select:focus,button:focus-visible,a:focus-visible{outline:3px solid rgba(22,92,84,.28);outline-offset:2px}button{padding:9px 18px;background:var(--accent);color:#fff;border-color:var(--accent);font-weight:700;cursor:pointer}.notice,.empty{padding:20px;border:1px solid #cbd8d5;border-radius:12px;background:var(--accent-soft)}.empty{display:grid;gap:4px;color:var(--muted)}.empty strong{color:var(--ink)}.result-section{margin-top:30px}.table-scroll{overflow:auto;margin:18px 0 28px;border:1px solid var(--line);border-radius:14px;background:var(--panel);box-shadow:var(--shadow)}table{width:100%;border-collapse:collapse;font-size:.9rem}caption{padding:14px 16px;text-align:left;color:var(--muted);font-weight:700;background:#f1f3ef}th,td{padding:11px 14px;border-top:1px solid #e5e8e5;text-align:left;vertical-align:top;max-width:420px;overflow-wrap:anywhere}th{position:sticky;top:0;background:#f8faf8;color:#536063;font-size:.73rem;letter-spacing:.055em;text-transform:uppercase}tbody tr:hover{background:#fbfcfa}code{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:.86em}.identifier code{color:var(--accent);text-decoration:underline;text-underline-offset:2px}.record{display:grid;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:1px;margin:16px 0;background:var(--line);border:1px solid var(--line);border-radius:12px;overflow:hidden}.record>div{min-width:0;padding:13px;background:#fff}.record dt{color:var(--muted);font-size:.73rem;font-weight:750;letter-spacing:.05em;text-transform:uppercase}.record dd{margin:5px 0 0;overflow-wrap:anywhere}.muted{color:#8a9596}.value-list{padding-left:22px}details{max-width:520px}summary{color:var(--accent);cursor:pointer}pre{max-width:100%;overflow:auto;white-space:pre-wrap}footer{padding:26px clamp(18px,4vw,52px);border-top:1px solid var(--line);color:var(--muted);font-size:.82rem}@media(max-width:680px){.site-header{align-items:flex-start;flex-direction:column}.trust{font-size:.82rem}main{padding:28px 14px}.filters{grid-template-columns:1fr}.field-limit{max-width:none}.table-scroll{border:0;background:transparent;box-shadow:none;overflow:visible}table,thead,tbody,tr,th,td{display:block}thead{position:absolute;width:1px;height:1px;overflow:hidden;clip:rect(0 0 0 0)}tbody{display:grid;gap:12px}tr{padding:8px 12px;border:1px solid var(--line);border-radius:12px;background:#fff;box-shadow:var(--shadow)}td{display:grid;grid-template-columns:minmax(110px,36%) 1fr;gap:10px;max-width:none;padding:8px 0;border-top:1px solid #edf0ed}td:first-child{border-top:0}td:before{content:attr(data-label);color:var(--muted);font-size:.69rem;font-weight:750;letter-spacing:.04em;text-transform:uppercase}.record{grid-template-columns:1fr}}
"""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Serve the private PIF intelligence UI on loopback only.")
    parser.add_argument("--db", default=str(db_path()), help="Canonical local SQLite database path.")
    parser.add_argument("--host", default=DEFAULT_HOST, help="Loopback host only (default: 127.0.0.1).")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if os.environ.get("RAILWAY_ENVIRONMENT"):
        raise RuntimeError("The private intelligence UI must not run on Railway")
    host = validate_loopback_host(args.host)
    database = Path(args.db).expanduser().resolve()
    if not database.is_file():
        raise FileNotFoundError(f"SQLite database does not exist: {database}")
    with private_query_service(database) as service:
        capabilities = service.capabilities()
        if not capabilities.get("read_only"):
            raise RuntimeError("private query service did not enter read-only mode")
    server = create_server(database, host=host, port=args.port)
    address = server.server_address
    display_host = f"[{address[0]}]" if ":" in str(address[0]) else address[0]
    print(f"PIF private UI: http://{display_host}:{address[1]}/search")
    try:
        server.serve_forever(poll_interval=0.25)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
