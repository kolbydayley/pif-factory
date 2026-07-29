from __future__ import annotations

import datetime as dt
import json
import os
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, Sequence

from .util import now_iso, sha256_text, stable_id


SCHEMA_VERSION = "research_radar_v1"
DEFAULT_WORKSPACE_NAME = "AI & Technology Radar"


class RadarError(RuntimeError):
    """Base error for the isolated Research Radar authority store."""


class RadarNotFoundError(RadarError):
    """Raised when a requested Radar record does not exist."""


class RadarTransitionError(RadarError):
    """Raised when an authority transition would violate the release contract."""


class RadarPolicyError(RadarError):
    """Raised when source, evidence, or budget policy is violated."""


class AuthorityStatus(str, Enum):
    CANDIDATE = "candidate"
    PROVISIONAL = "provisional"
    VERIFIED = "verified"
    AMENDED = "amended"
    RETRACTED = "retracted"
    DISMISSED = "dismissed"


class SourceTrustTier(str, Enum):
    PRIMARY = "primary"
    REPUTABLE_SECONDARY = "reputable_secondary"
    SPECIALIST_INDIVIDUAL = "specialist_individual"
    WEAK_SIGNAL = "weak_signal"


class SourceStatus(str, Enum):
    PROBATION = "probation"
    ACTIVE = "active"
    PAUSED = "paused"
    BLOCKED = "blocked"


class FeedbackRating(str, Enum):
    USEFUL = "useful"
    IRRELEVANT = "irrelevant"
    WRONG = "wrong"
    DUPLICATE = "duplicate"
    FOLLOW = "follow"


class PipelineStage(str, Enum):
    DISCOVER = "discover"
    FETCH = "fetch"
    NORMALIZE = "normalize"
    EXTRACT_EVIDENCE = "extract_evidence"
    RECONCILE_DOCUMENT = "reconcile_document"
    RECONCILE_GRAPH = "reconcile_graph"
    SCORE_SIGNIFICANCE = "score_significance"
    PUBLISH_PROVISIONAL = "publish_provisional"
    VERIFY = "verify_amend_retract"


PIPELINE_STAGES = tuple(stage.value for stage in PipelineStage)


@dataclass(frozen=True)
class PipelineBudgets:
    max_windows_per_item: int = 4
    max_input_tokens_per_item: int = 75_000
    max_item_seconds: int = 15 * 60
    max_items_per_cycle: int = 25
    max_cycle_seconds: int = 2 * 60 * 60
    max_schema_repairs_per_item: int = 1
    max_transport_retries_per_item: int = 1


@dataclass(frozen=True)
class PipelineContract:
    stages: tuple[str, ...] = PIPELINE_STAGES
    semantic_authority: str = "managed_auth_llm_only"
    deterministic_authority: tuple[str, ...] = (
        "fetching",
        "partitioning",
        "hashes",
        "exact_offsets",
        "schemas",
        "state_transitions",
        "lineage",
        "literal_and_structured_search",
    )
    embeddings_allowed: bool = False
    deterministic_semantic_matching_allowed: bool = False
    automatic_model_transport: bool = False


BUDGETS = PipelineBudgets()
PIPELINE_CONTRACT = PipelineContract()

AUTHORITY_VALUES = tuple(status.value for status in AuthorityStatus)
TRUST_TIER_VALUES = tuple(tier.value for tier in SourceTrustTier)
SOURCE_STATUS_VALUES = tuple(status.value for status in SourceStatus)
FEEDBACK_VALUES = tuple(rating.value for rating in FeedbackRating)

ALLOWED_TRANSITIONS: dict[str, frozenset[str]] = {
    AuthorityStatus.CANDIDATE.value: frozenset(
        {AuthorityStatus.PROVISIONAL.value, AuthorityStatus.DISMISSED.value}
    ),
    AuthorityStatus.PROVISIONAL.value: frozenset(
        {
            AuthorityStatus.VERIFIED.value,
            AuthorityStatus.AMENDED.value,
            AuthorityStatus.RETRACTED.value,
            AuthorityStatus.DISMISSED.value,
        }
    ),
    AuthorityStatus.VERIFIED.value: frozenset(
        {AuthorityStatus.AMENDED.value, AuthorityStatus.RETRACTED.value}
    ),
    AuthorityStatus.AMENDED.value: frozenset(
        {
            AuthorityStatus.VERIFIED.value,
            AuthorityStatus.AMENDED.value,
            AuthorityStatus.RETRACTED.value,
        }
    ),
    AuthorityStatus.RETRACTED.value: frozenset(),
    AuthorityStatus.DISMISSED.value: frozenset(),
}

ACCEPTED_STATUSES = (AuthorityStatus.VERIFIED.value, AuthorityStatus.AMENDED.value)

RECORD_TABLES = {
    "evidence_span": "evidence_spans",
    "entity": "entities",
    "topic": "topics",
    "development": "developments",
    "claim": "claims",
    "position_observation": "position_observations",
    "briefing": "briefings",
    "semantic_edge": "semantic_edges",
}

SEMANTIC_RECORD_TYPES = frozenset(
    {
        "entity",
        "topic",
        "development",
        "claim",
        "position_observation",
        "briefing",
        "semantic_edge",
    }
)

RELATION_TYPES = frozenset(
    {
        "about_topic",
        "asserts_claim",
        "concerns_entity",
        "has_position",
        "supports",
        "contradicts",
        "updates",
        "supersedes",
        "related_to",
        "reported_by",
    }
)


def default_radar_db_path() -> Path:
    """Return the Radar database path, deliberately outside Documents/FileProvider."""

    configured = os.environ.get("RESEARCH_RADAR_DB")
    if configured:
        return Path(configured).expanduser().resolve()
    return (
        Path.home()
        / "Library"
        / "Application Support"
        / "Research Radar"
        / "research-radar.sqlite3"
    ).resolve()


SCHEMA = f"""
CREATE TABLE IF NOT EXISTS radar_meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS workspaces (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  slug TEXT NOT NULL UNIQUE,
  topic TEXT NOT NULL,
  purpose TEXT NOT NULL,
  strategic_questions_json TEXT NOT NULL DEFAULT '[]',
  exclusions_json TEXT NOT NULL DEFAULT '[]',
  extension_schema_version TEXT NOT NULL DEFAULT 'ai_technology_v1',
  extension_schema_json TEXT NOT NULL DEFAULT '{{}}',
  status TEXT NOT NULL DEFAULT 'active' CHECK(status IN ('active','paused','archived')),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sources (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  canonical_url TEXT NOT NULL UNIQUE,
  source_type TEXT NOT NULL,
  trust_tier TEXT NOT NULL CHECK(trust_tier IN {TRUST_TIER_VALUES}),
  status TEXT NOT NULL DEFAULT 'probation' CHECK(status IN {SOURCE_STATUS_VALUES}),
  discovery_reason TEXT NOT NULL,
  probation_sample_count INTEGER NOT NULL DEFAULT 0 CHECK(probation_sample_count >= 0),
  consecutive_fetch_failures INTEGER NOT NULL DEFAULT 0 CHECK(consecutive_fetch_failures >= 0),
  last_fetched_at TEXT,
  last_relevant_at TEXT,
  paused_reason TEXT,
  metadata_json TEXT NOT NULL DEFAULT '{{}}',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS workspace_sources (
  workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
  source_id TEXT NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
  discovered_via_source_id TEXT REFERENCES sources(id),
  attached_at TEXT NOT NULL,
  PRIMARY KEY(workspace_id, source_id)
);

CREATE TABLE IF NOT EXISTS source_events (
  id TEXT PRIMARY KEY,
  source_id TEXT NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
  workspace_id TEXT REFERENCES workspaces(id) ON DELETE CASCADE,
  event_type TEXT NOT NULL,
  details_json TEXT NOT NULL DEFAULT '{{}}',
  recorded_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_source_events_type_time ON source_events(event_type, recorded_at);

CREATE TABLE IF NOT EXISTS content_items (
  id TEXT PRIMARY KEY,
  source_id TEXT NOT NULL REFERENCES sources(id),
  canonical_url TEXT NOT NULL UNIQUE,
  content_type TEXT NOT NULL,
  title TEXT NOT NULL,
  author TEXT,
  published_at TEXT,
  normalized_text TEXT NOT NULL,
  content_sha256 TEXT NOT NULL,
  token_count INTEGER NOT NULL DEFAULT 0 CHECK(token_count >= 0),
  fetch_status TEXT NOT NULL DEFAULT 'ready' CHECK(fetch_status IN ('queued','ready','unavailable','quarantined')),
  fetched_at TEXT,
  metadata_json TEXT NOT NULL DEFAULT '{{}}',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_content_source_published ON content_items(source_id, published_at DESC);

CREATE TABLE IF NOT EXISTS evidence_spans (
  id TEXT PRIMARY KEY,
  content_item_id TEXT NOT NULL REFERENCES content_items(id) ON DELETE CASCADE,
  start_char INTEGER NOT NULL CHECK(start_char >= 0),
  end_char INTEGER NOT NULL CHECK(end_char > start_char),
  quote_text TEXT NOT NULL,
  quote_sha256 TEXT NOT NULL,
  support_kind TEXT NOT NULL CHECK(support_kind IN ('primary_evidence','reputable_reporting','attributed_claim','inference','speculation')),
  status TEXT NOT NULL DEFAULT 'candidate' CHECK(status IN {AUTHORITY_VALUES}),
  confidence REAL NOT NULL CHECK(confidence >= 0 AND confidence <= 1),
  observed_at TEXT,
  extractor_version TEXT NOT NULL,
  release_id TEXT NOT NULL,
  lineage_json TEXT NOT NULL DEFAULT '{{}}',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(content_item_id, start_char, end_char, quote_sha256, extractor_version)
);

CREATE TABLE IF NOT EXISTS entities (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
  entity_type TEXT NOT NULL,
  canonical_name TEXT NOT NULL,
  aliases_json TEXT NOT NULL DEFAULT '[]',
  status TEXT NOT NULL DEFAULT 'candidate' CHECK(status IN {AUTHORITY_VALUES}),
  confidence REAL NOT NULL CHECK(confidence >= 0 AND confidence <= 1),
  evidence_span_id TEXT NOT NULL REFERENCES evidence_spans(id),
  observed_at TEXT,
  extractor_version TEXT NOT NULL,
  release_id TEXT NOT NULL,
  lineage_json TEXT NOT NULL DEFAULT '{{}}',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(workspace_id, entity_type, canonical_name)
);

CREATE TABLE IF NOT EXISTS topics (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  description TEXT,
  status TEXT NOT NULL DEFAULT 'candidate' CHECK(status IN {AUTHORITY_VALUES}),
  confidence REAL NOT NULL CHECK(confidence >= 0 AND confidence <= 1),
  evidence_span_id TEXT NOT NULL REFERENCES evidence_spans(id),
  observed_at TEXT,
  extractor_version TEXT NOT NULL,
  release_id TEXT NOT NULL,
  lineage_json TEXT NOT NULL DEFAULT '{{}}',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(workspace_id, name)
);

CREATE TABLE IF NOT EXISTS developments (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
  topic_id TEXT REFERENCES topics(id),
  title TEXT NOT NULL,
  summary TEXT NOT NULL,
  significance REAL NOT NULL DEFAULT 0 CHECK(significance >= 0 AND significance <= 1),
  event_at TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'candidate' CHECK(status IN {AUTHORITY_VALUES}),
  confidence REAL NOT NULL CHECK(confidence >= 0 AND confidence <= 1),
  evidence_span_id TEXT NOT NULL REFERENCES evidence_spans(id),
  observed_at TEXT,
  extractor_version TEXT NOT NULL,
  release_id TEXT NOT NULL,
  lineage_json TEXT NOT NULL DEFAULT '{{}}',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_developments_workspace_event ON developments(workspace_id, event_at DESC);

CREATE TABLE IF NOT EXISTS claims (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
  development_id TEXT REFERENCES developments(id),
  subject_entity_id TEXT REFERENCES entities(id),
  claim_type TEXT NOT NULL,
  claim_text TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'candidate' CHECK(status IN {AUTHORITY_VALUES}),
  confidence REAL NOT NULL CHECK(confidence >= 0 AND confidence <= 1),
  evidence_span_id TEXT NOT NULL REFERENCES evidence_spans(id),
  observed_at TEXT,
  extractor_version TEXT NOT NULL,
  release_id TEXT NOT NULL,
  lineage_json TEXT NOT NULL DEFAULT '{{}}',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_claims_development ON claims(development_id, observed_at DESC);

CREATE TABLE IF NOT EXISTS position_observations (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
  entity_id TEXT NOT NULL REFERENCES entities(id),
  claim_id TEXT REFERENCES claims(id),
  development_id TEXT REFERENCES developments(id),
  stance TEXT NOT NULL,
  position_text TEXT NOT NULL,
  prior_position_observation_id TEXT REFERENCES position_observations(id),
  changed_position INTEGER NOT NULL DEFAULT 0 CHECK(changed_position IN (0,1)),
  status TEXT NOT NULL DEFAULT 'candidate' CHECK(status IN {AUTHORITY_VALUES}),
  confidence REAL NOT NULL CHECK(confidence >= 0 AND confidence <= 1),
  evidence_span_id TEXT NOT NULL REFERENCES evidence_spans(id),
  observed_at TEXT NOT NULL,
  extractor_version TEXT NOT NULL,
  release_id TEXT NOT NULL,
  lineage_json TEXT NOT NULL DEFAULT '{{}}',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_positions_entity_time ON position_observations(entity_id, observed_at DESC);

CREATE TABLE IF NOT EXISTS briefings (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
  development_id TEXT NOT NULL REFERENCES developments(id),
  headline TEXT NOT NULL,
  what_changed TEXT NOT NULL,
  why_it_matters TEXT NOT NULL,
  competing_interpretations_json TEXT NOT NULL DEFAULT '[]',
  position_changes_json TEXT NOT NULL DEFAULT '[]',
  unresolved_questions_json TEXT NOT NULL DEFAULT '[]',
  watch_next_json TEXT NOT NULL DEFAULT '[]',
  priority TEXT NOT NULL DEFAULT 'normal' CHECK(priority IN ('low','normal','high')),
  status TEXT NOT NULL DEFAULT 'candidate' CHECK(status IN {AUTHORITY_VALUES}),
  confidence REAL NOT NULL CHECK(confidence >= 0 AND confidence <= 1),
  evidence_span_id TEXT NOT NULL REFERENCES evidence_spans(id),
  event_at TEXT NOT NULL,
  observed_at TEXT,
  extractor_version TEXT NOT NULL,
  release_id TEXT NOT NULL,
  lineage_json TEXT NOT NULL DEFAULT '{{}}',
  published_at TEXT,
  read_at TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_briefings_inbox ON briefings(workspace_id, status, event_at DESC);

CREATE TABLE IF NOT EXISTS semantic_edges (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
  from_type TEXT NOT NULL,
  from_id TEXT NOT NULL,
  relation_type TEXT NOT NULL CHECK(relation_type IN {tuple(sorted(RELATION_TYPES))}),
  to_type TEXT NOT NULL,
  to_id TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'candidate' CHECK(status IN {AUTHORITY_VALUES}),
  confidence REAL NOT NULL CHECK(confidence >= 0 AND confidence <= 1),
  evidence_span_id TEXT NOT NULL REFERENCES evidence_spans(id),
  observed_at TEXT NOT NULL,
  extractor_version TEXT NOT NULL,
  release_id TEXT NOT NULL,
  lineage_json TEXT NOT NULL DEFAULT '{{}}',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(workspace_id, from_type, from_id, relation_type, to_type, to_id, evidence_span_id)
);

CREATE TABLE IF NOT EXISTS semantic_evidence (
  record_type TEXT NOT NULL CHECK(record_type IN {tuple(sorted(SEMANTIC_RECORD_TYPES))}),
  record_id TEXT NOT NULL,
  evidence_span_id TEXT NOT NULL REFERENCES evidence_spans(id) ON DELETE CASCADE,
  role TEXT NOT NULL CHECK(role IN ('supports','contradicts','context')),
  directly_establishes INTEGER NOT NULL DEFAULT 0 CHECK(directly_establishes IN (0,1)),
  material_statement INTEGER NOT NULL DEFAULT 1 CHECK(material_statement IN (0,1)),
  linked_at TEXT NOT NULL,
  PRIMARY KEY(record_type, record_id, evidence_span_id, role)
);
CREATE INDEX IF NOT EXISTS idx_semantic_evidence_record ON semantic_evidence(record_type, record_id);

CREATE TABLE IF NOT EXISTS authority_events (
  id TEXT PRIMARY KEY,
  record_type TEXT NOT NULL CHECK(record_type IN {tuple(sorted(RECORD_TABLES))}),
  record_id TEXT NOT NULL,
  from_status TEXT CHECK(from_status IS NULL OR from_status IN {AUTHORITY_VALUES}),
  to_status TEXT NOT NULL CHECK(to_status IN {AUTHORITY_VALUES}),
  reason TEXT NOT NULL,
  release_id TEXT,
  recorded_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_authority_record ON authority_events(record_type, record_id, recorded_at);

CREATE TABLE IF NOT EXISTS feedback (
  id TEXT PRIMARY KEY,
  briefing_id TEXT NOT NULL REFERENCES briefings(id) ON DELETE CASCADE,
  rating TEXT NOT NULL CHECK(rating IN {FEEDBACK_VALUES}),
  note TEXT,
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_feedback_briefing ON feedback(briefing_id, created_at DESC);

CREATE TABLE IF NOT EXISTS pipeline_items (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
  content_item_id TEXT REFERENCES content_items(id),
  stage TEXT NOT NULL CHECK(stage IN {PIPELINE_STAGES}),
  status TEXT NOT NULL DEFAULT 'queued' CHECK(status IN ('queued','in_progress','succeeded','dead_letter')),
  idempotency_key TEXT NOT NULL UNIQUE,
  input_tokens INTEGER NOT NULL DEFAULT 0 CHECK(input_tokens >= 0),
  window_count INTEGER NOT NULL DEFAULT 0 CHECK(window_count >= 0),
  schema_repairs INTEGER NOT NULL DEFAULT 0 CHECK(schema_repairs >= 0),
  transport_retries INTEGER NOT NULL DEFAULT 0 CHECK(transport_retries >= 0),
  wall_seconds REAL NOT NULL DEFAULT 0 CHECK(wall_seconds >= 0),
  attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts >= 0),
  available_at TEXT NOT NULL,
  claimed_at TEXT,
  completed_at TEXT,
  last_error TEXT,
  metadata_json TEXT NOT NULL DEFAULT '{{}}',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_pipeline_claim ON pipeline_items(status, available_at, created_at);

CREATE TABLE IF NOT EXISTS operations (
  id TEXT PRIMARY KEY,
  workspace_id TEXT REFERENCES workspaces(id) ON DELETE CASCADE,
  kind TEXT NOT NULL,
  status TEXT NOT NULL,
  item_count INTEGER NOT NULL DEFAULT 0,
  input_tokens INTEGER NOT NULL DEFAULT 0,
  wall_seconds REAL NOT NULL DEFAULT 0,
  details_json TEXT NOT NULL DEFAULT '{{}}',
  started_at TEXT NOT NULL,
  completed_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_operations_kind_time ON operations(kind, started_at DESC);

CREATE TABLE IF NOT EXISTS dead_letters (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
  content_item_id TEXT REFERENCES content_items(id),
  pipeline_item_id TEXT REFERENCES pipeline_items(id),
  stage TEXT NOT NULL CHECK(stage IN {PIPELINE_STAGES}),
  error_class TEXT NOT NULL,
  error_message TEXT NOT NULL,
  attempt_count INTEGER NOT NULL,
  status TEXT NOT NULL DEFAULT 'open' CHECK(status IN ('open','resolved','dismissed')),
  details_json TEXT NOT NULL DEFAULT '{{}}',
  created_at TEXT NOT NULL,
  resolved_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_dead_letters_status ON dead_letters(status, created_at DESC);

CREATE VIEW IF NOT EXISTS accepted_evidence_spans AS
  SELECT * FROM evidence_spans WHERE status IN ('verified','amended');
CREATE VIEW IF NOT EXISTS accepted_entities AS
  SELECT * FROM entities WHERE status IN ('verified','amended');
CREATE VIEW IF NOT EXISTS accepted_topics AS
  SELECT * FROM topics WHERE status IN ('verified','amended');
CREATE VIEW IF NOT EXISTS accepted_developments AS
  SELECT * FROM developments WHERE status IN ('verified','amended');
CREATE VIEW IF NOT EXISTS accepted_claims AS
  SELECT * FROM claims WHERE status IN ('verified','amended');
CREATE VIEW IF NOT EXISTS accepted_position_observations AS
  SELECT * FROM position_observations WHERE status IN ('verified','amended');
CREATE VIEW IF NOT EXISTS accepted_briefings AS
  SELECT * FROM briefings WHERE status IN ('verified','amended');
CREATE VIEW IF NOT EXISTS accepted_semantic_edges AS
  SELECT * FROM semantic_edges WHERE status IN ('verified','amended');
"""


JSON_COLUMNS = frozenset(
    {
        "strategic_questions_json",
        "exclusions_json",
        "extension_schema_json",
        "metadata_json",
        "aliases_json",
        "lineage_json",
        "competing_interpretations_json",
        "position_changes_json",
        "unresolved_questions_json",
        "watch_next_json",
        "details_json",
    }
)


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _identifier(prefix: str, *parts: str) -> str:
    if parts and all(part for part in parts):
        return stable_id(*parts, prefix=f"{prefix}_")
    return f"{prefix}_{uuid.uuid4().hex}"


def _as_status(value: str | AuthorityStatus) -> str:
    status = value.value if isinstance(value, AuthorityStatus) else str(value)
    if status not in AUTHORITY_VALUES:
        raise RadarPolicyError(f"unsupported authority status: {status}")
    return status


def _as_mapping(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    result: dict[str, Any] = dict(row)
    for key in JSON_COLUMNS.intersection(result):
        raw = result[key]
        try:
            result[key[:-5] if key.endswith("_json") else key] = json.loads(raw or "null")
        except (TypeError, json.JSONDecodeError):
            result[key[:-5] if key.endswith("_json") else key] = None
        del result[key]
    for key in ("changed_position", "directly_establishes", "material_statement"):
        if key in result:
            result[key] = bool(result[key])
    return result


class ResearchRadar:
    """Isolated, local-first persistence and deterministic authority core for Radar v1."""

    def __init__(self, db_path: str | Path | None = None) -> None:
        self.db_path = Path(db_path or default_radar_db_path()).expanduser().resolve()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(str(self.db_path), timeout=30, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.execute("PRAGMA busy_timeout = 30000")
        self.conn.execute("PRAGMA journal_mode = WAL")
        self.fts_available = False
        self.init()

    def close(self) -> None:
        with self._lock:
            self.conn.close()

    def __enter__(self) -> "ResearchRadar":
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            try:
                self.conn.execute("BEGIN IMMEDIATE")
                yield self.conn
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise

    def init(self) -> dict[str, Any]:
        with self._lock:
            self.conn.executescript(SCHEMA)
            timestamp = now_iso()
            self.conn.execute(
                "INSERT INTO radar_meta(key,value,updated_at) VALUES('schema_version',?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at",
                (SCHEMA_VERSION, timestamp),
            )
            try:
                self.conn.execute(
                    "CREATE VIRTUAL TABLE IF NOT EXISTS radar_fts USING fts5("
                    "record_type UNINDEXED, record_id UNINDEXED, title, body)"
                )
                self.fts_available = True
            except sqlite3.OperationalError:
                self.fts_available = False
            self.conn.execute(
                "INSERT INTO radar_meta(key,value,updated_at) VALUES('fts5_available',?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at",
                ("1" if self.fts_available else "0", timestamp),
            )
            self.conn.commit()
        return {
            "schema_version": SCHEMA_VERSION,
            "database_path": str(self.db_path),
            "fts5_available": self.fts_available,
            "scheduler_enabled": False,
        }

    def _one(self, sql: str, params: Sequence[Any] = ()) -> dict[str, Any] | None:
        with self._lock:
            return _as_mapping(self.conn.execute(sql, params).fetchone())

    def _many(self, sql: str, params: Sequence[Any] = ()) -> list[dict[str, Any]]:
        with self._lock:
            return [
                item
                for item in (_as_mapping(row) for row in self.conn.execute(sql, params).fetchall())
                if item is not None
            ]

    def _require(self, table: str, record_id: str) -> dict[str, Any]:
        row = self._one(f"SELECT * FROM {table} WHERE id = ?", (record_id,))
        if row is None:
            raise RadarNotFoundError(f"{table} record not found: {record_id}")
        return row

    def _insert_authority_event(
        self,
        conn: sqlite3.Connection,
        *,
        record_type: str,
        record_id: str,
        from_status: str | None,
        to_status: str,
        reason: str,
        release_id: str | None,
        timestamp: str,
    ) -> None:
        conn.execute(
            """
            INSERT INTO authority_events
              (id,record_type,record_id,from_status,to_status,reason,release_id,recorded_at)
            VALUES (?,?,?,?,?,?,?,?)
            """,
            (
                _identifier("auth"),
                record_type,
                record_id,
                from_status,
                to_status,
                reason,
                release_id,
                timestamp,
            ),
        )

    def create_workspace(
        self,
        *,
        name: str = DEFAULT_WORKSPACE_NAME,
        topic: str = "AI and technology developments",
        purpose: str = "Track material changes, positions, evidence, and unresolved questions.",
        strategic_questions: Sequence[str] | None = None,
        exclusions: Sequence[str] | None = None,
        extension_schema_version: str = "ai_technology_v1",
        extension_schema: Mapping[str, Any] | None = None,
        workspace_id: str | None = None,
        slug: str | None = None,
    ) -> dict[str, Any]:
        timestamp = now_iso()
        derived_slug = slug or "-".join(name.lower().replace("&", "and").split())
        record_id = workspace_id or _identifier("ws", derived_slug)
        questions = list(
            strategic_questions
            or (
                "What materially changed?",
                "Which people or organizations changed position?",
                "What evidence supports or contradicts the development?",
                "What remains uncertain, and what should be watched next?",
            )
        )
        with self._transaction() as conn:
            conn.execute(
                """
                INSERT INTO workspaces
                  (id,name,slug,topic,purpose,strategic_questions_json,exclusions_json,
                   extension_schema_version,extension_schema_json,status,created_at,updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,'active',?,?)
                ON CONFLICT(id) DO UPDATE SET
                  name=excluded.name, topic=excluded.topic, purpose=excluded.purpose,
                  strategic_questions_json=excluded.strategic_questions_json,
                  exclusions_json=excluded.exclusions_json,
                  extension_schema_version=excluded.extension_schema_version,
                  extension_schema_json=excluded.extension_schema_json, updated_at=excluded.updated_at
                """,
                (
                    record_id,
                    name,
                    derived_slug,
                    topic,
                    purpose,
                    _json(questions),
                    _json(list(exclusions or ("private or paywalled sources", "paid transcription"))),
                    extension_schema_version,
                    _json(dict(extension_schema or {})),
                    timestamp,
                    timestamp,
                ),
            )
        return self._require("workspaces", record_id)

    def create_source(
        self,
        *,
        name: str,
        canonical_url: str,
        source_type: str,
        trust_tier: str | SourceTrustTier,
        discovery_reason: str,
        workspace_id: str | None = None,
        source_id: str | None = None,
        metadata: Mapping[str, Any] | None = None,
        discovered_via_source_id: str | None = None,
    ) -> dict[str, Any]:
        tier = trust_tier.value if isinstance(trust_tier, SourceTrustTier) else str(trust_tier)
        if tier not in TRUST_TIER_VALUES:
            raise RadarPolicyError(f"unsupported source trust tier: {tier}")
        if not canonical_url.startswith(("http://", "https://")):
            raise RadarPolicyError("v1 sources must have a public HTTP(S) URL")
        timestamp = now_iso()
        record_id = source_id or _identifier("src", canonical_url)
        with self._transaction() as conn:
            conn.execute(
                """
                INSERT INTO sources
                  (id,name,canonical_url,source_type,trust_tier,status,discovery_reason,
                   metadata_json,created_at,updated_at)
                VALUES (?,?,?,?,?,'probation',?,?,?,?)
                ON CONFLICT(id) DO UPDATE SET
                  name=excluded.name, source_type=excluded.source_type,
                  trust_tier=excluded.trust_tier, discovery_reason=excluded.discovery_reason,
                  metadata_json=excluded.metadata_json, updated_at=excluded.updated_at
                """,
                (
                    record_id,
                    name,
                    canonical_url,
                    source_type,
                    tier,
                    discovery_reason,
                    _json(dict(metadata or {})),
                    timestamp,
                    timestamp,
                ),
            )
            if workspace_id:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO workspace_sources
                      (workspace_id,source_id,discovered_via_source_id,attached_at)
                    VALUES (?,?,?,?)
                    """,
                    (workspace_id, record_id, discovered_via_source_id, timestamp),
                )
        return self._require("sources", record_id)

    def attach_source(
        self,
        workspace_id: str,
        source_id: str,
        *,
        discovered_via_source_id: str | None = None,
    ) -> None:
        self._require("workspaces", workspace_id)
        self._require("sources", source_id)
        with self._transaction() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO workspace_sources
                  (workspace_id,source_id,discovered_via_source_id,attached_at)
                VALUES (?,?,?,?)
                """,
                (workspace_id, source_id, discovered_via_source_id, now_iso()),
            )

    def record_source_sample(
        self,
        source_id: str,
        *,
        workspace_id: str | None = None,
        relevant: bool,
        fetch_succeeded: bool = True,
        sampled_at: str | None = None,
    ) -> dict[str, Any]:
        source = self._require("sources", source_id)
        timestamp = sampled_at or now_iso()
        if source["status"] == SourceStatus.BLOCKED.value:
            raise RadarPolicyError("blocked sources cannot contribute samples")
        with self._transaction() as conn:
            if fetch_succeeded:
                conn.execute(
                    """
                    UPDATE sources SET consecutive_fetch_failures=0,last_fetched_at=?,
                      last_relevant_at=CASE WHEN ? THEN ? ELSE last_relevant_at END,
                      probation_sample_count=probation_sample_count + CASE WHEN ? THEN 1 ELSE 0 END,
                      updated_at=? WHERE id=?
                    """,
                    (timestamp, int(relevant), timestamp, int(relevant), timestamp, source_id),
                )
            else:
                conn.execute(
                    """
                    UPDATE sources SET consecutive_fetch_failures=consecutive_fetch_failures+1,
                      last_fetched_at=?,updated_at=? WHERE id=?
                    """,
                    (timestamp, timestamp, source_id),
                )
                failures = conn.execute(
                    "SELECT consecutive_fetch_failures FROM sources WHERE id=?", (source_id,)
                ).fetchone()[0]
                if failures >= 5:
                    conn.execute(
                        "UPDATE sources SET status='paused',paused_reason='five_consecutive_fetch_failures',updated_at=? WHERE id=?",
                        (timestamp, source_id),
                    )
            conn.execute(
                "INSERT INTO source_events(id,source_id,workspace_id,event_type,details_json,recorded_at) VALUES(?,?,?,?,?,?)",
                (
                    _identifier("sevt"),
                    source_id,
                    workspace_id,
                    "sample",
                    _json({"relevant": relevant, "fetch_succeeded": fetch_succeeded}),
                    timestamp,
                ),
            )
        return self._require("sources", source_id)

    def promote_source(
        self, source_id: str, *, workspace_id: str | None = None, promoted_at: str | None = None
    ) -> dict[str, Any]:
        source = self._require("sources", source_id)
        if source["status"] != SourceStatus.PROBATION.value:
            raise RadarPolicyError("only probation sources may be promoted")
        if int(source["probation_sample_count"]) < 3:
            raise RadarPolicyError("source needs three relevant probation samples")
        timestamp = promoted_at or now_iso()
        parsed = dt.datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        since = (parsed - dt.timedelta(hours=24)).isoformat()
        with self._transaction() as conn:
            active_count = int(
                conn.execute("SELECT COUNT(*) FROM sources WHERE status='active'").fetchone()[0]
            )
            if active_count >= 50:
                raise RadarPolicyError("pilot permits at most 50 active sources")
            promotion_count = int(
                conn.execute(
                    "SELECT COUNT(*) FROM source_events WHERE event_type='promoted' AND recorded_at>=?",
                    (since,),
                ).fetchone()[0]
            )
            if promotion_count >= 3:
                raise RadarPolicyError("pilot permits at most three source promotions per 24 hours")
            conn.execute(
                "UPDATE sources SET status='active',paused_reason=NULL,updated_at=? WHERE id=?",
                (timestamp, source_id),
            )
            conn.execute(
                "INSERT INTO source_events(id,source_id,workspace_id,event_type,details_json,recorded_at) VALUES(?,?,?,?,?,?)",
                (_identifier("sevt"), source_id, workspace_id, "promoted", "{}", timestamp),
            )
        return self._require("sources", source_id)

    def pause_stale_sources(self, *, as_of: str | None = None) -> list[str]:
        timestamp = as_of or now_iso()
        parsed = dt.datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        cutoff = (parsed - dt.timedelta(days=30)).isoformat()
        with self._transaction() as conn:
            ids = [
                str(row[0])
                for row in conn.execute(
                    """
                    SELECT id FROM sources
                    WHERE status='active' AND (last_relevant_at IS NULL OR last_relevant_at < ?)
                    """,
                    (cutoff,),
                ).fetchall()
            ]
            for source_id in ids:
                conn.execute(
                    "UPDATE sources SET status='paused',paused_reason='no_relevant_contribution_30_days',updated_at=? WHERE id=?",
                    (timestamp, source_id),
                )
                conn.execute(
                    "INSERT INTO source_events(id,source_id,event_type,details_json,recorded_at) VALUES(?,?,?,?,?)",
                    (_identifier("sevt"), source_id, "paused", "{}", timestamp),
                )
        return ids

    def set_source_status(self, source_id: str, status: str, *, reason: str) -> dict[str, Any]:
        if status not in SOURCE_STATUS_VALUES:
            raise RadarPolicyError(f"unsupported source status: {status}")
        self._require("sources", source_id)
        timestamp = now_iso()
        with self._transaction() as conn:
            conn.execute(
                "UPDATE sources SET status=?,paused_reason=?,updated_at=? WHERE id=?",
                (status, reason if status in {"paused", "blocked"} else None, timestamp, source_id),
            )
            conn.execute(
                "INSERT INTO source_events(id,source_id,event_type,details_json,recorded_at) VALUES(?,?,?,?,?)",
                (_identifier("sevt"), source_id, status, _json({"reason": reason}), timestamp),
            )
        return self._require("sources", source_id)

    def create_content_item(
        self,
        *,
        source_id: str,
        canonical_url: str,
        content_type: str,
        title: str,
        normalized_text: str,
        published_at: str | None = None,
        author: str | None = None,
        token_count: int = 0,
        content_item_id: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        self._require("sources", source_id)
        if token_count > BUDGETS.max_input_tokens_per_item:
            raise RadarPolicyError("content exceeds the 75,000 input-token item budget")
        timestamp = now_iso()
        record_id = content_item_id or _identifier("content", canonical_url)
        digest = sha256_text(normalized_text)
        with self._transaction() as conn:
            conn.execute(
                """
                INSERT INTO content_items
                  (id,source_id,canonical_url,content_type,title,author,published_at,normalized_text,
                   content_sha256,token_count,fetch_status,fetched_at,metadata_json,created_at,updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,'ready',?,?,?,?)
                ON CONFLICT(id) DO UPDATE SET
                  title=excluded.title,author=excluded.author,published_at=excluded.published_at,
                  normalized_text=excluded.normalized_text,content_sha256=excluded.content_sha256,
                  token_count=excluded.token_count,metadata_json=excluded.metadata_json,updated_at=excluded.updated_at
                """,
                (
                    record_id,
                    source_id,
                    canonical_url,
                    content_type,
                    title,
                    author,
                    published_at,
                    normalized_text,
                    digest,
                    token_count,
                    timestamp,
                    _json(dict(metadata or {})),
                    timestamp,
                    timestamp,
                ),
            )
        return self._require("content_items", record_id)

    def create_evidence_span(
        self,
        *,
        content_item_id: str,
        start_char: int,
        end_char: int,
        support_kind: str,
        confidence: float,
        extractor_version: str,
        release_id: str,
        lineage: Mapping[str, Any],
        observed_at: str | None = None,
        status: str | AuthorityStatus = AuthorityStatus.CANDIDATE,
        evidence_span_id: str | None = None,
    ) -> dict[str, Any]:
        content = self._require("content_items", content_item_id)
        text = str(content["normalized_text"])
        if start_char < 0 or end_char <= start_char or end_char > len(text):
            raise RadarPolicyError("evidence offsets do not resolve inside normalized content")
        quote = text[start_char:end_char]
        if not quote.strip():
            raise RadarPolicyError("evidence span cannot be empty or whitespace")
        if support_kind not in {
            "primary_evidence",
            "reputable_reporting",
            "attributed_claim",
            "inference",
            "speculation",
        }:
            raise RadarPolicyError(f"unsupported evidence kind: {support_kind}")
        authority = _as_status(status)
        if authority not in {
            AuthorityStatus.CANDIDATE.value,
            AuthorityStatus.PROVISIONAL.value,
        }:
            raise RadarPolicyError(
                "evidence must enter as candidate or provisional and be explicitly accepted"
            )
        timestamp = now_iso()
        record_id = evidence_span_id or _identifier(
            "evidence", content_item_id, str(start_char), str(end_char), sha256_text(quote)
        )
        with self._transaction() as conn:
            conn.execute(
                """
                INSERT INTO evidence_spans
                  (id,content_item_id,start_char,end_char,quote_text,quote_sha256,support_kind,
                   status,confidence,observed_at,extractor_version,release_id,lineage_json,created_at,updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    record_id,
                    content_item_id,
                    start_char,
                    end_char,
                    quote,
                    sha256_text(quote),
                    support_kind,
                    authority,
                    confidence,
                    observed_at,
                    extractor_version,
                    release_id,
                    _json(dict(lineage)),
                    timestamp,
                    timestamp,
                ),
            )
            self._insert_authority_event(
                conn,
                record_type="evidence_span",
                record_id=record_id,
                from_status=None,
                to_status=authority,
                reason="created",
                release_id=release_id,
                timestamp=timestamp,
            )
        return self._require("evidence_spans", record_id)

    def _semantic_common(
        self,
        *,
        status: str | AuthorityStatus,
        confidence: float,
        evidence_span_id: str,
        extractor_version: str,
        release_id: str,
        lineage: Mapping[str, Any],
    ) -> tuple[str, str]:
        authority = _as_status(status)
        if authority not in {
            AuthorityStatus.CANDIDATE.value,
            AuthorityStatus.PROVISIONAL.value,
        }:
            raise RadarPolicyError(
                "semantic records must enter as candidate or provisional and be explicitly accepted"
            )
        if not 0 <= confidence <= 1:
            raise RadarPolicyError("semantic confidence must be between 0 and 1")
        self._require("evidence_spans", evidence_span_id)
        if not extractor_version or not release_id or not lineage:
            raise RadarPolicyError("semantic records require extractor, release, and lineage")
        return authority, now_iso()

    def _finish_semantic_create(
        self,
        conn: sqlite3.Connection,
        *,
        record_type: str,
        record_id: str,
        status: str,
        evidence_span_id: str,
        release_id: str,
        timestamp: str,
        directly_establishes: bool = False,
    ) -> None:
        self._insert_authority_event(
            conn,
            record_type=record_type,
            record_id=record_id,
            from_status=None,
            to_status=status,
            reason="created",
            release_id=release_id,
            timestamp=timestamp,
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO semantic_evidence
              (record_type,record_id,evidence_span_id,role,directly_establishes,material_statement,linked_at)
            VALUES (?,?,?,'supports',?,1,?)
            """,
            (record_type, record_id, evidence_span_id, int(directly_establishes), timestamp),
        )

    def create_entity(
        self,
        *,
        workspace_id: str,
        entity_type: str,
        canonical_name: str,
        evidence_span_id: str,
        extractor_version: str,
        release_id: str,
        lineage: Mapping[str, Any],
        aliases: Sequence[str] | None = None,
        confidence: float = 1.0,
        observed_at: str | None = None,
        status: str | AuthorityStatus = AuthorityStatus.CANDIDATE,
        entity_id: str | None = None,
    ) -> dict[str, Any]:
        self._require("workspaces", workspace_id)
        authority, timestamp = self._semantic_common(
            status=status,
            confidence=confidence,
            evidence_span_id=evidence_span_id,
            extractor_version=extractor_version,
            release_id=release_id,
            lineage=lineage,
        )
        record_id = entity_id or _identifier("entity", workspace_id, entity_type, canonical_name.lower())
        with self._transaction() as conn:
            conn.execute(
                """
                INSERT INTO entities
                  (id,workspace_id,entity_type,canonical_name,aliases_json,status,confidence,
                   evidence_span_id,observed_at,extractor_version,release_id,lineage_json,created_at,updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    record_id,
                    workspace_id,
                    entity_type,
                    canonical_name,
                    _json(list(aliases or [])),
                    authority,
                    confidence,
                    evidence_span_id,
                    observed_at,
                    extractor_version,
                    release_id,
                    _json(dict(lineage)),
                    timestamp,
                    timestamp,
                ),
            )
            self._finish_semantic_create(
                conn,
                record_type="entity",
                record_id=record_id,
                status=authority,
                evidence_span_id=evidence_span_id,
                release_id=release_id,
                timestamp=timestamp,
            )
        return self._require("entities", record_id)

    def create_topic(
        self,
        *,
        workspace_id: str,
        name: str,
        evidence_span_id: str,
        extractor_version: str,
        release_id: str,
        lineage: Mapping[str, Any],
        description: str | None = None,
        confidence: float = 1.0,
        observed_at: str | None = None,
        status: str | AuthorityStatus = AuthorityStatus.CANDIDATE,
        topic_id: str | None = None,
    ) -> dict[str, Any]:
        self._require("workspaces", workspace_id)
        authority, timestamp = self._semantic_common(
            status=status,
            confidence=confidence,
            evidence_span_id=evidence_span_id,
            extractor_version=extractor_version,
            release_id=release_id,
            lineage=lineage,
        )
        record_id = topic_id or _identifier("topic", workspace_id, name.lower())
        with self._transaction() as conn:
            conn.execute(
                """
                INSERT INTO topics
                  (id,workspace_id,name,description,status,confidence,evidence_span_id,observed_at,
                   extractor_version,release_id,lineage_json,created_at,updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    record_id,
                    workspace_id,
                    name,
                    description,
                    authority,
                    confidence,
                    evidence_span_id,
                    observed_at,
                    extractor_version,
                    release_id,
                    _json(dict(lineage)),
                    timestamp,
                    timestamp,
                ),
            )
            self._finish_semantic_create(
                conn,
                record_type="topic",
                record_id=record_id,
                status=authority,
                evidence_span_id=evidence_span_id,
                release_id=release_id,
                timestamp=timestamp,
            )
        return self._require("topics", record_id)

    def create_development(
        self,
        *,
        workspace_id: str,
        title: str,
        summary: str,
        event_at: str,
        evidence_span_id: str,
        extractor_version: str,
        release_id: str,
        lineage: Mapping[str, Any],
        topic_id: str | None = None,
        significance: float = 0.0,
        confidence: float = 1.0,
        observed_at: str | None = None,
        status: str | AuthorityStatus = AuthorityStatus.CANDIDATE,
        directly_establishes: bool = False,
        development_id: str | None = None,
    ) -> dict[str, Any]:
        self._require("workspaces", workspace_id)
        authority, timestamp = self._semantic_common(
            status=status,
            confidence=confidence,
            evidence_span_id=evidence_span_id,
            extractor_version=extractor_version,
            release_id=release_id,
            lineage=lineage,
        )
        if not 0 <= significance <= 1:
            raise RadarPolicyError("significance must be between 0 and 1")
        record_id = development_id or _identifier("dev", workspace_id, title, event_at)
        with self._transaction() as conn:
            conn.execute(
                """
                INSERT INTO developments
                  (id,workspace_id,topic_id,title,summary,significance,event_at,status,confidence,
                   evidence_span_id,observed_at,extractor_version,release_id,lineage_json,created_at,updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    record_id,
                    workspace_id,
                    topic_id,
                    title,
                    summary,
                    significance,
                    event_at,
                    authority,
                    confidence,
                    evidence_span_id,
                    observed_at,
                    extractor_version,
                    release_id,
                    _json(dict(lineage)),
                    timestamp,
                    timestamp,
                ),
            )
            self._finish_semantic_create(
                conn,
                record_type="development",
                record_id=record_id,
                status=authority,
                evidence_span_id=evidence_span_id,
                release_id=release_id,
                timestamp=timestamp,
                directly_establishes=directly_establishes,
            )
        return self._require("developments", record_id)

    def create_claim(
        self,
        *,
        workspace_id: str,
        claim_type: str,
        claim_text: str,
        evidence_span_id: str,
        extractor_version: str,
        release_id: str,
        lineage: Mapping[str, Any],
        development_id: str | None = None,
        subject_entity_id: str | None = None,
        confidence: float = 1.0,
        observed_at: str | None = None,
        status: str | AuthorityStatus = AuthorityStatus.CANDIDATE,
        claim_id: str | None = None,
    ) -> dict[str, Any]:
        self._require("workspaces", workspace_id)
        authority, timestamp = self._semantic_common(
            status=status,
            confidence=confidence,
            evidence_span_id=evidence_span_id,
            extractor_version=extractor_version,
            release_id=release_id,
            lineage=lineage,
        )
        record_id = claim_id or _identifier("claim", workspace_id, claim_text, evidence_span_id)
        with self._transaction() as conn:
            conn.execute(
                """
                INSERT INTO claims
                  (id,workspace_id,development_id,subject_entity_id,claim_type,claim_text,status,
                   confidence,evidence_span_id,observed_at,extractor_version,release_id,lineage_json,created_at,updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    record_id,
                    workspace_id,
                    development_id,
                    subject_entity_id,
                    claim_type,
                    claim_text,
                    authority,
                    confidence,
                    evidence_span_id,
                    observed_at,
                    extractor_version,
                    release_id,
                    _json(dict(lineage)),
                    timestamp,
                    timestamp,
                ),
            )
            self._finish_semantic_create(
                conn,
                record_type="claim",
                record_id=record_id,
                status=authority,
                evidence_span_id=evidence_span_id,
                release_id=release_id,
                timestamp=timestamp,
            )
        return self._require("claims", record_id)

    def create_position_observation(
        self,
        *,
        workspace_id: str,
        entity_id: str,
        stance: str,
        position_text: str,
        observed_at: str,
        evidence_span_id: str,
        extractor_version: str,
        release_id: str,
        lineage: Mapping[str, Any],
        claim_id: str | None = None,
        development_id: str | None = None,
        prior_position_observation_id: str | None = None,
        changed_position: bool = False,
        confidence: float = 1.0,
        status: str | AuthorityStatus = AuthorityStatus.CANDIDATE,
        position_observation_id: str | None = None,
    ) -> dict[str, Any]:
        self._require("workspaces", workspace_id)
        self._require("entities", entity_id)
        authority, timestamp = self._semantic_common(
            status=status,
            confidence=confidence,
            evidence_span_id=evidence_span_id,
            extractor_version=extractor_version,
            release_id=release_id,
            lineage=lineage,
        )
        if changed_position and not prior_position_observation_id:
            raise RadarPolicyError("position changes must identify the prior observation")
        record_id = position_observation_id or _identifier(
            "pos", entity_id, observed_at, position_text, evidence_span_id
        )
        with self._transaction() as conn:
            conn.execute(
                """
                INSERT INTO position_observations
                  (id,workspace_id,entity_id,claim_id,development_id,stance,position_text,
                   prior_position_observation_id,changed_position,status,confidence,evidence_span_id,
                   observed_at,extractor_version,release_id,lineage_json,created_at,updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    record_id,
                    workspace_id,
                    entity_id,
                    claim_id,
                    development_id,
                    stance,
                    position_text,
                    prior_position_observation_id,
                    int(changed_position),
                    authority,
                    confidence,
                    evidence_span_id,
                    observed_at,
                    extractor_version,
                    release_id,
                    _json(dict(lineage)),
                    timestamp,
                    timestamp,
                ),
            )
            self._finish_semantic_create(
                conn,
                record_type="position_observation",
                record_id=record_id,
                status=authority,
                evidence_span_id=evidence_span_id,
                release_id=release_id,
                timestamp=timestamp,
            )
        return self._require("position_observations", record_id)

    def create_briefing(
        self,
        *,
        workspace_id: str,
        development_id: str,
        headline: str,
        what_changed: str,
        why_it_matters: str,
        event_at: str,
        evidence_span_id: str,
        extractor_version: str,
        release_id: str,
        lineage: Mapping[str, Any],
        competing_interpretations: Sequence[str] | None = None,
        position_changes: Sequence[str] | None = None,
        unresolved_questions: Sequence[str] | None = None,
        watch_next: Sequence[str] | None = None,
        priority: str = "normal",
        confidence: float = 1.0,
        observed_at: str | None = None,
        status: str | AuthorityStatus = AuthorityStatus.CANDIDATE,
        directly_establishes: bool = False,
        briefing_id: str | None = None,
    ) -> dict[str, Any]:
        self._require("workspaces", workspace_id)
        self._require("developments", development_id)
        authority, timestamp = self._semantic_common(
            status=status,
            confidence=confidence,
            evidence_span_id=evidence_span_id,
            extractor_version=extractor_version,
            release_id=release_id,
            lineage=lineage,
        )
        if priority not in {"low", "normal", "high"}:
            raise RadarPolicyError(f"unsupported briefing priority: {priority}")
        record_id = briefing_id or _identifier("brief", development_id, headline)
        published_at = timestamp if authority in {"provisional", "verified", "amended"} else None
        with self._transaction() as conn:
            conn.execute(
                """
                INSERT INTO briefings
                  (id,workspace_id,development_id,headline,what_changed,why_it_matters,
                   competing_interpretations_json,position_changes_json,unresolved_questions_json,
                   watch_next_json,priority,status,confidence,evidence_span_id,event_at,observed_at,
                   extractor_version,release_id,lineage_json,published_at,created_at,updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    record_id,
                    workspace_id,
                    development_id,
                    headline,
                    what_changed,
                    why_it_matters,
                    _json(list(competing_interpretations or [])),
                    _json(list(position_changes or [])),
                    _json(list(unresolved_questions or [])),
                    _json(list(watch_next or [])),
                    priority,
                    authority,
                    confidence,
                    evidence_span_id,
                    event_at,
                    observed_at,
                    extractor_version,
                    release_id,
                    _json(dict(lineage)),
                    published_at,
                    timestamp,
                    timestamp,
                ),
            )
            self._finish_semantic_create(
                conn,
                record_type="briefing",
                record_id=record_id,
                status=authority,
                evidence_span_id=evidence_span_id,
                release_id=release_id,
                timestamp=timestamp,
                directly_establishes=directly_establishes,
            )
            self._upsert_fts(conn, "briefing", record_id, headline, f"{what_changed}\n{why_it_matters}")
        return self._require("briefings", record_id)

    def create_semantic_edge(
        self,
        *,
        workspace_id: str,
        from_type: str,
        from_id: str,
        relation_type: str,
        to_type: str,
        to_id: str,
        observed_at: str,
        evidence_span_id: str,
        extractor_version: str,
        release_id: str,
        lineage: Mapping[str, Any],
        confidence: float = 1.0,
        status: str | AuthorityStatus = AuthorityStatus.CANDIDATE,
        semantic_edge_id: str | None = None,
    ) -> dict[str, Any]:
        if relation_type not in RELATION_TYPES:
            raise RadarPolicyError("models cannot invent permanent relation types")
        authority, timestamp = self._semantic_common(
            status=status,
            confidence=confidence,
            evidence_span_id=evidence_span_id,
            extractor_version=extractor_version,
            release_id=release_id,
            lineage=lineage,
        )
        record_id = semantic_edge_id or _identifier(
            "edge", workspace_id, from_type, from_id, relation_type, to_type, to_id, evidence_span_id
        )
        with self._transaction() as conn:
            conn.execute(
                """
                INSERT INTO semantic_edges
                  (id,workspace_id,from_type,from_id,relation_type,to_type,to_id,status,confidence,
                   evidence_span_id,observed_at,extractor_version,release_id,lineage_json,created_at,updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    record_id,
                    workspace_id,
                    from_type,
                    from_id,
                    relation_type,
                    to_type,
                    to_id,
                    authority,
                    confidence,
                    evidence_span_id,
                    observed_at,
                    extractor_version,
                    release_id,
                    _json(dict(lineage)),
                    timestamp,
                    timestamp,
                ),
            )
            self._finish_semantic_create(
                conn,
                record_type="semantic_edge",
                record_id=record_id,
                status=authority,
                evidence_span_id=evidence_span_id,
                release_id=release_id,
                timestamp=timestamp,
            )
        return self._require("semantic_edges", record_id)

    def link_evidence(
        self,
        record_type: str,
        record_id: str,
        evidence_span_id: str,
        *,
        role: str = "supports",
        directly_establishes: bool = False,
        material_statement: bool = True,
    ) -> None:
        if record_type not in SEMANTIC_RECORD_TYPES:
            raise RadarPolicyError(f"unsupported semantic record type: {record_type}")
        table = RECORD_TABLES[record_type]
        self._require(table, record_id)
        self._require("evidence_spans", evidence_span_id)
        if role not in {"supports", "contradicts", "context"}:
            raise RadarPolicyError(f"unsupported evidence role: {role}")
        with self._transaction() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO semantic_evidence
                  (record_type,record_id,evidence_span_id,role,directly_establishes,material_statement,linked_at)
                VALUES (?,?,?,?,?,?,?)
                """,
                (
                    record_type,
                    record_id,
                    evidence_span_id,
                    role,
                    int(directly_establishes),
                    int(material_statement),
                    now_iso(),
                ),
            )

    def verification_evidence(self, record_type: str, record_id: str) -> dict[str, Any]:
        if record_type not in {"development", "briefing"}:
            raise RadarPolicyError("source verification policy applies to developments and briefings")
        table = RECORD_TABLES[record_type]
        self._require(table, record_id)
        rows = self._many(
            """
            SELECT s.id AS source_id,s.name AS source_name,s.trust_tier,se.role,
                   se.directly_establishes,se.material_statement,es.id AS evidence_span_id
            FROM semantic_evidence se
            JOIN evidence_spans es ON es.id=se.evidence_span_id
            JOIN content_items ci ON ci.id=es.content_item_id
            JOIN sources s ON s.id=ci.source_id
            WHERE se.record_type=? AND se.record_id=? AND se.role='supports'
              AND es.status IN ('verified','amended') AND s.status='active'
            """,
            (record_type, record_id),
        )
        primary_ids = {
            row["source_id"]
            for row in rows
            if row["trust_tier"] == SourceTrustTier.PRIMARY.value
            and row["directly_establishes"]
        }
        reputable_ids = {
            row["source_id"]
            for row in rows
            if row["trust_tier"] == SourceTrustTier.REPUTABLE_SECONDARY.value
        }
        accepted = bool(primary_ids) or len(reputable_ids) >= 2
        return {
            "accepted": accepted,
            "rule": "one_direct_primary_or_two_independent_reputable_sources",
            "direct_primary_source_ids": sorted(primary_ids),
            "reputable_source_ids": sorted(reputable_ids),
            "evidence": rows,
        }

    def transition_authority(
        self,
        record_type: str,
        record_id: str,
        to_status: str | AuthorityStatus,
        *,
        reason: str,
        release_id: str | None = None,
    ) -> dict[str, Any]:
        if record_type not in RECORD_TABLES:
            raise RadarPolicyError(f"unsupported authority record type: {record_type}")
        target = _as_status(to_status)
        table = RECORD_TABLES[record_type]
        current = self._require(table, record_id)
        old = str(current["status"])
        if target not in ALLOWED_TRANSITIONS[old]:
            raise RadarTransitionError(f"illegal authority transition: {old} -> {target}")
        if target == AuthorityStatus.VERIFIED.value and record_type in {"development", "briefing"}:
            decision = self.verification_evidence(record_type, record_id)
            if not decision["accepted"]:
                raise RadarPolicyError(decision["rule"])
            if record_type == "briefing":
                development = self._require("developments", str(current["development_id"]))
                if development["status"] not in ACCEPTED_STATUSES:
                    raise RadarPolicyError(
                        "a briefing cannot be verified before its development is accepted"
                    )
        if target == AuthorityStatus.VERIFIED.value and record_type in SEMANTIC_RECORD_TYPES:
            unaccepted_evidence = self._one(
                """
                SELECT COUNT(*) AS count
                FROM semantic_evidence se
                JOIN evidence_spans es ON es.id=se.evidence_span_id
                WHERE se.record_type=? AND se.record_id=? AND se.material_statement=1
                  AND es.status NOT IN ('verified','amended')
                """,
                (record_type, record_id),
            )
            if unaccepted_evidence and int(unaccepted_evidence["count"]) > 0:
                raise RadarPolicyError(
                    "all material evidence must be explicitly accepted before verification"
                )
        timestamp = now_iso()
        effective_release = release_id or current.get("release_id")
        with self._transaction() as conn:
            if record_type == "briefing":
                conn.execute(
                    f"UPDATE {table} SET status=?,updated_at=?,published_at=COALESCE(published_at,?) WHERE id=?",
                    (target, timestamp, timestamp, record_id),
                )
            else:
                conn.execute(
                    f"UPDATE {table} SET status=?,updated_at=? WHERE id=?",
                    (target, timestamp, record_id),
                )
            self._insert_authority_event(
                conn,
                record_type=record_type,
                record_id=record_id,
                from_status=old,
                to_status=target,
                reason=reason,
                release_id=effective_release,
                timestamp=timestamp,
            )
        return self._require(table, record_id)

    def authority_history(self, record_type: str, record_id: str) -> list[dict[str, Any]]:
        return self._many(
            "SELECT * FROM authority_events WHERE record_type=? AND record_id=? ORDER BY recorded_at,rowid",
            (record_type, record_id),
        )

    def record_feedback(
        self, briefing_id: str, rating: str | FeedbackRating, note: str | None = None
    ) -> dict[str, Any]:
        self._require("briefings", briefing_id)
        value = rating.value if isinstance(rating, FeedbackRating) else str(rating)
        if value not in FEEDBACK_VALUES:
            raise RadarPolicyError(f"unsupported feedback rating: {value}")
        record_id = _identifier("feedback")
        with self._transaction() as conn:
            conn.execute(
                "INSERT INTO feedback(id,briefing_id,rating,note,created_at) VALUES(?,?,?,?,?)",
                (record_id, briefing_id, value, note, now_iso()),
            )
        return self._require("feedback", record_id)

    def mark_briefing_read(self, briefing_id: str, *, read: bool = True) -> dict[str, Any]:
        self._require("briefings", briefing_id)
        with self._transaction() as conn:
            conn.execute(
                "UPDATE briefings SET read_at=?,updated_at=? WHERE id=?",
                (now_iso() if read else None, now_iso(), briefing_id),
            )
        return self._require("briefings", briefing_id)

    def list_workspaces(self, limit: int = 50) -> list[dict[str, Any]]:
        return self._many(
            """
            SELECT w.*,
              (SELECT COUNT(*) FROM workspace_sources ws WHERE ws.workspace_id=w.id) AS source_count,
              (SELECT COUNT(*) FROM briefings b WHERE b.workspace_id=w.id) AS briefing_count
            FROM workspaces w ORDER BY w.updated_at DESC LIMIT ?
            """,
            (max(1, min(int(limit), 200)),),
        )

    def list_briefings(
        self,
        workspace_id: str | None = None,
        status: str | None = None,
        unread_only: bool = False,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = ["1=1"]
        params: list[Any] = []
        if workspace_id:
            clauses.append("b.workspace_id=?")
            params.append(workspace_id)
        if status:
            if status == "accepted":
                clauses.append("b.status IN ('verified','amended')")
            else:
                _as_status(status)
                clauses.append("b.status=?")
                params.append(status)
        if unread_only:
            clauses.append("b.read_at IS NULL")
        params.append(max(1, min(int(limit), 200)))
        return self._many(
            f"""
            SELECT b.*,d.title AS development_title,w.name AS workspace_name,
              (SELECT COUNT(*) FROM feedback f WHERE f.briefing_id=b.id) AS feedback_count
            FROM briefings b
            JOIN developments d ON d.id=b.development_id
            JOIN workspaces w ON w.id=b.workspace_id
            WHERE {' AND '.join(clauses)}
            ORDER BY CASE b.status WHEN 'retracted' THEN 0 WHEN 'amended' THEN 1
                     WHEN 'verified' THEN 2 WHEN 'provisional' THEN 3 ELSE 4 END,
                     b.event_at DESC,b.created_at DESC LIMIT ?
            """,
            params,
        )

    def get_briefing(self, briefing_id: str) -> dict[str, Any]:
        briefing = self._one(
            """
            SELECT b.*,d.title AS development_title,d.summary AS development_summary,
                   w.name AS workspace_name
            FROM briefings b JOIN developments d ON d.id=b.development_id
            JOIN workspaces w ON w.id=b.workspace_id WHERE b.id=?
            """,
            (briefing_id,),
        )
        if briefing is None:
            raise RadarNotFoundError(f"briefing record not found: {briefing_id}")
        briefing["evidence"] = self._many(
            """
            SELECT es.*,se.role,se.directly_establishes,se.material_statement,
                   ci.title AS content_title,ci.canonical_url,ci.published_at,
                   s.id AS source_id,s.name AS source_name,s.trust_tier
            FROM semantic_evidence se JOIN evidence_spans es ON es.id=se.evidence_span_id
            JOIN content_items ci ON ci.id=es.content_item_id JOIN sources s ON s.id=ci.source_id
            WHERE se.record_type='briefing' AND se.record_id=?
            ORDER BY CASE se.role WHEN 'supports' THEN 0 WHEN 'contradicts' THEN 1 ELSE 2 END,s.name
            """,
            (briefing_id,),
        )
        briefing["feedback"] = self._many(
            "SELECT * FROM feedback WHERE briefing_id=? ORDER BY created_at DESC", (briefing_id,)
        )
        briefing["status_history"] = self.authority_history("briefing", briefing_id)
        return briefing

    def workspace_timeline(
        self, workspace_id: str, status: str | None = None, limit: int = 50
    ) -> list[dict[str, Any]]:
        self._require("workspaces", workspace_id)
        params: list[Any] = [workspace_id]
        condition = ""
        if status:
            if status == "accepted":
                condition = " AND d.status IN ('verified','amended')"
            else:
                _as_status(status)
                condition = " AND d.status=?"
                params.append(status)
        params.append(max(1, min(int(limit), 200)))
        return self._many(
            f"""
            SELECT d.*,b.id AS briefing_id,b.headline,b.status AS briefing_status,b.read_at
            FROM developments d LEFT JOIN briefings b ON b.development_id=d.id
            WHERE d.workspace_id=? {condition}
            ORDER BY d.event_at DESC,d.created_at DESC LIMIT ?
            """,
            params,
        )

    def entity_positions(self, entity_id: str, limit: int = 50) -> dict[str, Any]:
        entity = self._require("entities", entity_id)
        positions = self._many(
            """
            SELECT p.*,c.claim_text,d.title AS development_title
            FROM position_observations p
            LEFT JOIN claims c ON c.id=p.claim_id
            LEFT JOIN developments d ON d.id=p.development_id
            WHERE p.entity_id=? ORDER BY p.observed_at DESC LIMIT ?
            """,
            (entity_id, max(1, min(int(limit), 200))),
        )
        return {"entity": entity, "positions": positions}

    def list_sources(
        self,
        workspace_id: str | None = None,
        status: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        clauses = ["1=1"]
        params: list[Any] = []
        join = ""
        if workspace_id:
            join = "JOIN workspace_sources ws ON ws.source_id=s.id"
            clauses.append("ws.workspace_id=?")
            params.append(workspace_id)
        if status:
            if status not in SOURCE_STATUS_VALUES:
                raise RadarPolicyError(f"unsupported source status: {status}")
            clauses.append("s.status=?")
            params.append(status)
        params.append(max(1, min(int(limit), 200)))
        return self._many(
            f"""
            SELECT s.*,(SELECT COUNT(*) FROM content_items ci WHERE ci.source_id=s.id) AS content_count
            FROM sources s {join} WHERE {' AND '.join(clauses)}
            ORDER BY CASE s.status WHEN 'active' THEN 0 WHEN 'probation' THEN 1
                     WHEN 'paused' THEN 2 ELSE 3 END,s.name LIMIT ?
            """,
            params,
        )

    def _upsert_fts(
        self,
        conn: sqlite3.Connection,
        record_type: str,
        record_id: str,
        title: str,
        body: str,
    ) -> None:
        if not self.fts_available:
            return
        conn.execute("DELETE FROM radar_fts WHERE record_type=? AND record_id=?", (record_type, record_id))
        conn.execute(
            "INSERT INTO radar_fts(record_type,record_id,title,body) VALUES(?,?,?,?)",
            (record_type, record_id, title, body),
        )

    def search(self, query: str, *, accepted_only: bool = True, limit: int = 50) -> list[dict[str, Any]]:
        text = query.strip()
        if not text:
            return []
        size = max(1, min(int(limit), 200))
        if self.fts_available:
            phrase = '"' + text.replace('"', '""') + '"'
            status_filter = "AND b.status IN ('verified','amended')" if accepted_only else ""
            try:
                return self._many(
                    f"""
                    SELECT f.record_type,f.record_id,b.headline AS title,b.what_changed AS summary,
                           b.status,b.event_at,b.workspace_id
                    FROM radar_fts f JOIN briefings b ON b.id=f.record_id
                    WHERE radar_fts MATCH ? AND f.record_type='briefing' {status_filter}
                    ORDER BY rank LIMIT ?
                    """,
                    (phrase, size),
                )
            except sqlite3.OperationalError:
                pass
        status_filter = "AND status IN ('verified','amended')" if accepted_only else ""
        pattern = f"%{text.lower()}%"
        return self._many(
            f"""
            SELECT 'briefing' AS record_type,id AS record_id,headline AS title,
                   what_changed AS summary,status,event_at,workspace_id
            FROM briefings WHERE (lower(headline) LIKE ? OR lower(what_changed) LIKE ?
              OR lower(why_it_matters) LIKE ?) {status_filter}
            ORDER BY event_at DESC LIMIT ?
            """,
            (pattern, pattern, pattern, size),
        )

    def enqueue_pipeline_item(
        self,
        *,
        workspace_id: str,
        stage: str | PipelineStage,
        content_item_id: str | None = None,
        idempotency_key: str | None = None,
        metadata: Mapping[str, Any] | None = None,
        available_at: str | None = None,
    ) -> dict[str, Any]:
        self._require("workspaces", workspace_id)
        stage_value = stage.value if isinstance(stage, PipelineStage) else str(stage)
        if stage_value not in PIPELINE_STAGES:
            raise RadarPolicyError(f"unsupported pipeline stage: {stage_value}")
        if content_item_id:
            self._require("content_items", content_item_id)
        key = idempotency_key or sha256_text(
            f"{workspace_id}\n{content_item_id or ''}\n{stage_value}"
        )
        timestamp = now_iso()
        record_id = _identifier("pipe", key)
        with self._transaction() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO pipeline_items
                  (id,workspace_id,content_item_id,stage,status,idempotency_key,available_at,
                   metadata_json,created_at,updated_at)
                VALUES (?,?,?,?,'queued',?,?,?,?,?)
                """,
                (
                    record_id,
                    workspace_id,
                    content_item_id,
                    stage_value,
                    key,
                    available_at or timestamp,
                    _json(dict(metadata or {})),
                    timestamp,
                    timestamp,
                ),
            )
            actual = conn.execute(
                "SELECT id FROM pipeline_items WHERE idempotency_key=?", (key,)
            ).fetchone()[0]
        return self._require("pipeline_items", str(actual))

    def _validate_pipeline_metrics(self, metrics: Mapping[str, Any]) -> dict[str, Any]:
        values = {
            "input_tokens": int(metrics.get("input_tokens", 0)),
            "window_count": int(metrics.get("window_count", metrics.get("windows", 0))),
            "schema_repairs": int(metrics.get("schema_repairs", 0)),
            "transport_retries": int(metrics.get("transport_retries", 0)),
            "wall_seconds": float(metrics.get("wall_seconds", 0)),
        }
        limits = {
            "input_tokens": BUDGETS.max_input_tokens_per_item,
            "window_count": BUDGETS.max_windows_per_item,
            "schema_repairs": BUDGETS.max_schema_repairs_per_item,
            "transport_retries": BUDGETS.max_transport_retries_per_item,
            "wall_seconds": BUDGETS.max_item_seconds,
        }
        exceeded = [key for key, value in values.items() if value > limits[key]]
        if exceeded:
            raise RadarPolicyError("item budget exceeded: " + ", ".join(exceeded))
        if any(value < 0 for value in values.values()):
            raise RadarPolicyError("pipeline metrics cannot be negative")
        return values

    def _dead_letter(
        self,
        conn: sqlite3.Connection,
        *,
        item: Mapping[str, Any],
        error_class: str,
        error_message: str,
        details: Mapping[str, Any] | None = None,
    ) -> str:
        timestamp = now_iso()
        dead_id = _identifier("dead")
        conn.execute(
            """
            INSERT INTO dead_letters
              (id,workspace_id,content_item_id,pipeline_item_id,stage,error_class,error_message,
               attempt_count,status,details_json,created_at)
            VALUES (?,?,?,?,?,?,?,?,'open',?,?)
            """,
            (
                dead_id,
                item["workspace_id"],
                item.get("content_item_id"),
                item["id"],
                item["stage"],
                error_class,
                error_message,
                int(item.get("attempts", 0)),
                _json(dict(details or {})),
                timestamp,
            ),
        )
        conn.execute(
            "UPDATE pipeline_items SET status='dead_letter',last_error=?,completed_at=?,updated_at=? WHERE id=?",
            (error_message, timestamp, timestamp, item["id"]),
        )
        return dead_id

    def run_cycle(
        self,
        processor: Callable[[dict[str, Any], PipelineBudgets], Mapping[str, Any]] | None = None,
        *,
        workspace_id: str | None = None,
        max_items: int = BUDGETS.max_items_per_cycle,
        now_monotonic: Callable[[], float] = time.monotonic,
    ) -> dict[str, Any]:
        """Run one bounded deterministic cycle; this core never starts model transport."""

        operation_id = _identifier("op")
        started_at = now_iso()
        start_clock = now_monotonic()
        started_datetime = dt.datetime.fromisoformat(started_at.replace("Z", "+00:00"))
        stale_claim_cutoff = (
            started_datetime - dt.timedelta(seconds=BUDGETS.max_item_seconds)
        ).isoformat()
        params: list[Any] = [started_at]
        workspace_clause = ""
        if workspace_id:
            self._require("workspaces", workspace_id)
            workspace_clause = " AND workspace_id=?"
            params.append(workspace_id)
        bounded_max_items = max(0, min(int(max_items), BUDGETS.max_items_per_cycle))
        params.append(bounded_max_items)
        with self._transaction() as conn:
            stale_ids = [
                str(row[0])
                for row in conn.execute(
                    """
                    SELECT id FROM pipeline_items
                    WHERE status='in_progress' AND claimed_at IS NOT NULL AND claimed_at<=?
                    """,
                    (stale_claim_cutoff,),
                ).fetchall()
            ]
            if stale_ids:
                conn.executemany(
                    """
                    UPDATE pipeline_items SET status='queued',claimed_at=NULL,
                      last_error='reclaimed_stale_item_lease',updated_at=? WHERE id=?
                    """,
                    [(started_at, item_id) for item_id in stale_ids],
                )
            conn.execute(
                "INSERT INTO operations(id,workspace_id,kind,status,details_json,started_at) VALUES(?,?,'cycle','running',?,?)",
                (
                    operation_id,
                    workspace_id,
                    _json(
                        {
                            "budgets": asdict(BUDGETS),
                            "automatic_model_transport": False,
                            "reclaimed_stale_item_leases": len(stale_ids),
                        }
                    ),
                    started_at,
                ),
            )
            candidates = [
                _as_mapping(row)
                for row in conn.execute(
                    f"""
                    SELECT * FROM pipeline_items
                    WHERE status='queued' AND available_at<=? {workspace_clause}
                    ORDER BY created_at,id LIMIT ?
                    """,
                    params,
                ).fetchall()
            ]
            items = [item for item in candidates if item is not None]
            if processor is not None:
                for item in items:
                    conn.execute(
                        "UPDATE pipeline_items SET status='in_progress',claimed_at=?,attempts=attempts+1,updated_at=? WHERE id=?",
                        (started_at, started_at, item["id"]),
                    )
                    item["attempts"] = int(item["attempts"]) + 1
        if processor is None:
            completed_at = now_iso()
            with self._transaction() as conn:
                conn.execute(
                    """
                    UPDATE operations SET status='dry_run',item_count=?,wall_seconds=?,
                      details_json=?,completed_at=? WHERE id=?
                    """,
                    (
                        len(items),
                        max(0.0, now_monotonic() - start_clock),
                        _json(
                            {
                                "budgets": asdict(BUDGETS),
                                "automatic_model_transport": False,
                                "reclaimed_stale_item_leases": len(stale_ids),
                                "queued_item_ids": [item["id"] for item in items],
                            }
                        ),
                        completed_at,
                        operation_id,
                    ),
                )
            return self._require("operations", operation_id)

        processed = 0
        succeeded = 0
        dead_lettered = 0
        total_tokens = 0
        for item in items:
            if now_monotonic() - start_clock >= BUDGETS.max_cycle_seconds:
                with self._transaction() as conn:
                    conn.execute(
                        "UPDATE pipeline_items SET status='queued',claimed_at=NULL,updated_at=? WHERE id=?",
                        (now_iso(), item["id"]),
                    )
                continue
            processed += 1
            try:
                item_clock = now_monotonic()
                result = dict(processor(dict(item), BUDGETS))
                measured_item_seconds = max(0.0, now_monotonic() - item_clock)
                result["wall_seconds"] = max(
                    float(result.get("wall_seconds", 0)), measured_item_seconds
                )
                metrics = self._validate_pipeline_metrics(result)
                total_tokens += metrics["input_tokens"]
                if result.get("status", "succeeded") != "succeeded":
                    raise RadarPolicyError(str(result.get("error") or "processor reported failure"))
                completed_at = now_iso()
                with self._transaction() as conn:
                    conn.execute(
                        """
                        UPDATE pipeline_items SET status='succeeded',input_tokens=?,window_count=?,
                          schema_repairs=?,transport_retries=?,wall_seconds=?,completed_at=?,updated_at=?
                        WHERE id=?
                        """,
                        (
                            metrics["input_tokens"],
                            metrics["window_count"],
                            metrics["schema_repairs"],
                            metrics["transport_retries"],
                            metrics["wall_seconds"],
                            completed_at,
                            completed_at,
                            item["id"],
                        ),
                    )
                succeeded += 1
            except Exception as exc:
                with self._transaction() as conn:
                    self._dead_letter(
                        conn,
                        item=item,
                        error_class=type(exc).__name__,
                        error_message=str(exc),
                    )
                dead_lettered += 1
        completed_at = now_iso()
        cycle_seconds = max(0.0, now_monotonic() - start_clock)
        details = {
            "budgets": asdict(BUDGETS),
            "automatic_model_transport": False,
            "reclaimed_stale_item_leases": len(stale_ids),
            "succeeded": succeeded,
            "dead_lettered": dead_lettered,
            "deferred": len(items) - processed,
        }
        with self._transaction() as conn:
            conn.execute(
                """
                UPDATE operations SET status=?,item_count=?,input_tokens=?,wall_seconds=?,
                  details_json=?,completed_at=? WHERE id=?
                """,
                (
                    "completed" if dead_lettered == 0 else "completed_with_dead_letters",
                    processed,
                    total_tokens,
                    cycle_seconds,
                    _json(details),
                    completed_at,
                    operation_id,
                ),
            )
        return self._require("operations", operation_id)

    def operations_summary(self) -> dict[str, Any]:
        queue = {
            row["status"]: int(row["count"])
            for row in self._many(
                "SELECT status,COUNT(*) AS count FROM pipeline_items GROUP BY status"
            )
        }
        dead_letters = {
            row["status"]: int(row["count"])
            for row in self._many("SELECT status,COUNT(*) AS count FROM dead_letters GROUP BY status")
        }
        return {
            "budgets": asdict(BUDGETS),
            "pipeline_contract": asdict(PIPELINE_CONTRACT),
            "scheduler_enabled": False,
            "queue": queue,
            "dead_letters": dead_letters,
            "last_cycle": self._one(
                "SELECT * FROM operations WHERE kind='cycle' ORDER BY started_at DESC,id DESC LIMIT 1"
            ),
            "last_successful_cycle": self._one(
                """
                SELECT * FROM operations WHERE kind='cycle' AND status IN ('completed','dry_run')
                ORDER BY started_at DESC,id DESC LIMIT 1
                """
            ),
        }

    def status(self) -> dict[str, Any]:
        tables = (
            "workspaces",
            "sources",
            "content_items",
            "evidence_spans",
            "entities",
            "topics",
            "developments",
            "claims",
            "position_observations",
            "briefings",
            "feedback",
        )
        counts = {table: int(self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]) for table in tables}
        accepted_counts = {
            name: int(self.conn.execute(f"SELECT COUNT(*) FROM accepted_{name}").fetchone()[0])
            for name in (
                "evidence_spans",
                "entities",
                "topics",
                "developments",
                "claims",
                "position_observations",
                "briefings",
                "semantic_edges",
            )
        }
        return {
            "schema_version": SCHEMA_VERSION,
            "database_path": str(self.db_path),
            "private_local_only": True,
            "legacy_database_attached": False,
            "scheduler_enabled": False,
            "fts5_available": self.fts_available,
            "counts": counts,
            "accepted_counts": accepted_counts,
            "operations": self.operations_summary(),
        }

    def seed_demo(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        """Seed a deterministic vertical-slice payload without reading any legacy authority tables."""

        workspace_data = dict(payload.get("workspace") or {})
        if "id" in workspace_data and "workspace_id" not in workspace_data:
            workspace_data["workspace_id"] = workspace_data.pop("id")
        workspace = self.create_workspace(**workspace_data)
        result: dict[str, Any] = {"workspace_id": workspace["id"], "items": []}
        items = list(payload.get("items") or [payload])
        for item in items:
            seeded = self._seed_demo_item(workspace, dict(item))
            result["items"].append(seeded)
        result["inserted_items"] = len(result["items"])
        if len(result["items"]) == 1:
            result.update(result["items"][0])
        result["status"] = self.status()
        return result

    def _seed_demo_item(
        self, workspace: Mapping[str, Any], payload: Mapping[str, Any]
    ) -> dict[str, Any]:
        def normalize_id(data: dict[str, Any], field: str) -> dict[str, Any]:
            if "id" in data and field not in data:
                data[field] = data.pop("id")
            return data

        def existing(table: str, record_id: str | None) -> dict[str, Any] | None:
            return self._one(f"SELECT * FROM {table} WHERE id=?", (record_id,)) if record_id else None

        result: dict[str, Any] = {}
        source_data = normalize_id(dict(payload.get("source") or {}), "source_id")
        if "source_format" in source_data and "source_type" not in source_data:
            source_data["source_type"] = source_data.pop("source_format")
        source_data.pop("status", None)
        source_data.setdefault("workspace_id", workspace["id"])
        source = self.create_source(**source_data)
        result["source_id"] = source["id"]
        content_data = normalize_id(
            dict(payload.get("content") or payload.get("content_item") or {}),
            "content_item_id",
        )
        if "content_id" in content_data and "content_item_id" not in content_data:
            content_data["content_item_id"] = content_data.pop("content_id")
        if "content_format" in content_data and "content_type" not in content_data:
            content_data["content_type"] = content_data.pop("content_format")
        content_data.pop("workspace_id", None)
        content_data.setdefault("source_id", source["id"])
        content = self.create_content_item(**content_data)
        result["content_item_id"] = content["id"]
        evidence_data = normalize_id(
            dict(payload.get("evidence") or payload.get("evidence_span") or {}),
            "evidence_span_id",
        )
        if "evidence_id" in evidence_data and "evidence_span_id" not in evidence_data:
            evidence_data["evidence_span_id"] = evidence_data.pop("evidence_id")
        if "content_id" in evidence_data and "content_item_id" not in evidence_data:
            evidence_data["content_item_id"] = evidence_data.pop("content_id")
        expected_quote = evidence_data.pop("text", None)
        evidence_data.setdefault("content_item_id", content["id"])
        evidence_data.setdefault("confidence", 0.95)
        evidence_data.setdefault("status", "provisional")
        evidence_data.setdefault("extractor_version", "demo-fixture-v1")
        evidence_data.setdefault("release_id", "demo-release-v1")
        evidence_data.setdefault(
            "lineage",
            {
                "fixture": "research-radar-five-item-demo-v1",
                "content_sha256": content["content_sha256"],
                "window": 0,
            },
        )
        if expected_quote is not None:
            resolved_quote = content["normalized_text"][
                int(evidence_data["start_char"]) : int(evidence_data["end_char"])
            ]
            if resolved_quote != expected_quote:
                raise RadarPolicyError("demo evidence text does not match its exact offsets")
        evidence = existing("evidence_spans", evidence_data.get("evidence_span_id"))
        if evidence is None:
            evidence = self.create_evidence_span(**evidence_data)
        result["evidence_span_id"] = evidence["id"]

        common = {
            "workspace_id": workspace["id"],
            "evidence_span_id": evidence["id"],
            "extractor_version": evidence["extractor_version"],
            "release_id": evidence["release_id"],
            "lineage": evidence["lineage"],
        }
        entity_data = normalize_id(dict(payload.get("entity") or {}), "entity_id")
        if "name" in entity_data and "canonical_name" not in entity_data:
            entity_data["canonical_name"] = entity_data.pop("name")
        entity_data.setdefault("confidence", 0.9)
        entity_data.setdefault("status", "provisional")
        entity_data = {**common, **entity_data}
        entity = existing("entities", entity_data.get("entity_id"))
        if entity is None:
            entity = self.create_entity(**entity_data)
        result["entity_id"] = entity["id"]

        development_data = normalize_id(
            dict(payload.get("development") or {}), "development_id"
        )
        if "release_status" in development_data and "status" not in development_data:
            development_data["status"] = development_data.pop("release_status")
        if "significance_score" in development_data and "significance" not in development_data:
            development_data["significance"] = development_data.pop("significance_score")
        development_data.pop("workspace_id", None)
        development_data.setdefault("confidence", 0.9)
        development_data.setdefault(
            "directly_establishes", source["trust_tier"] == SourceTrustTier.PRIMARY.value
        )
        development_data = {**common, **development_data}
        development = existing("developments", development_data.get("development_id"))
        if development is None:
            development = self.create_development(**development_data)
        result["development_id"] = development["id"]

        claim_data = normalize_id(dict(payload.get("claim") or {}), "claim_id")
        if "statement" in claim_data and "claim_text" not in claim_data:
            claim_data["claim_text"] = claim_data.pop("statement")
        if "statement_kind" in claim_data and "claim_type" not in claim_data:
            claim_data["claim_type"] = claim_data.pop("statement_kind")
        claim_data.pop("evidence_id", None)
        claim_data.setdefault("status", "provisional")
        claim_data = {
            **common,
            "development_id": development["id"],
            "subject_entity_id": entity["id"],
            **claim_data,
        }
        claim = existing("claims", claim_data.get("claim_id"))
        if claim is None:
            claim = self.create_claim(**claim_data)
        result["claim_id"] = claim["id"]

        position_data = normalize_id(
            dict(payload.get("position") or payload.get("position_observation") or {}),
            "position_observation_id",
        )
        if "position_id" in position_data and "position_observation_id" not in position_data:
            position_data["position_observation_id"] = position_data.pop("position_id")
        if "release_status" in position_data and "status" not in position_data:
            position_data["status"] = position_data.pop("release_status")
        position_data.pop("evidence_id", None)
        position_data.setdefault("position_text", claim["claim_text"])
        position_data.setdefault("lineage", common["lineage"])
        position_data = {
            **common,
            "entity_id": entity["id"],
            "development_id": development["id"],
            "claim_id": claim["id"],
            **position_data,
        }
        position = existing(
            "position_observations", position_data.get("position_observation_id")
        )
        if position is None:
            position = self.create_position_observation(**position_data)
        result["position_observation_id"] = position["id"]

        briefing_data = normalize_id(
            dict(payload.get("brief") or payload.get("briefing") or {}), "briefing_id"
        )
        if "brief_id" in briefing_data and "briefing_id" not in briefing_data:
            briefing_data["briefing_id"] = briefing_data.pop("brief_id")
        if "title" in briefing_data and "headline" not in briefing_data:
            briefing_data["headline"] = briefing_data.pop("title")
        if "why_it_may_matter" in briefing_data and "why_it_matters" not in briefing_data:
            briefing_data["why_it_matters"] = briefing_data.pop("why_it_may_matter")
        if "release_status" in briefing_data and "status" not in briefing_data:
            briefing_data["status"] = briefing_data.pop("release_status")
        briefing_data.pop("workspace_id", None)
        briefing_data.pop("evidence_ids", None)
        briefing_data.setdefault("confidence", development["confidence"])
        briefing_data.setdefault(
            "directly_establishes", source["trust_tier"] == SourceTrustTier.PRIMARY.value
        )
        briefing_data = {**common, "development_id": development["id"], **briefing_data}
        briefing = existing("briefings", briefing_data.get("briefing_id"))
        if briefing is None:
            briefing = self.create_briefing(**briefing_data)
        result["briefing_id"] = briefing["id"]
        return result


__all__ = [
    "ACCEPTED_STATUSES",
    "ALLOWED_TRANSITIONS",
    "AuthorityStatus",
    "BUDGETS",
    "FeedbackRating",
    "PIPELINE_CONTRACT",
    "PIPELINE_STAGES",
    "PipelineBudgets",
    "PipelineContract",
    "PipelineStage",
    "RadarError",
    "RadarNotFoundError",
    "RadarPolicyError",
    "RadarTransitionError",
    "ResearchRadar",
    "SCHEMA_VERSION",
    "SourceStatus",
    "SourceTrustTier",
    "default_radar_db_path",
]
