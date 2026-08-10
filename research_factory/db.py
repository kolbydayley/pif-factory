from __future__ import annotations

import fcntl
import os
import re
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable

from .paths import db_path
from .util import dumps_json, now_iso, sha256_text


INIT_LOCK_TIMEOUT_SECONDS = 30.0


def connect(path: Path | None = None) -> sqlite3.Connection:
    conn = sqlite3.connect(path or db_path())
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA busy_timeout = 30000")
    return conn


SCHEMA = """
CREATE TABLE IF NOT EXISTS sources (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  rss_url TEXT,
  homepage_url TEXT,
  category TEXT,
  policy TEXT NOT NULL DEFAULT 'private_analysis_only',
  transcript_policy TEXT NOT NULL DEFAULT 'creator_rss_transcripts_only',
  enabled INTEGER NOT NULL DEFAULT 1,
  metadata_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS episodes (
  id TEXT PRIMARY KEY,
  source_id TEXT NOT NULL REFERENCES sources(id),
  guid TEXT NOT NULL,
  title TEXT NOT NULL,
  description TEXT,
  url TEXT,
  audio_url TEXT,
  published_at TEXT,
  duration_seconds INTEGER,
  feed_transcript_url TEXT,
  feed_transcript_type TEXT,
  verified_transcript_url TEXT,
  verified_transcript_type TEXT,
  verified_transcript_source_kind TEXT,
  transcript_url TEXT,
  transcript_type TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(source_id, guid)
);

CREATE TABLE IF NOT EXISTS transcripts (
  id TEXT PRIMARY KEY,
  episode_id TEXT NOT NULL REFERENCES episodes(id),
  source_kind TEXT NOT NULL,
  source_url TEXT,
  content_type TEXT,
  raw_text_path TEXT NOT NULL,
  raw_text_sha256 TEXT NOT NULL,
  status TEXT NOT NULL,
  fetched_at TEXT,
  policy_json TEXT NOT NULL DEFAULT '{}',
  word_count INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS transcript_preparations (
  id TEXT PRIMARY KEY,
  transcript_id TEXT NOT NULL REFERENCES transcripts(id),
  artifact_type TEXT NOT NULL,
  status TEXT NOT NULL,
  cleaned_text_path TEXT,
  cleaned_text_sha256 TEXT,
  original_word_count INTEGER NOT NULL DEFAULT 0,
  substantive_word_count INTEGER NOT NULL DEFAULT 0,
  boilerplate_word_count INTEGER NOT NULL DEFAULT 0,
  boilerplate_ratio REAL NOT NULL DEFAULT 0,
  speaker_turn_count INTEGER NOT NULL DEFAULT 0,
  quality_score REAL NOT NULL DEFAULT 0,
  notes_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(transcript_id)
);

CREATE TABLE IF NOT EXISTS transcript_acquisition_attempts (
  id TEXT PRIMARY KEY,
  episode_id TEXT NOT NULL REFERENCES episodes(id),
  source_id TEXT REFERENCES sources(id),
  method TEXT NOT NULL,
  status TEXT NOT NULL,
  result_url TEXT,
  result_source_kind TEXT,
  official_public INTEGER NOT NULL DEFAULT 0,
  policy_allowed INTEGER NOT NULL DEFAULT 1,
  error_class TEXT,
  notes TEXT,
  worker_id TEXT,
  idempotency_key TEXT,
  next_eligible_at TEXT,
  metadata_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS transcript_acquisition_status (
  episode_id TEXT PRIMARY KEY REFERENCES episodes(id),
  source_id TEXT REFERENCES sources(id),
  status TEXT NOT NULL,
  last_method TEXT,
  attempts_count INTEGER NOT NULL DEFAULT 0,
  eligible_for_transcription INTEGER NOT NULL DEFAULT 0,
  next_eligible_at TEXT,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS transcription_runs (
  id TEXT PRIMARY KEY,
  job_id INTEGER REFERENCES jobs(id),
  episode_id TEXT NOT NULL REFERENCES episodes(id),
  provider TEXT NOT NULL,
  status TEXT NOT NULL,
  transcript_id TEXT,
  output_path TEXT,
  error TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  completed_at TEXT
);

CREATE TABLE IF NOT EXISTS segments (
  id TEXT PRIMARY KEY,
  transcript_id TEXT NOT NULL REFERENCES transcripts(id),
  episode_id TEXT NOT NULL REFERENCES episodes(id),
  source_id TEXT NOT NULL REFERENCES sources(id),
  segment_index INTEGER NOT NULL,
  start_char INTEGER NOT NULL,
  end_char INTEGER NOT NULL,
  text_path TEXT NOT NULL,
  text_sha256 TEXT NOT NULL,
  word_count INTEGER NOT NULL,
  created_at TEXT NOT NULL,
  UNIQUE(transcript_id, segment_index)
);

CREATE TABLE IF NOT EXISTS people (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  org_id TEXT,
  metadata_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS orgs (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  metadata_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS products (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  org_id TEXT,
  metadata_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS labels (
  id TEXT PRIMARY KEY,
  segment_id TEXT NOT NULL REFERENCES segments(id),
  label_pack TEXT NOT NULL,
  label_pack_version TEXT NOT NULL,
  model TEXT NOT NULL,
  status TEXT NOT NULL,
  output_json TEXT NOT NULL,
  evidence_start INTEGER,
  evidence_end INTEGER,
  confidence REAL,
  needs_review INTEGER NOT NULL DEFAULT 0,
  worker_id TEXT,
  prompt_path TEXT,
  output_path TEXT,
  created_at TEXT NOT NULL,
  UNIQUE(segment_id, label_pack, label_pack_version, model)
);

CREATE TABLE IF NOT EXISTS claims (
  id TEXT PRIMARY KEY,
  label_id TEXT NOT NULL REFERENCES labels(id),
  segment_id TEXT NOT NULL REFERENCES segments(id),
  text TEXT NOT NULL,
  stance TEXT,
  confidence REAL,
  evidence_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS label_runs (
  id TEXT PRIMARY KEY,
  job_id INTEGER,
  segment_id TEXT NOT NULL REFERENCES segments(id),
  label_pack TEXT NOT NULL,
  model TEXT NOT NULL,
  prompt_path TEXT,
  output_path TEXT,
  status TEXT NOT NULL,
  claimed_at TEXT,
  completed_at TEXT,
  error TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS episode_context_runs (
  id TEXT PRIMARY KEY,
  job_id INTEGER,
  episode_id TEXT NOT NULL REFERENCES episodes(id),
  transcript_id TEXT REFERENCES transcripts(id),
  label_pack TEXT NOT NULL,
  model TEXT NOT NULL,
  status TEXT NOT NULL,
  prompt_path TEXT,
  output_path TEXT,
  context_artifact_path TEXT,
  speaker_map_json TEXT NOT NULL DEFAULT '[]',
  section_map_json TEXT NOT NULL DEFAULT '[]',
  entity_seed_json TEXT NOT NULL DEFAULT '{}',
  concept_seed_json TEXT NOT NULL DEFAULT '[]',
  extraction_guidance TEXT,
  error TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  completed_at TEXT,
  UNIQUE(episode_id, label_pack, model)
);

CREATE TABLE IF NOT EXISTS episode_context_run_attempts (
  id TEXT PRIMARY KEY,
  canonical_run_id TEXT NOT NULL REFERENCES episode_context_runs(id),
  job_id INTEGER,
  episode_id TEXT NOT NULL REFERENCES episodes(id),
  transcript_id TEXT REFERENCES transcripts(id),
  label_pack TEXT NOT NULL,
  queue_payload_model TEXT NOT NULL,
  semantic_model TEXT NOT NULL,
  reasoning_effort TEXT,
  attempt_kind TEXT NOT NULL,
  status TEXT NOT NULL,
  attempt_number INTEGER NOT NULL,
  claim_id TEXT,
  contract_sha256 TEXT,
  frozen_configuration_sha256 TEXT,
  prompt_path TEXT,
  output_path TEXT,
  context_artifact_path TEXT,
  launch_path TEXT,
  sidecar_path TEXT,
  canonical_snapshot_json TEXT NOT NULL DEFAULT '{}',
  artifact_records_json TEXT NOT NULL DEFAULT '{}',
  error TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  completed_at TEXT,
  UNIQUE(canonical_run_id, attempt_number)
);

CREATE TABLE IF NOT EXISTS quality_audits (
  id TEXT PRIMARY KEY,
  label_id TEXT REFERENCES labels(id),
  segment_id TEXT REFERENCES segments(id),
  label_pack TEXT NOT NULL,
  auditor_model TEXT,
  status TEXT NOT NULL,
  score REAL,
  disagreement_json TEXT NOT NULL DEFAULT '{}',
  notes TEXT,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS reviewer_audits (
  id TEXT PRIMARY KEY,
  pilot_id TEXT NOT NULL,
  episode_id TEXT NOT NULL REFERENCES episodes(id),
  model TEXT NOT NULL,
  review_mode TEXT NOT NULL DEFAULT 'full',
  patch_tag TEXT,
  focus_json TEXT NOT NULL DEFAULT '{}',
  status TEXT NOT NULL,
  overall_score REAL NOT NULL DEFAULT 0,
  coverage_score REAL NOT NULL DEFAULT 0,
  precision_score REAL NOT NULL DEFAULT 0,
  grounding_score REAL NOT NULL DEFAULT 0,
  identity_score REAL NOT NULL DEFAULT 0,
  product_market_score REAL NOT NULL DEFAULT 0,
  missed_signals_count INTEGER NOT NULL DEFAULT 0,
  false_or_weak_events_count INTEGER NOT NULL DEFAULT 0,
  p0_issue_count INTEGER NOT NULL DEFAULT 0,
  review_json TEXT NOT NULL DEFAULT '{}',
  prompt_path TEXT,
  output_path TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  completed_at TEXT
);

CREATE TABLE IF NOT EXISTS quality_iterations (
  id TEXT PRIMARY KEY,
  pilot_id TEXT NOT NULL,
  patch_tag TEXT NOT NULL,
  tier TEXT NOT NULL,
  status TEXT NOT NULL,
  metrics_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL,
  completed_at TEXT
);

CREATE TABLE IF NOT EXISTS guest_appearances (
  id TEXT PRIMARY KEY,
  episode_id TEXT NOT NULL REFERENCES episodes(id),
  person_id TEXT NOT NULL REFERENCES people(id),
  role TEXT NOT NULL,
  confidence REAL,
  evidence_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL,
  UNIQUE(episode_id, person_id, role)
);

CREATE TABLE IF NOT EXISTS release_events (
  id TEXT PRIMARY KEY,
  org_id TEXT,
  product_id TEXT,
  event_name TEXT NOT NULL,
  event_date TEXT,
  source_url TEXT,
  metadata_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS trend_memos (
  id TEXT PRIMARY KEY,
  topic TEXT NOT NULL,
  window TEXT NOT NULL,
  memo_path TEXT NOT NULL,
  evidence_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS coded_observations (
  id TEXT PRIMARY KEY,
  label_id TEXT NOT NULL REFERENCES labels(id),
  segment_id TEXT NOT NULL REFERENCES segments(id),
  observation_index INTEGER NOT NULL,
  code_family TEXT NOT NULL,
  code_id TEXT NOT NULL,
  construct_type TEXT NOT NULL,
  speaker TEXT,
  speaker_role TEXT,
  stance TEXT,
  temporal_horizon TEXT,
  claim_strength TEXT,
  confidence REAL,
  evidence_text TEXT NOT NULL,
  evidence_start INTEGER NOT NULL,
  evidence_end INTEGER NOT NULL,
  entities_json TEXT NOT NULL DEFAULT '{}',
  status TEXT NOT NULL DEFAULT 'ready',
  audit_status TEXT,
  created_at TEXT NOT NULL,
  UNIQUE(label_id, observation_index)
);

CREATE TABLE IF NOT EXISTS topic_mentions (
  id TEXT PRIMARY KEY,
  observation_id TEXT NOT NULL REFERENCES coded_observations(id),
  segment_id TEXT NOT NULL REFERENCES segments(id),
  topic TEXT NOT NULL,
  stance TEXT,
  confidence REAL,
  evidence_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS term_mentions (
  id TEXT PRIMARY KEY,
  observation_id TEXT NOT NULL REFERENCES coded_observations(id),
  segment_id TEXT NOT NULL REFERENCES segments(id),
  term TEXT NOT NULL,
  term_role TEXT NOT NULL,
  speaker TEXT,
  confidence REAL,
  evidence_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS entity_mentions (
  id TEXT PRIMARY KEY,
  observation_id TEXT NOT NULL REFERENCES coded_observations(id),
  segment_id TEXT NOT NULL REFERENCES segments(id),
  entity_type TEXT NOT NULL,
  name TEXT NOT NULL,
  role TEXT,
  confidence REAL,
  evidence_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS speaker_positions (
  id TEXT PRIMARY KEY,
  observation_id TEXT NOT NULL REFERENCES coded_observations(id),
  segment_id TEXT NOT NULL REFERENCES segments(id),
  speaker TEXT,
  speaker_role TEXT,
  code_id TEXT NOT NULL,
  stance TEXT,
  confidence REAL,
  evidence_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS relationship_edges (
  id TEXT PRIMARY KEY,
  observation_id TEXT NOT NULL REFERENCES coded_observations(id),
  segment_id TEXT NOT NULL REFERENCES segments(id),
  source_name TEXT NOT NULL,
  target_name TEXT NOT NULL,
  relationship TEXT NOT NULL,
  confidence REAL,
  evidence_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS product_signals (
  id TEXT PRIMARY KEY,
  observation_id TEXT NOT NULL REFERENCES coded_observations(id),
  segment_id TEXT NOT NULL REFERENCES segments(id),
  product TEXT,
  organization TEXT,
  signal_type TEXT NOT NULL,
  signal_text TEXT NOT NULL,
  confidence REAL,
  evidence_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS release_signal_links (
  id TEXT PRIMARY KEY,
  observation_id TEXT NOT NULL REFERENCES coded_observations(id),
  segment_id TEXT NOT NULL REFERENCES segments(id),
  organization TEXT,
  product TEXT,
  signal_type TEXT NOT NULL,
  release_event_id TEXT,
  confidence REAL,
  evidence_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS concepts (
  id TEXT PRIMARY KEY,
  canonical_name TEXT NOT NULL,
  normalized_name TEXT NOT NULL UNIQUE,
  status TEXT NOT NULL DEFAULT 'candidate',
  description TEXT,
  usefulness_score REAL NOT NULL DEFAULT 0,
  first_seen_at TEXT,
  last_seen_at TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS concept_aliases (
  id TEXT PRIMARY KEY,
  concept_id TEXT NOT NULL REFERENCES concepts(id),
  alias TEXT NOT NULL,
  normalized_alias TEXT NOT NULL,
  alias_type TEXT NOT NULL DEFAULT 'surface_term',
  status TEXT NOT NULL DEFAULT 'candidate',
  version INTEGER NOT NULL DEFAULT 1,
  evidence_count INTEGER NOT NULL DEFAULT 0,
  source_diversity INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(concept_id, normalized_alias, version)
);

CREATE TABLE IF NOT EXISTS concept_versions (
  id TEXT PRIMARY KEY,
  concept_id TEXT NOT NULL REFERENCES concepts(id),
  version INTEGER NOT NULL,
  change_type TEXT NOT NULL,
  change_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL,
  UNIQUE(concept_id, version, change_type)
);

CREATE TABLE IF NOT EXISTS discourse_events (
  id TEXT PRIMARY KEY,
  label_id TEXT NOT NULL REFERENCES labels(id),
  segment_id TEXT NOT NULL REFERENCES segments(id),
  event_index INTEGER NOT NULL,
  event_type TEXT NOT NULL,
  actor_name TEXT,
  actor_type TEXT,
  actor_affiliation TEXT,
  target_raw TEXT,
  candidate_concept TEXT,
  canonical_concept_id TEXT REFERENCES concepts(id),
  canonical_concept_name TEXT,
  stance TEXT,
  claim_text TEXT,
  claim_type TEXT,
  certainty TEXT,
  temporal_horizon TEXT,
  frame TEXT,
  causal_mechanism TEXT,
  counterclaim TEXT,
  confidence REAL,
  evidence_text TEXT NOT NULL,
  evidence_start INTEGER NOT NULL,
  evidence_end INTEGER NOT NULL,
  surface_terms_json TEXT NOT NULL DEFAULT '[]',
  model_names_json TEXT NOT NULL DEFAULT '[]',
  product_names_json TEXT NOT NULL DEFAULT '[]',
  organizations_json TEXT NOT NULL DEFAULT '[]',
  people_json TEXT NOT NULL DEFAULT '[]',
  audit_status TEXT,
  created_at TEXT NOT NULL,
  UNIQUE(label_id, event_index)
);

CREATE TABLE IF NOT EXISTS discourse_event_contexts (
  discourse_event_id TEXT PRIMARY KEY REFERENCES discourse_events(id),
  label_id TEXT NOT NULL REFERENCES labels(id),
  segment_id TEXT NOT NULL REFERENCES segments(id),
  label_pack TEXT NOT NULL,
  source_context_kind TEXT NOT NULL,
  source_context_confidence REAL,
  speaker_name TEXT,
  speaker_role TEXT,
  speaker_affiliation TEXT,
  speaker_confidence REAL,
  reported_actor_name TEXT,
  reported_actor_type TEXT,
  reported_actor_affiliation TEXT,
  reported_actor_confidence REAL,
  event_subtype TEXT,
  signal_reason TEXT,
  metric_json TEXT NOT NULL DEFAULT '{}',
  exclusion_flags_json TEXT NOT NULL DEFAULT '[]',
  quality_flags_json TEXT NOT NULL DEFAULT '[]',
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS canonical_people (
  id TEXT PRIMARY KEY,
  display_name TEXT NOT NULL,
  normalized_name TEXT NOT NULL UNIQUE,
  primary_org_id TEXT,
  confidence REAL NOT NULL DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'candidate',
  canonical_version INTEGER NOT NULL DEFAULT 1,
  evidence_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS canonical_orgs (
  id TEXT PRIMARY KEY,
  display_name TEXT NOT NULL,
  normalized_name TEXT NOT NULL UNIQUE,
  confidence REAL NOT NULL DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'candidate',
  canonical_version INTEGER NOT NULL DEFAULT 1,
  evidence_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS canonical_products (
  id TEXT PRIMARY KEY,
  display_name TEXT NOT NULL,
  normalized_name TEXT NOT NULL UNIQUE,
  canonical_org_id TEXT REFERENCES canonical_orgs(id),
  confidence REAL NOT NULL DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'candidate',
  canonical_version INTEGER NOT NULL DEFAULT 1,
  evidence_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS canonical_models (
  id TEXT PRIMARY KEY,
  display_name TEXT NOT NULL,
  normalized_name TEXT NOT NULL UNIQUE,
  canonical_org_id TEXT REFERENCES canonical_orgs(id),
  canonical_product_id TEXT REFERENCES canonical_products(id),
  confidence REAL NOT NULL DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'candidate',
  canonical_version INTEGER NOT NULL DEFAULT 1,
  evidence_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS raw_speaker_mentions (
  id TEXT PRIMARY KEY,
  episode_id TEXT NOT NULL REFERENCES episodes(id),
  segment_id TEXT REFERENCES segments(id),
  discourse_event_id TEXT REFERENCES discourse_events(id),
  surface_name TEXT NOT NULL,
  role TEXT,
  affiliation_surface TEXT,
  canonical_person_id TEXT REFERENCES canonical_people(id),
  resolution_status TEXT NOT NULL DEFAULT 'unresolved',
  confidence REAL,
  evidence_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS raw_actor_mentions (
  id TEXT PRIMARY KEY,
  episode_id TEXT NOT NULL REFERENCES episodes(id),
  segment_id TEXT REFERENCES segments(id),
  discourse_event_id TEXT REFERENCES discourse_events(id),
  surface_name TEXT NOT NULL,
  mention_type TEXT NOT NULL,
  speaker_surface TEXT,
  reported_actor_surface TEXT,
  role_context TEXT,
  canonical_entity_type TEXT,
  canonical_entity_id TEXT,
  resolution_status TEXT NOT NULL DEFAULT 'unresolved',
  alias_flag INTEGER NOT NULL DEFAULT 0,
  confidence REAL,
  why_matters TEXT,
  evidence_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS identity_aliases (
  id TEXT PRIMARY KEY,
  canonical_entity_type TEXT NOT NULL,
  canonical_entity_id TEXT NOT NULL,
  alias TEXT NOT NULL,
  normalized_alias TEXT NOT NULL,
  alias_type TEXT NOT NULL DEFAULT 'surface_form',
  status TEXT NOT NULL DEFAULT 'candidate',
  canonical_version INTEGER NOT NULL DEFAULT 1,
  evidence_count INTEGER NOT NULL DEFAULT 0,
  evidence_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(canonical_entity_type, canonical_entity_id, normalized_alias, canonical_version)
);

CREATE TABLE IF NOT EXISTS identity_resolution_candidates (
  id TEXT PRIMARY KEY,
  raw_mention_table TEXT NOT NULL,
  raw_mention_id TEXT NOT NULL,
  candidate_entity_type TEXT NOT NULL,
  candidate_entity_id TEXT,
  candidate_display_name TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'candidate_match',
  confidence REAL NOT NULL DEFAULT 0,
  judge_model TEXT,
  rationale TEXT,
  evidence_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS identity_merge_decisions (
  id TEXT PRIMARY KEY,
  source_entity_type TEXT NOT NULL,
  source_entity_id TEXT NOT NULL,
  target_entity_type TEXT NOT NULL,
  target_entity_id TEXT NOT NULL,
  decision TEXT NOT NULL,
  judge_model TEXT NOT NULL,
  confidence REAL NOT NULL DEFAULT 0,
  rationale TEXT NOT NULL,
  evidence_json TEXT NOT NULL DEFAULT '{}',
  decided_at TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS identity_split_decisions (
  id TEXT PRIMARY KEY,
  canonical_entity_type TEXT NOT NULL,
  canonical_entity_id TEXT NOT NULL,
  split_reason TEXT NOT NULL,
  judge_model TEXT NOT NULL,
  confidence REAL NOT NULL DEFAULT 0,
  rationale TEXT NOT NULL,
  evidence_json TEXT NOT NULL DEFAULT '{}',
  decided_at TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS speaker_identity_runs (
  id TEXT PRIMARY KEY,
  run_type TEXT NOT NULL,
  model TEXT NOT NULL,
  status TEXT NOT NULL,
  parameters_json TEXT NOT NULL DEFAULT '{}',
  metrics_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS person_org_affiliations (
  id TEXT PRIMARY KEY,
  canonical_person_id TEXT NOT NULL REFERENCES canonical_people(id),
  canonical_org_id TEXT NOT NULL REFERENCES canonical_orgs(id),
  role TEXT,
  start_date TEXT,
  end_date TEXT,
  status TEXT NOT NULL DEFAULT 'candidate',
  confidence REAL NOT NULL DEFAULT 0,
  evidence_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(canonical_person_id, canonical_org_id, role, start_date, end_date)
);

CREATE TABLE IF NOT EXISTS podcast_guest_edges (
  id TEXT PRIMARY KEY,
  source_id TEXT NOT NULL REFERENCES sources(id),
  episode_id TEXT NOT NULL REFERENCES episodes(id),
  canonical_person_id TEXT NOT NULL REFERENCES canonical_people(id),
  role TEXT NOT NULL DEFAULT 'guest',
  confidence REAL NOT NULL DEFAULT 0,
  evidence_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL,
  UNIQUE(source_id, episode_id, canonical_person_id, role)
);

CREATE TABLE IF NOT EXISTS claim_clusters (
  id TEXT PRIMARY KEY,
  canonical_claim_text TEXT NOT NULL,
  concept_id TEXT REFERENCES concepts(id),
  status TEXT NOT NULL DEFAULT 'candidate',
  judge_model TEXT,
  confidence REAL NOT NULL DEFAULT 0,
  evidence_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS claim_canonicalization_runs (
  id TEXT PRIMARY KEY,
  scope TEXT NOT NULL,
  model TEXT NOT NULL,
  status TEXT NOT NULL,
  parameters_json TEXT NOT NULL DEFAULT '{}',
  metrics_json TEXT NOT NULL DEFAULT '{}',
  started_at TEXT NOT NULL,
  completed_at TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS claim_cluster_members (
  id TEXT PRIMARY KEY,
  cluster_id TEXT NOT NULL REFERENCES claim_clusters(id),
  claim_id TEXT NOT NULL REFERENCES claims(id),
  relation TEXT NOT NULL DEFAULT 'supports_canonical_claim',
  confidence REAL NOT NULL DEFAULT 0,
  method TEXT NOT NULL,
  run_id TEXT REFERENCES claim_canonicalization_runs(id),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(cluster_id, claim_id, relation)
);

CREATE TABLE IF NOT EXISTS claim_subject_runs (
  id TEXT PRIMARY KEY,
  scope TEXT NOT NULL,
  model TEXT NOT NULL,
  status TEXT NOT NULL,
  parameters_json TEXT NOT NULL DEFAULT '{}',
  metrics_json TEXT NOT NULL DEFAULT '{}',
  started_at TEXT NOT NULL,
  completed_at TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS claim_subjects (
  id TEXT PRIMARY KEY,
  subject_text TEXT NOT NULL,
  subject_key TEXT NOT NULL,
  domain TEXT,
  subject_type TEXT NOT NULL DEFAULT 'technology_issue',
  status TEXT NOT NULL DEFAULT 'candidate',
  confidence REAL NOT NULL DEFAULT 0,
  method TEXT NOT NULL,
  evidence_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(subject_key, method)
);

CREATE TABLE IF NOT EXISTS claim_subject_members (
  id TEXT PRIMARY KEY,
  subject_id TEXT NOT NULL REFERENCES claim_subjects(id),
  claim_id TEXT NOT NULL REFERENCES claims(id),
  relation TEXT NOT NULL DEFAULT 'about_subject',
  confidence REAL NOT NULL DEFAULT 0,
  method TEXT NOT NULL,
  run_id TEXT REFERENCES claim_subject_runs(id),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(subject_id, claim_id, relation)
);

CREATE TABLE IF NOT EXISTS claim_proposition_variants (
  id TEXT PRIMARY KEY,
  subject_id TEXT NOT NULL REFERENCES claim_subjects(id),
  variant_text TEXT NOT NULL,
  variant_key TEXT NOT NULL,
  predicate TEXT,
  object_text TEXT,
  technology_system TEXT,
  market_context TEXT,
  geography TEXT,
  horizon TEXT,
  numeric_value TEXT,
  numeric_unit TEXT,
  condition_text TEXT,
  polarity TEXT,
  status TEXT NOT NULL DEFAULT 'candidate',
  confidence REAL NOT NULL DEFAULT 0,
  method TEXT NOT NULL,
  evidence_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(subject_id, variant_key, method)
);

CREATE TABLE IF NOT EXISTS claim_position_observations (
  id TEXT PRIMARY KEY,
  subject_id TEXT NOT NULL REFERENCES claim_subjects(id),
  variant_id TEXT NOT NULL REFERENCES claim_proposition_variants(id),
  claim_id TEXT NOT NULL REFERENCES claims(id),
  discourse_event_id TEXT REFERENCES discourse_events(id),
  canonical_person_id TEXT REFERENCES canonical_people(id),
  speaker_name TEXT,
  speaker_affiliation TEXT,
  source_id TEXT REFERENCES sources(id),
  episode_id TEXT REFERENCES episodes(id),
  stance TEXT,
  certainty TEXT,
  frame TEXT,
  published_at TEXT,
  evidence_json TEXT NOT NULL DEFAULT '{}',
  confidence REAL NOT NULL DEFAULT 0,
  method TEXT NOT NULL,
  run_id TEXT REFERENCES claim_subject_runs(id),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(claim_id, subject_id, variant_id)
);

CREATE TABLE IF NOT EXISTS claim_subject_event_observations (
  id TEXT PRIMARY KEY,
  subject_id TEXT NOT NULL REFERENCES claim_subjects(id),
  discourse_event_id TEXT NOT NULL REFERENCES discourse_events(id),
  event_type TEXT NOT NULL,
  event_role TEXT NOT NULL,
  canonical_person_id TEXT REFERENCES canonical_people(id),
  speaker_name TEXT,
  speaker_affiliation TEXT,
  source_id TEXT REFERENCES sources(id),
  episode_id TEXT REFERENCES episodes(id),
  stance TEXT,
  certainty TEXT,
  frame TEXT,
  concept_name TEXT,
  published_at TEXT,
  evidence_json TEXT NOT NULL DEFAULT '{}',
  confidence REAL NOT NULL DEFAULT 0,
  method TEXT NOT NULL,
  run_id TEXT REFERENCES claim_subject_runs(id),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(discourse_event_id, subject_id, method)
);

CREATE TABLE IF NOT EXISTS claim_subject_expert_positions (
  id TEXT PRIMARY KEY,
  subject_id TEXT NOT NULL REFERENCES claim_subjects(id),
  canonical_person_id TEXT REFERENCES canonical_people(id),
  speaker_key TEXT NOT NULL,
  speaker_name TEXT NOT NULL,
  speaker_affiliation TEXT,
  stance TEXT NOT NULL,
  observation_count INTEGER NOT NULL DEFAULT 0,
  claim_observation_count INTEGER NOT NULL DEFAULT 0,
  event_observation_count INTEGER NOT NULL DEFAULT 0,
  source_count INTEGER NOT NULL DEFAULT 0,
  episode_count INTEGER NOT NULL DEFAULT 0,
  first_published_at TEXT,
  last_published_at TEXT,
  confidence REAL NOT NULL DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'candidate',
  evidence_json TEXT NOT NULL DEFAULT '{}',
  method TEXT NOT NULL,
  run_id TEXT REFERENCES claim_subject_runs(id),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(subject_id, speaker_key, stance, method)
);

CREATE TABLE IF NOT EXISTS agreement_edges (
  id TEXT PRIMARY KEY,
  source_claim_id TEXT NOT NULL REFERENCES claims(id),
  target_claim_id TEXT NOT NULL REFERENCES claims(id),
  source_person_id TEXT REFERENCES canonical_people(id),
  target_person_id TEXT REFERENCES canonical_people(id),
  relation TEXT NOT NULL DEFAULT 'supporting',
  judge_model TEXT NOT NULL,
  confidence REAL NOT NULL DEFAULT 0,
  rationale TEXT NOT NULL,
  evidence_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS disagreement_edges (
  id TEXT PRIMARY KEY,
  source_claim_id TEXT NOT NULL REFERENCES claims(id),
  target_claim_id TEXT NOT NULL REFERENCES claims(id),
  source_person_id TEXT REFERENCES canonical_people(id),
  target_person_id TEXT REFERENCES canonical_people(id),
  relation TEXT NOT NULL DEFAULT 'conflicting',
  judge_model TEXT NOT NULL,
  confidence REAL NOT NULL DEFAULT 0,
  rationale TEXT NOT NULL,
  evidence_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS person_concept_edges (
  id TEXT PRIMARY KEY,
  canonical_person_id TEXT NOT NULL REFERENCES canonical_people(id),
  concept_id TEXT REFERENCES concepts(id),
  concept_name TEXT,
  edge_type TEXT NOT NULL,
  weight REAL NOT NULL DEFAULT 1,
  confidence REAL NOT NULL DEFAULT 0,
  evidence_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS person_person_mentions (
  id TEXT PRIMARY KEY,
  source_person_id TEXT REFERENCES canonical_people(id),
  target_person_id TEXT REFERENCES canonical_people(id),
  source_surface TEXT,
  target_surface TEXT NOT NULL,
  episode_id TEXT NOT NULL REFERENCES episodes(id),
  segment_id TEXT REFERENCES segments(id),
  discourse_event_id TEXT REFERENCES discourse_events(id),
  confidence REAL NOT NULL DEFAULT 0,
  evidence_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS person_org_edges (
  id TEXT PRIMARY KEY,
  canonical_person_id TEXT REFERENCES canonical_people(id),
  canonical_org_id TEXT REFERENCES canonical_orgs(id),
  person_surface TEXT,
  org_surface TEXT NOT NULL,
  edge_type TEXT NOT NULL,
  confidence REAL NOT NULL DEFAULT 0,
  evidence_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS expert_authority_scores (
  id TEXT PRIMARY KEY,
  canonical_person_id TEXT REFERENCES canonical_people(id),
  concept_id TEXT REFERENCES concepts(id),
  topic TEXT,
  score REAL NOT NULL,
  status TEXT NOT NULL DEFAULT 'preliminary',
  components_json TEXT NOT NULL DEFAULT '{}',
  graph_score_run_id TEXT,
  evidence_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS podcast_reach_scores (
  id TEXT PRIMARY KEY,
  source_id TEXT REFERENCES sources(id),
  canonical_person_id TEXT REFERENCES canonical_people(id),
  concept_id TEXT REFERENCES concepts(id),
  score REAL NOT NULL,
  components_json TEXT NOT NULL DEFAULT '{}',
  graph_score_run_id TEXT,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS forecast_outcome_checks (
  id TEXT PRIMARY KEY,
  claim_id TEXT NOT NULL REFERENCES claims(id),
  canonical_person_id TEXT REFERENCES canonical_people(id),
  outcome_status TEXT NOT NULL DEFAULT 'pending',
  outcome_date TEXT,
  judge_model TEXT,
  confidence REAL NOT NULL DEFAULT 0,
  rationale TEXT,
  evidence_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS graph_score_runs (
  id TEXT PRIMARY KEY,
  run_type TEXT NOT NULL,
  model TEXT,
  status TEXT NOT NULL,
  parameters_json TEXT NOT NULL DEFAULT '{}',
  metrics_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS term_usages (
  id TEXT PRIMARY KEY,
  discourse_event_id TEXT NOT NULL REFERENCES discourse_events(id),
  segment_id TEXT NOT NULL REFERENCES segments(id),
  term TEXT NOT NULL,
  concept_id TEXT REFERENCES concepts(id),
  actor_name TEXT,
  term_role TEXT NOT NULL,
  confidence REAL,
  evidence_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS frame_usages (
  id TEXT PRIMARY KEY,
  discourse_event_id TEXT NOT NULL REFERENCES discourse_events(id),
  segment_id TEXT NOT NULL REFERENCES segments(id),
  frame TEXT NOT NULL,
  concept_id TEXT REFERENCES concepts(id),
  actor_name TEXT,
  stance TEXT,
  confidence REAL,
  evidence_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS actor_positions (
  id TEXT PRIMARY KEY,
  discourse_event_id TEXT NOT NULL REFERENCES discourse_events(id),
  segment_id TEXT NOT NULL REFERENCES segments(id),
  actor_name TEXT,
  actor_type TEXT,
  actor_affiliation TEXT,
  concept_id TEXT REFERENCES concepts(id),
  concept_name TEXT,
  stance TEXT,
  claim_type TEXT,
  certainty TEXT,
  temporal_horizon TEXT,
  confidence REAL,
  evidence_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS concept_candidates (
  id TEXT PRIMARY KEY,
  candidate TEXT NOT NULL,
  normalized_candidate TEXT NOT NULL UNIQUE,
  status TEXT NOT NULL DEFAULT 'candidate',
  surface_terms_json TEXT NOT NULL DEFAULT '[]',
  evidence_count INTEGER NOT NULL DEFAULT 0,
  source_diversity INTEGER NOT NULL DEFAULT 0,
  usefulness_score REAL NOT NULL DEFAULT 0,
  rationale TEXT,
  evidence_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS signal_runs (
  id TEXT PRIMARY KEY,
  run_type TEXT NOT NULL,
  window TEXT NOT NULL,
  slice_key TEXT,
  parameters_json TEXT NOT NULL DEFAULT '{}',
  status TEXT NOT NULL,
  metrics_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS shift_signals (
  id TEXT PRIMARY KEY,
  signal_run_id TEXT NOT NULL REFERENCES signal_runs(id),
  signal_type TEXT NOT NULL,
  slice_key TEXT,
  concept_id TEXT REFERENCES concepts(id),
  concept_name TEXT,
  term_a TEXT,
  term_b TEXT,
  window_start TEXT,
  window_end TEXT,
  score REAL NOT NULL DEFAULT 0,
  support INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'llm_adjudicated',
  summary TEXT NOT NULL,
  evidence_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS jobs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  lane TEXT NOT NULL,
  job_type TEXT NOT NULL,
  target_id TEXT NOT NULL,
  payload_json TEXT NOT NULL DEFAULT '{}',
  status TEXT NOT NULL DEFAULT 'pending',
  priority INTEGER NOT NULL DEFAULT 100,
  attempts INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 2,
  lease_owner TEXT,
  leased_until TEXT,
  dedupe_key TEXT NOT NULL UNIQUE,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  completed_at TEXT,
  error TEXT
);

CREATE TABLE IF NOT EXISTS content_sources (
  id TEXT PRIMARY KEY,
  legacy_source_id TEXT REFERENCES sources(id),
  source_type TEXT NOT NULL,
  name TEXT NOT NULL,
  homepage_url TEXT,
  feed_url TEXT,
  category TEXT,
  policy TEXT NOT NULL DEFAULT 'private_analysis_only',
  enabled INTEGER NOT NULL DEFAULT 1,
  metadata_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(source_type, name)
);

CREATE TABLE IF NOT EXISTS content_items (
  id TEXT PRIMARY KEY,
  content_source_id TEXT REFERENCES content_sources(id),
  legacy_episode_id TEXT REFERENCES episodes(id),
  content_type TEXT NOT NULL,
  external_id TEXT,
  title TEXT NOT NULL,
  canonical_url TEXT,
  published_at TEXT,
  acquisition_status TEXT NOT NULL DEFAULT 'candidate',
  privacy_tier TEXT NOT NULL DEFAULT 'local_only',
  metadata_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(content_source_id, content_type, external_id)
);

CREATE TABLE IF NOT EXISTS content_artifacts (
  id TEXT PRIMARY KEY,
  content_item_id TEXT NOT NULL REFERENCES content_items(id),
  artifact_type TEXT NOT NULL,
  source_kind TEXT NOT NULL,
  source_url TEXT,
  local_path TEXT,
  sha256 TEXT,
  word_count INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'ready',
  privacy_tier TEXT NOT NULL DEFAULT 'local_only',
  policy_json TEXT NOT NULL DEFAULT '{}',
  metadata_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(content_item_id, artifact_type, source_kind, source_url)
);

CREATE TABLE IF NOT EXISTS content_spans (
  id TEXT PRIMARY KEY,
  content_item_id TEXT NOT NULL REFERENCES content_items(id),
  content_artifact_id TEXT REFERENCES content_artifacts(id),
  legacy_segment_id TEXT REFERENCES segments(id),
  span_index INTEGER NOT NULL,
  start_char INTEGER NOT NULL,
  end_char INTEGER NOT NULL,
  text_path TEXT,
  text_sha256 TEXT,
  word_count INTEGER NOT NULL DEFAULT 0,
  speaker TEXT,
  span_kind TEXT NOT NULL DEFAULT 'text_chunk',
  metadata_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL,
  UNIQUE(content_artifact_id, span_index)
);

CREATE TABLE IF NOT EXISTS context_packages (
  id TEXT PRIMARY KEY,
  content_item_id TEXT NOT NULL REFERENCES content_items(id),
  legacy_episode_context_run_id TEXT REFERENCES episode_context_runs(id),
  label_pack TEXT NOT NULL,
  model TEXT NOT NULL,
  status TEXT NOT NULL,
  artifact_path TEXT,
  summary_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(content_item_id, label_pack, model)
);

CREATE TABLE IF NOT EXISTS extraction_runs (
  id TEXT PRIMARY KEY,
  content_item_id TEXT REFERENCES content_items(id),
  content_span_id TEXT REFERENCES content_spans(id),
  legacy_label_id TEXT REFERENCES labels(id),
  label_pack TEXT NOT NULL,
  model TEXT NOT NULL,
  worker_id TEXT,
  status TEXT NOT NULL,
  output_ref TEXT,
  metrics_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS queue_envelopes (
  job_id INTEGER PRIMARY KEY REFERENCES jobs(id) ON DELETE CASCADE,
  queue_name TEXT NOT NULL,
  worker_role TEXT NOT NULL,
  content_type TEXT NOT NULL,
  source_id TEXT,
  item_id TEXT,
  artifact_id TEXT,
  label_pack TEXT,
  model_required TEXT,
  privacy_tier TEXT NOT NULL DEFAULT 'local_only',
  lease_ttl_seconds INTEGER NOT NULL DEFAULT 2700,
  input_context_ref TEXT,
  output_schema_ref TEXT,
  callback_mode TEXT NOT NULL DEFAULT 'local_submit',
  run_tag TEXT,
  allowed_tools_json TEXT NOT NULL DEFAULT '[]',
  capabilities_json TEXT NOT NULL DEFAULT '[]',
  remote_claimable INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS worker_runs (
  id TEXT PRIMARY KEY,
  worker_id TEXT NOT NULL,
  worker_role TEXT NOT NULL,
  model TEXT,
  status TEXT NOT NULL,
  claimed_jobs INTEGER NOT NULL DEFAULT 0,
  completed_jobs INTEGER NOT NULL DEFAULT 0,
  failed_jobs INTEGER NOT NULL DEFAULT 0,
  metrics_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  completed_at TEXT
);

CREATE TABLE IF NOT EXISTS job_claims (
  id TEXT PRIMARY KEY,
  job_id INTEGER NOT NULL REFERENCES jobs(id),
  worker_run_id TEXT REFERENCES worker_runs(id),
  worker_id TEXT NOT NULL,
  worker_role TEXT NOT NULL,
  claim_status TEXT NOT NULL,
  claimed_at TEXT NOT NULL,
  leased_until TEXT,
  released_at TEXT,
  notes TEXT
);

CREATE TABLE IF NOT EXISTS output_submissions (
  id TEXT PRIMARY KEY,
  job_id INTEGER NOT NULL REFERENCES jobs(id),
  worker_run_id TEXT REFERENCES worker_runs(id),
  worker_id TEXT NOT NULL,
  output_ref TEXT,
  status TEXT NOT NULL,
  validation_json TEXT NOT NULL DEFAULT '{}',
  submitted_at TEXT NOT NULL,
  imported_at TEXT
);

CREATE TABLE IF NOT EXISTS queue_events (
  id TEXT PRIMARY KEY,
  job_id INTEGER REFERENCES jobs(id),
  worker_run_id TEXT REFERENCES worker_runs(id),
  event_type TEXT NOT NULL,
  event_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status, priority, id);
CREATE INDEX IF NOT EXISTS idx_jobs_lane ON jobs(lane, status);
CREATE INDEX IF NOT EXISTS idx_queue_envelopes_role ON queue_envelopes(worker_role, content_type, privacy_tier);
CREATE INDEX IF NOT EXISTS idx_content_items_type ON content_items(content_type, acquisition_status);
CREATE INDEX IF NOT EXISTS idx_content_artifacts_item ON content_artifacts(content_item_id, artifact_type, status);
CREATE INDEX IF NOT EXISTS idx_content_spans_item ON content_spans(content_item_id, span_kind);
CREATE INDEX IF NOT EXISTS idx_worker_runs_role ON worker_runs(worker_role, status, created_at);
CREATE INDEX IF NOT EXISTS idx_job_claims_job ON job_claims(job_id, claim_status);
CREATE INDEX IF NOT EXISTS idx_output_submissions_job ON output_submissions(job_id, status);
CREATE INDEX IF NOT EXISTS idx_queue_events_job ON queue_events(job_id, event_type);
CREATE INDEX IF NOT EXISTS idx_segments_episode ON segments(episode_id);
CREATE INDEX IF NOT EXISTS idx_segments_source ON segments(source_id);
CREATE INDEX IF NOT EXISTS idx_labels_pack ON labels(label_pack, label_pack_version);
CREATE INDEX IF NOT EXISTS idx_labels_segment ON labels(segment_id);
CREATE INDEX IF NOT EXISTS idx_labels_created_at ON labels(created_at);
CREATE INDEX IF NOT EXISTS idx_episode_context_runs_episode_pack ON episode_context_runs(episode_id, label_pack, model, status);
CREATE INDEX IF NOT EXISTS idx_observations_label ON coded_observations(label_id);
CREATE INDEX IF NOT EXISTS idx_observations_segment ON coded_observations(segment_id, code_family, code_id);
CREATE INDEX IF NOT EXISTS idx_discourse_events_label ON discourse_events(label_id);
CREATE INDEX IF NOT EXISTS idx_discourse_events_segment ON discourse_events(segment_id, event_type, candidate_concept);
CREATE INDEX IF NOT EXISTS idx_discourse_event_contexts_label_pack ON discourse_event_contexts(label_pack, source_context_kind);
CREATE INDEX IF NOT EXISTS idx_raw_speaker_mentions_resolution ON raw_speaker_mentions(resolution_status, surface_name);
CREATE INDEX IF NOT EXISTS idx_raw_actor_mentions_resolution ON raw_actor_mentions(resolution_status, mention_type, surface_name);
CREATE INDEX IF NOT EXISTS idx_identity_aliases_alias ON identity_aliases(normalized_alias, canonical_entity_type);
CREATE INDEX IF NOT EXISTS idx_identity_resolution_status ON identity_resolution_candidates(status, candidate_entity_type);
CREATE INDEX IF NOT EXISTS idx_person_concept_edges_person ON person_concept_edges(canonical_person_id, concept_id);
CREATE INDEX IF NOT EXISTS idx_person_person_mentions_target ON person_person_mentions(target_surface, episode_id);
CREATE INDEX IF NOT EXISTS idx_expert_authority_scores_topic ON expert_authority_scores(topic, score);
CREATE INDEX IF NOT EXISTS idx_term_usages_term ON term_usages(term, concept_id);
CREATE INDEX IF NOT EXISTS idx_actor_positions_actor ON actor_positions(actor_name, actor_affiliation, concept_id);
CREATE INDEX IF NOT EXISTS idx_shift_signals_type ON shift_signals(signal_type, slice_key, window_end);
CREATE INDEX IF NOT EXISTS idx_transcript_preparations_type ON transcript_preparations(artifact_type, status);
CREATE INDEX IF NOT EXISTS idx_transcript_acquisition_attempts_episode ON transcript_acquisition_attempts(episode_id, method, status);
CREATE INDEX IF NOT EXISTS idx_transcript_acquisition_status_status ON transcript_acquisition_status(status, eligible_for_transcription);
CREATE INDEX IF NOT EXISTS idx_transcription_runs_status ON transcription_runs(status, provider);
CREATE INDEX IF NOT EXISTS idx_reviewer_audits_pilot ON reviewer_audits(pilot_id, status, episode_id);
CREATE INDEX IF NOT EXISTS idx_quality_iterations_pilot ON quality_iterations(pilot_id, patch_tag, tier, created_at);
CREATE INDEX IF NOT EXISTS idx_episodes_published ON episodes(published_at);
CREATE INDEX IF NOT EXISTS idx_episodes_source_published ON episodes(source_id, published_at);
CREATE INDEX IF NOT EXISTS idx_transcripts_episode ON transcripts(episode_id);
CREATE INDEX IF NOT EXISTS idx_jobs_episode_context_active ON jobs(job_type, target_id, status);
CREATE INDEX IF NOT EXISTS idx_queue_envelopes_remote_ready ON queue_envelopes(remote_claimable, privacy_tier, worker_role, job_id);
CREATE INDEX IF NOT EXISTS idx_claim_cluster_members_cluster ON claim_cluster_members(cluster_id, relation);
CREATE INDEX IF NOT EXISTS idx_claim_cluster_members_claim ON claim_cluster_members(claim_id, relation);
CREATE INDEX IF NOT EXISTS idx_claim_canonicalization_runs_status ON claim_canonicalization_runs(status, started_at);
CREATE INDEX IF NOT EXISTS idx_claim_subjects_key ON claim_subjects(subject_key, method);
CREATE INDEX IF NOT EXISTS idx_claim_subject_members_subject ON claim_subject_members(subject_id, relation);
CREATE INDEX IF NOT EXISTS idx_claim_subject_members_claim ON claim_subject_members(claim_id, relation);
CREATE INDEX IF NOT EXISTS idx_claim_proposition_variants_subject ON claim_proposition_variants(subject_id, variant_key);
CREATE INDEX IF NOT EXISTS idx_claim_position_observations_subject ON claim_position_observations(subject_id, stance, published_at);
CREATE INDEX IF NOT EXISTS idx_claim_subject_event_observations_subject ON claim_subject_event_observations(subject_id, event_type, published_at);
CREATE INDEX IF NOT EXISTS idx_claim_subject_event_observations_event ON claim_subject_event_observations(discourse_event_id, method);
CREATE INDEX IF NOT EXISTS idx_claim_subject_expert_positions_subject ON claim_subject_expert_positions(subject_id, stance, observation_count);
CREATE INDEX IF NOT EXISTS idx_claim_subject_expert_positions_person ON claim_subject_expert_positions(canonical_person_id, subject_id);
CREATE INDEX IF NOT EXISTS idx_claim_subject_runs_status ON claim_subject_runs(status, started_at);
"""


# ``SCHEMA`` remains the compatibility bootstrap for the original factory.  New
# schema is applied through this ordered ledger so an existing corpus is only
# ever moved forward and a migration that has already run cannot silently
# change underneath it.
INTELLIGENCE_SCHEMA_V1 = (
    """
    CREATE TABLE IF NOT EXISTS corpus_releases (
      id TEXT PRIMARY KEY,
      schema_version TEXT NOT NULL DEFAULT 'corpus_release_v1'
        CHECK(schema_version = 'corpus_release_v1'),
      release_version INTEGER NOT NULL CHECK(release_version > 0),
      parent_release_id TEXT REFERENCES corpus_releases(id),
      cutoff_at TEXT NOT NULL,
      manifest_sha256 TEXT NOT NULL CHECK(length(manifest_sha256) = 64),
      manifest_json TEXT NOT NULL DEFAULT '{}',
      source_count INTEGER NOT NULL DEFAULT 0 CHECK(source_count >= 0),
      item_count INTEGER NOT NULL DEFAULT 0 CHECK(item_count >= 0),
      claim_count INTEGER NOT NULL DEFAULT 0 CHECK(claim_count >= 0),
      status TEXT NOT NULL DEFAULT 'accepted'
        CHECK(status IN ('draft', 'accepted', 'retired')),
      created_at TEXT NOT NULL,
      UNIQUE(release_version),
      UNIQUE(manifest_sha256)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS corpus_release_episodes (
      corpus_release_id TEXT NOT NULL REFERENCES corpus_releases(id),
      episode_id TEXT NOT NULL REFERENCES episodes(id),
      content_sha256 TEXT NOT NULL CHECK(length(content_sha256) = 64),
      member_index INTEGER NOT NULL CHECK(member_index >= 0),
      created_at TEXT NOT NULL,
      PRIMARY KEY(corpus_release_id, episode_id),
      UNIQUE(corpus_release_id, member_index)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS corpus_release_transcripts (
      corpus_release_id TEXT NOT NULL REFERENCES corpus_releases(id),
      transcript_id TEXT NOT NULL REFERENCES transcripts(id),
      episode_id TEXT NOT NULL REFERENCES episodes(id),
      content_sha256 TEXT NOT NULL CHECK(length(content_sha256) = 64),
      member_index INTEGER NOT NULL CHECK(member_index >= 0),
      created_at TEXT NOT NULL,
      PRIMARY KEY(corpus_release_id, transcript_id),
      UNIQUE(corpus_release_id, member_index)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS corpus_release_segments (
      corpus_release_id TEXT NOT NULL REFERENCES corpus_releases(id),
      segment_id TEXT NOT NULL REFERENCES segments(id),
      transcript_id TEXT NOT NULL REFERENCES transcripts(id),
      episode_id TEXT NOT NULL REFERENCES episodes(id),
      content_sha256 TEXT NOT NULL CHECK(length(content_sha256) = 64),
      member_index INTEGER NOT NULL CHECK(member_index >= 0),
      created_at TEXT NOT NULL,
      PRIMARY KEY(corpus_release_id, segment_id),
      UNIQUE(corpus_release_id, member_index)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS corpus_release_labels (
      corpus_release_id TEXT NOT NULL REFERENCES corpus_releases(id),
      label_id TEXT NOT NULL REFERENCES labels(id),
      segment_id TEXT NOT NULL REFERENCES segments(id),
      content_sha256 TEXT NOT NULL CHECK(length(content_sha256) = 64),
      member_index INTEGER NOT NULL CHECK(member_index >= 0),
      accepted_status TEXT NOT NULL,
      created_at TEXT NOT NULL,
      PRIMARY KEY(corpus_release_id, label_id),
      UNIQUE(corpus_release_id, member_index)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS pipeline_runs (
      id TEXT PRIMARY KEY,
      schema_version TEXT NOT NULL DEFAULT 'pipeline_run_v1'
        CHECK(schema_version = 'pipeline_run_v1'),
      run_type TEXT NOT NULL,
      run_schema TEXT NOT NULL,
      run_schema_version TEXT NOT NULL,
      corpus_release_id TEXT REFERENCES corpus_releases(id),
      parent_run_id TEXT REFERENCES pipeline_runs(id),
      model TEXT,
      model_version TEXT,
      prompt_version TEXT,
      configuration_sha256 TEXT NOT NULL CHECK(length(configuration_sha256) = 64),
      status TEXT NOT NULL DEFAULT 'pending'
        CHECK(status IN ('pending', 'running', 'succeeded', 'failed', 'canceled')),
      parameters_json TEXT NOT NULL DEFAULT '{}',
      metrics_json TEXT NOT NULL DEFAULT '{}',
      receipt_json TEXT NOT NULL DEFAULT '{}',
      input_count INTEGER NOT NULL DEFAULT 0 CHECK(input_count >= 0),
      output_count INTEGER NOT NULL DEFAULT 0 CHECK(output_count >= 0),
      failure_count INTEGER NOT NULL DEFAULT 0 CHECK(failure_count >= 0),
      input_sha256 TEXT,
      output_sha256 TEXT,
      started_at TEXT,
      completed_at TEXT,
      error TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS corpus_release_promotions (
      id TEXT PRIMARY KEY,
      schema_version TEXT NOT NULL DEFAULT 'corpus_release_promotion_v1'
        CHECK(schema_version = 'corpus_release_promotion_v1'),
      promotion_revision INTEGER NOT NULL CHECK(promotion_revision > 0),
      corpus_release_id TEXT NOT NULL REFERENCES corpus_releases(id),
      pipeline_run_id TEXT NOT NULL REFERENCES pipeline_runs(id),
      previous_promotion_id TEXT REFERENCES corpus_release_promotions(id),
      action TEXT NOT NULL DEFAULT 'promote' CHECK(action IN ('promote', 'withdraw')),
      promoted_by TEXT NOT NULL,
      rationale TEXT NOT NULL,
      created_at TEXT NOT NULL,
      UNIQUE(promotion_revision)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS atomic_claims (
      id TEXT PRIMARY KEY,
      schema_version TEXT NOT NULL DEFAULT 'atomic_claim_v1'
        CHECK(schema_version = 'atomic_claim_v1'),
      claim_lineage_id TEXT NOT NULL,
      revision INTEGER NOT NULL DEFAULT 1 CHECK(revision > 0),
      supersedes_claim_id TEXT REFERENCES atomic_claims(id),
      corpus_release_id TEXT NOT NULL REFERENCES corpus_releases(id),
      pipeline_run_id TEXT NOT NULL REFERENCES pipeline_runs(id),
      claim_text TEXT NOT NULL CHECK(length(trim(claim_text)) > 0),
      claim_type TEXT NOT NULL CHECK(length(trim(claim_type)) > 0),
      raw_speaker TEXT NOT NULL CHECK(length(trim(raw_speaker)) > 0),
      canonical_person_id TEXT REFERENCES canonical_people(id),
      stance TEXT NOT NULL CHECK(length(trim(stance)) > 0),
      certainty TEXT NOT NULL CHECK(length(trim(certainty)) > 0),
      time_horizon TEXT NOT NULL CHECK(length(trim(time_horizon)) > 0),
      discourse_event_id TEXT NOT NULL REFERENCES discourse_events(id),
      segment_id TEXT NOT NULL REFERENCES segments(id),
      source_id TEXT REFERENCES sources(id),
      episode_id TEXT REFERENCES episodes(id),
      evidence_unit_type TEXT NOT NULL DEFAULT 'segment',
      evidence_unit_id TEXT NOT NULL,
      evidence_text TEXT NOT NULL CHECK(length(evidence_text) > 0),
      evidence_start INTEGER NOT NULL CHECK(evidence_start >= 0),
      evidence_end INTEGER NOT NULL CHECK(evidence_end > evidence_start),
      extractor_model TEXT NOT NULL,
      extractor_schema TEXT NOT NULL,
      extractor_schema_version TEXT NOT NULL,
      extractor_prompt_sha256 TEXT,
      source_artifact_sha256 TEXT,
      provenance_json TEXT NOT NULL DEFAULT '{}',
      confidence REAL NOT NULL CHECK(confidence >= 0 AND confidence <= 1),
      review_status TEXT NOT NULL
        CHECK(review_status IN ('pending', 'accepted', 'rejected', 'needs_review')),
      reviewed_by_model TEXT,
      reviewed_at TEXT,
      forecast_probability REAL
        CHECK(forecast_probability IS NULL OR (forecast_probability >= 0 AND forecast_probability <= 1)),
      observed_at TEXT NOT NULL,
      created_at TEXT NOT NULL,
      UNIQUE(claim_lineage_id, revision)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS identity_resolution_judgments (
      id TEXT PRIMARY KEY,
      schema_version TEXT NOT NULL DEFAULT 'identity_resolution_judgment_v1'
        CHECK(schema_version = 'identity_resolution_judgment_v1'),
      identity_lineage_id TEXT NOT NULL,
      revision INTEGER NOT NULL CHECK(revision > 0),
      supersedes_judgment_id TEXT REFERENCES identity_resolution_judgments(id),
      corpus_release_id TEXT NOT NULL REFERENCES corpus_releases(id),
      pipeline_run_id TEXT NOT NULL REFERENCES pipeline_runs(id),
      raw_mention_type TEXT NOT NULL
        CHECK(raw_mention_type IN ('speaker', 'actor', 'guest_metadata', 'episode_metadata')),
      raw_mention_id TEXT NOT NULL,
      canonical_person_id TEXT REFERENCES canonical_people(id),
      decision TEXT NOT NULL
        CHECK(decision IN ('candidate', 'accepted', 'rejected', 'merged', 'unknown')),
      rationale TEXT NOT NULL,
      evidence_json TEXT NOT NULL DEFAULT '{}',
      judge_model TEXT NOT NULL,
      judge_schema_version TEXT NOT NULL,
      confidence REAL NOT NULL CHECK(confidence >= 0 AND confidence <= 1),
      review_status TEXT NOT NULL
        CHECK(review_status IN ('pending', 'accepted', 'rejected', 'needs_review')),
      decided_at TEXT NOT NULL,
      created_at TEXT NOT NULL,
      UNIQUE(identity_lineage_id, revision)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS accepted_claim_subjects (
      id TEXT PRIMARY KEY,
      schema_version TEXT NOT NULL DEFAULT 'claim_subject_judgment_v1'
        CHECK(schema_version = 'claim_subject_judgment_v1'),
      subject_lineage_id TEXT NOT NULL,
      revision INTEGER NOT NULL CHECK(revision > 0),
      supersedes_subject_id TEXT REFERENCES accepted_claim_subjects(id),
      corpus_release_id TEXT NOT NULL REFERENCES corpus_releases(id),
      pipeline_run_id TEXT NOT NULL REFERENCES pipeline_runs(id),
      subject_text TEXT NOT NULL CHECK(length(trim(subject_text)) > 0),
      subject_type TEXT NOT NULL CHECK(length(trim(subject_type)) > 0),
      domain TEXT,
      scope_note TEXT,
      judge_model TEXT NOT NULL,
      judge_schema_version TEXT NOT NULL,
      confidence REAL NOT NULL CHECK(confidence >= 0 AND confidence <= 1),
      rationale TEXT NOT NULL,
      evidence_json TEXT NOT NULL DEFAULT '{}',
      review_status TEXT NOT NULL
        CHECK(review_status IN ('pending', 'accepted', 'rejected', 'needs_review')),
      decided_at TEXT NOT NULL,
      created_at TEXT NOT NULL,
      UNIQUE(subject_lineage_id, revision)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS accepted_proposition_variants (
      id TEXT PRIMARY KEY,
      schema_version TEXT NOT NULL DEFAULT 'proposition_variant_judgment_v1'
        CHECK(schema_version = 'proposition_variant_judgment_v1'),
      variant_lineage_id TEXT NOT NULL,
      revision INTEGER NOT NULL CHECK(revision > 0),
      supersedes_variant_id TEXT REFERENCES accepted_proposition_variants(id),
      corpus_release_id TEXT NOT NULL REFERENCES corpus_releases(id),
      pipeline_run_id TEXT NOT NULL REFERENCES pipeline_runs(id),
      subject_id TEXT NOT NULL REFERENCES accepted_claim_subjects(id),
      proposition_text TEXT NOT NULL CHECK(length(trim(proposition_text)) > 0),
      predicate_text TEXT,
      object_text TEXT,
      polarity TEXT,
      time_horizon TEXT,
      conditions_json TEXT NOT NULL DEFAULT '{}',
      judge_model TEXT NOT NULL,
      judge_schema_version TEXT NOT NULL,
      confidence REAL NOT NULL CHECK(confidence >= 0 AND confidence <= 1),
      rationale TEXT NOT NULL,
      evidence_json TEXT NOT NULL DEFAULT '{}',
      review_status TEXT NOT NULL
        CHECK(review_status IN ('pending', 'accepted', 'rejected', 'needs_review')),
      decided_at TEXT NOT NULL,
      created_at TEXT NOT NULL,
      UNIQUE(variant_lineage_id, revision)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS accepted_position_observations (
      id TEXT PRIMARY KEY,
      schema_version TEXT NOT NULL DEFAULT 'position_observation_judgment_v1'
        CHECK(schema_version = 'position_observation_judgment_v1'),
      position_lineage_id TEXT NOT NULL,
      revision INTEGER NOT NULL CHECK(revision > 0),
      supersedes_position_id TEXT REFERENCES accepted_position_observations(id),
      corpus_release_id TEXT NOT NULL REFERENCES corpus_releases(id),
      pipeline_run_id TEXT NOT NULL REFERENCES pipeline_runs(id),
      subject_id TEXT NOT NULL REFERENCES accepted_claim_subjects(id),
      variant_id TEXT NOT NULL REFERENCES accepted_proposition_variants(id),
      atomic_claim_id TEXT NOT NULL REFERENCES atomic_claims(id),
      canonical_person_id TEXT NOT NULL REFERENCES canonical_people(id),
      position TEXT NOT NULL CHECK(length(trim(position)) > 0),
      certainty TEXT NOT NULL CHECK(length(trim(certainty)) > 0),
      observed_at TEXT NOT NULL,
      judge_model TEXT NOT NULL,
      judge_schema_version TEXT NOT NULL,
      confidence REAL NOT NULL CHECK(confidence >= 0 AND confidence <= 1),
      rationale TEXT NOT NULL,
      evidence_json TEXT NOT NULL DEFAULT '{}',
      review_status TEXT NOT NULL
        CHECK(review_status IN ('pending', 'accepted', 'rejected', 'needs_review')),
      decided_at TEXT NOT NULL,
      created_at TEXT NOT NULL,
      UNIQUE(position_lineage_id, revision)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS source_affiliations (
      id TEXT PRIMARY KEY,
      schema_version TEXT NOT NULL DEFAULT 'source_affiliation_v1'
        CHECK(schema_version = 'source_affiliation_v1'),
      affiliation_lineage_id TEXT NOT NULL,
      revision INTEGER NOT NULL DEFAULT 1 CHECK(revision > 0),
      supersedes_affiliation_id TEXT REFERENCES source_affiliations(id),
      corpus_release_id TEXT NOT NULL REFERENCES corpus_releases(id),
      source_id TEXT NOT NULL REFERENCES sources(id),
      affiliation_kind TEXT NOT NULL
        CHECK(affiliation_kind IN ('network', 'publisher', 'owner', 'independent')),
      affiliation_key TEXT NOT NULL,
      affiliation_name TEXT NOT NULL,
      canonical_org_id TEXT REFERENCES canonical_orgs(id),
      valid_from TEXT,
      valid_to TEXT,
      confidence REAL NOT NULL CHECK(confidence >= 0 AND confidence <= 1),
      review_status TEXT NOT NULL
        CHECK(review_status IN ('pending', 'accepted', 'rejected', 'needs_review')),
      judge_model TEXT NOT NULL,
      pipeline_run_id TEXT NOT NULL REFERENCES pipeline_runs(id),
      evidence_json TEXT NOT NULL DEFAULT '{}',
      created_at TEXT NOT NULL,
      UNIQUE(affiliation_lineage_id, revision),
      CHECK(valid_to IS NULL OR valid_from IS NULL OR valid_to >= valid_from)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS person_appearances (
      id TEXT PRIMARY KEY,
      schema_version TEXT NOT NULL DEFAULT 'person_appearance_v1'
        CHECK(schema_version = 'person_appearance_v1'),
      appearance_lineage_id TEXT NOT NULL,
      revision INTEGER NOT NULL DEFAULT 1 CHECK(revision > 0),
      supersedes_appearance_id TEXT REFERENCES person_appearances(id),
      corpus_release_id TEXT NOT NULL REFERENCES corpus_releases(id),
      canonical_person_id TEXT NOT NULL REFERENCES canonical_people(id),
      source_id TEXT NOT NULL REFERENCES sources(id),
      episode_id TEXT NOT NULL REFERENCES episodes(id),
      source_affiliation_id TEXT REFERENCES source_affiliations(id),
      role TEXT NOT NULL
        CHECK(role IN ('host', 'cohost', 'guest', 'panelist', 'unknown')),
      appeared_at TEXT NOT NULL,
      confidence REAL NOT NULL CHECK(confidence >= 0 AND confidence <= 1),
      review_status TEXT NOT NULL
        CHECK(review_status IN ('pending', 'accepted', 'rejected', 'needs_review')),
      judge_model TEXT NOT NULL,
      pipeline_run_id TEXT NOT NULL REFERENCES pipeline_runs(id),
      evidence_json TEXT NOT NULL DEFAULT '{}',
      created_at TEXT NOT NULL,
      UNIQUE(appearance_lineage_id, revision)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS claim_relation_judgments (
      id TEXT PRIMARY KEY,
      schema_version TEXT NOT NULL DEFAULT 'claim_relation_judgment_v1'
        CHECK(schema_version = 'claim_relation_judgment_v1'),
      relation_lineage_id TEXT NOT NULL,
      revision INTEGER NOT NULL DEFAULT 1 CHECK(revision > 0),
      supersedes_judgment_id TEXT REFERENCES claim_relation_judgments(id),
      corpus_release_id TEXT NOT NULL REFERENCES corpus_releases(id),
      source_claim_id TEXT NOT NULL REFERENCES atomic_claims(id),
      target_claim_id TEXT NOT NULL REFERENCES atomic_claims(id),
      relation TEXT NOT NULL
        CHECK(relation IN ('equivalent', 'supports', 'contradicts', 'qualifies', 'orthogonal', 'incomparable')),
      confidence REAL NOT NULL CHECK(confidence >= 0 AND confidence <= 1),
      rationale TEXT NOT NULL CHECK(length(trim(rationale)) > 0),
      evidence_json TEXT NOT NULL DEFAULT '{}',
      judge_model TEXT NOT NULL,
      pipeline_run_id TEXT NOT NULL REFERENCES pipeline_runs(id),
      review_status TEXT NOT NULL
        CHECK(review_status IN ('pending', 'accepted', 'rejected', 'needs_review')),
      decided_at TEXT NOT NULL,
      created_at TEXT NOT NULL,
      UNIQUE(relation_lineage_id, revision),
      CHECK(source_claim_id <> target_claim_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS consensus_snapshots (
      id TEXT PRIMARY KEY,
      schema_version TEXT NOT NULL DEFAULT 'consensus_snapshot_v1'
        CHECK(schema_version = 'consensus_snapshot_v1'),
      corpus_release_id TEXT NOT NULL REFERENCES corpus_releases(id),
      pipeline_run_id TEXT NOT NULL REFERENCES pipeline_runs(id),
      focal_claim_id TEXT NOT NULL REFERENCES atomic_claims(id),
      calculation_version TEXT NOT NULL DEFAULT 'accepted_relations_v1',
      as_of TEXT NOT NULL,
      window_start TEXT NOT NULL,
      window_end TEXT NOT NULL,
      window_days INTEGER NOT NULL DEFAULT 90 CHECK(window_days > 0),
      people_count INTEGER NOT NULL CHECK(people_count >= 0),
      show_count INTEGER NOT NULL CHECK(show_count >= 0),
      network_count INTEGER NOT NULL CHECK(network_count >= 0),
      aligned_count INTEGER NOT NULL CHECK(aligned_count >= 0),
      qualified_count INTEGER NOT NULL CHECK(qualified_count >= 0),
      opposed_count INTEGER NOT NULL CHECK(opposed_count >= 0),
      excluded_count INTEGER NOT NULL CHECK(excluded_count >= 0),
      consensus_score REAL CHECK(consensus_score IS NULL OR (consensus_score >= 0 AND consensus_score <= 1)),
      dominant_bucket TEXT CHECK(dominant_bucket IS NULL OR dominant_bucket IN ('aligned', 'qualified', 'opposed')),
      dominant_share REAL CHECK(dominant_share IS NULL OR (dominant_share >= 0 AND dominant_share <= 1)),
      inputs_sha256 TEXT NOT NULL CHECK(length(inputs_sha256) = 64),
      included_claim_ids_json TEXT NOT NULL DEFAULT '[]',
      metrics_json TEXT NOT NULL DEFAULT '{}',
      review_status TEXT NOT NULL DEFAULT 'accepted'
        CHECK(review_status IN ('accepted', 'rejected')),
      created_at TEXT NOT NULL,
      UNIQUE(focal_claim_id, calculation_version, as_of, inputs_sha256)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS contrarian_snapshots (
      id TEXT PRIMARY KEY,
      schema_version TEXT NOT NULL DEFAULT 'contrarian_snapshot_v1'
        CHECK(schema_version = 'contrarian_snapshot_v1'),
      corpus_release_id TEXT NOT NULL REFERENCES corpus_releases(id),
      pipeline_run_id TEXT NOT NULL REFERENCES pipeline_runs(id),
      target_claim_id TEXT NOT NULL REFERENCES atomic_claims(id),
      consensus_snapshot_id TEXT REFERENCES consensus_snapshots(id),
      calculation_version TEXT NOT NULL DEFAULT 'contrarian_v1',
      as_of TEXT NOT NULL,
      window_start TEXT NOT NULL,
      window_end TEXT NOT NULL,
      window_days INTEGER NOT NULL DEFAULT 90 CHECK(window_days > 0),
      people_count INTEGER NOT NULL CHECK(people_count >= 0),
      show_count INTEGER NOT NULL CHECK(show_count >= 0),
      network_count INTEGER NOT NULL CHECK(network_count >= 0),
      dominant_bucket TEXT CHECK(dominant_bucket IS NULL OR dominant_bucket IN ('aligned', 'qualified', 'opposed')),
      dominant_share REAL CHECK(dominant_share IS NULL OR (dominant_share >= 0 AND dominant_share <= 1)),
      target_share REAL CHECK(target_share IS NULL OR (target_share >= 0 AND target_share <= 1)),
      dominant_threshold REAL NOT NULL DEFAULT 0.5 CHECK(dominant_threshold >= 0 AND dominant_threshold <= 1),
      target_share_threshold REAL NOT NULL DEFAULT 0.25 CHECK(target_share_threshold >= 0 AND target_share_threshold <= 1),
      minimum_people INTEGER NOT NULL DEFAULT 5 CHECK(minimum_people > 0),
      minimum_shows INTEGER NOT NULL DEFAULT 3 CHECK(minimum_shows > 0),
      minimum_networks INTEGER NOT NULL DEFAULT 2 CHECK(minimum_networks > 0),
      is_contrarian INTEGER NOT NULL CHECK(is_contrarian IN (0, 1)),
      exclusion_reason TEXT,
      inputs_sha256 TEXT NOT NULL CHECK(length(inputs_sha256) = 64),
      included_claim_ids_json TEXT NOT NULL DEFAULT '[]',
      metrics_json TEXT NOT NULL DEFAULT '{}',
      review_status TEXT NOT NULL DEFAULT 'accepted'
        CHECK(review_status IN ('accepted', 'rejected')),
      created_at TEXT NOT NULL,
      UNIQUE(target_claim_id, calculation_version, as_of, inputs_sha256)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS outcome_resolution_revisions (
      id TEXT PRIMARY KEY,
      schema_version TEXT NOT NULL DEFAULT 'outcome_resolution_v1'
        CHECK(schema_version = 'outcome_resolution_v1'),
      claim_id TEXT NOT NULL REFERENCES atomic_claims(id),
      corpus_release_id TEXT NOT NULL REFERENCES corpus_releases(id),
      revision INTEGER NOT NULL CHECK(revision > 0),
      supersedes_resolution_id TEXT REFERENCES outcome_resolution_revisions(id),
      resolution_question TEXT NOT NULL CHECK(length(trim(resolution_question)) > 0),
      due_at TEXT,
      resolution_window_start TEXT,
      resolution_window_end TEXT,
      resolution_criteria TEXT NOT NULL CHECK(length(trim(resolution_criteria)) > 0),
      outcome TEXT NOT NULL
        CHECK(outcome IN ('true', 'false', 'mixed', 'unresolved', 'unverifiable')),
      categorical_score REAL
        CHECK(categorical_score IS NULL OR categorical_score IN (0.0, 0.5, 1.0)),
      brier_score REAL CHECK(brier_score IS NULL OR (brier_score >= 0 AND brier_score <= 1)),
      resolved_at TEXT NOT NULL,
      confidence REAL NOT NULL CHECK(confidence >= 0 AND confidence <= 1),
      rationale TEXT NOT NULL,
      evidence_json TEXT NOT NULL DEFAULT '{}',
      authoritative_evidence_json TEXT NOT NULL DEFAULT '{}',
      as_of TEXT NOT NULL,
      resolver_model TEXT NOT NULL,
      resolver_version TEXT NOT NULL,
      reviewer_version TEXT NOT NULL,
      pipeline_run_id TEXT NOT NULL REFERENCES pipeline_runs(id),
      review_status TEXT NOT NULL
        CHECK(review_status IN ('pending', 'accepted', 'rejected', 'needs_review')),
      created_at TEXT NOT NULL,
      UNIQUE(claim_id, revision),
      CHECK(resolution_window_end IS NULL OR resolution_window_start IS NULL OR resolution_window_end >= resolution_window_start)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_corpus_releases_status ON corpus_releases(status, release_version)",
    "CREATE INDEX IF NOT EXISTS idx_corpus_release_promotions_revision ON corpus_release_promotions(promotion_revision, action)",
    "CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status ON pipeline_runs(run_type, status, created_at)",
    "CREATE INDEX IF NOT EXISTS idx_atomic_claims_current ON atomic_claims(claim_lineage_id, review_status, revision)",
    "CREATE INDEX IF NOT EXISTS idx_atomic_claims_person_time ON atomic_claims(canonical_person_id, observed_at)",
    "CREATE INDEX IF NOT EXISTS idx_atomic_claims_source_time ON atomic_claims(source_id, observed_at)",
    "CREATE INDEX IF NOT EXISTS idx_identity_judgments_person ON identity_resolution_judgments(canonical_person_id, review_status, decision, revision)",
    "CREATE INDEX IF NOT EXISTS idx_identity_judgments_raw ON identity_resolution_judgments(raw_mention_type, raw_mention_id, revision)",
    "CREATE INDEX IF NOT EXISTS idx_accepted_claim_subjects_current ON accepted_claim_subjects(subject_lineage_id, review_status, revision)",
    "CREATE INDEX IF NOT EXISTS idx_accepted_variants_subject ON accepted_proposition_variants(subject_id, review_status, revision)",
    "CREATE INDEX IF NOT EXISTS idx_accepted_positions_variant_person ON accepted_position_observations(variant_id, canonical_person_id, observed_at)",
    "CREATE INDEX IF NOT EXISTS idx_accepted_positions_claim ON accepted_position_observations(atomic_claim_id, review_status, revision)",
    "CREATE INDEX IF NOT EXISTS idx_source_affiliations_current ON source_affiliations(source_id, affiliation_kind, review_status, revision)",
    "CREATE INDEX IF NOT EXISTS idx_person_appearances_person_time ON person_appearances(canonical_person_id, appeared_at)",
    "CREATE INDEX IF NOT EXISTS idx_claim_relations_target ON claim_relation_judgments(target_claim_id, review_status, decided_at)",
    "CREATE INDEX IF NOT EXISTS idx_claim_relations_source ON claim_relation_judgments(source_claim_id, review_status, decided_at)",
    "CREATE INDEX IF NOT EXISTS idx_consensus_snapshots_focal ON consensus_snapshots(focal_claim_id, as_of)",
    "CREATE INDEX IF NOT EXISTS idx_contrarian_snapshots_target ON contrarian_snapshots(target_claim_id, as_of)",
    "CREATE INDEX IF NOT EXISTS idx_outcome_resolutions_claim ON outcome_resolution_revisions(claim_id, review_status, revision)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_transcript_attempts_idempotency ON transcript_acquisition_attempts(idempotency_key) WHERE idempotency_key IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS idx_transcript_attempts_next_eligible ON transcript_acquisition_attempts(next_eligible_at) WHERE next_eligible_at IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS idx_transcript_status_next_eligible ON transcript_acquisition_status(next_eligible_at) WHERE next_eligible_at IS NOT NULL",
    """
    CREATE VIEW IF NOT EXISTS atomic_claim_v1 AS
    SELECT * FROM atomic_claims WHERE schema_version = 'atomic_claim_v1'
    """,
    """
    CREATE VIEW IF NOT EXISTS current_accepted_corpus_releases AS
    SELECT
      corpus_releases.*,
      corpus_release_promotions.id AS promotion_id,
      corpus_release_promotions.promotion_revision,
      corpus_release_promotions.pipeline_run_id AS promotion_pipeline_run_id,
      corpus_release_promotions.created_at AS promoted_at
    FROM corpus_release_promotions
    JOIN corpus_releases
      ON corpus_releases.id = corpus_release_promotions.corpus_release_id
     AND corpus_releases.status = 'accepted'
    JOIN pipeline_runs
      ON pipeline_runs.id = corpus_release_promotions.pipeline_run_id
     AND pipeline_runs.status = 'succeeded'
     AND pipeline_runs.corpus_release_id = corpus_releases.id
    WHERE corpus_release_promotions.action = 'promote'
      AND corpus_release_promotions.promotion_revision = (
        SELECT MAX(promotion_revision) FROM corpus_release_promotions
    )
    """,
    """
    CREATE VIEW IF NOT EXISTS current_accepted_identity_resolutions AS
    SELECT ranked.*
    FROM (
      SELECT judgments.*,
             ROW_NUMBER() OVER (
               PARTITION BY judgments.identity_lineage_id
               ORDER BY judgments.revision DESC, judgments.decided_at DESC, judgments.created_at DESC, judgments.id DESC
             ) AS _current_rank
      FROM identity_resolution_judgments AS judgments
      JOIN current_accepted_corpus_releases AS release
        ON release.id = judgments.corpus_release_id
      JOIN pipeline_runs AS producing_run
        ON producing_run.id = judgments.pipeline_run_id
       AND producing_run.status = 'succeeded'
       AND producing_run.corpus_release_id = release.id
    ) AS ranked
    JOIN canonical_people AS person
      ON person.id = ranked.canonical_person_id
     AND person.status = 'accepted'
    WHERE ranked._current_rank = 1
      AND ranked.review_status = 'accepted'
      AND ranked.decision IN ('accepted', 'merged')
    """,
    """
    CREATE VIEW IF NOT EXISTS current_accepted_people AS
    SELECT people.*
    FROM canonical_people AS people
    WHERE people.status = 'accepted'
      AND EXISTS (
        SELECT 1 FROM current_accepted_identity_resolutions AS resolutions
        WHERE resolutions.canonical_person_id = people.id
      )
    """,
    """
    CREATE VIEW IF NOT EXISTS current_accepted_atomic_claims AS
    SELECT ranked.*
    FROM (
      SELECT claims.*,
             ROW_NUMBER() OVER (
               PARTITION BY claims.claim_lineage_id
               ORDER BY claims.revision DESC, claims.created_at DESC, claims.id DESC
             ) AS _current_rank
      FROM atomic_claims AS claims
      JOIN current_accepted_corpus_releases AS release
        ON release.id = claims.corpus_release_id
      JOIN pipeline_runs AS producing_run
        ON producing_run.id = claims.pipeline_run_id
       AND producing_run.status = 'succeeded'
       AND producing_run.corpus_release_id = release.id
    ) AS ranked
    LEFT JOIN current_accepted_people AS person
      ON person.id = ranked.canonical_person_id
    WHERE ranked._current_rank = 1
      AND ranked.review_status = 'accepted'
      AND (ranked.canonical_person_id IS NULL OR person.id IS NOT NULL)
    """,
    """
    CREATE VIEW IF NOT EXISTS current_accepted_claim_subjects AS
    SELECT ranked.*
    FROM (
      SELECT subjects.*,
             ROW_NUMBER() OVER (
               PARTITION BY subjects.subject_lineage_id
               ORDER BY subjects.revision DESC, subjects.decided_at DESC, subjects.created_at DESC, subjects.id DESC
             ) AS _current_rank
      FROM accepted_claim_subjects AS subjects
      JOIN current_accepted_corpus_releases AS release
        ON release.id = subjects.corpus_release_id
      JOIN pipeline_runs AS producing_run
        ON producing_run.id = subjects.pipeline_run_id
       AND producing_run.status = 'succeeded'
       AND producing_run.corpus_release_id = release.id
    ) AS ranked
    WHERE ranked._current_rank = 1
      AND ranked.review_status = 'accepted'
    """,
    """
    CREATE VIEW IF NOT EXISTS current_accepted_proposition_variants AS
    SELECT ranked.*
    FROM (
      SELECT variants.*,
             ROW_NUMBER() OVER (
               PARTITION BY variants.variant_lineage_id
               ORDER BY variants.revision DESC, variants.decided_at DESC, variants.created_at DESC, variants.id DESC
             ) AS _current_rank
      FROM accepted_proposition_variants AS variants
      JOIN current_accepted_corpus_releases AS release
        ON release.id = variants.corpus_release_id
      JOIN pipeline_runs AS producing_run
        ON producing_run.id = variants.pipeline_run_id
       AND producing_run.status = 'succeeded'
       AND producing_run.corpus_release_id = release.id
    ) AS ranked
    JOIN current_accepted_claim_subjects AS subject
      ON subject.id = ranked.subject_id
    WHERE ranked._current_rank = 1
      AND ranked.review_status = 'accepted'
    """,
    """
    CREATE VIEW IF NOT EXISTS current_accepted_position_observations AS
    SELECT ranked.*
    FROM (
      SELECT positions.*,
             ROW_NUMBER() OVER (
               PARTITION BY positions.position_lineage_id
               ORDER BY positions.revision DESC, positions.decided_at DESC, positions.created_at DESC, positions.id DESC
             ) AS _current_rank
      FROM accepted_position_observations AS positions
      JOIN current_accepted_corpus_releases AS release
        ON release.id = positions.corpus_release_id
      JOIN pipeline_runs AS producing_run
        ON producing_run.id = positions.pipeline_run_id
       AND producing_run.status = 'succeeded'
       AND producing_run.corpus_release_id = release.id
    ) AS ranked
    JOIN current_accepted_claim_subjects AS subject
      ON subject.id = ranked.subject_id
    JOIN current_accepted_proposition_variants AS variant
      ON variant.id = ranked.variant_id
     AND variant.subject_id = ranked.subject_id
    JOIN current_accepted_atomic_claims AS claim
      ON claim.id = ranked.atomic_claim_id
    JOIN current_accepted_people AS person
      ON person.id = ranked.canonical_person_id
    WHERE ranked._current_rank = 1
      AND ranked.review_status = 'accepted'
    """,
    """
    CREATE VIEW IF NOT EXISTS current_accepted_source_affiliations AS
    SELECT ranked.*
    FROM (
      SELECT affiliations.*,
             ROW_NUMBER() OVER (
               PARTITION BY affiliations.affiliation_lineage_id
               ORDER BY affiliations.revision DESC, affiliations.created_at DESC, affiliations.id DESC
             ) AS _current_rank
      FROM source_affiliations AS affiliations
      JOIN current_accepted_corpus_releases AS release
        ON release.id = affiliations.corpus_release_id
      JOIN pipeline_runs AS producing_run
        ON producing_run.id = affiliations.pipeline_run_id
       AND producing_run.status = 'succeeded'
       AND producing_run.corpus_release_id = release.id
    ) AS ranked
    WHERE ranked._current_rank = 1
      AND ranked.review_status = 'accepted'
    """,
    """
    CREATE VIEW IF NOT EXISTS content_source_affiliations AS
    SELECT
      affiliations.*,
      content_sources.id AS content_source_id
    FROM source_affiliations AS affiliations
    LEFT JOIN content_sources
      ON content_sources.legacy_source_id = affiliations.source_id
    """,
    """
    CREATE VIEW IF NOT EXISTS current_accepted_content_source_affiliations AS
    SELECT
      affiliations.*,
      content_sources.id AS content_source_id
    FROM current_accepted_source_affiliations AS affiliations
    LEFT JOIN content_sources
      ON content_sources.legacy_source_id = affiliations.source_id
    """,
    """
    CREATE VIEW IF NOT EXISTS current_accepted_person_appearances AS
    SELECT ranked.*
    FROM (
      SELECT appearances.*,
             ROW_NUMBER() OVER (
               PARTITION BY appearances.appearance_lineage_id
               ORDER BY appearances.revision DESC, appearances.created_at DESC, appearances.id DESC
             ) AS _current_rank
      FROM person_appearances AS appearances
      JOIN current_accepted_corpus_releases AS release
        ON release.id = appearances.corpus_release_id
      JOIN pipeline_runs AS producing_run
        ON producing_run.id = appearances.pipeline_run_id
       AND producing_run.status = 'succeeded'
       AND producing_run.corpus_release_id = release.id
    ) AS ranked
    JOIN current_accepted_people AS person
      ON person.id = ranked.canonical_person_id
    WHERE ranked._current_rank = 1
      AND ranked.review_status = 'accepted'
    """,
    """
    CREATE VIEW IF NOT EXISTS current_accepted_claim_relations AS
    SELECT ranked.*
    FROM (
      SELECT judgments.*,
             ROW_NUMBER() OVER (
               PARTITION BY judgments.source_claim_id, judgments.target_claim_id
               ORDER BY judgments.revision DESC, judgments.decided_at DESC, judgments.created_at DESC, judgments.id DESC
             ) AS _current_rank
      FROM claim_relation_judgments AS judgments
      JOIN current_accepted_corpus_releases AS release
        ON release.id = judgments.corpus_release_id
      JOIN pipeline_runs AS producing_run
        ON producing_run.id = judgments.pipeline_run_id
       AND producing_run.status = 'succeeded'
       AND producing_run.corpus_release_id = release.id
    ) AS ranked
    JOIN current_accepted_atomic_claims AS source_claim
      ON source_claim.id = ranked.source_claim_id
    JOIN current_accepted_atomic_claims AS target_claim
      ON target_claim.id = ranked.target_claim_id
    WHERE ranked._current_rank = 1
      AND ranked.review_status = 'accepted'
    """,
    """
    CREATE VIEW IF NOT EXISTS current_accepted_outcome_resolutions AS
    SELECT ranked.*
    FROM (
      SELECT resolutions.*,
             ROW_NUMBER() OVER (
               PARTITION BY resolutions.claim_id
               ORDER BY resolutions.revision DESC, resolutions.resolved_at DESC, resolutions.created_at DESC, resolutions.id DESC
             ) AS _current_rank
      FROM outcome_resolution_revisions AS resolutions
      JOIN current_accepted_corpus_releases AS release
        ON release.id = resolutions.corpus_release_id
      JOIN pipeline_runs AS producing_run
        ON producing_run.id = resolutions.pipeline_run_id
       AND producing_run.status = 'succeeded'
       AND producing_run.corpus_release_id = release.id
    ) AS ranked
    JOIN current_accepted_atomic_claims AS claim
      ON claim.id = ranked.claim_id
    WHERE ranked._current_rank = 1
      AND ranked.review_status = 'accepted'
    """,
    """
    CREATE VIEW IF NOT EXISTS current_accepted_consensus_snapshots AS
    SELECT ranked.*
    FROM (
      SELECT snapshots.*,
             ROW_NUMBER() OVER (
               PARTITION BY snapshots.focal_claim_id, snapshots.calculation_version
               ORDER BY snapshots.as_of DESC, snapshots.created_at DESC, snapshots.id DESC
             ) AS _current_rank
      FROM consensus_snapshots AS snapshots
      JOIN current_accepted_corpus_releases AS release
        ON release.id = snapshots.corpus_release_id
      JOIN pipeline_runs AS producing_run
        ON producing_run.id = snapshots.pipeline_run_id
       AND producing_run.status = 'succeeded'
       AND producing_run.corpus_release_id = release.id
    ) AS ranked
    JOIN current_accepted_atomic_claims AS claim
      ON claim.id = ranked.focal_claim_id
    WHERE ranked._current_rank = 1
      AND ranked.review_status = 'accepted'
    """,
    """
    CREATE VIEW IF NOT EXISTS current_accepted_contrarian_snapshots AS
    SELECT ranked.*
    FROM (
      SELECT snapshots.*,
             ROW_NUMBER() OVER (
               PARTITION BY snapshots.target_claim_id, snapshots.calculation_version
               ORDER BY snapshots.as_of DESC, snapshots.created_at DESC, snapshots.id DESC
             ) AS _current_rank
      FROM contrarian_snapshots AS snapshots
      JOIN current_accepted_corpus_releases AS release
        ON release.id = snapshots.corpus_release_id
      JOIN pipeline_runs AS producing_run
        ON producing_run.id = snapshots.pipeline_run_id
       AND producing_run.status = 'succeeded'
       AND producing_run.corpus_release_id = release.id
    ) AS ranked
    JOIN current_accepted_atomic_claims AS claim
      ON claim.id = ranked.target_claim_id
    WHERE ranked._current_rank = 1
      AND ranked.review_status = 'accepted'
    """,
    """
    CREATE TRIGGER IF NOT EXISTS atomic_claims_no_update
    BEFORE UPDATE ON atomic_claims BEGIN
      SELECT RAISE(ABORT, 'atomic_claim_v1 rows are immutable');
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS atomic_claims_no_delete
    BEFORE DELETE ON atomic_claims BEGIN
      SELECT RAISE(ABORT, 'atomic_claim_v1 rows are immutable');
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS identity_resolution_judgments_no_update
    BEFORE UPDATE ON identity_resolution_judgments BEGIN
      SELECT RAISE(ABORT, 'identity resolution judgments are append-only');
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS identity_resolution_judgments_no_delete
    BEFORE DELETE ON identity_resolution_judgments BEGIN
      SELECT RAISE(ABORT, 'identity resolution judgments are append-only');
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS accepted_claim_subjects_no_update
    BEFORE UPDATE ON accepted_claim_subjects BEGIN
      SELECT RAISE(ABORT, 'accepted claim subject judgments are append-only');
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS accepted_claim_subjects_no_delete
    BEFORE DELETE ON accepted_claim_subjects BEGIN
      SELECT RAISE(ABORT, 'accepted claim subject judgments are append-only');
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS accepted_proposition_variants_no_update
    BEFORE UPDATE ON accepted_proposition_variants BEGIN
      SELECT RAISE(ABORT, 'accepted proposition variant judgments are append-only');
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS accepted_proposition_variants_no_delete
    BEFORE DELETE ON accepted_proposition_variants BEGIN
      SELECT RAISE(ABORT, 'accepted proposition variant judgments are append-only');
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS accepted_position_observations_no_update
    BEFORE UPDATE ON accepted_position_observations BEGIN
      SELECT RAISE(ABORT, 'accepted position observation judgments are append-only');
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS accepted_position_observations_no_delete
    BEFORE DELETE ON accepted_position_observations BEGIN
      SELECT RAISE(ABORT, 'accepted position observation judgments are append-only');
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS corpus_releases_no_update
    BEFORE UPDATE ON corpus_releases BEGIN
      SELECT RAISE(ABORT, 'corpus releases are immutable');
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS corpus_releases_no_delete
    BEFORE DELETE ON corpus_releases BEGIN
      SELECT RAISE(ABORT, 'corpus releases are immutable');
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS corpus_release_episodes_no_update
    BEFORE UPDATE ON corpus_release_episodes BEGIN
      SELECT RAISE(ABORT, 'corpus release episode membership is immutable');
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS corpus_release_episodes_no_delete
    BEFORE DELETE ON corpus_release_episodes BEGIN
      SELECT RAISE(ABORT, 'corpus release episode membership is immutable');
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS corpus_release_transcripts_no_update
    BEFORE UPDATE ON corpus_release_transcripts BEGIN
      SELECT RAISE(ABORT, 'corpus release transcript membership is immutable');
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS corpus_release_transcripts_no_delete
    BEFORE DELETE ON corpus_release_transcripts BEGIN
      SELECT RAISE(ABORT, 'corpus release transcript membership is immutable');
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS corpus_release_segments_no_update
    BEFORE UPDATE ON corpus_release_segments BEGIN
      SELECT RAISE(ABORT, 'corpus release segment membership is immutable');
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS corpus_release_segments_no_delete
    BEFORE DELETE ON corpus_release_segments BEGIN
      SELECT RAISE(ABORT, 'corpus release segment membership is immutable');
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS corpus_release_labels_no_update
    BEFORE UPDATE ON corpus_release_labels BEGIN
      SELECT RAISE(ABORT, 'corpus release label membership is immutable');
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS corpus_release_labels_no_delete
    BEFORE DELETE ON corpus_release_labels BEGIN
      SELECT RAISE(ABORT, 'corpus release label membership is immutable');
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS corpus_release_promotions_no_update
    BEFORE UPDATE ON corpus_release_promotions BEGIN
      SELECT RAISE(ABORT, 'corpus release promotions are append-only');
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS corpus_release_promotions_no_delete
    BEFORE DELETE ON corpus_release_promotions BEGIN
      SELECT RAISE(ABORT, 'corpus release promotions are append-only');
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS source_affiliations_no_update
    BEFORE UPDATE ON source_affiliations BEGIN
      SELECT RAISE(ABORT, 'source affiliation judgments are append-only');
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS source_affiliations_no_delete
    BEFORE DELETE ON source_affiliations BEGIN
      SELECT RAISE(ABORT, 'source affiliation judgments are append-only');
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS person_appearances_no_update
    BEFORE UPDATE ON person_appearances BEGIN
      SELECT RAISE(ABORT, 'person appearance judgments are append-only');
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS person_appearances_no_delete
    BEFORE DELETE ON person_appearances BEGIN
      SELECT RAISE(ABORT, 'person appearance judgments are append-only');
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS claim_relation_judgments_no_update
    BEFORE UPDATE ON claim_relation_judgments BEGIN
      SELECT RAISE(ABORT, 'claim relation judgments are append-only');
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS claim_relation_judgments_no_delete
    BEFORE DELETE ON claim_relation_judgments BEGIN
      SELECT RAISE(ABORT, 'claim relation judgments are append-only');
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS consensus_snapshots_no_update
    BEFORE UPDATE ON consensus_snapshots BEGIN
      SELECT RAISE(ABORT, 'consensus snapshots are immutable');
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS consensus_snapshots_no_delete
    BEFORE DELETE ON consensus_snapshots BEGIN
      SELECT RAISE(ABORT, 'consensus snapshots are immutable');
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS contrarian_snapshots_no_update
    BEFORE UPDATE ON contrarian_snapshots BEGIN
      SELECT RAISE(ABORT, 'contrarian snapshots are immutable');
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS contrarian_snapshots_no_delete
    BEFORE DELETE ON contrarian_snapshots BEGIN
      SELECT RAISE(ABORT, 'contrarian snapshots are immutable');
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS outcome_resolution_revisions_no_update
    BEFORE UPDATE ON outcome_resolution_revisions BEGIN
      SELECT RAISE(ABORT, 'outcome resolution revisions are append-only');
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS outcome_resolution_revisions_no_delete
    BEFORE DELETE ON outcome_resolution_revisions BEGIN
      SELECT RAISE(ABORT, 'outcome resolution revisions are append-only');
    END
    """,
)


QUEUE_ENVELOPE_ORPHAN_ARCHIVE_V2: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS queue_envelope_orphan_archive (
      archive_id INTEGER PRIMARY KEY AUTOINCREMENT,
      job_id INTEGER NOT NULL,
      queue_name TEXT NOT NULL,
      worker_role TEXT NOT NULL,
      content_type TEXT NOT NULL,
      source_id TEXT,
      item_id TEXT,
      artifact_id TEXT,
      label_pack TEXT,
      model_required TEXT,
      privacy_tier TEXT NOT NULL,
      lease_ttl_seconds INTEGER NOT NULL,
      input_context_ref TEXT,
      output_schema_ref TEXT,
      callback_mode TEXT NOT NULL,
      run_tag TEXT,
      allowed_tools_json TEXT NOT NULL,
      capabilities_json TEXT NOT NULL,
      remote_claimable INTEGER NOT NULL,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      reason TEXT NOT NULL,
      archived_at TEXT NOT NULL,
      UNIQUE(job_id)
    )
    """,
    """
    CREATE TRIGGER IF NOT EXISTS queue_envelope_orphan_archive_no_update
    BEFORE UPDATE ON queue_envelope_orphan_archive BEGIN
      SELECT RAISE(ABORT, 'queue envelope orphan archive is immutable');
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS queue_envelope_orphan_archive_no_delete
    BEFORE DELETE ON queue_envelope_orphan_archive BEGIN
      SELECT RAISE(ABORT, 'queue envelope orphan archive is immutable');
    END
    """,
    """
    INSERT OR IGNORE INTO queue_envelope_orphan_archive (
      job_id,
      queue_name,
      worker_role,
      content_type,
      source_id,
      item_id,
      artifact_id,
      label_pack,
      model_required,
      privacy_tier,
      lease_ttl_seconds,
      input_context_ref,
      output_schema_ref,
      callback_mode,
      run_tag,
      allowed_tools_json,
      capabilities_json,
      remote_claimable,
      created_at,
      updated_at,
      reason,
      archived_at
    )
    SELECT
      envelope.job_id,
      envelope.queue_name,
      envelope.worker_role,
      envelope.content_type,
      envelope.source_id,
      envelope.item_id,
      envelope.artifact_id,
      envelope.label_pack,
      envelope.model_required,
      envelope.privacy_tier,
      envelope.lease_ttl_seconds,
      envelope.input_context_ref,
      envelope.output_schema_ref,
      envelope.callback_mode,
      envelope.run_tag,
      envelope.allowed_tools_json,
      envelope.capabilities_json,
      envelope.remote_claimable,
      envelope.created_at,
      envelope.updated_at,
      'missing_jobs_parent_at_migration_v2',
      strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    FROM queue_envelopes AS envelope
    WHERE NOT EXISTS (
      SELECT 1 FROM jobs WHERE jobs.id = envelope.job_id
    )
    """,
    """
    DELETE FROM queue_envelopes
    WHERE NOT EXISTS (
      SELECT 1 FROM jobs WHERE jobs.id = queue_envelopes.job_id
    )
    """,
)


SEMANTIC_SCOPE_AND_COVERAGE_V3: tuple[str, ...] = (
    """
    ALTER TABLE claim_relation_judgments
    ADD COLUMN temporal_scope TEXT
      CHECK (temporal_scope IS NULL OR length(trim(temporal_scope)) > 0)
    """,
    """
    ALTER TABLE contrarian_snapshots
    ADD COLUMN classification TEXT
      CHECK (
        classification IS NULL
        OR classification IN ('contrarian', 'not_contrarian', 'insufficient_coverage')
      )
    """,
    """
    CREATE TRIGGER IF NOT EXISTS claim_relation_judgments_require_temporal_scope
    BEFORE INSERT ON claim_relation_judgments
    WHEN NEW.temporal_scope IS NULL OR length(trim(NEW.temporal_scope)) = 0
    BEGIN
      SELECT RAISE(ABORT, 'claim relation temporal_scope is required');
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS contrarian_snapshots_require_classification
    BEFORE INSERT ON contrarian_snapshots
    WHEN NEW.classification IS NULL
    BEGIN
      SELECT RAISE(ABORT, 'contrarian classification is required');
    END
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_contrarian_snapshots_classification
    ON contrarian_snapshots(corpus_release_id, classification, as_of DESC)
    """,
    "DROP VIEW IF EXISTS current_accepted_claim_relations",
    """
    CREATE VIEW current_accepted_claim_relations AS
    SELECT ranked.*
    FROM (
      SELECT judgments.*,
             ROW_NUMBER() OVER (
               PARTITION BY judgments.source_claim_id, judgments.target_claim_id
               ORDER BY judgments.revision DESC, judgments.decided_at DESC,
                        judgments.created_at DESC, judgments.id DESC
             ) AS _current_rank
      FROM claim_relation_judgments AS judgments
      JOIN current_accepted_corpus_releases AS release
        ON release.id = judgments.corpus_release_id
      JOIN pipeline_runs AS producing_run
        ON producing_run.id = judgments.pipeline_run_id
       AND producing_run.status = 'succeeded'
       AND producing_run.corpus_release_id = release.id
    ) AS ranked
    JOIN current_accepted_atomic_claims AS source_claim
      ON source_claim.id = ranked.source_claim_id
    JOIN current_accepted_atomic_claims AS target_claim
      ON target_claim.id = ranked.target_claim_id
    WHERE ranked._current_rank = 1
      AND ranked.review_status = 'accepted'
      AND ranked.temporal_scope IS NOT NULL
    """,
    "DROP VIEW IF EXISTS current_accepted_contrarian_snapshots",
    """
    CREATE VIEW current_accepted_contrarian_snapshots AS
    SELECT ranked.*
    FROM (
      SELECT snapshots.*,
             ROW_NUMBER() OVER (
               PARTITION BY snapshots.target_claim_id, snapshots.calculation_version
               ORDER BY snapshots.as_of DESC, snapshots.created_at DESC, snapshots.id DESC
             ) AS _current_rank
      FROM contrarian_snapshots AS snapshots
      JOIN current_accepted_corpus_releases AS release
        ON release.id = snapshots.corpus_release_id
      JOIN pipeline_runs AS producing_run
        ON producing_run.id = snapshots.pipeline_run_id
       AND producing_run.status = 'succeeded'
       AND producing_run.corpus_release_id = release.id
    ) AS ranked
    JOIN current_accepted_atomic_claims AS claim
      ON claim.id = ranked.target_claim_id
    WHERE ranked._current_rank = 1
      AND ranked.review_status = 'accepted'
      AND ranked.classification IS NOT NULL
    """,
)


ACCEPTED_PIPELINE_RUN_AUTHORITY_V4: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS pipeline_run_authority_decisions (
      id TEXT PRIMARY KEY,
      schema_version TEXT NOT NULL DEFAULT 'pipeline_run_authority_v1'
        CHECK(schema_version = 'pipeline_run_authority_v1'),
      authority_lineage_id TEXT NOT NULL,
      revision INTEGER NOT NULL CHECK(revision > 0),
      supersedes_decision_id TEXT REFERENCES pipeline_run_authority_decisions(id),
      stage TEXT NOT NULL
        CHECK(stage IN (
          'release', 'atomic_claims', 'identities', 'claims', 'networks',
          'relations', 'consensus', 'contrarian', 'outcomes'
        )),
      pipeline_run_id TEXT NOT NULL REFERENCES pipeline_runs(id),
      replacement_pipeline_run_id TEXT REFERENCES pipeline_runs(id),
      run_type TEXT NOT NULL,
      run_schema TEXT NOT NULL,
      run_schema_version TEXT NOT NULL,
      configuration_sha256 TEXT NOT NULL CHECK(length(configuration_sha256) = 64),
      model TEXT,
      model_version TEXT,
      prompt_version TEXT,
      corpus_release_id TEXT NOT NULL REFERENCES corpus_releases(id),
      run_status TEXT NOT NULL
        CHECK(run_status IN ('pending', 'running', 'succeeded', 'failed', 'canceled')),
      decision TEXT NOT NULL
        CHECK(decision IN ('accepted', 'rejected', 'superseded')),
      reviewed_by TEXT NOT NULL CHECK(length(trim(reviewed_by)) > 0),
      rationale TEXT NOT NULL CHECK(length(trim(rationale)) > 0),
      decided_at TEXT NOT NULL,
      created_at TEXT NOT NULL,
      UNIQUE(authority_lineage_id, revision),
      CHECK(
        (decision = 'superseded' AND replacement_pipeline_run_id IS NOT NULL)
        OR (decision <> 'superseded' AND replacement_pipeline_run_id IS NULL)
      ),
      CHECK(replacement_pipeline_run_id IS NULL OR replacement_pipeline_run_id <> pipeline_run_id)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_pipeline_run_authority_current
    ON pipeline_run_authority_decisions(authority_lineage_id, revision DESC, decided_at DESC)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_pipeline_run_authority_stage_release
    ON pipeline_run_authority_decisions(stage, corpus_release_id, decision, decided_at DESC)
    """,
    """
    CREATE TRIGGER IF NOT EXISTS pipeline_run_authority_decisions_no_update
    BEFORE UPDATE ON pipeline_run_authority_decisions BEGIN
      SELECT RAISE(ABORT, 'pipeline run authority decisions are append-only');
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS pipeline_run_authority_decisions_no_delete
    BEFORE DELETE ON pipeline_run_authority_decisions BEGIN
      SELECT RAISE(ABORT, 'pipeline run authority decisions are append-only');
    END
    """,
    """
    INSERT OR IGNORE INTO pipeline_run_authority_decisions (
      id, schema_version, authority_lineage_id, revision, supersedes_decision_id,
      stage, pipeline_run_id, replacement_pipeline_run_id, run_type, run_schema,
      run_schema_version, configuration_sha256, model, model_version, prompt_version,
      corpus_release_id, run_status, decision, reviewed_by, rationale, decided_at, created_at
    )
    SELECT
      'pra_backfill_release_' || runs.id,
      'pipeline_run_authority_v1',
      'prauth_release_' || runs.id,
      1,
      NULL,
      'release',
      runs.id,
      NULL,
      runs.run_type,
      runs.run_schema,
      runs.run_schema_version,
      runs.configuration_sha256,
      runs.model,
      runs.model_version,
      runs.prompt_version,
      runs.corpus_release_id,
      runs.status,
      'accepted',
      promotions.promoted_by,
      'Forward migration preserved an explicitly promoted, verified release-build run: '
        || promotions.rationale,
      promotions.created_at,
      promotions.created_at
    FROM corpus_release_promotions AS promotions
    JOIN pipeline_runs AS runs
      ON runs.id = promotions.pipeline_run_id
     AND runs.corpus_release_id = promotions.corpus_release_id
    WHERE promotions.action = 'promote'
      AND runs.status = 'succeeded'
      AND (
        (
          runs.run_type = 'release_build'
          AND runs.run_schema = 'corpus_release_build'
          AND runs.run_schema_version = 'corpus_release_build_v1'
        )
        OR (
          runs.run_type = 'atomic_claim_import'
          AND runs.run_schema = 'atomic_claim_v1'
          AND runs.run_schema_version = 'atomic_claim_import_v1'
          AND EXISTS (
            SELECT 1 FROM pipeline_runs AS release_build
            WHERE release_build.corpus_release_id = runs.corpus_release_id
              AND release_build.status = 'succeeded'
              AND release_build.run_type = 'release_build'
              AND release_build.run_schema = 'corpus_release_build'
              AND release_build.run_schema_version = 'corpus_release_build_v1'
          )
        )
      )
    """,
    """
    INSERT OR IGNORE INTO pipeline_run_authority_decisions (
      id, schema_version, authority_lineage_id, revision, supersedes_decision_id,
      stage, pipeline_run_id, replacement_pipeline_run_id, run_type, run_schema,
      run_schema_version, configuration_sha256, model, model_version, prompt_version,
      corpus_release_id, run_status, decision, reviewed_by, rationale, decided_at, created_at
    )
    SELECT
      'pra_backfill_atomic_' || runs.id,
      'pipeline_run_authority_v1',
      'prauth_atomic_claims_' || runs.id,
      1,
      NULL,
      'atomic_claims',
      runs.id,
      NULL,
      runs.run_type,
      runs.run_schema,
      runs.run_schema_version,
      runs.configuration_sha256,
      runs.model,
      runs.model_version,
      runs.prompt_version,
      runs.corpus_release_id,
      runs.status,
      'accepted',
      'migration_v4_verified_contract_backfill',
      'Forward migration preserved a succeeded exact-contract atomic import for an explicitly promoted release.',
      COALESCE(runs.completed_at, runs.updated_at),
      COALESCE(runs.completed_at, runs.updated_at)
    FROM pipeline_runs AS runs
    WHERE runs.status = 'succeeded'
      AND runs.run_type = 'atomic_claim_import'
      AND runs.run_schema = 'atomic_claim_v1'
      AND runs.run_schema_version = 'atomic_claim_import_v1'
      AND EXISTS (
        SELECT 1 FROM atomic_claims AS claims
        WHERE claims.pipeline_run_id = runs.id
          AND claims.corpus_release_id = runs.corpus_release_id
      )
      AND EXISTS (
        SELECT 1 FROM corpus_release_promotions AS promotions
        WHERE promotions.corpus_release_id = runs.corpus_release_id
          AND promotions.action = 'promote'
      )
      AND EXISTS (
        SELECT 1 FROM pipeline_runs AS release_run
        WHERE release_run.corpus_release_id = runs.corpus_release_id
          AND release_run.status = 'succeeded'
          AND release_run.run_type = 'release_build'
          AND release_run.run_schema = 'corpus_release_build'
          AND release_run.run_schema_version = 'corpus_release_build_v1'
      )
    """,
    "DROP VIEW IF EXISTS current_accepted_contrarian_snapshots",
    "DROP VIEW IF EXISTS current_accepted_consensus_snapshots",
    "DROP VIEW IF EXISTS current_accepted_outcome_resolutions",
    "DROP VIEW IF EXISTS current_accepted_claim_relations",
    "DROP VIEW IF EXISTS current_accepted_person_appearances",
    "DROP VIEW IF EXISTS current_accepted_position_observations",
    "DROP VIEW IF EXISTS current_accepted_proposition_variants",
    "DROP VIEW IF EXISTS current_accepted_content_source_affiliations",
    "DROP VIEW IF EXISTS current_accepted_source_affiliations",
    "DROP VIEW IF EXISTS current_accepted_claim_subjects",
    "DROP VIEW IF EXISTS current_accepted_atomic_claims",
    "DROP VIEW IF EXISTS current_accepted_people",
    "DROP VIEW IF EXISTS current_accepted_identity_resolutions",
    "DROP VIEW IF EXISTS current_accepted_corpus_releases",
    "DROP VIEW IF EXISTS current_accepted_pipeline_stage_runs",
    """
    CREATE VIEW current_accepted_pipeline_stage_runs AS
    SELECT
      runs.*,
      ranked.stage AS accepted_stage,
      ranked.id AS authority_decision_id,
      ranked.revision AS authority_revision,
      ranked.reviewed_by AS authority_reviewed_by,
      ranked.rationale AS authority_rationale,
      ranked.decided_at AS authority_decided_at
    FROM (
      SELECT decisions.*,
             ROW_NUMBER() OVER (
               PARTITION BY decisions.stage, decisions.pipeline_run_id
               ORDER BY decisions.revision DESC, decisions.decided_at DESC,
                        decisions.created_at DESC, decisions.id DESC
             ) AS _current_rank
      FROM pipeline_run_authority_decisions AS decisions
    ) AS ranked
    JOIN pipeline_runs AS runs
      ON runs.id = ranked.pipeline_run_id
     AND runs.status = 'succeeded'
     AND runs.run_type = ranked.run_type
     AND runs.run_schema = ranked.run_schema
     AND runs.run_schema_version = ranked.run_schema_version
     AND runs.configuration_sha256 = ranked.configuration_sha256
     AND runs.model IS ranked.model
     AND runs.model_version IS ranked.model_version
     AND runs.prompt_version IS ranked.prompt_version
     AND runs.corpus_release_id = ranked.corpus_release_id
    WHERE ranked._current_rank = 1
      AND ranked.decision = 'accepted'
    """,
    """
    CREATE VIEW current_accepted_corpus_releases AS
    SELECT
      corpus_releases.*,
      corpus_release_promotions.id AS promotion_id,
      corpus_release_promotions.promotion_revision,
      corpus_release_promotions.pipeline_run_id AS promotion_pipeline_run_id,
      corpus_release_promotions.created_at AS promoted_at
    FROM corpus_release_promotions
    JOIN corpus_releases
      ON corpus_releases.id = corpus_release_promotions.corpus_release_id
     AND corpus_releases.status = 'accepted'
    JOIN current_accepted_pipeline_stage_runs AS pipeline_runs
      ON pipeline_runs.id = corpus_release_promotions.pipeline_run_id
     AND pipeline_runs.accepted_stage = 'release'
     AND pipeline_runs.corpus_release_id = corpus_releases.id
    WHERE corpus_release_promotions.action = 'promote'
      AND corpus_release_promotions.promotion_revision = (
        SELECT MAX(promotion_revision) FROM corpus_release_promotions
      )
    """,
    """
    CREATE VIEW current_accepted_identity_resolutions AS
    SELECT ranked.*
    FROM (
      SELECT judgments.*,
             ROW_NUMBER() OVER (
               PARTITION BY judgments.identity_lineage_id
               ORDER BY judgments.revision DESC, judgments.decided_at DESC,
                        judgments.created_at DESC, judgments.id DESC
             ) AS _current_rank
      FROM identity_resolution_judgments AS judgments
      JOIN current_accepted_corpus_releases AS release
        ON release.id = judgments.corpus_release_id
      JOIN current_accepted_pipeline_stage_runs AS producing_run
        ON producing_run.id = judgments.pipeline_run_id
       AND producing_run.accepted_stage = 'identities'
       AND producing_run.corpus_release_id = release.id
    ) AS ranked
    JOIN canonical_people AS person
      ON person.id = ranked.canonical_person_id
     AND person.status = 'accepted'
    WHERE ranked._current_rank = 1
      AND ranked.review_status = 'accepted'
      AND ranked.decision IN ('accepted', 'merged')
    """,
    """
    CREATE VIEW current_accepted_people AS
    SELECT people.*
    FROM canonical_people AS people
    WHERE people.status = 'accepted'
      AND EXISTS (
        SELECT 1 FROM current_accepted_identity_resolutions AS resolutions
        WHERE resolutions.canonical_person_id = people.id
      )
    """,
    """
    CREATE VIEW current_accepted_atomic_claims AS
    SELECT ranked.*
    FROM (
      SELECT claims.*,
             ROW_NUMBER() OVER (
               PARTITION BY claims.claim_lineage_id
               ORDER BY claims.revision DESC, claims.created_at DESC, claims.id DESC
             ) AS _current_rank
      FROM atomic_claims AS claims
      JOIN current_accepted_corpus_releases AS release
        ON release.id = claims.corpus_release_id
      JOIN current_accepted_pipeline_stage_runs AS producing_run
        ON producing_run.id = claims.pipeline_run_id
       AND producing_run.accepted_stage = 'atomic_claims'
       AND producing_run.corpus_release_id = release.id
    ) AS ranked
    LEFT JOIN current_accepted_people AS person
      ON person.id = ranked.canonical_person_id
    WHERE ranked._current_rank = 1
      AND ranked.review_status = 'accepted'
      AND (ranked.canonical_person_id IS NULL OR person.id IS NOT NULL)
    """,
    """
    CREATE VIEW current_accepted_claim_subjects AS
    SELECT ranked.*
    FROM (
      SELECT subjects.*,
             ROW_NUMBER() OVER (
               PARTITION BY subjects.subject_lineage_id
               ORDER BY subjects.revision DESC, subjects.decided_at DESC,
                        subjects.created_at DESC, subjects.id DESC
             ) AS _current_rank
      FROM accepted_claim_subjects AS subjects
      JOIN current_accepted_corpus_releases AS release
        ON release.id = subjects.corpus_release_id
      JOIN current_accepted_pipeline_stage_runs AS producing_run
        ON producing_run.id = subjects.pipeline_run_id
       AND producing_run.accepted_stage = 'claims'
       AND producing_run.corpus_release_id = release.id
    ) AS ranked
    WHERE ranked._current_rank = 1
      AND ranked.review_status = 'accepted'
    """,
    """
    CREATE VIEW current_accepted_proposition_variants AS
    SELECT ranked.*
    FROM (
      SELECT variants.*,
             ROW_NUMBER() OVER (
               PARTITION BY variants.variant_lineage_id
               ORDER BY variants.revision DESC, variants.decided_at DESC,
                        variants.created_at DESC, variants.id DESC
             ) AS _current_rank
      FROM accepted_proposition_variants AS variants
      JOIN current_accepted_corpus_releases AS release
        ON release.id = variants.corpus_release_id
      JOIN current_accepted_pipeline_stage_runs AS producing_run
        ON producing_run.id = variants.pipeline_run_id
       AND producing_run.accepted_stage = 'claims'
       AND producing_run.corpus_release_id = release.id
    ) AS ranked
    JOIN current_accepted_claim_subjects AS subject
      ON subject.id = ranked.subject_id
    WHERE ranked._current_rank = 1
      AND ranked.review_status = 'accepted'
    """,
    """
    CREATE VIEW current_accepted_position_observations AS
    SELECT ranked.*
    FROM (
      SELECT positions.*,
             ROW_NUMBER() OVER (
               PARTITION BY positions.position_lineage_id
               ORDER BY positions.revision DESC, positions.decided_at DESC,
                        positions.created_at DESC, positions.id DESC
             ) AS _current_rank
      FROM accepted_position_observations AS positions
      JOIN current_accepted_corpus_releases AS release
        ON release.id = positions.corpus_release_id
      JOIN current_accepted_pipeline_stage_runs AS producing_run
        ON producing_run.id = positions.pipeline_run_id
       AND producing_run.accepted_stage = 'claims'
       AND producing_run.corpus_release_id = release.id
    ) AS ranked
    JOIN current_accepted_claim_subjects AS subject
      ON subject.id = ranked.subject_id
    JOIN current_accepted_proposition_variants AS variant
      ON variant.id = ranked.variant_id
     AND variant.subject_id = ranked.subject_id
    JOIN current_accepted_atomic_claims AS claim
      ON claim.id = ranked.atomic_claim_id
    JOIN current_accepted_people AS person
      ON person.id = ranked.canonical_person_id
    WHERE ranked._current_rank = 1
      AND ranked.review_status = 'accepted'
    """,
    """
    CREATE VIEW current_accepted_source_affiliations AS
    SELECT ranked.*
    FROM (
      SELECT affiliations.*,
             ROW_NUMBER() OVER (
               PARTITION BY affiliations.affiliation_lineage_id
               ORDER BY affiliations.revision DESC, affiliations.created_at DESC,
                        affiliations.id DESC
             ) AS _current_rank
      FROM source_affiliations AS affiliations
      JOIN current_accepted_corpus_releases AS release
        ON release.id = affiliations.corpus_release_id
      JOIN current_accepted_pipeline_stage_runs AS producing_run
        ON producing_run.id = affiliations.pipeline_run_id
       AND producing_run.accepted_stage = 'networks'
       AND producing_run.corpus_release_id = release.id
    ) AS ranked
    WHERE ranked._current_rank = 1
      AND ranked.review_status = 'accepted'
    """,
    """
    CREATE VIEW current_accepted_content_source_affiliations AS
    SELECT affiliations.*, content_sources.id AS content_source_id
    FROM current_accepted_source_affiliations AS affiliations
    LEFT JOIN content_sources
      ON content_sources.legacy_source_id = affiliations.source_id
    """,
    """
    CREATE VIEW current_accepted_person_appearances AS
    SELECT ranked.*
    FROM (
      SELECT appearances.*,
             ROW_NUMBER() OVER (
               PARTITION BY appearances.appearance_lineage_id
               ORDER BY appearances.revision DESC, appearances.created_at DESC,
                        appearances.id DESC
             ) AS _current_rank
      FROM person_appearances AS appearances
      JOIN current_accepted_corpus_releases AS release
        ON release.id = appearances.corpus_release_id
      JOIN current_accepted_pipeline_stage_runs AS producing_run
        ON producing_run.id = appearances.pipeline_run_id
       AND producing_run.accepted_stage = 'networks'
       AND producing_run.corpus_release_id = release.id
    ) AS ranked
    JOIN current_accepted_people AS person
      ON person.id = ranked.canonical_person_id
    WHERE ranked._current_rank = 1
      AND ranked.review_status = 'accepted'
    """,
    """
    CREATE VIEW current_accepted_claim_relations AS
    SELECT ranked.*
    FROM (
      SELECT judgments.*,
             ROW_NUMBER() OVER (
               PARTITION BY judgments.source_claim_id, judgments.target_claim_id
               ORDER BY judgments.revision DESC, judgments.decided_at DESC,
                        judgments.created_at DESC, judgments.id DESC
             ) AS _current_rank
      FROM claim_relation_judgments AS judgments
      JOIN current_accepted_corpus_releases AS release
        ON release.id = judgments.corpus_release_id
      JOIN current_accepted_pipeline_stage_runs AS producing_run
        ON producing_run.id = judgments.pipeline_run_id
       AND producing_run.accepted_stage = 'relations'
       AND producing_run.corpus_release_id = release.id
    ) AS ranked
    JOIN current_accepted_atomic_claims AS source_claim
      ON source_claim.id = ranked.source_claim_id
    JOIN current_accepted_atomic_claims AS target_claim
      ON target_claim.id = ranked.target_claim_id
    WHERE ranked._current_rank = 1
      AND ranked.review_status = 'accepted'
      AND ranked.temporal_scope IS NOT NULL
    """,
    """
    CREATE VIEW current_accepted_outcome_resolutions AS
    SELECT ranked.*
    FROM (
      SELECT resolutions.*,
             ROW_NUMBER() OVER (
               PARTITION BY resolutions.claim_id
               ORDER BY resolutions.revision DESC, resolutions.resolved_at DESC,
                        resolutions.created_at DESC, resolutions.id DESC
             ) AS _current_rank
      FROM outcome_resolution_revisions AS resolutions
      JOIN current_accepted_corpus_releases AS release
        ON release.id = resolutions.corpus_release_id
      JOIN current_accepted_pipeline_stage_runs AS producing_run
        ON producing_run.id = resolutions.pipeline_run_id
       AND producing_run.accepted_stage = 'outcomes'
       AND producing_run.corpus_release_id = release.id
    ) AS ranked
    JOIN current_accepted_atomic_claims AS claim
      ON claim.id = ranked.claim_id
    WHERE ranked._current_rank = 1
      AND ranked.review_status = 'accepted'
    """,
    """
    CREATE VIEW current_accepted_consensus_snapshots AS
    SELECT ranked.*
    FROM (
      SELECT snapshots.*,
             ROW_NUMBER() OVER (
               PARTITION BY snapshots.focal_claim_id, snapshots.calculation_version
               ORDER BY snapshots.as_of DESC, snapshots.created_at DESC, snapshots.id DESC
             ) AS _current_rank
      FROM consensus_snapshots AS snapshots
      JOIN current_accepted_corpus_releases AS release
        ON release.id = snapshots.corpus_release_id
      JOIN current_accepted_pipeline_stage_runs AS producing_run
        ON producing_run.id = snapshots.pipeline_run_id
       AND producing_run.accepted_stage = 'consensus'
       AND producing_run.corpus_release_id = release.id
    ) AS ranked
    JOIN current_accepted_atomic_claims AS claim
      ON claim.id = ranked.focal_claim_id
    WHERE ranked._current_rank = 1
      AND ranked.review_status = 'accepted'
    """,
    """
    CREATE VIEW current_accepted_contrarian_snapshots AS
    SELECT ranked.*
    FROM (
      SELECT snapshots.*,
             ROW_NUMBER() OVER (
               PARTITION BY snapshots.target_claim_id, snapshots.calculation_version
               ORDER BY snapshots.as_of DESC, snapshots.created_at DESC, snapshots.id DESC
             ) AS _current_rank
      FROM contrarian_snapshots AS snapshots
      JOIN current_accepted_corpus_releases AS release
        ON release.id = snapshots.corpus_release_id
      JOIN current_accepted_pipeline_stage_runs AS producing_run
        ON producing_run.id = snapshots.pipeline_run_id
       AND producing_run.accepted_stage = 'contrarian'
       AND producing_run.corpus_release_id = release.id
    ) AS ranked
    JOIN current_accepted_atomic_claims AS claim
      ON claim.id = ranked.target_claim_id
    WHERE ranked._current_rank = 1
      AND ranked.review_status = 'accepted'
      AND ranked.classification IS NOT NULL
    """,
)


LABEL_METRIC_QUARANTINE_V5: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS label_metric_quarantines (
      id TEXT PRIMARY KEY,
      label_id TEXT NOT NULL REFERENCES labels(id),
      label_run_id TEXT NOT NULL REFERENCES label_runs(id),
      segment_id TEXT NOT NULL REFERENCES segments(id),
      event_index INTEGER NOT NULL CHECK(event_index >= 0),
      claim_text TEXT NOT NULL,
      original_metric_json TEXT NOT NULL,
      evidence TEXT NOT NULL,
      failed_rules_json TEXT NOT NULL,
      quarantined_at TEXT NOT NULL,
      UNIQUE(label_id, label_run_id, event_index)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_label_metric_quarantines_label
    ON label_metric_quarantines(label_id, quarantined_at)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_label_metric_quarantines_segment
    ON label_metric_quarantines(segment_id, quarantined_at)
    """,
    """
    CREATE TRIGGER IF NOT EXISTS label_metric_quarantines_no_update
    BEFORE UPDATE ON label_metric_quarantines BEGIN
      SELECT RAISE(ABORT, 'label metric quarantine is immutable');
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS label_metric_quarantines_no_delete
    BEFORE DELETE ON label_metric_quarantines BEGIN
      SELECT RAISE(ABORT, 'label metric quarantine is immutable');
    END
    """,
)


SCHEMA_MIGRATIONS: tuple[tuple[int, str, tuple[str, ...]], ...] = (
    (1, "versioned_intelligence_v1", INTELLIGENCE_SCHEMA_V1),
    (2, "archive_orphan_queue_envelopes_v2", QUEUE_ENVELOPE_ORPHAN_ARCHIVE_V2),
    (3, "semantic_scope_and_coverage_v3", SEMANTIC_SCOPE_AND_COVERAGE_V3),
    (4, "accepted_pipeline_run_authority_v4", ACCEPTED_PIPELINE_RUN_AUTHORITY_V4),
    (5, "label_metric_quarantine_v5", LABEL_METRIC_QUARANTINE_V5),
)

VERSIONED_INTELLIGENCE_V1_SUPERSEDED_CHECKSUM = (
    "0ea8dd4afc02b1a668dc872a0a0f9f445c75da53833ebc7bca5144229b94e0fa"
)
VERSIONED_INTELLIGENCE_V1_DIVERGENT_VIEWS = (
    "current_accepted_atomic_claims",
    "current_accepted_claim_relations",
    "current_accepted_claim_subjects",
    "current_accepted_consensus_snapshots",
    "current_accepted_contrarian_snapshots",
    "current_accepted_corpus_releases",
    "current_accepted_identity_resolutions",
    "current_accepted_outcome_resolutions",
    "current_accepted_person_appearances",
    "current_accepted_position_observations",
    "current_accepted_proposition_variants",
    "current_accepted_source_affiliations",
)
VERSIONED_INTELLIGENCE_V1_ADOPTION_JUSTIFICATION = (
    "Migration 1 source evolved after production application. Read-only schema "
    "forensics verified that all 90 declared objects exist, that all 18 tables, "
    "22 indexes, and 34 triggers are equivalent, and that exactly 12 derived "
    "accepted-state views require forward reconciliation by pending migration 4."
)


def init_db(conn: sqlite3.Connection | None = None) -> None:
    _init_db(conn, record_verified_v1_baseline=False)


def init_db_adopting_verified_versioned_intelligence_v1_baseline(
    conn: sqlite3.Connection | None = None,
) -> None:
    """Initialize after recording the one exact, audited production V1 baseline."""

    _init_db(conn, record_verified_v1_baseline=True)


def _init_db(
    conn: sqlite3.Connection | None,
    *,
    record_verified_v1_baseline: bool,
) -> None:
    own_conn = conn is None
    conn = conn or connect()
    try:
        with init_db_lock(conn):
            conn.executescript(SCHEMA)
            migrate_schema(conn)
            if record_verified_v1_baseline:
                adopt_versioned_intelligence_v1_baseline(conn)
            apply_schema_migrations(conn)
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        if own_conn:
            conn.close()


@contextmanager
def init_db_lock(conn: sqlite3.Connection, *, timeout_seconds: float = INIT_LOCK_TIMEOUT_SECONDS):
    timeout_seconds = float(os.environ.get("RESEARCH_FACTORY_INIT_LOCK_TIMEOUT_SECONDS", timeout_seconds))
    path = _connection_path(conn)
    lock_path = path.with_suffix(path.suffix + ".init.lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + timeout_seconds
    with lock_path.open("a+", encoding="utf-8") as lock_file:
        while True:
            try:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError as exc:
                if time.monotonic() >= deadline:
                    raise RuntimeError(
                        "another research_factory process is initializing the SQLite schema; retry after it finishes"
                    ) from exc
                time.sleep(0.05)
        try:
            yield
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def _connection_path(conn: sqlite3.Connection) -> Path:
    row = conn.execute("PRAGMA database_list").fetchone()
    if row is not None:
        file_path = row["file"] if isinstance(row, sqlite3.Row) else row[2]
        if file_path:
            return Path(file_path).expanduser().resolve()
    return db_path()


def migrate_schema(conn: sqlite3.Connection) -> None:
    columns = {row["name"] for row in conn.execute("PRAGMA table_info(episodes)").fetchall()}
    episode_columns = {
        "feed_transcript_url": "TEXT",
        "feed_transcript_type": "TEXT",
        "verified_transcript_url": "TEXT",
        "verified_transcript_type": "TEXT",
        "verified_transcript_source_kind": "TEXT",
    }
    for name, column_type in episode_columns.items():
        if name not in columns:
            conn.execute(f"ALTER TABLE episodes ADD COLUMN {name} {column_type}")
    reviewer_columns = {row["name"] for row in conn.execute("PRAGMA table_info(reviewer_audits)").fetchall()}
    reviewer_columns_to_add = {
        "review_mode": "TEXT NOT NULL DEFAULT 'full'",
        "patch_tag": "TEXT",
        "focus_json": "TEXT NOT NULL DEFAULT '{}'",
    }
    for name, column_type in reviewer_columns_to_add.items():
        if name not in reviewer_columns:
            conn.execute(f"ALTER TABLE reviewer_audits ADD COLUMN {name} {column_type}")
    acquisition_attempt_columns = {
        row["name"]
        for row in conn.execute("PRAGMA table_info(transcript_acquisition_attempts)").fetchall()
    }
    for name, column_type in {
        "idempotency_key": "TEXT",
        "next_eligible_at": "TEXT",
    }.items():
        if name not in acquisition_attempt_columns:
            conn.execute(
                f"ALTER TABLE transcript_acquisition_attempts ADD COLUMN {name} {column_type}"
            )
    acquisition_status_columns = {
        row["name"]
        for row in conn.execute("PRAGMA table_info(transcript_acquisition_status)").fetchall()
    }
    if "next_eligible_at" not in acquisition_status_columns:
        conn.execute("ALTER TABLE transcript_acquisition_status ADD COLUMN next_eligible_at TEXT")
    _backfill_content_abstractions(conn)
    conn.execute(
        """
        UPDATE episodes
        SET feed_transcript_url = COALESCE(feed_transcript_url, transcript_url),
            feed_transcript_type = COALESCE(feed_transcript_type, transcript_type)
        WHERE transcript_url IS NOT NULL
        """
    )


def _ensure_schema_migration_history(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
          version INTEGER PRIMARY KEY CHECK(version > 0),
          name TEXT NOT NULL UNIQUE,
          checksum_sha256 TEXT NOT NULL CHECK(length(checksum_sha256) = 64),
          applied_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migration_baseline_adoptions (
          migration_version INTEGER PRIMARY KEY CHECK(migration_version > 0),
          migration_name TEXT NOT NULL,
          superseded_checksum_sha256 TEXT NOT NULL
            CHECK(length(superseded_checksum_sha256) = 64),
          adopted_checksum_sha256 TEXT NOT NULL
            CHECK(length(adopted_checksum_sha256) = 64),
          adopted_at TEXT NOT NULL,
          declared_object_count INTEGER NOT NULL CHECK(declared_object_count > 0),
          present_object_count INTEGER NOT NULL
            CHECK(present_object_count = declared_object_count),
          declared_objects_json TEXT NOT NULL,
          divergent_views_json TEXT NOT NULL,
          justification TEXT NOT NULL CHECK(length(trim(justification)) > 0),
          UNIQUE (
            migration_version,
            superseded_checksum_sha256,
            adopted_checksum_sha256
          )
        )
        """
    )
    conn.execute(
        """
        CREATE TRIGGER IF NOT EXISTS schema_migrations_no_update
        BEFORE UPDATE ON schema_migrations BEGIN
          SELECT RAISE(ABORT, 'schema migration history is immutable');
        END
        """
    )
    conn.execute(
        """
        CREATE TRIGGER IF NOT EXISTS schema_migrations_no_delete
        BEFORE DELETE ON schema_migrations BEGIN
          SELECT RAISE(ABORT, 'schema migration history is immutable');
        END
        """
    )
    conn.execute(
        """
        CREATE TRIGGER IF NOT EXISTS schema_migration_baseline_adoptions_no_update
        BEFORE UPDATE ON schema_migration_baseline_adoptions BEGIN
          SELECT RAISE(ABORT, 'schema migration baseline adoption history is immutable');
        END
        """
    )
    conn.execute(
        """
        CREATE TRIGGER IF NOT EXISTS schema_migration_baseline_adoptions_no_delete
        BEFORE DELETE ON schema_migration_baseline_adoptions BEGIN
          SELECT RAISE(ABORT, 'schema migration baseline adoption history is immutable');
        END
        """
    )


def schema_migration_checksum(name: str, statements: Iterable[str]) -> str:
    return sha256_text(name + "\n" + "\n".join(statement.strip() for statement in statements))


def record_schema_migration_baseline_adoption(
    conn: sqlite3.Connection,
    *,
    migration_version: int,
    migration_name: str,
    superseded_checksum_sha256: str,
    adopted_checksum_sha256: str,
    declared_objects: Iterable[str],
    divergent_views: Iterable[str],
    justification: str,
) -> None:
    """Record one exact, immutable migration-checksum transition."""

    _ensure_schema_migration_history(conn)
    declared = tuple(sorted(set(str(value) for value in declared_objects)))
    divergent = tuple(sorted(set(str(value) for value in divergent_views)))
    if not declared:
        raise ValueError("baseline adoption requires declared schema objects")
    if not justification.strip():
        raise ValueError("baseline adoption requires a justification")
    if len(superseded_checksum_sha256) != 64 or len(adopted_checksum_sha256) != 64:
        raise ValueError("baseline adoption checksums must be sha256 hex digests")

    applied = conn.execute(
        """
        SELECT name, checksum_sha256
        FROM schema_migrations
        WHERE version = ?
        """,
        (int(migration_version),),
    ).fetchone()
    if (
        applied is None
        or applied["name"] != migration_name
        or applied["checksum_sha256"] != superseded_checksum_sha256
    ):
        raise RuntimeError(
            f"schema migration {migration_version} baseline does not match applied history"
        )

    expected = {
        "migration_version": int(migration_version),
        "migration_name": migration_name,
        "superseded_checksum_sha256": superseded_checksum_sha256,
        "adopted_checksum_sha256": adopted_checksum_sha256,
        "declared_object_count": len(declared),
        "present_object_count": len(declared),
        "declared_objects_json": dumps_json(list(declared)),
        "divergent_views_json": dumps_json(list(divergent)),
        "justification": justification.strip(),
    }
    existing = conn.execute(
        """
        SELECT *
        FROM schema_migration_baseline_adoptions
        WHERE migration_version = ?
        """,
        (int(migration_version),),
    ).fetchone()
    if existing is not None:
        if any(existing[key] != value for key, value in expected.items()):
            raise RuntimeError(
                f"schema migration {migration_version} already has a different baseline adoption"
            )
        return

    conn.execute(
        """
        INSERT INTO schema_migration_baseline_adoptions (
          migration_version,
          migration_name,
          superseded_checksum_sha256,
          adopted_checksum_sha256,
          adopted_at,
          declared_object_count,
          present_object_count,
          declared_objects_json,
          divergent_views_json,
          justification
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            expected["migration_version"],
            expected["migration_name"],
            expected["superseded_checksum_sha256"],
            expected["adopted_checksum_sha256"],
            now_iso(),
            expected["declared_object_count"],
            expected["present_object_count"],
            expected["declared_objects_json"],
            expected["divergent_views_json"],
            expected["justification"],
        ),
    )


def _declared_schema_objects(statements: Iterable[str]) -> dict[str, tuple[str, str]]:
    objects: dict[str, tuple[str, str]] = {}
    create_pattern = re.compile(
        r"^\s*CREATE\s+(?:UNIQUE\s+)?"
        r"(TABLE|INDEX|VIEW|TRIGGER)\s+(?:IF\s+NOT\s+EXISTS\s+)?"
        r"([A-Za-z_][A-Za-z0-9_]*)",
        re.IGNORECASE,
    )
    for statement in statements:
        match = create_pattern.search(statement)
        if match is None:
            continue
        kind, name = match.groups()
        objects[name] = (kind.lower(), statement.strip())
    return objects


def _normalized_schema_ddl(statement: str) -> str:
    normalized = re.sub(
        r"\bIF\s+NOT\s+EXISTS\b",
        "",
        statement.strip().rstrip(";"),
        flags=re.IGNORECASE,
    )
    normalized = re.sub(r"\s+", " ", normalized).strip().lower()
    return re.sub(r"\s*([(),=])\s*", r"\1", normalized)


def adopt_versioned_intelligence_v1_baseline(conn: sqlite3.Connection) -> None:
    """Adopt the one verified production V1 baseline after exact schema checks."""

    _ensure_schema_migration_history(conn)
    version, name, statements = SCHEMA_MIGRATIONS[0]
    if version != 1 or name != "versioned_intelligence_v1":
        raise RuntimeError("versioned intelligence V1 is not migration 1")
    adopted_checksum = schema_migration_checksum(name, statements)
    declared = _declared_schema_objects(statements)
    live_rows = {
        str(row["name"]): (str(row["type"]), str(row["sql"] or ""))
        for row in conn.execute(
            """
            SELECT type, name, sql
            FROM sqlite_master
            WHERE type IN ('table', 'index', 'view', 'trigger')
            """
        ).fetchall()
    }
    missing = sorted(set(declared) - set(live_rows))
    if missing:
        raise RuntimeError(
            "versioned intelligence V1 baseline is missing declared objects: "
            + ", ".join(missing)
        )
    divergent: list[str] = []
    for object_name, (kind, statement) in declared.items():
        live_kind, live_statement = live_rows[object_name]
        if live_kind != kind:
            raise RuntimeError(
                f"versioned intelligence V1 object {object_name} has unexpected type"
            )
        if _normalized_schema_ddl(statement) != _normalized_schema_ddl(live_statement):
            divergent.append(object_name)
    divergent.sort()
    if len(declared) != 90:
        raise RuntimeError(
            f"versioned intelligence V1 baseline expected 90 objects, found {len(declared)}"
        )
    if tuple(divergent) != VERSIONED_INTELLIGENCE_V1_DIVERGENT_VIEWS:
        raise RuntimeError(
            "versioned intelligence V1 divergence set does not match audited evidence"
        )
    record_schema_migration_baseline_adoption(
        conn,
        migration_version=version,
        migration_name=name,
        superseded_checksum_sha256=VERSIONED_INTELLIGENCE_V1_SUPERSEDED_CHECKSUM,
        adopted_checksum_sha256=adopted_checksum,
        declared_objects=declared,
        divergent_views=divergent,
        justification=VERSIONED_INTELLIGENCE_V1_ADOPTION_JUSTIFICATION,
    )


def apply_schema_migrations(conn: sqlite3.Connection) -> list[int]:
    """Apply known migrations once, in increasing order, without rewriting history."""

    _ensure_schema_migration_history(conn)

    ordered = sorted(SCHEMA_MIGRATIONS, key=lambda item: item[0])
    versions = [version for version, _name, _statements in ordered]
    if versions != sorted(set(versions)) or any(version <= 0 for version in versions):
        raise RuntimeError("schema migrations must have unique positive versions")

    applied_rows = {
        int(row["version"]): row
        for row in conn.execute(
            "SELECT version, name, checksum_sha256 FROM schema_migrations"
        ).fetchall()
    }
    applied_now: list[int] = []
    for version, name, statements in ordered:
        checksum = schema_migration_checksum(name, statements)
        existing = applied_rows.get(version)
        if existing is not None:
            if existing["name"] != name:
                raise RuntimeError(
                    f"schema migration {version} differs from immutable applied history"
                )
            if existing["checksum_sha256"] != checksum:
                adoption = conn.execute(
                    """
                    SELECT 1
                    FROM schema_migration_baseline_adoptions
                    WHERE migration_version = ?
                      AND migration_name = ?
                      AND superseded_checksum_sha256 = ?
                      AND adopted_checksum_sha256 = ?
                    """,
                    (version, name, existing["checksum_sha256"], checksum),
                ).fetchone()
                if adoption is None:
                    raise RuntimeError(
                        f"schema migration {version} differs from immutable applied history"
                    )
            continue

        savepoint = f"schema_migration_{version}"
        conn.execute(f"SAVEPOINT {savepoint}")
        try:
            for statement in statements:
                conn.execute(statement)
            conn.execute(
                """
                INSERT INTO schema_migrations (version, name, checksum_sha256, applied_at)
                VALUES (?, ?, ?, ?)
                """,
                (version, name, checksum, now_iso()),
            )
            conn.execute(f"RELEASE SAVEPOINT {savepoint}")
        except Exception:
            conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
            conn.execute(f"RELEASE SAVEPOINT {savepoint}")
            raise
        applied_now.append(version)
    return applied_now


def schema_migration_version(conn: sqlite3.Connection) -> int:
    """Return the greatest applied forward migration, or zero before migration."""

    exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'schema_migrations'"
    ).fetchone()
    if exists is None:
        return 0
    row = conn.execute("SELECT COALESCE(MAX(version), 0) AS version FROM schema_migrations").fetchone()
    return int(row["version"] if isinstance(row, sqlite3.Row) else row[0])


def _backfill_content_abstractions(conn: sqlite3.Connection) -> None:
    ts = now_iso()
    conn.execute(
        """
        INSERT OR IGNORE INTO content_sources
          (id, legacy_source_id, source_type, name, homepage_url, feed_url, category, policy, enabled, metadata_json, created_at, updated_at)
        SELECT
          'cs_' || sources.id,
          sources.id,
          'podcast',
          sources.name,
          sources.homepage_url,
          sources.rss_url,
          sources.category,
          sources.policy,
          sources.enabled,
          sources.metadata_json,
          ?,
          ?
        FROM sources
        """,
        (ts, ts),
    )
    conn.execute(
        """
        INSERT OR IGNORE INTO content_items
          (id, content_source_id, legacy_episode_id, content_type, external_id, title, canonical_url, published_at,
           acquisition_status, privacy_tier, metadata_json, created_at, updated_at)
        SELECT
          'ci_' || episodes.id,
          'cs_' || episodes.source_id,
          episodes.id,
          'podcast_episode',
          episodes.guid,
          episodes.title,
          episodes.url,
          episodes.published_at,
          CASE
            WHEN EXISTS (SELECT 1 FROM transcripts WHERE transcripts.episode_id = episodes.id AND transcripts.status = 'ready') THEN 'ready'
            WHEN episodes.transcript_url IS NOT NULL THEN 'transcript_url_known'
            ELSE 'candidate'
          END,
          'local_only',
          '{}',
          ?,
          ?
        FROM episodes
        """,
        (ts, ts),
    )
    conn.execute(
        """
        INSERT OR IGNORE INTO content_artifacts
          (id, content_item_id, artifact_type, source_kind, source_url, local_path, sha256, word_count, status,
           privacy_tier, policy_json, metadata_json, created_at, updated_at)
        SELECT
          'ca_' || transcripts.id,
          'ci_' || transcripts.episode_id,
          'transcript',
          transcripts.source_kind,
          transcripts.source_url,
          transcripts.raw_text_path,
          transcripts.raw_text_sha256,
          transcripts.word_count,
          transcripts.status,
          'local_only',
          transcripts.policy_json,
          '{}',
          ?,
          ?
        FROM transcripts
        """,
        (ts, ts),
    )
    conn.execute(
        """
        INSERT OR IGNORE INTO content_spans
          (id, content_item_id, content_artifact_id, legacy_segment_id, span_index, start_char, end_char,
           text_path, text_sha256, word_count, speaker, span_kind, metadata_json, created_at)
        SELECT
          'cspan_' || segments.id,
          content_items.id,
          content_artifacts.id,
          segments.id,
          segments.segment_index,
          segments.start_char,
          segments.end_char,
          segments.text_path,
          segments.text_sha256,
          segments.word_count,
          NULL,
          'transcript_segment',
          '{}',
          ?
        FROM segments
        JOIN transcripts ON transcripts.id = segments.transcript_id
        JOIN content_items ON content_items.id = 'ci_' || segments.episode_id
        JOIN content_artifacts
          ON content_artifacts.content_item_id = content_items.id
         AND content_artifacts.artifact_type = 'transcript'
         AND content_artifacts.source_kind = transcripts.source_kind
         AND (
              content_artifacts.source_url = transcripts.source_url
              OR (content_artifacts.source_url IS NULL AND transcripts.source_url IS NULL)
         )
        """,
        (ts,),
    )
    conn.execute(
        """
        UPDATE episodes
        SET verified_transcript_url = (
              SELECT transcripts.source_url
              FROM transcripts
              WHERE transcripts.episode_id = episodes.id
                AND transcripts.source_kind IN ('official_show_transcript', 'youtube_captions')
              ORDER BY transcripts.updated_at DESC
              LIMIT 1
            ),
            verified_transcript_type = COALESCE(
              verified_transcript_type,
              (
                SELECT transcripts.content_type
                FROM transcripts
                WHERE transcripts.episode_id = episodes.id
                  AND transcripts.source_kind IN ('official_show_transcript', 'youtube_captions')
                ORDER BY transcripts.updated_at DESC
                LIMIT 1
              ),
              transcript_type
            ),
            verified_transcript_source_kind = COALESCE(
              verified_transcript_source_kind,
              (
                SELECT transcripts.source_kind
                FROM transcripts
                WHERE transcripts.episode_id = episodes.id
                  AND transcripts.source_kind IN ('official_show_transcript', 'youtube_captions')
                ORDER BY transcripts.updated_at DESC
                LIMIT 1
              )
            )
        WHERE EXISTS (
          SELECT 1
          FROM transcripts
          WHERE transcripts.episode_id = episodes.id
            AND transcripts.source_kind IN ('official_show_transcript', 'youtube_captions')
        )
        """
    )
    conn.execute(
        """
        UPDATE episodes
        SET transcript_url = COALESCE(verified_transcript_url, feed_transcript_url, transcript_url),
            transcript_type = COALESCE(verified_transcript_type, feed_transcript_type, transcript_type)
        """
    )


def execute(conn: sqlite3.Connection, sql: str, params: Iterable[Any] = ()) -> sqlite3.Cursor:
    return conn.execute(sql, tuple(params))


def enqueue_job(
    conn: sqlite3.Connection,
    *,
    lane: str,
    job_type: str,
    target_id: str,
    payload: dict[str, Any] | None = None,
    priority: int = 100,
    max_attempts: int = 2,
) -> int | None:
    ts = now_iso()
    payload_json = dumps_json(payload or {})
    dedupe_key = f"{lane}:{job_type}:{target_id}:{payload_json}"
    cur = conn.execute(
        """
        INSERT OR IGNORE INTO jobs
          (lane, job_type, target_id, payload_json, status, priority, attempts, max_attempts, dedupe_key, created_at, updated_at)
        VALUES (?, ?, ?, ?, 'pending', ?, 0, ?, ?, ?, ?)
        """,
        (lane, job_type, target_id, payload_json, priority, max_attempts, dedupe_key, ts, ts),
    )
    if cur.rowcount == 0:
        row = conn.execute("SELECT id FROM jobs WHERE dedupe_key = ?", (dedupe_key,)).fetchone()
        return int(row["id"]) if row else None
    return int(cur.lastrowid)


def counts(conn: sqlite3.Connection) -> dict[str, int]:
    tables = [
        "sources",
        "episodes",
        "transcripts",
        "segments",
        "labels",
        "claims",
        "episode_context_runs",
        "transcript_preparations",
        "transcript_acquisition_attempts",
        "transcript_acquisition_status",
        "transcription_runs",
        "reviewer_audits",
        "content_sources",
        "content_items",
        "content_artifacts",
        "content_spans",
        "context_packages",
        "extraction_runs",
        "queue_envelopes",
        "worker_runs",
        "job_claims",
        "output_submissions",
        "queue_events",
        "coded_observations",
        "topic_mentions",
        "term_mentions",
        "entity_mentions",
        "speaker_positions",
        "relationship_edges",
        "product_signals",
        "release_signal_links",
        "concepts",
        "concept_aliases",
        "concept_versions",
        "discourse_events",
        "discourse_event_contexts",
        "canonical_people",
        "canonical_orgs",
        "canonical_products",
        "canonical_models",
        "raw_speaker_mentions",
        "raw_actor_mentions",
        "identity_aliases",
        "identity_resolution_candidates",
        "identity_merge_decisions",
        "identity_split_decisions",
        "speaker_identity_runs",
        "person_org_affiliations",
        "podcast_guest_edges",
        "claim_clusters",
        "claim_canonicalization_runs",
        "claim_cluster_members",
        "agreement_edges",
        "disagreement_edges",
        "person_concept_edges",
        "person_person_mentions",
        "person_org_edges",
        "expert_authority_scores",
        "podcast_reach_scores",
        "forecast_outcome_checks",
        "graph_score_runs",
        "term_usages",
        "frame_usages",
        "actor_positions",
        "concept_candidates",
        "shift_signals",
        "signal_runs",
        "quality_audits",
        "jobs",
    ]
    return {table: int(conn.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()["c"]) for table in tables}
