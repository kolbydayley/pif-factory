from __future__ import annotations

import fcntl
import os
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable

from .paths import db_path
from .util import dumps_json, now_iso


INIT_LOCK_TIMEOUT_SECONDS = 30.0


def connect(path: Path | None = None) -> sqlite3.Connection:
    conn = sqlite3.connect(path or db_path())
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
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
CREATE INDEX IF NOT EXISTS idx_labels_pack ON labels(label_pack, label_pack_version);
CREATE INDEX IF NOT EXISTS idx_episode_context_runs_episode_pack ON episode_context_runs(episode_id, label_pack, model, status);
CREATE INDEX IF NOT EXISTS idx_observations_segment ON coded_observations(segment_id, code_family, code_id);
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
"""


def init_db(conn: sqlite3.Connection | None = None) -> None:
    own_conn = conn is None
    conn = conn or connect()
    try:
        with init_db_lock(conn):
            conn.executescript(SCHEMA)
            migrate_schema(conn)
            conn.commit()
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
    _backfill_content_abstractions(conn)
    conn.execute(
        """
        UPDATE episodes
        SET feed_transcript_url = COALESCE(feed_transcript_url, transcript_url),
            feed_transcript_type = COALESCE(feed_transcript_type, transcript_type)
        WHERE transcript_url IS NOT NULL
        """
    )


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
          'ci_' || segments.episode_id,
          'ca_' || segments.transcript_id,
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
