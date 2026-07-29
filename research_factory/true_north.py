"""Isolated downstream benchmark for the frozen AI-safety podcast cohort.

The benchmark begins at already-extracted discourse-event candidates.  It can
read the authoritative factory database, but every benchmark mutation is
confined to a suite-specific shadow database and private local artifacts.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import difflib
import hashlib
import html
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from . import db, intelligence, semantic_reconcile
from .labels import ValidationError, _validate_schema
from .paths import db_path
from .util import dumps_json, now_iso, sha256_text, stable_id


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PRIVATE_ROOT = (
    Path.home()
    / "Library"
    / "Application Support"
    / "Podcast Intelligence Factory"
    / "true-north"
)
SUITE_ID = "ai-safety-v1"
SUITE_SCHEMA_VERSION = "pif_true_north_suite_v1"
BUNDLE_SCHEMA_VERSION = "pif_true_north_candidate_bundle_v1"
WORK_OUTPUT_SCHEMA_VERSION = "pif_true_north_atomic_output_v2"
ATOMIC_AUDIT_CHUNK_SIZE = 5
ATOMIC_AUDIT_WORKERS = 4
SEMANTIC_WORKERS = 4
RUN_SCHEMA_VERSION = "pif_true_north_run_v1"
LEDGER_SCHEMA_VERSION = "pif_true_north_falloff_ledger_v1"
REPORT_SCHEMA_VERSION = "pif_true_north_report_v1"
GOLD_SCHEMA_VERSION = "pif_true_north_gold_v1"
CONSENSUS_GOLD_SCHEMA_VERSION = "pif_true_north_consensus_gold_v1"
CONSENSUS_GOLD_POLICY_VERSION = "pif_true_north_consensus_policy_v1"
WORKHORSE_PROMPT_VERSION = "pif_true_north_atomic_prompt_v2"
GOLD_PROMPT_VERSION = "pif_true_north_gold_prompt_v1"
PROMPT_OPTIMIZATION_SCHEMA_VERSION = "pif_true_north_system_prompt_optimization_v1"
PROMPT_OPTIMIZATION_MODEL = "zai-coding-plan/glm-5.2"
MULTIPASS_SCHEMA_VERSION = "pif_true_north_multipass_v1"
MULTIPASS_MODEL = "zai-coding-plan/glm-5.2"
MULTIPASS_STAGES = ("disposition", "decomposition", "attribution")
# Stage B has two contracts.  "decomposition" is the committed default, and its
# frozen run configuration stays byte-identical so in-flight runs still resume.
# "adjudication" is the opt-in split/no-split minimal-edit contract: it anchors
# on the frozen upstream claim_text proposal rather than re-deriving
# propositions from clipped evidence.
DEFAULT_MULTIPASS_STAGE_B_MODE = "decomposition"
MULTIPASS_STAGE_B_MODES = ("decomposition", "adjudication")
MULTIPASS_ADJUDICATION_STAGES = ("disposition", "adjudication", "attribution")
MULTIPASS_ALL_STAGES = (
    "disposition",
    "decomposition",
    "adjudication",
    "attribution",
)
MULTIPASS_EDIT_REASONS = (
    "none",
    "pronoun_expansion",
    "attribution_embed",
    "compound_split",
    "qualifier_repair",
)
MULTIPASS_JUNK_REASONS = (
    "metadata",
    "introduction_or_bio",
    "question_or_setup",
    "banter",
    "bare_mention",
    "fragment",
    "repetition",
    "unsupported_inference",
)
PHASE_C_JUNK_VERIFY_JACCARD = 0.6
PHASE_C_JUNK_VERIFY_FRAGMENT_MAX_CHARS = 120
PHASE_C_JUNK_VERIFY_MAX_GLM_CALLS = 8
PHASE_C_JUNK_VERIFY_MAX_SPARK_CALLS = 3
PHASE_C_JUNK_VERIFY_MAX_TOKENS = 120_000
PHASE_C_JUNK_VERIFY_BATCH_SIZE = 25
PHASE_C_JUNK_VERIFY_SYSTEM_PROMPT = """You are a junk auditor for a private podcast research corpus. Do not use
tools. Every candidate you receive has been provisionally retained, and most
are genuinely valuable; your task is to catch the rare junk that slipped
through. Reject a candidate only when you can quote a concrete deficiency
from its exact evidence: the evidence merely repeats an assertion already
made elsewhere without adding new content; the evidence is a truncated
fragment whose assertion cannot be completed from the text present; the
evidence only names a person, product, or document without asserting anything
about it; the evidence is an unanswered question or setup with no recoverable
assertion; or the evidence is page chrome, navigation, or metadata. If the
evidence contains any complete, substantive asserted proposition a researcher
could verify, compare, contradict, or qualify, confirm the retention even
when the candidate is also partly repetitive or fragmentary. For each reject,
name the junk class and copy the deficient text verbatim as the deficiency
quote. Return only the exact schema-valid JSON requested by the packet."""
PHASE_C_MARGINAL_JACCARD = 0.45
PHASE_C_MARGINAL_SPARK_CONFIRM_JACCARD = 0.60
PHASE_C_MARGINAL_LOCAL_DISTANCE = 600
PHASE_C_MARGINAL_LOCAL_MIN_STEMS = 2
PHASE_C_MARGINAL_SEGMENT_RADIUS = 400
PHASE_C_MARGINAL_MAX_GLM_CALLS = 6
PHASE_C_MARGINAL_MAX_SPARK_CALLS = 4
PHASE_C_MARGINAL_MAX_TOKENS = 150_000
PHASE_C_MARGINAL_BATCH_SIZE = 37
PHASE_C_MARGINAL_PREFLIGHT_CHARS_PER_TOKEN = 3
PHASE_C_MARGINAL_PREFLIGHT_NONINPUT_TOKENS_PER_CALL = 10_000
PHASE_C_MARGINAL_SYSTEM_PROMPT = """You are a marginal-utility auditor for a private podcast research corpus. Do
not use tools. Each packet shows one provisionally retained candidate, the
surrounding transcript text of its segment, and the most similar already
retained candidates. Most inputs are valuable; your task is to catch the rare
candidate that adds nothing to the corpus. Reject a candidate only in these
cases. Repetition: every proposition in its evidence is already carried by
one of the shown retained neighbors, in the same or different words, and the
candidate adds no new subject, predicate, outcome, qualifier, or attribution;
name which neighbor carries it. Non-assertion: read the surrounding segment
text and confirm the evidence is a question, setup, or fragment whose
recoverable content is either absent or already carried by a shown neighbor.
Bare reference: the evidence only points to a document, product, page
element, or name without asserting any verifiable proposition about it. If
the candidate contributes any independently citable proposition that no shown
neighbor carries, confirm the retention, even when partly repetitive,
interrogative, or fragmentary. Quote the deficiency verbatim for every
non-repetition reject. Return only the exact schema-valid JSON requested by
the packet."""
MULTIPASS_ENUMS = {
    "certainty": ("low", "medium", "high", "hedged"),
    "stance": (
        "supportive",
        "skeptical",
        "neutral",
        "mixed",
        "warning",
        "competitive",
        "promotional",
        "uncertain",
        "not_applicable",
    ),
    "polarity": ("positive", "negative", "neutral"),
    "time_horizon": (
        "past",
        "present",
        "near_future",
        "long_future",
        "timeless",
        "unspecified",
    ),
}
MULTIPASS_DEFAULT_BUDGET = {
    "max_calls": 120,
    "max_tokens": 4_000_000,
    "max_wall_seconds": 6 * 60 * 60,
}
APPROVED_GATE_POLICY_VERSION = "pif_true_north_gate_policy_v2"
APPROVED_GATE_POLICY = {
    "consensus_candidate_state_macro_f1": (">=", 0.90),
    "retained_value_recall": (">=", 0.90),
    "consensus_junk_escape_rate": ("<=", 0.02),
    "acceptable_atomic_count_rate": (">=", 0.90),
    "claim_text_faithfulness_proxy": (">=", 0.75),
    "speaker_exactness": (">=", 0.97),
    "reported_actor_exactness": (">=", 0.903182),
    "hallucination_rate_proxy": ("<=", 0.02),
    "schema_parse_success_rate": (">=", 0.99),
}
MULTIPASS_OPEN_DEVELOPMENT_EPISODES = (
    "ep_90c3b5c995bce501c9aef55c",
    "ep_7ec9f808a3955c720aeb94ff",
    "ep_903cc763f0f106d7f4610f17",
)
DEFAULT_ROUTER = "spark-first"
ROUTERS = {
    "spark-first": (
        "openai/gpt-5.3-codex-spark",
        "opencode-go/glm-5.2",
        "zai-coding-plan/glm-5.2",
    ),
}
OPERATIONAL_FAILURE_PATTERNS = (
    "rate limit",
    "rate_limit",
    "quota",
    "capacity",
    "overloaded",
    "temporarily unavailable",
    "provider unavailable",
    "service unavailable",
    "timeout",
    "timed out",
    "too many requests",
    "429",
    "502",
    "503",
    "504",
)

BASE_SYSTEM_PROMPT = (
    "You are a careful semantic adjudicator for a private local podcast corpus. "
    "Do not use tools. Read the attached packet completely and return only its "
    "strict JSON result. Preserve exact evidence and distinguish direct speakers "
    "from quoted or reported actors."
)

MULTIPASS_SYSTEM_PROMPTS = {
    "disposition": """You are a strict junk filter for a private podcast research corpus. Do not use
tools. Your only task is disposition. For each candidate, read its exact
evidence and decide whether it contains a substantive asserted proposition a
researcher could verify, compare, contradict, or qualify. Reject metadata,
introductions and biographies, questions and setup, banter, bare mentions of a
name or product, fragments, repetition, and inferences the evidence does not
literally state, and name which of those junk classes applies. Do not reject a
substantive assertion merely because it is uncertain, conditional,
hypothetical, summarized, or phrased as a headline. Use hold only when the
evidence supports multiple incompatible readings whose resolution would change
the proposition or its speaker; never for low confidence or low value. Do not
decompose, rewrite, or attribute claims — a later stage does that. A question, fragment, or
repeated construction is junk only when the evidence provides no recoverable
asserted proposition; a rhetorical or self-answered question, a fragment whose
assertion is completed within its own evidence, a repeated but substantive
assertion, and an explicit forecast or skeptical stance are all value.
Conversely, when the exact evidence is only setup, a bare mention, metadata,
banter, or an unsupported fragment, reject it even if the candidate claim_text
rewrites it to sound factual; candidate wording cannot create evidence.
Treat an explicit interrogative hypothesis, risk, analogy, or uncertainty as a
substantive stance, but treat a term gloss, definition, or existence mention
without a consequence, evaluation, forecast, or contested position as junk
even when it is grammatically factual. Return
only the exact schema-valid JSON requested by the packet.""",
    "decomposition": """You are an atomic-claim decomposer for a private podcast research corpus. Do
not use tools. Every candidate you receive has already been accepted as
containing substantive research content; do not re-judge acceptance and do not
attribute speakers — other stages own those. For each candidate, first build a
proposition inventory: one row per independently checkable predicate in the
exact candidate evidence, with subject, predicate, and object or outcome.
Surrounding segment text may resolve a pronoun, ellipsis, or immediate contrast,
but a proposition stated only outside the candidate's exact evidence is not a
second inventory row. Two clauses
are separate rows when either could be true while the other is false,
including separate list items, forecasts, and stances. A cause and its effect
stay one row when they express one causal relationship; split them only when
each is also asserted as a standalone conclusion. A correction shaped like
"not X; instead Y" is one counterclaim when X only establishes the contrast
for Y, not two propositions. They
are one row when one clause only supplies the mechanism, reason, example,
condition, qualification, or scope of the same conclusion. Then emit one
atomic claim per inventory row, referencing the row's index. Each claim must
be the smallest independently citable assertion, must preserve time, scope,
condition, polarity, and certainty qualifiers from the evidence, and must be
meaningful when read alone. Phrase the claim as a minimal grammatical edit of
the evidence; do not add a rationale, timing detail, actor, or mechanism merely
because it appears elsewhere in the segment. Do not merge rows with different truth conditions
and do not split one conclusion into fragments. Return only the exact
schema-valid JSON requested by the packet.""",
    # Approved verbatim in docs/plans/2026-07-28-true-north-gate-closure-v2.md
    # (Task 5).  Do not reflow or reword: the run configuration pins its
    # SHA-256.
    "adjudication": """You are a claim adjudication editor for a private podcast research corpus. Do
not use tools. Each candidate arrives with a proposed claim_text hypothesis
and its exact evidence. Your default action is to adopt the proposed
claim_text verbatim as one atomic claim; most candidates need exactly this.
Depart from the proposal only for a concrete, evidence-grounded reason. Split
into multiple claims only when the proposal asserts two or more conclusions
that could independently be true or false — separate list items, separate
forecasts, a separately asserted cause and effect, or distinct stances; when
you split, reuse the proposal's own wording for each part, changing only what
grammar requires. Edit wording only to expand an unclear pronoun to its
explicit referent from the evidence, to repair a qualifier the proposal
dropped or added relative to the evidence, or to remove content the evidence
does not state. Never introduce vocabulary absent from both the proposal and
the evidence, and never compress or restyle wording that is already accurate.
Report the edit reason for every claim. Return only the exact schema-valid
JSON requested by the packet.""",
    "attribution": """You are a speaker-attribution resolver for a private podcast research corpus.
Do not use tools. For each atomic claim, decide who directly asserted it in
the exact evidence, selecting the speaker strictly from the numbered roster
provided in the packet. The direct speaker is the person whose turn contains
the assertion — never a person merely mentioned. If the direct speaker is
quoting, paraphrasing, or reporting someone else's statement, position, or
finding, set attribution_mode to reported and identify that source as the
reported actor: use the roster ID when the actor is a roster participant,
otherwise give the actor's name exactly as the evidence states it. A claim the
speaker asserts in their own voice is direct. The reported_actor field records
the focal non-speaker person or organization whose action, state, finding,
position, or outcome the proposition describes; do not treat a merely
incidental mention as an actor. It does not mean the citation source. When
evidence says "according to X, Y is Z," choose Y, not X. Thus "according to
Ramp data, Anthropic is most popular" has Anthropic as actor, not Ramp. For a
non-roster actor, copy the shortest exact name from the evidence verbatim;
never add "U.S.", a title, or another qualifier that is absent. Return only the
exact schema-valid JSON requested by the packet. You must explicitly set
actor_presence for every claim before filling either actor field.""",
}

SYSTEM_PROMPT_ARMS: dict[str, str] = {
    "baseline": BASE_SYSTEM_PROMPT,
    "evidence-gate": (
        "You are a precision-first proposition extractor for a private podcast "
        "research corpus. Do not use tools. Treat every supplied candidate and "
        "prior label as an untrusted hypothesis. For each item, read only the exact "
        "evidence first and ask whether it contains a useful asserted proposition "
        "that a researcher could verify, compare, contradict, or qualify. Reject "
        "metadata, identity or biography, introductions, headlines or setup that "
        "do not themselves supply research evidence, questions, bare mentions, "
        "banter, fragments, and unsupported inference. For useful evidence, count "
        "independently true-or-false predicates before writing: split whenever one "
        "clause could be false while another remains true, while preserving scope, "
        "conditions, time, polarity, certainty, and attribution. The direct speaker "
        "is never replaced by a mentioned or reported actor. Return only the exact "
        "strict JSON requested by the attached packet."
    ),
    "counterfactual-boundary": (
        "You are a skeptical evidence-bound atomic-claim judge for a private local "
        "podcast corpus. Do not use tools and do not defer to candidate wording. "
        "Apply two tests independently to every candidate. Eligibility test: if the "
        "evidence is only metadata, an introduction, biography, question, setup, "
        "bare name or product, banter, fragment, repetition, or an inference not "
        "literally supported, it has no research proposition and must be rejected. "
        "Atomicity test: enumerate the minimum truth conditions required by the "
        "evidence; if any clause, list member, cause, consequence, rationale, "
        "forecast, example, contrast, or condition could vary in truth independently, "
        "write separate atomic claims. Never add a mechanism or actor absent from "
        "the evidence. Preserve qualifiers and distinguish the direct speaker from "
        "a quoted or reported actor. Return only schema-valid JSON."
    ),
    "decision-checklist": (
        "You are the final high-precision semantic editor for a private podcast "
        "claim corpus. Do not use tools. Silently follow this order for every item: "
        "(1) quote-ground the asserted content in the exact evidence; (2) exclude "
        "non-claims such as metadata, introductions, biographies, questions, setup, "
        "banter, bare mentions, fragments, and unsupported interpretations; (3) "
        "identify the direct speaker and any separately reported actor; (4) count "
        "each proposition that can independently be true or false; (5) emit one "
        "atomic claim per proposition, retaining all material time, scope, condition, "
        "polarity, stance, and certainty; (6) verify that no output claim combines "
        "a cause with its effect, a forecast with its rationale, an assertion with "
        "an example, or multiple list members. Candidate labels are suggestions, "
        "not authority. Return only the attached schema's strict JSON."
    ),
    "research-atomicity": (
        "You are a conservative research-claim editor for a private local podcast "
        "corpus. Do not use tools. Judge only the exact supplied evidence; candidate "
        "wording and labels are untrusted. First reject anything that is merely "
        "metadata, an introduction or biography, a question or setup, banter, a "
        "bare mention, a fragment, repetition, or unsupported inference. For useful "
        "evidence, an atomic claim is the smallest independently citable research "
        "assertion, not every grammatical clause. Default to one claim when causes, "
        "conditions, qualifications, examples, contrasts, or rationale are necessary "
        "to preserve the assertion's meaning. Split only when the evidence clearly "
        "asserts multiple standalone conclusions that a researcher could cite, "
        "compare, support, or contradict separately and each remains meaningful "
        "without the other. Do not split a single assertion into subject, mechanism, "
        "and consequence fragments, and do not merge genuinely independent list "
        "items or forecasts. Preserve time, scope, condition, polarity, certainty, "
        "and direct-speaker versus reported-actor attribution. Return only the exact "
        "schema-valid JSON requested by the packet."
    ),
    "balanced-boundaries": (
        "You are a high-precision evidence editor for a private local podcast "
        "research corpus. Do not use tools. Candidate wording and labels are "
        "untrusted; decide from the exact evidence. Eligibility: reject only when "
        "the evidence contains no substantive asserted proposition a researcher "
        "could verify, compare, contradict, or qualify. Metadata, introductions, "
        "biographies, questions or setup, banter, bare mentions, fragments, "
        "repetition, and unsupported inference are not propositions. But do not "
        "reject an explicit substantive assertion merely because it appears in a "
        "headline, summary, hypothetical discussion, or contains uncertainty. Use "
        "hold only for genuinely unresolved useful meaning or identity. Atomicity: "
        "make one claim per standalone conclusion. Split when clauses assert "
        "different subjects, predicates, outcomes, forecasts, stances, time "
        "horizons, or independently citable list items. Keep a mechanism, reason, "
        "example, condition, qualification, or contrast with its conclusion when "
        "removing it would materially change that conclusion rather than leave a "
        "second useful assertion. Do not turn one causal explanation into fragments, "
        "but do split a separately asserted cause from a separately asserted effect. "
        "After drafting, verify that each claim is meaningful alone and that no two "
        "outputs merely restate pieces of the same proposition. Preserve direct "
        "speaker versus reported actor, scope, time, condition, polarity, stance, "
        "and certainty. Return only the exact schema-valid JSON requested."
    ),
    "proposition-inventory": (
        "You are a precise evidence-bound proposition annotator for a private local "
        "podcast research corpus. Do not use tools, and treat candidate labels as "
        "untrusted. Silently process each candidate in two passes. Pass one, "
        "eligibility: locate an explicit substantive assertion in the exact evidence. "
        "Reject metadata, introductions, identities or biographies, questions and "
        "setup, banter, bare mentions, fragments, repetition, and interpretations "
        "not supported by the evidence. Retain or revise any explicit research-useful "
        "assertion even when uncertain, conditional, hypothetical, summarized, or in "
        "headline language. Hold is extremely rare: use it only when useful evidence "
        "itself supports multiple incompatible readings whose resolution changes the "
        "proposition or speaker. Never use hold merely for low confidence, weak value, "
        "or missing background; choose reject or retain/revise. Pass two, atomicity: "
        "silently inventory every standalone proposition as subject, predicate, "
        "object or outcome, polarity, modality, time, and condition. Two clauses are "
        "separate claims when either could be true or false without changing the "
        "other, including separate list items, forecasts, stances, causes, or effects. "
        "They are one claim when one clause only defines the mechanism, reason, "
        "example, condition, qualification, or scope of the same conclusion. Merge "
        "inventory rows only when their truth conditions are the same; do not default "
        "to one claim for a dense candidate. Then output the minimum complete set of "
        "independently citable claims, preserving all qualifiers and direct-speaker "
        "versus reported-actor attribution. Return only the exact schema-valid JSON."
    ),
}
LEDGER_CATEGORIES = (
    "retained_canonical_member",
    "retained_supported_singleton",
    "merged_duplicate_retained",
    "revised_to_valid",
    "held_needs_review",
    "rejected_junk",
    "blocked_by_upstream_stage",
    "mechanical_failure",
)
DISPOSITIONS = ("retain", "revise", "hold", "reject")
RELATIONS = (
    "equivalent",
    "supports",
    "contradicts",
    "qualifies",
    "orthogonal",
    "incomparable",
)
UTILITY_QUESTIONS: tuple[tuple[str, str], ...] = (
    ("consensus_01", "Which model-risk concerns recur across episodes, and which speakers advance them?"),
    ("consensus_02", "What recurring arguments are made for independent safety evaluation?"),
    ("consensus_03", "What recurring limits of red-teaming are identified?"),
    ("consensus_04", "What recurring controls are proposed for AI-agent access and permissions?"),
    ("consensus_05", "What recurring governance responsibilities are assigned to model developers?"),
    ("consensus_06", "What recurring governance responsibilities are assigned to governments or regulators?"),
    ("consensus_07", "What deployment risks recur across more than one episode?"),
    ("consensus_08", "What claims recur about open-source or open-weight model risk?"),
    ("consensus_09", "What recurring tradeoffs appear between capability progress and safety controls?"),
    ("consensus_10", "Which claims about Anthropic or frontier-model governance recur across episodes?"),
    ("speaker_01", "Who directly argues that AI systems or models may be dangerous, and what exactly do they claim?"),
    ("speaker_02", "Who directly discusses red-teaming, and what position does each speaker take?"),
    ("speaker_03", "Who directly discusses agent access control or zero trust, and what do they recommend?"),
    ("speaker_04", "Which claims are spoken by a host while reporting another person's or organization's position?"),
    ("speaker_05", "Which potentially useful claims remain unresolved because the direct speaker cannot be established?"),
    ("disagreement_01", "Where do speakers disagree about whether regulation improves or harms AI safety?"),
    ("disagreement_02", "Where does one claim qualify another claim about model danger or deployment risk?"),
    ("disagreement_03", "Which claims appear contradictory only because their time horizon differs?"),
    ("disagreement_04", "Which claims appear similar but differ in scope, conditions, or affected actors?"),
    ("disagreement_05", "What genuine contradictions remain after temporal and conditional qualifications are preserved?"),
    ("provenance_01", "What exact evidence supports the strongest cross-episode consensus claim?"),
    ("provenance_02", "What exact evidence supports the strongest identified contradiction?"),
    ("provenance_03", "What exact evidence supports a useful singleton that was not merged?"),
    ("provenance_04", "What exact evidence shows a direct speaker reporting a different actor's position?"),
    ("provenance_05", "What exact evidence supports a claim that was revised rather than retained verbatim?"),
)


@dataclass(frozen=True)
class EpisodeSpec:
    episode_id: str
    transcript_id: str
    transcript_sha256: str
    partition: str
    source_name: str
    title: str


EPISODES: tuple[EpisodeSpec, ...] = (
    EpisodeSpec(
        "ep_8a0c6919d7cfe6bcd9cacd30",
        "tr_4dba68a0d523e23fa01bcda5",
        "79a7769a6429bf55e6abda22a635f0ca88168b0ae08f0435d59b3be8c5ce27d4",
        "development",
        "Odd Lots",
        "Anthropic's Co-Founder and Top Economist on Doing Research at the AI Frontier",
    ),
    EpisodeSpec(
        "ep_7ec9f808a3955c720aeb94ff",
        "tr_386504e2bf50c8b71a1833dc",
        "24dcb17b352c8f9bd169a3fcaca2dd608b3e9f5211bac871149931ddc22b635a",
        "development",
        "Decoder",
        "Who decides when AI is too dangerous?",
    ),
    EpisodeSpec(
        "ep_90c3b5c995bce501c9aef55c",
        "tr_b3d7b8881a0a5217aa9da777",
        "6757f8a9a8094bc8e0df6d2ab3095d974b4795344da08803d506c85a5371e20c",
        "development",
        "Big Technology Podcast",
        "AI Fact or Fiction: The Fable Ban, Tokenmaxxing, Saaspocolypse — With Ara Kharazian",
    ),
    EpisodeSpec(
        "ep_903cc763f0f106d7f4610f17",
        "tr_cba6daade44c33d88f1fec2d",
        "2b7e271efd3b08d16782ee1c819ac6f51ed84c7cc6cfa79144e0de0b2cfee155",
        "development",
        "Practical AI",
        "Zero Trust for AI Agents",
    ),
    EpisodeSpec(
        "ep_e7630540911fc5dea9850204",
        "tr_9623b5fb938385bd1cdcc300",
        "3b4e6dedb0e8082ba361a3b152c50b07f63bd3e8e87b5d89f999ccca6291bcf9",
        "development",
        "Latent Space",
        "Red-Teaming after Mythos — Zico Kolter & Matt Fredrikson, Gray Swan",
    ),
    EpisodeSpec(
        "ep_044f1d2d020e021cfaf99e90",
        "tr_fc534bca3c17abe5210ca59a",
        "3c20665dc31f41d435f8e98dd87a2384859c9d444d1607989380a9011a368707",
        "holdout",
        "The Cognitive Revolution",
        "Scaling Intelligence Out: Cisco's Vision for the Internet of Cognition, with Vijoy Pandey",
    ),
    EpisodeSpec(
        "ep_97a45100ce0d305f58d7dd69",
        "tr_4eacfa1927b80098a0e3e8de",
        "e4c6d404cb5f10ad3870498a00cc5a747d3c8904ea4bc7ec1c33b77b883d5e79",
        "holdout",
        "Microsoft Research Podcast",
        "Reimagining healthcare delivery and public health with AI",
    ),
)


class TrueNorthError(RuntimeError):
    """A fail-closed true-north benchmark error."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _write_json(path: Path, value: Any, *, immutable: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
    if path.exists():
        current = path.read_text(encoding="utf-8")
        if current == rendered:
            return
        if immutable:
            raise TrueNorthError(f"immutable artifact differs: {path}")
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(rendered, encoding="utf-8")
    os.replace(temporary, path)


def _write_text(path: Path, value: str, *, immutable: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        current = path.read_text(encoding="utf-8")
        if current == value:
            return
        if immutable:
            raise TrueNorthError(f"immutable artifact differs: {path}")
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(value, encoding="utf-8")
    os.replace(temporary, path)


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise TrueNorthError(f"cannot read JSON artifact: {path}") from exc


def _suite_root(output_root: str | Path | None, suite: str) -> Path:
    if suite != SUITE_ID:
        raise TrueNorthError(f"unknown true-north suite: {suite}")
    base = Path(output_root).expanduser().resolve() if output_root else DEFAULT_PRIVATE_ROOT
    return base / suite


def _readonly_connect(path: Path) -> sqlite3.Connection:
    uri = f"file:{path.expanduser().resolve()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = ON")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _source_snapshot(conn: sqlite3.Connection, path: Path, *, include_file_hash: bool) -> dict[str, Any]:
    release_counts = {
        table: int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
        for table in (
            "corpus_releases",
            "corpus_release_promotions",
            "atomic_claims",
            "identity_resolution_judgments",
            "accepted_claim_subjects",
            "accepted_proposition_variants",
            "accepted_position_observations",
            "claim_relation_judgments",
        )
    }
    stat = path.stat()
    return {
        "path": str(path.resolve()),
        "size_bytes": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
        "sha256": _sha256_file(path) if include_file_hash else None,
        "canonical_counts": release_counts,
    }


def _table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    return [str(row["name"]) for row in conn.execute(f"PRAGMA table_info({table})")]


def _copy_rows(
    source: sqlite3.Connection,
    target: sqlite3.Connection,
    table: str,
    rows: Iterable[sqlite3.Row],
) -> int:
    values = list(rows)
    if not values:
        return 0
    target_columns = set(_table_columns(target, table))
    columns = [name for name in values[0].keys() if name in target_columns]
    placeholders = ",".join("?" for _ in columns)
    target.executemany(
        f"INSERT OR IGNORE INTO {table} ({','.join(columns)}) VALUES ({placeholders})",
        [tuple(row[name] for name in columns) for row in values],
    )
    return len(values)


def _placeholders(values: Sequence[Any]) -> str:
    if not values:
        raise TrueNorthError("empty SQL membership set")
    return ",".join("?" for _ in values)


def _resolve_text_path(raw_path: str) -> Path:
    path = Path(raw_path).expanduser()
    return path.resolve() if path.is_absolute() else (PROJECT_ROOT / path).resolve()


TRUE_NORTH_SCHEMA = """
CREATE TABLE IF NOT EXISTS true_north_suites (
  suite_id TEXT PRIMARY KEY,
  schema_version TEXT NOT NULL,
  manifest_sha256 TEXT NOT NULL,
  source_db_path TEXT NOT NULL,
  source_db_sha256 TEXT,
  release_id TEXT NOT NULL,
  development_episode_count INTEGER NOT NULL,
  holdout_episode_count INTEGER NOT NULL,
  candidate_count INTEGER NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS true_north_runs (
  run_id TEXT PRIMARY KEY,
  schema_version TEXT NOT NULL,
  suite_id TEXT NOT NULL,
  partition_name TEXT NOT NULL,
  router TEXT NOT NULL,
  configuration_sha256 TEXT NOT NULL,
  status TEXT NOT NULL,
  input_count INTEGER NOT NULL DEFAULT 0,
  output_count INTEGER NOT NULL DEFAULT 0,
  failure_count INTEGER NOT NULL DEFAULT 0,
  receipts_json TEXT NOT NULL DEFAULT '[]',
  started_at TEXT NOT NULL,
  completed_at TEXT,
  error TEXT
);
CREATE TABLE IF NOT EXISTS true_north_stage_ledger (
  run_id TEXT NOT NULL,
  candidate_id TEXT NOT NULL,
  episode_id TEXT NOT NULL,
  stage TEXT NOT NULL,
  category TEXT NOT NULL,
  reason_code TEXT NOT NULL,
  producer_model TEXT,
  atomic_claim_ids_json TEXT NOT NULL DEFAULT '[]',
  details_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL,
  PRIMARY KEY (run_id, candidate_id, stage)
);
CREATE TABLE IF NOT EXISTS true_north_call_receipts (
  id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  stage TEXT NOT NULL,
  packet_sha256 TEXT NOT NULL,
  provider_model TEXT NOT NULL,
  attempt INTEGER NOT NULL,
  operational_failure INTEGER NOT NULL,
  timed_out INTEGER NOT NULL,
  exit_code INTEGER,
  elapsed_seconds REAL NOT NULL,
  usage_json TEXT NOT NULL DEFAULT '{}',
  fallback_reason TEXT,
  output_sha256 TEXT,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS true_north_subject_canonical_map (
  run_id TEXT NOT NULL,
  subject_id TEXT NOT NULL,
  canonical_subject_key TEXT NOT NULL,
  producer_model TEXT NOT NULL,
  packet_sha256 TEXT NOT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY (run_id, subject_id)
);
CREATE TABLE IF NOT EXISTS true_north_variant_canonical_map (
  run_id TEXT NOT NULL,
  variant_id TEXT NOT NULL,
  subject_id TEXT NOT NULL,
  canonical_subject_key TEXT NOT NULL,
  canonical_proposition_key TEXT NOT NULL,
  producer_model TEXT NOT NULL,
  packet_sha256 TEXT NOT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY (run_id, variant_id)
);
CREATE TABLE IF NOT EXISTS true_north_scores (
  run_id TEXT NOT NULL,
  metric TEXT NOT NULL,
  value REAL,
  numerator REAL,
  denominator REAL,
  passed INTEGER,
  details_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL,
  PRIMARY KEY (run_id, metric)
);
"""


def _create_shadow_database(
    source: sqlite3.Connection,
    destination: Path,
    specs: Sequence[EpisodeSpec],
) -> tuple[sqlite3.Connection, dict[str, int]]:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{os.getpid()}.tmp")
    for stale in (
        temporary,
        Path(f"{temporary}-wal"),
        Path(f"{temporary}-shm"),
        temporary.with_suffix(f"{temporary.suffix}.init.lock"),
    ):
        if stale.exists():
            stale.unlink()
    target = db.connect(temporary)
    try:
        db.init_db(target)
        target.executescript(TRUE_NORTH_SCHEMA)
        episode_ids = [spec.episode_id for spec in specs]
        transcript_ids = [spec.transcript_id for spec in specs]
        episode_rows = source.execute(
            f"SELECT * FROM episodes WHERE id IN ({_placeholders(episode_ids)})",
            episode_ids,
        ).fetchall()
        if len(episode_rows) != len(episode_ids):
            raise TrueNorthError("one or more frozen episodes are missing")
        source_ids = sorted({str(row["source_id"]) for row in episode_rows})
        counts: dict[str, int] = {}
        counts["sources"] = _copy_rows(
            source,
            target,
            "sources",
            source.execute(
                f"SELECT * FROM sources WHERE id IN ({_placeholders(source_ids)})",
                source_ids,
            ).fetchall(),
        )
        counts["episodes"] = _copy_rows(source, target, "episodes", episode_rows)
        transcript_rows = source.execute(
            f"SELECT * FROM transcripts WHERE id IN ({_placeholders(transcript_ids)})",
            transcript_ids,
        ).fetchall()
        if len(transcript_rows) != len(transcript_ids):
            raise TrueNorthError("one or more frozen transcripts are missing")
        counts["transcripts"] = _copy_rows(source, target, "transcripts", transcript_rows)
        segment_rows = source.execute(
            f"SELECT * FROM segments WHERE transcript_id IN ({_placeholders(transcript_ids)})",
            transcript_ids,
        ).fetchall()
        segment_ids = [str(row["id"]) for row in segment_rows]
        counts["segments"] = _copy_rows(source, target, "segments", segment_rows)
        label_run_rows = source.execute(
            f"""
            SELECT * FROM label_runs
            WHERE segment_id IN ({_placeholders(segment_ids)})
              AND label_pack = 'ai_discourse_v3_1'
              AND model = 'gpt-5.5'
            """,
            segment_ids,
        ).fetchall()
        referenced_job_ids = {
            int(row["job_id"]) for row in label_run_rows if row["job_id"] is not None
        }
        for spec in specs:
            referenced_job_ids.update(
                int(row["job_id"])
                for row in source.execute(
                    """
                    SELECT job_id FROM episode_context_runs
                    WHERE episode_id = ? AND transcript_id = ? AND job_id IS NOT NULL
                    """,
                    (spec.episode_id, spec.transcript_id),
                )
            )
        if referenced_job_ids:
            job_ids = sorted(referenced_job_ids)
            counts["jobs"] = _copy_rows(
                source,
                target,
                "jobs",
                source.execute(
                    f"SELECT * FROM jobs WHERE id IN ({_placeholders(job_ids)})",
                    job_ids,
                ).fetchall(),
            )
        counts["label_runs"] = _copy_rows(
            source,
            target,
            "label_runs",
            label_run_rows,
        )
        label_rows = source.execute(
            f"""
            SELECT * FROM labels
            WHERE segment_id IN ({_placeholders(segment_ids)})
              AND status = 'ready'
              AND label_pack = 'ai_discourse_v3_1'
              AND model = 'gpt-5.5'
            """,
            segment_ids,
        ).fetchall()
        label_ids = [str(row["id"]) for row in label_rows]
        counts["labels"] = _copy_rows(source, target, "labels", label_rows)
        event_rows = source.execute(
            f"SELECT * FROM discourse_events WHERE label_id IN ({_placeholders(label_ids)})",
            label_ids,
        ).fetchall()
        concept_ids = sorted(
            {
                str(row["canonical_concept_id"])
                for row in event_rows
                if row["canonical_concept_id"] is not None
            }
        )
        if concept_ids:
            counts["concepts"] = _copy_rows(
                source,
                target,
                "concepts",
                source.execute(
                    f"SELECT * FROM concepts WHERE id IN ({_placeholders(concept_ids)})",
                    concept_ids,
                ).fetchall(),
            )
        event_ids = [str(row["id"]) for row in event_rows]
        counts["discourse_events"] = _copy_rows(
            source, target, "discourse_events", event_rows
        )
        if event_ids:
            counts["discourse_event_contexts"] = _copy_rows(
                source,
                target,
                "discourse_event_contexts",
                source.execute(
                    f"SELECT * FROM discourse_event_contexts "
                    f"WHERE discourse_event_id IN ({_placeholders(event_ids)})",
                    event_ids,
                ).fetchall(),
            )
        context_rows: list[sqlite3.Row] = []
        for spec in specs:
            row = source.execute(
                """
                SELECT * FROM episode_context_runs
                WHERE episode_id = ? AND transcript_id = ? AND status = 'completed'
                ORDER BY completed_at DESC, updated_at DESC, id DESC LIMIT 1
                """,
                (spec.episode_id, spec.transcript_id),
            ).fetchone()
            if row is not None:
                context_rows.append(row)
        counts["episode_context_runs"] = _copy_rows(
            source, target, "episode_context_runs", context_rows
        )
        synthetic_context_count = 0
        context_episode_ids = {str(row["episode_id"]) for row in context_rows}
        for spec in specs:
            if spec.episode_id in context_episode_ids:
                continue
            speaker_rows = target.execute(
                """
                SELECT DISTINCT c.speaker_name, c.speaker_role, c.speaker_affiliation
                FROM discourse_event_contexts AS c
                JOIN discourse_events AS d ON d.id = c.discourse_event_id
                JOIN labels AS l ON l.id = d.label_id
                JOIN segments AS s ON s.id = l.segment_id
                JOIN transcripts AS t ON t.id = s.transcript_id
                WHERE t.episode_id = ? AND c.speaker_name IS NOT NULL
                ORDER BY c.speaker_name, c.speaker_role, c.speaker_affiliation
                """,
                (spec.episode_id,),
            ).fetchall()
            speaker_map = [
                {
                    "speaker": row["speaker_name"],
                    "role": row["speaker_role"],
                    "affiliation": row["speaker_affiliation"],
                    "source": "frozen_candidate_context",
                }
                for row in speaker_rows
            ]
            created_at = str(
                target.execute(
                    "SELECT created_at FROM transcripts WHERE id = ?",
                    (spec.transcript_id,),
                ).fetchone()[0]
            )
            target.execute(
                """
                INSERT INTO episode_context_runs (
                  id, job_id, episode_id, transcript_id, label_pack, model, status,
                  prompt_path, output_path, context_artifact_path, speaker_map_json,
                  section_map_json, entity_seed_json, concept_seed_json,
                  extraction_guidance, error, created_at, updated_at, completed_at
                ) VALUES (?, NULL, ?, ?, 'ai_discourse_v3_1', 'gpt-5.5', 'completed',
                          NULL, NULL, NULL, ?, '[]', '{}', '[]', ?, NULL, ?, ?, ?)
                """,
                (
                    stable_id(
                        SUITE_ID,
                        spec.episode_id,
                        "candidate-context-fallback",
                        prefix="ctx_tn_",
                    ),
                    spec.episode_id,
                    spec.transcript_id,
                    dumps_json(speaker_map),
                    "Benchmark-only context reconstructed solely from frozen "
                    "candidate speaker fields because the source episode has no "
                    "episode_context_run. Extraction was not rerun.",
                    created_at,
                    created_at,
                    created_at,
                ),
            )
            synthetic_context_count += 1
        counts["synthetic_episode_context_runs"] = synthetic_context_count
        target.commit()
        target.close()
        for sidecar in (Path(f"{destination}-wal"), Path(f"{destination}-shm")):
            if sidecar.exists():
                sidecar.unlink()
        os.replace(temporary, destination)
        target = db.connect(destination)
        target.executescript(TRUE_NORTH_SCHEMA)
        return target, counts
    except Exception:
        target.close()
        for stale in (
            temporary,
            Path(f"{temporary}-wal"),
            Path(f"{temporary}-shm"),
            temporary.with_suffix(f"{temporary.suffix}.init.lock"),
        ):
            if stale.exists():
                stale.unlink()
        raise


def _validate_episode_spec(conn: sqlite3.Connection, spec: EpisodeSpec) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT t.*, e.title AS episode_title, s.name AS source_name
        FROM transcripts AS t
        JOIN episodes AS e ON e.id = t.episode_id
        JOIN sources AS s ON s.id = e.source_id
        WHERE t.id = ?
        """,
        (spec.transcript_id,),
    ).fetchone()
    if row is None or row["episode_id"] != spec.episode_id:
        raise TrueNorthError(f"frozen transcript is missing or misbound: {spec.transcript_id}")
    if row["status"] != "ready":
        raise TrueNorthError(f"frozen transcript is not ready: {spec.transcript_id}")
    if row["raw_text_sha256"] != spec.transcript_sha256:
        raise TrueNorthError(f"frozen transcript hash drifted: {spec.transcript_id}")
    if row["source_name"] != spec.source_name or row["episode_title"] != spec.title:
        raise TrueNorthError(f"frozen episode metadata drifted: {spec.episode_id}")
    return dict(row)


def _latest_episode_context(
    conn: sqlite3.Connection, spec: EpisodeSpec
) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT * FROM episode_context_runs
        WHERE episode_id = ? AND transcript_id = ? AND status = 'completed'
        ORDER BY completed_at DESC, updated_at DESC, id DESC LIMIT 1
        """,
        (spec.episode_id, spec.transcript_id),
    ).fetchone()
    if row is None:
        raise TrueNorthError(f"completed episode context is missing: {spec.episode_id}")
    return {
        "context_run_id": row["id"],
        "model": row["model"],
        "speaker_map": json.loads(row["speaker_map_json"] or "[]"),
        "section_map": json.loads(row["section_map_json"] or "[]"),
        "entity_seed": json.loads(row["entity_seed_json"] or "{}"),
        "concept_seed": json.loads(row["concept_seed_json"] or "[]"),
        "extraction_guidance": row["extraction_guidance"],
        "artifact_path": row["context_artifact_path"],
    }


def _candidate_bundle(conn: sqlite3.Connection, spec: EpisodeSpec) -> dict[str, Any]:
    segment_rows = conn.execute(
        """
        SELECT * FROM segments
        WHERE transcript_id = ?
        ORDER BY segment_index, id
        """,
        (spec.transcript_id,),
    ).fetchall()
    segments: list[dict[str, Any]] = []
    candidates: list[dict[str, Any]] = []
    for segment in segment_rows:
        text_path = _resolve_text_path(str(segment["text_path"]))
        try:
            segment_text = text_path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as exc:
            raise TrueNorthError(f"cannot read frozen segment text: {segment['id']}") from exc
        if _sha256_bytes(segment_text.encode("utf-8")) != segment["text_sha256"]:
            raise TrueNorthError(f"segment text hash drifted: {segment['id']}")
        segments.append(
            {
                "segment_id": segment["id"],
                "segment_index": int(segment["segment_index"]),
                "text_sha256": segment["text_sha256"],
                "text": segment_text,
            }
        )
        event_rows = conn.execute(
            """
            SELECT d.*, l.model AS label_model, l.label_pack, l.label_pack_version,
                   l.status AS label_status,
                   c.source_context_kind, c.source_context_confidence,
                   c.speaker_name, c.speaker_role, c.speaker_affiliation,
                   c.speaker_confidence, c.reported_actor_name,
                   c.reported_actor_type, c.reported_actor_affiliation,
                   c.reported_actor_confidence, c.event_subtype, c.signal_reason,
                   c.metric_json, c.exclusion_flags_json, c.quality_flags_json
            FROM discourse_events AS d
            JOIN labels AS l ON l.id = d.label_id
            LEFT JOIN discourse_event_contexts AS c
              ON c.discourse_event_id = d.id
            WHERE d.segment_id = ?
              AND l.status = 'ready'
              AND l.label_pack = 'ai_discourse_v3_1'
              AND l.model = 'gpt-5.5'
            ORDER BY d.event_index, d.id
            """,
            (segment["id"],),
        ).fetchall()
        for event in event_rows:
            evidence_start = int(event["evidence_start"])
            evidence_end = int(event["evidence_end"])
            evidence_text = str(event["evidence_text"])
            if (
                evidence_start < 0
                or evidence_end <= evidence_start
                or segment_text[evidence_start:evidence_end] != evidence_text
            ):
                raise TrueNorthError(
                    f"candidate exact-evidence mismatch: {event['id']}"
                )
            if event["label_status"] != "ready":
                raise TrueNorthError(f"candidate label is not ready: {event['label_id']}")
            candidates.append(
                {
                    "candidate_id": event["id"],
                    "label_id": event["label_id"],
                    "segment_id": event["segment_id"],
                    "segment_index": int(segment["segment_index"]),
                    "event_index": int(event["event_index"]),
                    "event_type": event["event_type"],
                    "actor_name": event["actor_name"],
                    "actor_type": event["actor_type"],
                    "actor_affiliation": event["actor_affiliation"],
                    "target_raw": event["target_raw"],
                    "candidate_concept": event["candidate_concept"],
                    "stance": event["stance"],
                    "claim_text": event["claim_text"],
                    "claim_type": event["claim_type"],
                    "certainty": event["certainty"],
                    "time_horizon": event["temporal_horizon"],
                    "frame": event["frame"],
                    "causal_mechanism": event["causal_mechanism"],
                    "counterclaim": event["counterclaim"],
                    "confidence": event["confidence"],
                    "evidence_text": evidence_text,
                    "evidence_start": evidence_start,
                    "evidence_end": evidence_end,
                    "speaker": {
                        "name": event["speaker_name"],
                        "role": event["speaker_role"],
                        "affiliation": event["speaker_affiliation"],
                        "confidence": event["speaker_confidence"],
                    },
                    "reported_actor": {
                        "name": event["reported_actor_name"],
                        "type": event["reported_actor_type"],
                        "affiliation": event["reported_actor_affiliation"],
                        "confidence": event["reported_actor_confidence"],
                    },
                    "source_context_kind": event["source_context_kind"],
                    "source_context_confidence": event["source_context_confidence"],
                    "signal_reason": event["signal_reason"],
                    "flags": {
                        "exclusion": json.loads(event["exclusion_flags_json"] or "[]"),
                        "quality": json.loads(event["quality_flags_json"] or "[]"),
                    },
                    "provenance": {
                        "transcript_id": spec.transcript_id,
                        "transcript_sha256": spec.transcript_sha256,
                        "label_model": event["label_model"],
                        "label_pack": event["label_pack"],
                        "label_pack_version": event["label_pack_version"],
                    },
                }
            )
    body = {
        "schema_version": BUNDLE_SCHEMA_VERSION,
        "suite_id": SUITE_ID,
        "partition": spec.partition,
        "episode": {
            "episode_id": spec.episode_id,
            "source_name": spec.source_name,
            "title": spec.title,
            "transcript_id": spec.transcript_id,
            "transcript_sha256": spec.transcript_sha256,
        },
        "episode_context": _latest_episode_context(conn, spec),
        "segments": segments,
        "candidates": candidates,
    }
    body["episode_context_sha256"] = sha256_text(
        dumps_json(body["episode_context"])
    )
    body["bundle_sha256"] = sha256_text(dumps_json(body))
    return body


def build_suite(
    *,
    source_db: str | Path | None = None,
    output_root: str | Path | None = None,
    suite: str = SUITE_ID,
    include_source_db_hash: bool = True,
    rebuild: bool = False,
) -> dict[str, Any]:
    source_path = Path(source_db or db_path()).expanduser().resolve()
    if not source_path.is_file():
        raise TrueNorthError(f"source database does not exist: {source_path}")
    suite_root = _suite_root(output_root, suite)
    suite_root.mkdir(parents=True, exist_ok=True)
    existing_manifest = suite_root / "manifest.json"
    prior_manifest = (
        _read_json(existing_manifest) if existing_manifest.is_file() else None
    )
    if (
        not rebuild
        and existing_manifest.is_file()
        and (suite_root / "shadow.sqlite").is_file()
    ):
        verified = verify_suite(output_root=output_root, suite=suite)
        if not verified["ok"]:
            raise TrueNorthError("existing true-north suite failed verification")
        return {
            "ok": True,
            "suite_id": SUITE_ID,
            "state": "built",
            "idempotent_replay": True,
            **verified,
        }
    source = _readonly_connect(source_path)
    shadow: sqlite3.Connection | None = None
    try:
        source_stat = source_path.stat()
        prior_source = (
            prior_manifest.get("source_database")
            if isinstance(prior_manifest, Mapping)
            else None
        )
        reuse_trusted_source_hash = bool(
            rebuild
            and include_source_db_hash
            and isinstance(prior_source, Mapping)
            and prior_source.get("sha256")
            and prior_source.get("path") == str(source_path)
            and int(prior_source.get("size_bytes") or -1) == source_stat.st_size
            and int(prior_source.get("mtime_ns") or -1) == source_stat.st_mtime_ns
        )
        before = _source_snapshot(
            source,
            source_path,
            include_file_hash=include_source_db_hash and not reuse_trusted_source_hash,
        )
        if reuse_trusted_source_hash:
            before["sha256"] = str(prior_source["sha256"])
        transcript_records = [_validate_episode_spec(source, spec) for spec in EPISODES]
        shadow_path = suite_root / "shadow.sqlite"
        shadow, copied = _create_shadow_database(source, shadow_path, EPISODES)
        bundles: list[dict[str, Any]] = []
        for spec in EPISODES:
            bundle = _candidate_bundle(shadow, spec)
            partition_dir = (
                suite_root / "bundles" / ("sealed-holdout" if spec.partition == "holdout" else "development")
            )
            bundle_path = partition_dir / f"{spec.episode_id}.private.json"
            # A manifest makes a suite immutable and returns above. Without one,
            # any bundle here is debris from an interrupted build and is replaced.
            _write_json(bundle_path, bundle, immutable=False)
            bundles.append(
                {
                    "episode_id": spec.episode_id,
                    "transcript_id": spec.transcript_id,
                    "partition": spec.partition,
                    "bundle_path": str(bundle_path),
                    "bundle_sha256": bundle["bundle_sha256"],
                    "episode_context_sha256": bundle["episode_context_sha256"],
                    "candidate_count": len(bundle["candidates"]),
                    "segment_count": len(bundle["segments"]),
                }
            )
        episode_ids = [spec.episode_id for spec in EPISODES]
        transcript_ids = [spec.transcript_id for spec in EPISODES]
        segment_ids = [
            str(row[0])
            for row in shadow.execute(
                f"SELECT id FROM segments WHERE transcript_id IN ({_placeholders(transcript_ids)}) "
                "ORDER BY id",
                transcript_ids,
            )
        ]
        label_ids = [
            str(row[0])
            for row in shadow.execute(
                f"SELECT id FROM labels WHERE segment_id IN ({_placeholders(segment_ids)}) "
                "AND status = 'ready' AND label_pack = 'ai_discourse_v3_1' "
                "AND model = 'gpt-5.5' ORDER BY id",
                segment_ids,
            )
        ]
        release = intelligence.create_corpus_release(
            shadow,
            pilot_id=SUITE_ID,
            episode_ids=episode_ids,
            transcript_ids=transcript_ids,
            segment_ids=segment_ids,
            accepted_label_ids=label_ids,
            manifest_metadata={
                "benchmark": True,
                "shadow_only": True,
                "extraction_frozen": True,
                "suite_schema_version": SUITE_SCHEMA_VERSION,
            },
            release_id=stable_id(SUITE_ID, "shadow-release", prefix="crel_tn_"),
            status="accepted",
        )
        after = _source_snapshot(
            source,
            source_path,
            include_file_hash=include_source_db_hash and not reuse_trusted_source_hash,
        )
        if reuse_trusted_source_hash:
            after["sha256"] = str(prior_source["sha256"])
        if before != after:
            raise TrueNorthError("authoritative source database changed during suite build")
        manifest_body = {
            "schema_version": SUITE_SCHEMA_VERSION,
            "suite_id": SUITE_ID,
            "privacy": "local_private_analysis_only",
            "extraction_frozen": True,
            "production_mutation_allowed": False,
            "created_at": now_iso(),
            "source_database": before,
            "shadow_database": str(shadow_path),
            "shadow_release_id": release["id"],
            "copied_rows": copied,
            "episodes": [
                {
                    "episode_id": spec.episode_id,
                    "transcript_id": spec.transcript_id,
                    "transcript_sha256": spec.transcript_sha256,
                    "partition": spec.partition,
                    "source_name": spec.source_name,
                    "title": spec.title,
                    "word_count": int(record["word_count"]),
                }
                for spec, record in zip(EPISODES, transcript_records)
            ],
            "bundles": bundles,
            "candidate_count": sum(row["candidate_count"] for row in bundles),
            "development_candidate_count": sum(
                row["candidate_count"] for row in bundles if row["partition"] == "development"
            ),
            "holdout_candidate_count": sum(
                row["candidate_count"] for row in bundles if row["partition"] == "holdout"
            ),
            "unselected_transcript_ids": ["tr_7cb45a071d624f958b89df96"],
            "routers": {key: list(value) for key, value in ROUTERS.items()},
            "frozen_interfaces": _interface_fingerprints(),
        }
        manifest_body["manifest_sha256"] = sha256_text(dumps_json(manifest_body))
        manifest_path = suite_root / "manifest.json"
        _write_json(manifest_path, manifest_body, immutable=not rebuild)
        shadow.execute(
            """
            INSERT OR REPLACE INTO true_north_suites
              (suite_id, schema_version, manifest_sha256, source_db_path,
               source_db_sha256, release_id, development_episode_count,
               holdout_episode_count, candidate_count, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                SUITE_ID,
                SUITE_SCHEMA_VERSION,
                manifest_body["manifest_sha256"],
                str(source_path),
                before["sha256"],
                release["id"],
                sum(spec.partition == "development" for spec in EPISODES),
                sum(spec.partition == "holdout" for spec in EPISODES),
                manifest_body["candidate_count"],
                manifest_body["created_at"],
            ),
        )
        shadow.commit()
        return {
            "ok": True,
            "suite_id": SUITE_ID,
            "state": "built",
            "manifest_path": str(manifest_path),
            "manifest_sha256": manifest_body["manifest_sha256"],
            "shadow_database": str(shadow_path),
            "shadow_release_id": release["id"],
            "candidate_count": manifest_body["candidate_count"],
            "development_candidate_count": manifest_body["development_candidate_count"],
            "holdout_candidate_count": manifest_body["holdout_candidate_count"],
            "production_source_unchanged": True,
            "canonical_mutation": False,
            "shadow_mutation": True,
        }
    finally:
        if shadow is not None:
            shadow.close()
        source.close()


def verify_suite(
    *,
    output_root: str | Path | None = None,
    suite: str = SUITE_ID,
) -> dict[str, Any]:
    suite_root = _suite_root(output_root, suite)
    manifest_path = suite_root / "manifest.json"
    manifest = _read_json(manifest_path)
    errors: list[str] = []
    digest_body = dict(manifest)
    expected_manifest_sha = str(digest_body.pop("manifest_sha256", ""))
    actual_manifest_sha = sha256_text(dumps_json(digest_body))
    if actual_manifest_sha != expected_manifest_sha:
        errors.append("manifest hash mismatch")
    shadow_path = Path(manifest["shadow_database"])
    if not shadow_path.is_file():
        errors.append("shadow database is missing")
    for record in manifest["bundles"]:
        path = Path(record["bundle_path"])
        if not path.is_file():
            errors.append(f"bundle missing: {record['episode_id']}")
            continue
        bundle = _read_json(path)
        body = dict(bundle)
        expected = str(body.pop("bundle_sha256", ""))
        actual = sha256_text(dumps_json(body))
        if expected != actual or expected != record["bundle_sha256"]:
            errors.append(f"bundle hash mismatch: {record['episode_id']}")
        if (
            bundle.get("episode_context_sha256")
            != record.get("episode_context_sha256")
        ):
            errors.append(
                f"episode context hash mismatch: {record['episode_id']}"
            )
    actual_interfaces = manifest.get("frozen_interfaces", {})
    expected_interfaces = _interface_fingerprints()
    if any(
        actual_interfaces.get(key) != value
        for key, value in expected_interfaces.items()
    ):
        errors.append("prompt, schema, model, or router interface drift")
    if actual_interfaces.get("gate_policy_version"):
        gate_policy_path = (
            suite_root / "diagnostics" / "gate-policy-v2.json"
        )
        gold_path = (
            suite_root / "gold" / "development" / "final" / "gold.private.json"
        )
        consensus_path = (
            suite_root
            / "gold"
            / "development"
            / "final"
            / "consensus.private.json"
        )
        if (
            actual_interfaces.get("gate_policy_version")
            != APPROVED_GATE_POLICY_VERSION
            or not gate_policy_path.is_file()
            or _read_json(gate_policy_path).get("gate_policy_sha256")
            != actual_interfaces.get("gate_policy_sha256")
            or not gold_path.is_file()
            or _read_json(gold_path).get("gold_sha256")
            != actual_interfaces.get("development_gold_sha256")
            or not consensus_path.is_file()
            or _read_json(consensus_path).get("consensus_sha256")
            != actual_interfaces.get("development_consensus_sha256")
        ):
            errors.append("approved gate or gold revision binding drift")
    if not _source_unchanged(manifest):
        errors.append("authoritative source database changed since suite freeze")
    if shadow_path.is_file():
        shadow = _readonly_connect(shadow_path)
        try:
            for spec in EPISODES:
                try:
                    _validate_episode_spec(shadow, spec)
                except TrueNorthError as exc:
                    errors.append(str(exc))
            excluded = int(
                shadow.execute(
                    "SELECT COUNT(*) FROM transcripts WHERE id = ?",
                    ("tr_7cb45a071d624f958b89df96",),
                ).fetchone()[0]
            )
            if excluded:
                errors.append("unselected Latent Space transcript entered shadow database")
        finally:
            shadow.close()
    return {
        "ok": not errors,
        "manifest_path": str(manifest_path),
        "manifest_sha256": expected_manifest_sha,
        "shadow_database": str(shadow_path),
        "candidate_count": int(manifest.get("candidate_count") or 0),
        "development_candidate_count": int(
            manifest.get("development_candidate_count") or 0
        ),
        "holdout_candidate_count": int(manifest.get("holdout_candidate_count") or 0),
        "production_source_unchanged": "authoritative source database changed since suite freeze"
        not in errors,
        "errors": errors,
        "canonical_mutation": False,
    }


def _atomic_item_schema(candidate_ids: Sequence[str]) -> dict[str, Any]:
    atomic = {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "claim_text",
            "claim_type",
            "raw_speaker",
            "reported_actor",
            "stance",
            "certainty",
            "time_horizon",
            "confidence",
            "subject_text",
            "subject_type",
            "domain",
            "proposition_text",
            "polarity",
            "position",
        ],
        "properties": {
            "claim_text": {"type": "string", "minLength": 1},
            "claim_type": {"type": "string", "minLength": 1},
            "raw_speaker": {"type": "string", "minLength": 1},
            "reported_actor": {"type": ["string", "null"]},
            "stance": {"type": "string", "minLength": 1},
            "certainty": {"type": "string", "minLength": 1},
            "time_horizon": {"type": "string", "minLength": 1},
            "confidence": {"type": "number", "minimum": 0, "maximum": 1},
            "subject_text": {"type": "string", "minLength": 1},
            "subject_type": {"type": "string", "minLength": 1},
            "domain": {"type": ["string", "null"]},
            "proposition_text": {"type": "string", "minLength": 1},
            "polarity": {"type": ["string", "null"]},
            "position": {"type": "string", "minLength": 1},
        },
    }
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["candidate_id", "disposition", "reason_code", "atomic_claims"],
        "properties": {
            "candidate_id": {"type": "string", "enum": list(candidate_ids)},
            "disposition": {"type": "string", "enum": list(DISPOSITIONS)},
            "reason_code": {"type": "string", "minLength": 1},
            "atomic_claims": {
                "type": "array",
                "maxItems": 8,
                "items": atomic,
            },
        },
    }


def atomic_output_schema(candidate_ids: Sequence[str]) -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["schema_version", "items"],
        "properties": {
            "schema_version": {"type": "string", "const": WORK_OUTPUT_SCHEMA_VERSION},
            "items": {
                "type": "array",
                "minItems": len(candidate_ids),
                "maxItems": len(candidate_ids),
                "items": _atomic_item_schema(candidate_ids),
            },
        },
    }


def multipass_disposition_schema(
    candidate_ids: Sequence[str],
) -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["schema_version", "items"],
        "properties": {
            "schema_version": {
                "type": "string",
                "const": MULTIPASS_SCHEMA_VERSION,
            },
            "items": {
                "type": "array",
                "minItems": len(candidate_ids),
                "maxItems": len(candidate_ids),
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": [
                        "candidate_id",
                        "disposition",
                        "junk_reason",
                    ],
                    "properties": {
                        "candidate_id": {
                            "type": "string",
                            "enum": list(candidate_ids),
                        },
                        "disposition": {
                            "type": "string",
                            "enum": list(DISPOSITIONS),
                        },
                        "junk_reason": {
                            "type": ["string", "null"],
                            "enum": [None, *MULTIPASS_JUNK_REASONS],
                        },
                    },
                },
            },
        },
    }


def phase_c_junk_verify_schema(
    candidate_ids: Sequence[str],
) -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["schema_version", "items"],
        "properties": {
            "schema_version": {
                "type": "string",
                "const": MULTIPASS_SCHEMA_VERSION,
            },
            "items": {
                "type": "array",
                "minItems": len(candidate_ids),
                "maxItems": len(candidate_ids),
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": [
                        "candidate_id",
                        "verdict",
                        "junk_reason",
                        "deficiency_quote",
                    ],
                    "properties": {
                        "candidate_id": {
                            "type": "string",
                            "enum": list(candidate_ids),
                        },
                        "verdict": {
                            "type": "string",
                            "enum": ["confirm_retain", "reject"],
                        },
                        "junk_reason": {
                            "type": ["string", "null"],
                            "enum": [None, *MULTIPASS_JUNK_REASONS],
                        },
                        "deficiency_quote": {
                            "type": ["string", "null"],
                        },
                    },
                },
            },
        },
    }


def phase_c_marginal_schema(
    candidate_ids: Sequence[str],
    neighbors_by_candidate: Mapping[str, Sequence[str]],
) -> dict[str, Any]:
    neighbor_ids = sorted(
        {
            str(neighbor_id)
            for candidate_id in candidate_ids
            for neighbor_id in neighbors_by_candidate[candidate_id]
        }
    )
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["schema_version", "items"],
        "properties": {
            "schema_version": {
                "type": "string",
                "const": MULTIPASS_SCHEMA_VERSION,
            },
            "items": {
                "type": "array",
                "minItems": len(candidate_ids),
                "maxItems": len(candidate_ids),
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": [
                        "candidate_id",
                        "verdict",
                        "junk_reason",
                        "duplicate_of",
                        "deficiency_quote",
                    ],
                    "properties": {
                        "candidate_id": {
                            "type": "string",
                            "enum": list(candidate_ids),
                        },
                        "verdict": {
                            "type": "string",
                            "enum": ["confirm_retain", "reject"],
                        },
                        "junk_reason": {
                            "type": ["string", "null"],
                            "enum": [None, *MULTIPASS_JUNK_REASONS],
                        },
                        "duplicate_of": {
                            "type": ["string", "null"],
                            "enum": [None, *neighbor_ids],
                        },
                        "deficiency_quote": {
                            "type": ["string", "null"],
                        },
                    },
                },
            },
        },
    }


def multipass_decomposition_schema(
    candidate_ids: Sequence[str],
) -> dict[str, Any]:
    inventory = {
        "type": "object",
        "additionalProperties": False,
        "required": ["index", "subject", "predicate", "object_or_outcome"],
        "properties": {
            "index": {"type": "integer", "minimum": 1},
            "subject": {"type": "string", "minLength": 1},
            "predicate": {"type": "string", "minLength": 1},
            "object_or_outcome": {"type": "string", "minLength": 1},
        },
    }
    atomic = {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "inventory_index",
            "claim_text",
            "certainty",
            "stance",
            "polarity",
            "time_horizon",
        ],
        "properties": {
            "inventory_index": {"type": "integer", "minimum": 1},
            "claim_text": {"type": "string", "minLength": 1},
            **{
                field: {"type": "string", "enum": list(values)}
                for field, values in MULTIPASS_ENUMS.items()
            },
        },
    }
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["schema_version", "items"],
        "properties": {
            "schema_version": {
                "type": "string",
                "const": MULTIPASS_SCHEMA_VERSION,
            },
            "items": {
                "type": "array",
                "minItems": len(candidate_ids),
                "maxItems": len(candidate_ids),
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": [
                        "candidate_id",
                        "proposition_inventory",
                        "atomic_claims",
                    ],
                    "properties": {
                        "candidate_id": {
                            "type": "string",
                            "enum": list(candidate_ids),
                        },
                        "proposition_inventory": {
                            "type": "array",
                            "minItems": 1,
                            "maxItems": 8,
                            "items": inventory,
                        },
                        "atomic_claims": {
                            "type": "array",
                            "minItems": 1,
                            "maxItems": 8,
                            "items": atomic,
                        },
                    },
                },
            },
        },
    }


def multipass_adjudication_schema(
    candidate_ids: Sequence[str],
) -> dict[str, Any]:
    """Schema for the split/no-split minimal-edit stage-B contract."""
    atomic = {
        "type": "object",
        "additionalProperties": False,
        "required": ["claim_text"],
        "properties": {
            "claim_text": {"type": "string", "minLength": 1},
        },
    }
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["schema_version", "items"],
        "properties": {
            "schema_version": {
                "type": "string",
                "const": MULTIPASS_SCHEMA_VERSION,
            },
            "items": {
                "type": "array",
                "minItems": len(candidate_ids),
                "maxItems": len(candidate_ids),
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": [
                        "candidate_id",
                        "split",
                        "atomic_claims",
                        "edit_reason",
                    ],
                    "properties": {
                        "candidate_id": {
                            "type": "string",
                            "enum": list(candidate_ids),
                        },
                        "split": {"type": "boolean"},
                        "atomic_claims": {
                            "type": "array",
                            "minItems": 1,
                            "maxItems": 8,
                            "items": atomic,
                        },
                        "edit_reason": {
                            "type": "string",
                            "enum": list(MULTIPASS_EDIT_REASONS),
                        },
                    },
                },
            },
        },
    }


def validate_multipass_adjudication(
    output: Mapping[str, Any],
    packet: Mapping[str, Any],
) -> None:
    """Bind split, claim count, and verbatim adoption to each other.

    The contract's whole value is that ``edit_reason="none"`` is a *checkable*
    promise: the emitted claim is the frozen upstream proposal byte for byte.
    A model that silently restyles an already-accurate claim fails here rather
    than quietly eroding faithfulness on the run.
    """
    _validate_schema(packet["output_schema"], output, path="$")
    proposals = {
        str(row["candidate_id"]): str(row["proposed_claim_text"])
        for row in packet["input"]["candidates"]
    }
    items = _validate_multipass_scope(output, list(proposals))
    for candidate_id, item in items.items():
        edit_reason = str(item["edit_reason"])
        if edit_reason not in MULTIPASS_EDIT_REASONS:
            raise TrueNorthError(
                f"edit_reason is outside the closed set: {candidate_id}"
            )
        claims = [str(row["claim_text"]) for row in item["atomic_claims"]]
        if any(not claim.strip() for claim in claims):
            raise TrueNorthError(f"atomic claim is blank: {candidate_id}")
        if len(claims) != len({claim.strip() for claim in claims}):
            raise TrueNorthError(
                f"duplicate atomic claim text: {candidate_id}"
            )
        split = bool(item["split"])
        if not split and len(claims) != 1:
            raise TrueNorthError(
                f"split=false requires exactly one atomic claim: {candidate_id}"
            )
        if split and len(claims) < 2:
            raise TrueNorthError(
                "split=true requires two or more atomic claims: "
                f"{candidate_id}"
            )
        if edit_reason == "compound_split" and not split:
            raise TrueNorthError(
                f"compound_split requires split=true: {candidate_id}"
            )
        adopted_verbatim = not split and claims[0] == proposals[candidate_id]
        if edit_reason == "none" and not adopted_verbatim:
            raise TrueNorthError(
                "edit_reason=none requires the proposal adopted verbatim as "
                f"one claim: {candidate_id}"
            )


def _speaker_map_name(entry: Mapping[str, Any] | str) -> str:
    if isinstance(entry, str):
        return entry
    return str(entry.get("name") or entry.get("speaker") or entry.get("display") or "")


def render_speaker_roster(
    speaker_map: Any,
) -> list[dict[str, Any]]:
    """Render the episode speaker map as a stable closed-set roster."""
    source: list[tuple[str, Any]] = []
    if isinstance(speaker_map, Mapping):
        source = [(str(key), value) for key, value in speaker_map.items()]
    elif isinstance(speaker_map, Sequence) and not isinstance(
        speaker_map, (str, bytes)
    ):
        source = [(str(index), value) for index, value in enumerate(speaker_map)]
    entries: list[dict[str, Any]] = []
    for source_key, raw in source:
        if not isinstance(raw, (Mapping, str)):
            continue
        name = _speaker_map_name(raw).strip()
        if not name:
            name = source_key.strip()
        if not name:
            continue
        aliases = (
            [str(value) for value in raw.get("aliases", []) if str(value).strip()]
            if isinstance(raw, Mapping)
            else []
        )
        role = str(raw.get("role") or raw.get("speaker_type") or "") if isinstance(raw, Mapping) else ""
        entries.append(
            {
                "source_key": source_key,
                "canonical_name": name,
                "aliases": sorted(set(aliases), key=lambda value: value.casefold()),
                "role": role,
            }
        )
    entries.sort(
        key=lambda row: (
            row["canonical_name"].casefold(),
            row["source_key"].casefold(),
        )
    )
    roster = [
        {
            "speaker_id": f"speaker_{index:03d}",
            **row,
        }
        for index, row in enumerate(entries, start=1)
    ]
    roster.append(
        {
            "speaker_id": "speaker_unresolved",
            "source_key": "unresolved",
            "canonical_name": "unknown",
            "aliases": [],
            "role": "unresolved",
        }
    )
    return roster


def multipass_attribution_schema(
    claim_refs: Sequence[tuple[str, int]],
    roster: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    candidate_ids = sorted({candidate_id for candidate_id, _ in claim_refs})
    speaker_ids = [str(row["speaker_id"]) for row in roster]
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["schema_version", "items"],
        "properties": {
            "schema_version": {
                "type": "string",
                "const": MULTIPASS_SCHEMA_VERSION,
            },
            "items": {
                "type": "array",
                "minItems": len(claim_refs),
                "maxItems": len(claim_refs),
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": [
                        "candidate_id",
                        "claim_index",
                        "speaker_id",
                        "attribution_mode",
                        "actor_presence",
                        "reported_actor_id",
                        "reported_actor_freetext",
                    ],
                    "properties": {
                        "candidate_id": {
                            "type": "string",
                            "enum": candidate_ids,
                        },
                        "claim_index": {"type": "integer", "minimum": 0},
                        "speaker_id": {
                            "type": "string",
                            "enum": speaker_ids,
                        },
                        "attribution_mode": {
                            "type": "string",
                            "enum": ["direct", "reported"],
                        },
                        "actor_presence": {
                            "type": "string",
                            "enum": ["present", "absent"],
                        },
                        "reported_actor_id": {
                            "type": ["string", "null"],
                            "enum": [None, *speaker_ids],
                        },
                        "reported_actor_freetext": {
                            "type": ["string", "null"],
                        },
                    },
                },
            },
        },
    }


def _validate_multipass_scope(
    output: Mapping[str, Any],
    expected_ids: Sequence[str],
) -> dict[str, dict[str, Any]]:
    seen: dict[str, dict[str, Any]] = {}
    for raw in output["items"]:
        candidate_id = str(raw["candidate_id"])
        if candidate_id in seen:
            raise TrueNorthError(
                f"duplicate multipass candidate decision: {candidate_id}"
            )
        seen[candidate_id] = dict(raw)
    if set(seen) != set(expected_ids):
        raise TrueNorthError(
            "multipass output did not account for every candidate"
        )
    return seen


def validate_multipass_disposition(
    output: Mapping[str, Any],
    packet: Mapping[str, Any],
) -> None:
    _validate_schema(packet["output_schema"], output, path="$")
    candidate_ids = [
        str(row["candidate_id"]) for row in packet["input"]["candidates"]
    ]
    items = _validate_multipass_scope(output, candidate_ids)
    for candidate_id, item in items.items():
        rejected = item["disposition"] == "reject"
        if rejected != (item["junk_reason"] is not None):
            raise TrueNorthError(
                "reject requires junk_reason and non-reject forbids it: "
                f"{candidate_id}"
            )


def validate_phase_c_junk_verify(
    output: Mapping[str, Any],
    packet: Mapping[str, Any],
) -> None:
    _validate_schema(packet["output_schema"], output, path="$")
    candidates = {
        str(row["candidate_id"]): row
        for row in packet["input"]["candidates"]
    }
    items = _validate_multipass_scope(output, list(candidates))
    for candidate_id, item in items.items():
        rejected = item["verdict"] == "reject"
        reason = item["junk_reason"]
        quote = item["deficiency_quote"]
        if rejected != (reason is not None and isinstance(quote, str)):
            raise TrueNorthError(
                "junk verifier reject requires reason and deficiency quote; "
                "confirm_retain forbids both: "
                f"{candidate_id}"
            )
        if not rejected and (reason is not None or quote is not None):
            raise TrueNorthError(
                "confirmed retention cannot carry junk evidence: "
                f"{candidate_id}"
            )
        if rejected:
            assert isinstance(quote, str)
            if not quote or quote not in str(
                candidates[candidate_id]["evidence_text"]
            ):
                raise TrueNorthError(
                    "junk deficiency quote must be copied verbatim from "
                    f"evidence: {candidate_id}"
                )


def validate_phase_c_marginal_verify(
    output: Mapping[str, Any],
    packet: Mapping[str, Any],
) -> None:
    _validate_schema(packet["output_schema"], output, path="$")
    candidates = {
        str(row["candidate_id"]): row
        for row in packet["input"]["candidates"]
    }
    items = _validate_multipass_scope(output, list(candidates))
    for candidate_id, item in items.items():
        rejected = item["verdict"] == "reject"
        reason = item["junk_reason"]
        duplicate_of = item["duplicate_of"]
        quote = item["deficiency_quote"]
        if not rejected:
            if any(
                value is not None
                for value in (reason, duplicate_of, quote)
            ):
                raise TrueNorthError(
                    "marginal confirm_retain forbids junk fields: "
                    f"{candidate_id}"
                )
            continue
        if reason is None:
            raise TrueNorthError(
                f"marginal reject requires junk_reason: {candidate_id}"
            )
        shown_neighbors = {
            str(row["candidate_id"])
            for row in candidates[candidate_id]["neighbors"]
        }
        if reason == "repetition":
            if duplicate_of not in shown_neighbors:
                raise TrueNorthError(
                    "repetition reject requires duplicate_of from shown "
                    f"neighbors: {candidate_id}"
                )
            if quote is not None and str(quote) not in str(
                candidates[candidate_id]["evidence_text"]
            ):
                raise TrueNorthError(
                    "optional repetition quote must be copied verbatim: "
                    f"{candidate_id}"
                )
            continue
        if duplicate_of is not None:
            raise TrueNorthError(
                "non-repetition reject forbids duplicate_of: "
                f"{candidate_id}"
            )
        if (
            not isinstance(quote, str)
            or not quote
            or quote
            not in str(candidates[candidate_id]["evidence_text"])
        ):
            raise TrueNorthError(
                "non-repetition reject requires a verbatim deficiency "
                f"quote: {candidate_id}"
            )


def validate_multipass_decomposition(
    output: Mapping[str, Any],
    packet: Mapping[str, Any],
) -> None:
    enum_repairs = {
        "polarity": {
            "affirmative": "positive",
            "mixed": "neutral",
            "conditional": "neutral",
            "restrictive": "neutral",
            "uncertain": "neutral",
        },
        "stance": {
            "reporting": "neutral",
            "critical": "skeptical",
            "cautious": "warning",
            "optimistic": "supportive",
            "reassuring": "supportive",
            "monitoring": "neutral",
            "conditional": "uncertain",
        },
        "certainty": {
            "approximate": "low",
            "aspirational": "low",
            "hypothetical": "low",
            "speculative": "low",
            "uncertain": "low",
            "assumed": "medium",
            "expected": "medium",
            "conditional": "hedged",
            "qualified": "hedged",
            "hedged rumor": "hedged",
        },
        "time_horizon": {
            "near future": "near_future",
            "future": "long_future",
            "recent": "past",
            "ongoing": "present",
            "past_to_present": "present",
            "present_to_future": "near_future",
            "present_and_future": "near_future",
            "present_and_near_future": "near_future",
            "present_or_near_future": "near_future",
        },
    }
    for item in output.get("items", []):
        for atomic in item.get("atomic_claims", []):
            for field, repairs in enum_repairs.items():
                value = atomic.get(field)
                if isinstance(value, str) and value.casefold() in repairs:
                    atomic[field] = repairs[value.casefold()]
    _validate_schema(packet["output_schema"], output, path="$")
    candidate_ids = [
        str(row["candidate_id"]) for row in packet["input"]["candidates"]
    ]
    items = _validate_multipass_scope(output, candidate_ids)
    for candidate_id, item in items.items():
        inventory_indices = [
            int(row["index"]) for row in item["proposition_inventory"]
        ]
        if len(inventory_indices) != len(set(inventory_indices)):
            raise TrueNorthError(
                f"duplicate proposition inventory index: {candidate_id}"
            )
        claim_indices = {
            int(row["inventory_index"]) for row in item["atomic_claims"]
        }
        if not claim_indices <= set(inventory_indices):
            raise TrueNorthError(
                f"atomic claim references missing inventory index: {candidate_id}"
            )
        if claim_indices != set(inventory_indices):
            raise TrueNorthError(
                f"proposition inventory is not fully emitted: {candidate_id}"
            )


def validate_multipass_attribution(
    output: Mapping[str, Any],
    packet: Mapping[str, Any],
) -> None:
    for item in output.get("items", []):
        actor_present = (
            item.get("reported_actor_id") is not None
            or item.get("reported_actor_freetext") is not None
        )
        # actor_presence is a required deliberation field, but the actor fields
        # are the semantic source of truth. Repair only this redundant bit.
        item["actor_presence"] = "present" if actor_present else "absent"
    _validate_schema(packet["output_schema"], output, path="$")
    expected = {
        (str(row["candidate_id"]), int(row["claim_index"]))
        for row in packet["input"]["claims"]
    }
    seen: set[tuple[str, int]] = set()
    roster_ids = {
        str(row["speaker_id"]) for row in packet["input"]["speaker_roster"]
    }
    for item in output["items"]:
        ref = (str(item["candidate_id"]), int(item["claim_index"]))
        if ref in seen:
            raise TrueNorthError(f"duplicate attribution decision: {ref}")
        seen.add(ref)
        if item["speaker_id"] not in roster_ids:
            raise TrueNorthError(f"speaker is outside roster: {ref}")
        actor_id = item["reported_actor_id"]
        actor_text = item["reported_actor_freetext"]
        if actor_id is not None and actor_text is not None:
            raise TrueNorthError(f"reported actor fields are mutually exclusive: {ref}")
        if (
            item["attribution_mode"] == "reported"
            and (actor_id is None) == (actor_text is None)
        ):
            raise TrueNorthError(
                f"reported attribution requires exactly one actor field: {ref}"
            )
        actor_is_present = actor_id is not None or actor_text is not None
        if (item["actor_presence"] == "present") != actor_is_present:
            raise TrueNorthError(
                f"actor_presence does not match actor fields: {ref}"
            )
    if seen != expected:
        raise TrueNorthError(
            "attribution output did not account for every atomic claim"
        )


def _multipass_candidate_projection(
    candidate: Mapping[str, Any],
) -> dict[str, Any]:
    """Expose frozen evidence and lineage, never the upstream proposed answer."""
    return {
        "candidate_id": str(candidate["candidate_id"]),
        "segment_id": str(candidate["segment_id"]),
        "evidence_text": str(candidate["evidence_text"]),
        "evidence_start": int(candidate["evidence_start"]),
        "evidence_end": int(candidate["evidence_end"]),
    }


def build_multipass_disposition_packet(
    base_job: Mapping[str, Any],
) -> dict[str, Any]:
    candidate_ids = [
        str(row["candidate_id"]) for row in base_job["input"]["candidates"]
    ]
    return {
        "schema_version": MULTIPASS_SCHEMA_VERSION,
        "suite_id": SUITE_ID,
        "multipass_stage": "disposition",
        "task": "Classify research-value disposition only.",
        "instructions": [
            "Return one disposition for every candidate and no other semantics.",
            "junk_reason is required only for reject.",
        ],
        "output_schema": multipass_disposition_schema(candidate_ids),
        "input": {
            "episode": base_job["input"]["episode"],
            "segment": base_job["input"]["segment"],
            "candidates": [
                _multipass_candidate_projection(row)
                for row in base_job["input"]["candidates"]
            ],
        },
    }


def build_multipass_decomposition_packet(
    base_job: Mapping[str, Any],
    disposition_output: Mapping[str, Any],
) -> dict[str, Any] | None:
    decisions = {
        str(row["candidate_id"]): row for row in disposition_output["items"]
    }
    candidates = [
        _multipass_candidate_projection(row)
        for row in base_job["input"]["candidates"]
        if decisions[str(row["candidate_id"])]["disposition"]
        in {"retain", "revise"}
    ]
    if not candidates:
        return None
    candidate_ids = [str(row["candidate_id"]) for row in candidates]
    return {
        "schema_version": MULTIPASS_SCHEMA_VERSION,
        "suite_id": SUITE_ID,
        "multipass_stage": "decomposition",
        "task": "Inventory and emit atomic propositions only.",
        "instructions": [
            "Do not change disposition or attribute speakers.",
            "Every proposition inventory index must be emitted.",
        ],
        "output_schema": multipass_decomposition_schema(candidate_ids),
        "input": {
            "episode": base_job["input"]["episode"],
            "segment": base_job["input"]["segment"],
            "candidates": candidates,
            "stage_a_decisions": [
                decisions[candidate_id] for candidate_id in candidate_ids
            ],
        },
    }


def _adjudication_candidate_projection(
    candidate: Mapping[str, Any],
) -> dict[str, Any]:
    """Expose the frozen upstream proposal alongside its exact evidence.

    The v2 plan's global constraints make candidate priors legitimate model
    inputs: they are frozen upstream hypotheses produced without gold access.
    ``proposed_claim_text`` is emitted first so the adopt-by-default contract
    lands on a visible hypothesis rather than being buried after the evidence.
    """
    proposal = " ".join(str(candidate.get("claim_text") or "").split())
    if not proposal:
        raise TrueNorthError(
            "adjudication requires a candidate claim_text proposal: "
            f"{candidate.get('candidate_id')}"
        )
    return {
        "candidate_id": str(candidate["candidate_id"]),
        "proposed_claim_text": proposal,
        "segment_id": str(candidate["segment_id"]),
        "evidence_text": str(candidate["evidence_text"]),
        "evidence_start": int(candidate["evidence_start"]),
        "evidence_end": int(candidate["evidence_end"]),
    }


def build_multipass_adjudication_packet(
    base_job: Mapping[str, Any],
    disposition_output: Mapping[str, Any],
) -> dict[str, Any] | None:
    """Build the opt-in stage-B packet for value candidates only."""
    decisions = {
        str(row["candidate_id"]): row for row in disposition_output["items"]
    }
    candidates = [
        _adjudication_candidate_projection(row)
        for row in base_job["input"]["candidates"]
        if decisions[str(row["candidate_id"])]["disposition"]
        in {"retain", "revise"}
    ]
    if not candidates:
        return None
    candidate_ids = [str(row["candidate_id"]) for row in candidates]
    return {
        "schema_version": MULTIPASS_SCHEMA_VERSION,
        "suite_id": SUITE_ID,
        "multipass_stage": "adjudication",
        "task": "Adopt, split, or minimally repair each proposed claim.",
        "instructions": [
            "Do not change disposition or attribute speakers.",
            "Adopt proposed_claim_text verbatim unless the evidence forces a change.",
            "split=false requires exactly one atomic claim; split=true requires two or more.",
            "edit_reason=none means the single claim byte-matches proposed_claim_text.",
        ],
        "output_schema": multipass_adjudication_schema(candidate_ids),
        "input": {
            "episode": base_job["input"]["episode"],
            "segment": base_job["input"]["segment"],
            "candidates": candidates,
            "stage_a_decisions": [
                decisions[candidate_id] for candidate_id in candidate_ids
            ],
        },
    }


def build_multipass_attribution_packet(
    base_job: Mapping[str, Any],
    decomposition_output: Mapping[str, Any],
) -> dict[str, Any] | None:
    claims: list[dict[str, Any]] = []
    for item in decomposition_output["items"]:
        for claim_index, claim in enumerate(item["atomic_claims"]):
            claims.append(
                {
                    "candidate_id": str(item["candidate_id"]),
                    "claim_index": claim_index,
                    "claim_text": str(claim["claim_text"]),
                }
            )
    if not claims:
        return None
    roster = render_speaker_roster(
        base_job["input"]["episode_context"].get("speaker_map", [])
    )
    refs = [
        (str(row["candidate_id"]), int(row["claim_index"])) for row in claims
    ]
    candidates = {
        str(row["candidate_id"]): row
        for row in base_job["input"]["candidates"]
    }
    return {
        "schema_version": MULTIPASS_SCHEMA_VERSION,
        "suite_id": SUITE_ID,
        "multipass_stage": "attribution",
        "task": "Resolve direct speaker and reported actor only.",
        "instructions": [
            "Select direct speakers strictly from the roster.",
            "Use free text only for a reported actor absent from the roster.",
        ],
        "output_schema": multipass_attribution_schema(refs, roster),
        "input": {
            "episode": base_job["input"]["episode"],
            "segment": base_job["input"]["segment"],
            "speaker_roster": roster,
            "claims": [
                {
                    **row,
                    "evidence_text": candidates[row["candidate_id"]][
                        "evidence_text"
                    ],
                }
                for row in claims
            ],
        },
    }


_ADJUDICATION_ABSENT_TEXT = frozenset(
    {"", "none", "null", "n/a", "na", "unknown", "unspecified"}
)


def _adjudication_prior(candidate: Mapping[str, Any], field: str) -> str | None:
    text = " ".join(str(candidate.get(field) or "").split())
    return None if text.casefold() in _ADJUDICATION_ABSENT_TEXT else text


def _adjudication_prior_fields(
    candidate: Mapping[str, Any],
    fallback_subject: str,
) -> dict[str, Any]:
    """Carry the frozen candidate priors the adjudication contract never asks
    the model to re-derive.

    This deliberately mirrors the Task 1 prior-adoption floor field for field,
    so the only difference between the floor and this stage is ``claim_text``
    and its split.  Any gate that moves is therefore attributable to stage B.
    """
    from .true_north_semantic_scoring import normalize_enum_field

    stance = normalize_enum_field(
        "stance", _adjudication_prior(candidate, "stance") or "neutral"
    )
    subject_text = (
        _adjudication_prior(candidate, "candidate_concept")
        or _adjudication_prior(candidate, "target_raw")
        or _adjudication_prior(candidate, "claim_text")
        or fallback_subject
    )
    return {
        "claim_type": normalize_enum_field(
            "claim_type",
            _adjudication_prior(candidate, "claim_type") or "assertion",
        ),
        "stance": stance,
        "certainty": normalize_enum_field(
            "certainty",
            _adjudication_prior(candidate, "certainty") or "unspecified",
        ),
        "time_horizon": normalize_enum_field(
            "time_horizon",
            _adjudication_prior(candidate, "time_horizon") or "unspecified",
        ),
        "polarity": normalize_enum_field(
            "polarity",
            _adjudication_prior(candidate, "polarity") or "neutral",
        ),
        "confidence": max(
            0.0, min(1.0, float(candidate.get("confidence") or 0.0))
        ),
        "subject_text": subject_text,
        "subject_type": _adjudication_prior(candidate, "event_type") or "topic",
        "domain": _adjudication_prior(candidate, "frame"),
        "position": stance,
    }


def compose_multipass_output(
    base_job: Mapping[str, Any],
    disposition_output: Mapping[str, Any],
    decomposition_output: Mapping[str, Any] | None,
    attribution_output: Mapping[str, Any] | None,
    *,
    stage_b_mode: str = DEFAULT_MULTIPASS_STAGE_B_MODE,
) -> dict[str, Any]:
    if stage_b_mode not in MULTIPASS_STAGE_B_MODES:
        raise TrueNorthError(f"unknown multipass stage-B mode: {stage_b_mode}")
    adjudicating = stage_b_mode == "adjudication"
    decisions = {
        str(row["candidate_id"]): row for row in disposition_output["items"]
    }
    decompositions = {
        str(row["candidate_id"]): row
        for row in (decomposition_output or {"items": []})["items"]
    }
    attributions = {
        (str(row["candidate_id"]), int(row["claim_index"])): row
        for row in (attribution_output or {"items": []})["items"]
    }
    roster = {
        str(row["speaker_id"]): row
        for row in render_speaker_roster(
            base_job["input"]["episode_context"].get("speaker_map", [])
        )
    }
    items: list[dict[str, Any]] = []
    for candidate in base_job["input"]["candidates"]:
        candidate_id = str(candidate["candidate_id"])
        decision = decisions[candidate_id]
        disposition = str(decision["disposition"])
        atomics: list[dict[str, Any]] = []
        if disposition in {"retain", "revise"}:
            decomposition = decompositions.get(candidate_id)
            if decomposition is None:
                raise TrueNorthError(
                    f"accepted candidate lacks decomposition: {candidate_id}"
                )
            inventory = (
                {}
                if adjudicating
                else {
                    int(row["index"]): row
                    for row in decomposition["proposition_inventory"]
                }
            )
            for claim_index, claim in enumerate(decomposition["atomic_claims"]):
                attribution = attributions.get((candidate_id, claim_index))
                if attribution is None:
                    raise TrueNorthError(
                        f"atomic claim lacks attribution: {candidate_id}/{claim_index}"
                    )
                speaker = roster[str(attribution["speaker_id"])]
                actor = None
                if (
                    attribution["reported_actor_id"] is not None
                    or attribution["reported_actor_freetext"] is not None
                ):
                    actor = (
                        roster[str(attribution["reported_actor_id"])][
                            "canonical_name"
                        ]
                        if attribution["reported_actor_id"] is not None
                        else attribution["reported_actor_freetext"]
                    )
                claim_text = str(claim["claim_text"])
                if adjudicating:
                    atomics.append(
                        {
                            "claim_text": claim_text,
                            "raw_speaker": str(speaker["canonical_name"]),
                            "reported_actor": actor,
                            "proposition_text": claim_text,
                            **_adjudication_prior_fields(
                                candidate, claim_text
                            ),
                        }
                    )
                    continue
                inventory_row = inventory[int(claim["inventory_index"])]
                atomics.append(
                    {
                        "claim_text": claim_text,
                        "claim_type": "assertion",
                        "raw_speaker": str(speaker["canonical_name"]),
                        "reported_actor": actor,
                        "stance": str(claim["stance"]),
                        "certainty": str(claim["certainty"]),
                        "time_horizon": str(claim["time_horizon"]),
                        "confidence": 0.8,
                        "subject_text": str(inventory_row["subject"]),
                        "subject_type": "topic",
                        "domain": None,
                        "proposition_text": claim_text,
                        "polarity": str(claim["polarity"]),
                        "position": "asserts",
                    }
                )
        items.append(
            {
                "candidate_id": candidate_id,
                "disposition": disposition,
                "reason_code": (
                    str(decision["junk_reason"])
                    if decision["junk_reason"] is not None
                    else f"multipass_{disposition}"
                ),
                "atomic_claims": atomics,
            }
        )
    output = {
        "schema_version": WORK_OUTPUT_SCHEMA_VERSION,
        "items": items,
    }
    validation_job = {
        **base_job,
        "output_schema": atomic_output_schema(
            [str(row["candidate_id"]) for row in base_job["input"]["candidates"]]
        ),
    }
    _validate_atomic_output(output, validation_job)
    return output


def _atomic_instructions(*, gold: bool) -> list[str]:
    authority = (
        "Act as an independent gold annotator. Do not assume the frozen candidate is correct."
        if gold
        else "Act as the downstream workhorse. Evaluate rather than rubber-stamping each candidate."
    )
    return [
        authority,
        "Return exactly one item for every candidate_id and no others.",
        "First decide whether the evidence contains a useful asserted proposition at all; factual-sounding metadata is not automatically useful.",
        "Reject introductions, biographies or bylines, product or policy names without an asserted proposition, question/setup frames, banter, fragments, ASR-corrupted inferences, and non-useful repetition.",
        "Before choosing retain or revise, explicitly count independently true-or-false predicates in the candidate claim and evidence.",
        "retain means the candidate already expresses exactly one valid atomic proposition and needs no semantic correction.",
        "revise is mandatory when useful evidence exists but the claim must be corrected, has an unsupported mechanism removed, or contains two or more independently true-or-false predicates.",
        "hold means the evidence is useful but identity or semantics are genuinely unresolved.",
        "reject means unsupported, setup, banter, bare mention, fragment, or non-useful repetition.",
        "A retain or revise item must contain at least one atomic_claim. A hold or reject item must contain none.",
        "For every revise caused by a compound, output one atomic_claim per independent predicate; never compress a list, contrast, conjunction, cause plus consequence, forecast plus rationale, or claim plus example into one proposition.",
        "Each atomic claim must express exactly one independently true-or-false proposition. If removing one clause could leave another claim true, split them.",
        "Preserve qualifications, conditions, time horizon, polarity, stance, and certainty.",
        "raw_speaker is the direct speaker. reported_actor is non-null only when the proposition explicitly attributes a claim, stance, forecast, or action to a distinct quoted or described actor; a merely mentioned subject, object, company, or background entity is not a reported actor.",
        "Do not return evidence text or offsets inside atomic_claims. The harness binds each atomic to its frozen parent-candidate evidence after validation.",
        "subject_text is the stable issue or question; proposition_text is the specific normalized assertion.",
        "Do not merge claims merely because they share words such as safety, governance, risk, agent, or Anthropic.",
        "Return only schema-valid JSON with no Markdown.",
    ]


def _atomic_audit_instructions() -> list[str]:
    return [
        "Act as a skeptical final downstream atomicity auditor.",
        "The prior proposal is untrusted working material, not an answer or authority.",
        "Return exactly one item for every candidate_id and no others.",
        "Re-read the frozen candidate and evidence before deciding. Preserve a prior proposal only when it survives independent review.",
        "A useful item must assert a proposition that a researcher could independently verify, compare, attribute, contradict, or qualify.",
        "Reject introductions, biographies or bylines, bare entity or product mentions, questions without an asserted answer, hypothetical or rhetorical setup, banter, recording or release chatter, fragments, unsupported ASR inferences, and non-useful repetition.",
        "hold is reserved for useful evidence whose identity or semantics are genuinely unresolved; it is not a softer reject.",
        "Count independent truth conditions. A conjunction, contrast, list, cause plus consequence, forecast plus rationale, or assertion plus example must be split whenever either part could be true while the other is false.",
        "retain means exactly one valid atomic proposition requiring no semantic correction.",
        "revise means useful evidence requires correction or one or more atomic propositions must be rewritten or split.",
        "retain or revise require at least one atomic_claim. hold or reject require none.",
        "Every atomic claim must preserve its material condition, scope, time horizon, polarity, stance, and certainty without importing unsupported mechanisms.",
        "raw_speaker is the direct speaker. reported_actor is non-null only for an explicitly attributed claim, stance, forecast, or action by a distinct actor.",
        "Do not return evidence text or offsets inside atomic_claims; the harness binds the exact frozen evidence after validation.",
        "Return only schema-valid JSON with no Markdown.",
    ]


def _atomic_audit_jobs(
    outputs: Sequence[tuple[Mapping[str, Any], Mapping[str, Any], str]],
) -> list[tuple[Mapping[str, Any], dict[str, Any]]]:
    """Create small independent review packets from the initial segment pass."""
    by_bundle: dict[str, dict[str, Any]] = {}
    bundle_refs: dict[str, Mapping[str, Any]] = {}
    for bundle, output, producer_model in outputs:
        bundle_key = str(bundle["bundle_sha256"])
        bundle_refs[bundle_key] = bundle
        bucket = by_bundle.setdefault(bundle_key, {})
        for item in output["items"]:
            candidate_id = str(item["candidate_id"])
            if candidate_id in bucket:
                raise TrueNorthError(
                    f"duplicate initial atomic proposal: {candidate_id}"
                )
            bucket[candidate_id] = {
                "proposal": item,
                "producer_model": producer_model,
            }

    jobs: list[tuple[Mapping[str, Any], dict[str, Any]]] = []
    for bundle_key in sorted(by_bundle):
        bundle = bundle_refs[bundle_key]
        candidates = {
            str(row["candidate_id"]): dict(row) for row in bundle["candidates"]
        }
        proposal_ids = set(by_bundle[bundle_key])
        if proposal_ids != set(candidates):
            raise TrueNorthError(
                "initial atomic proposals do not cover the frozen bundle"
            )
        ordered_ids = sorted(
            candidates,
            key=lambda candidate_id: (
                int(candidates[candidate_id]["segment_index"]),
                int(candidates[candidate_id]["event_index"]),
                candidate_id,
            ),
        )
        for offset in range(0, len(ordered_ids), ATOMIC_AUDIT_CHUNK_SIZE):
            candidate_ids = ordered_ids[offset : offset + ATOMIC_AUDIT_CHUNK_SIZE]
            jobs.append(
                (
                    bundle,
                    {
                        "schema_version": RUN_SCHEMA_VERSION,
                        "suite_id": SUITE_ID,
                        "task": (
                            "Independently audit a small frozen candidate set for "
                            "research value and atomic proposition boundaries."
                        ),
                        "instructions": _atomic_audit_instructions(),
                        "output_schema": atomic_output_schema(candidate_ids),
                        "input": {
                            "episode": bundle["episode"],
                            "episode_context": bundle["episode_context"],
                            "candidates": [
                                candidates[candidate_id]
                                for candidate_id in candidate_ids
                            ],
                            "prior_proposals": [
                                by_bundle[bundle_key][candidate_id]
                                for candidate_id in candidate_ids
                            ],
                        },
                    },
                )
            )
    return jobs


def _interface_fingerprints() -> dict[str, Any]:
    schema_sentinel = atomic_output_schema(("candidate-scope-sentinel",))
    semantic_schema_hashes = {
        target: sha256_text(
            dumps_json(
                _semantic_job(
                    {
                        "target": target,
                        "corpus_release_id": "release-sentinel",
                        "packet_sha256": "0" * 64,
                    }
                )["output_schema"]
            )
        )
        for target in ("identities", "claims", "relations")
    }
    router_config = {
        router: {
            model: _opencode_config(model, stage="atomic")
            for model in models
        }
        for router, models in ROUTERS.items()
    }
    semantic_prompt_hashes = {}
    for target in ("identities", "claims", "relations"):
        semantic_job = _semantic_job(
            {
                "target": target,
                "corpus_release_id": "release-sentinel",
                "packet_sha256": "0" * 64,
            }
        )
        semantic_prompt_hashes[target] = sha256_text(
            dumps_json(
                {
                    "task": semantic_job["task"],
                    "instructions": semantic_job["instructions"],
                }
            )
        )
    return {
        "workhorse_prompt_version": WORKHORSE_PROMPT_VERSION,
        "workhorse_instructions_sha256": sha256_text(
            dumps_json(_atomic_instructions(gold=False))
        ),
        "atomic_audit_chunk_size": ATOMIC_AUDIT_CHUNK_SIZE,
        "atomic_audit_workers": ATOMIC_AUDIT_WORKERS,
        "semantic_workers": SEMANTIC_WORKERS,
        "atomic_audit_instructions_sha256": sha256_text(
            dumps_json(_atomic_audit_instructions())
        ),
        "gold_prompt_version": GOLD_PROMPT_VERSION,
        "consensus_gold_schema_version": CONSENSUS_GOLD_SCHEMA_VERSION,
        "consensus_gold_policy_version": CONSENSUS_GOLD_POLICY_VERSION,
        "consensus_scoring_contract": (
            "value_state_consensus_atomic_count_range_utility_primary_v1"
        ),
        "gold_instructions_sha256": sha256_text(
            dumps_json(_atomic_instructions(gold=True))
        ),
        "atomic_output_schema_version": WORK_OUTPUT_SCHEMA_VERSION,
        "atomic_output_schema_template_sha256": sha256_text(
            dumps_json(schema_sentinel)
        ),
        "semantic_output_schema_sha256": semantic_schema_hashes,
        "semantic_prompt_sha256": semantic_prompt_hashes,
        "canonical_map_contract": (
            "global_subject_then_subject_preserving_proposition_v1"
        ),
        "relation_selection_contract": (
            "same_global_canonical_subject_all_pairs_v1"
        ),
        "utility_questions_sha256": sha256_text(dumps_json(UTILITY_QUESTIONS)),
        "router_configuration_sha256": sha256_text(dumps_json(router_config)),
    }


def _segment_jobs(bundle: Mapping[str, Any], *, gold: bool) -> list[dict[str, Any]]:
    segments = {str(row["segment_id"]): row for row in bundle["segments"]}
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for candidate in bundle["candidates"]:
        grouped[str(candidate["segment_id"])].append(dict(candidate))
    jobs: list[dict[str, Any]] = []
    for segment_id in sorted(
        grouped, key=lambda value: (int(segments[value]["segment_index"]), value)
    ):
        candidates = grouped[segment_id]
        candidate_ids = [str(row["candidate_id"]) for row in candidates]
        jobs.append(
            {
                "schema_version": (
                    GOLD_SCHEMA_VERSION if gold else RUN_SCHEMA_VERSION
                ),
                "suite_id": SUITE_ID,
                "task": (
                    "Create independent downstream gold for one frozen podcast segment."
                    if gold
                    else "Adjudicate and atomize one frozen podcast candidate batch."
                ),
                "instructions": _atomic_instructions(gold=gold),
                "output_schema": atomic_output_schema(candidate_ids),
                "input": {
                    "episode": bundle["episode"],
                    "episode_context": bundle["episode_context"],
                    "segment": segments[segment_id],
                    "candidates": candidates,
                },
            }
        )
    return jobs


def prepare_gold(
    *,
    output_root: str | Path | None = None,
    suite: str = SUITE_ID,
) -> dict[str, Any]:
    suite_root = _suite_root(output_root, suite)
    manifest_path = suite_root / "manifest.json"
    manifest = _read_json(manifest_path)
    existing_index_path = suite_root / "gold" / "index.json"
    existing_index = (
        _read_json(existing_index_path) if existing_index_path.is_file() else None
    )
    has_outputs = any(
        (suite_root / "gold").glob("**/outputs/*/validated.private.json")
    )
    prepared: Counter[str] = Counter()
    packet_records: list[dict[str, Any]] = []
    frozen_support_artifacts: list[dict[str, Any]] = []
    for partition, partition_dir in (
        ("development", "development"),
        ("holdout", "sealed-holdout"),
    ):
        questions = {
            "schema_version": "pif_true_north_utility_questions_v1",
            "suite_id": SUITE_ID,
            "partition": partition,
            "question_count": len(UTILITY_QUESTIONS),
            "questions": [
                {"question_id": question_id, "question": question}
                for question_id, question in UTILITY_QUESTIONS
            ],
        }
        questions["questions_sha256"] = sha256_text(dumps_json(questions))
        questions_path = (
            suite_root / "gold" / partition_dir / "utility" / "questions.json"
        )
        _write_json(questions_path, questions, immutable=has_outputs)
        relation_spec = {
            "schema_version": "pif_true_north_relation_gold_spec_v1",
            "suite_id": SUITE_ID,
            "partition": partition,
            "labels": list(RELATIONS),
            "positive_scope": "every expected positive relation",
            "hard_negatives": {
                "same_subject_nearest_non_edges_per_atomic": 2,
                "lexically_similar_cross_subject_per_atomic": 1,
                "include_every_system_proposed_pair": True,
            },
            "scope_preservation": [
                "temporal",
                "conditional",
                "actor",
                "deployment",
                "jurisdiction",
            ],
        }
        relation_spec["spec_sha256"] = sha256_text(dumps_json(relation_spec))
        relation_path = (
            suite_root / "gold" / partition_dir / "relations" / "spec.json"
        )
        _write_json(relation_path, relation_spec, immutable=has_outputs)
        frozen_support_artifacts.extend(
            (
                {
                    "partition": partition,
                    "kind": "utility_questions",
                    "path": str(questions_path),
                    "sha256": _sha256_file(questions_path),
                },
                {
                    "partition": partition,
                    "kind": "relation_gold_spec",
                    "path": str(relation_path),
                    "sha256": _sha256_file(relation_path),
                },
            )
        )
    for bundle_record in manifest["bundles"]:
        bundle = _read_json(Path(bundle_record["bundle_path"]))
        partition = str(bundle_record["partition"])
        partition_dir = "sealed-holdout" if partition == "holdout" else "development"
        for pass_name in ("pass-a", "pass-b"):
            for job in _segment_jobs(bundle, gold=True):
                segment_id = str(job["input"]["segment"]["segment_id"])
                packet_path = (
                    suite_root
                    / "gold"
                    / partition_dir
                    / pass_name
                    / "jobs"
                    / f"{segment_id}.private.json"
                )
                _write_json(packet_path, job, immutable=has_outputs)
                schema_path = packet_path.parent.parent / "schemas" / f"{segment_id}.json"
                _write_json(schema_path, job["output_schema"], immutable=has_outputs)
                prepared[f"{partition}:{pass_name}"] += 1
                packet_records.append(
                    {
                        "partition": partition,
                        "pass": pass_name,
                        "episode_id": bundle["episode"]["episode_id"],
                        "segment_id": segment_id,
                        "packet_path": str(packet_path),
                        "packet_sha256": _sha256_file(packet_path),
                        "schema_path": str(schema_path),
                    }
                )
    index = {
        "schema_version": GOLD_SCHEMA_VERSION,
        "suite_id": SUITE_ID,
        "gold_model": "gpt-5.6-sol",
        "gold_effort": "high",
        "passes": ["pass-a", "pass-b", "pass-c-adjudication"],
        "holdout_sealed": True,
        "packets": packet_records,
        "frozen_support_artifacts": frozen_support_artifacts,
        "prepared_counts": dict(sorted(prepared.items())),
        "created_at": (
            existing_index.get("created_at")
            if isinstance(existing_index, Mapping)
            else now_iso()
        ),
    }
    index["index_sha256"] = sha256_text(dumps_json(index))
    index_path = existing_index_path
    _write_json(index_path, index, immutable=has_outputs)
    return {
        "ok": True,
        "suite_id": SUITE_ID,
        "state": "prepared",
        "index_path": str(index_path),
        "index_sha256": index["index_sha256"],
        "prepared_counts": index["prepared_counts"],
        "model_execution_attempted": False,
        "holdout_sealed": True,
    }


def _opencode_config(
    model: str,
    *,
    stage: str,
    system_prompt: str | None = None,
) -> dict[str, Any]:
    agent_name = f"pif-true-north-{stage}"
    return {
        "$schema": "https://opencode.ai/config.json",
        "share": "disabled",
        "snapshot": False,
        "default_agent": agent_name,
        "permission": {"*": "deny"},
        "agent": {
            agent_name: {
                "description": "Private structured podcast downstream benchmark",
                "mode": "primary",
                "model": model,
                "temperature": 0.1,
                "steps": 12,
                "prompt": system_prompt or BASE_SYSTEM_PROMPT,
                "permission": {"*": "deny"},
            }
        },
    }


def _parse_opencode_stream(value: str) -> tuple[str, dict[str, Any] | None, int]:
    text_parts: list[str] = []
    finish = None
    count = 0
    for line in value.splitlines():
        if not line.strip():
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(event, dict):
            continue
        count += 1
        part = event.get("part")
        if event.get("type") == "text" and isinstance(part, dict):
            if isinstance(part.get("text"), str):
                text_parts.append(part["text"])
        if event.get("type") == "step_finish" and isinstance(part, dict):
            finish = part
    return "".join(text_parts).strip(), finish, count


def _decode_json_answer(answer: str) -> dict[str, Any]:
    value = answer.strip()
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError:
        stripped = re.sub(r"^```(?:json)?[ \t\r\n]*", "", value, flags=re.I)
        stripped = re.sub(r"[ \t\r\n]*```$", "", stripped)
        decoded = json.loads(stripped)
    if not isinstance(decoded, dict):
        raise TrueNorthError("model answer is not a JSON object")
    return decoded


def _usage(finish: Mapping[str, Any] | None) -> dict[str, Any]:
    if not isinstance(finish, Mapping):
        return {}
    tokens = finish.get("tokens") if isinstance(finish.get("tokens"), Mapping) else {}
    cache = tokens.get("cache") if isinstance(tokens.get("cache"), Mapping) else {}
    return {
        "input_tokens": tokens.get("input"),
        "cached_input_tokens": cache.get("read"),
        "output_tokens": tokens.get("output"),
        "reasoning_tokens": tokens.get("reasoning"),
        "total_tokens": tokens.get("total"),
        "estimated_cost_usd": finish.get("cost"),
    }


def _is_operational_failure(
    *, timed_out: bool, exit_code: int | None, stderr: str, stdout: str
) -> tuple[bool, str | None]:
    if timed_out:
        return True, "hard_timeout"
    # Successful model text is untrusted semantic content. It routinely contains
    # words such as "unavailable", "capacity", or "rate limit" when those are
    # the subject of a podcast claim; those words must never trigger routing.
    if exit_code in (0, None):
        return False, None
    combined = f"{stderr}\n{stdout}".lower()
    for pattern in OPERATIONAL_FAILURE_PATTERNS:
        if pattern in combined:
            return True, pattern.replace(" ", "_")
    if exit_code not in (0, None) and not combined.strip():
        return True, f"empty_transport_exit_{exit_code}"
    return False, None


def _validate_atomic_output(
    output: Mapping[str, Any],
    job: Mapping[str, Any],
) -> None:
    # Provenance is not a generative field. Discard any model attempt to echo or
    # normalize it, validate only semantics, then bind exact frozen bytes below.
    for item in output.get("items", []):
        for atomic in item.get("atomic_claims", []):
            atomic.pop("evidence_text", None)
            atomic.pop("evidence_start", None)
            atomic.pop("evidence_end", None)
    schema = job["output_schema"]
    _validate_schema(schema, output, path="$")
    expected = {
        str(row["candidate_id"]): row for row in job["input"]["candidates"]
    }
    seen: set[str] = set()
    for item in output["items"]:
        candidate_id = str(item["candidate_id"])
        if candidate_id in seen:
            raise TrueNorthError(f"duplicate candidate decision: {candidate_id}")
        seen.add(candidate_id)
        candidate = expected[candidate_id]
        disposition = str(item["disposition"])
        atomics = list(item["atomic_claims"])
        if disposition in {"retain", "revise"} and not atomics:
            raise TrueNorthError(f"retained candidate has no atomic claims: {candidate_id}")
        if disposition in {"hold", "reject"} and atomics:
            raise TrueNorthError(f"non-retained candidate has atomic claims: {candidate_id}")
        for atomic in atomics:
            atomic["evidence_text"] = candidate["evidence_text"]
            atomic["evidence_start"] = int(candidate["evidence_start"])
            atomic["evidence_end"] = int(candidate["evidence_end"])
    if seen != set(expected):
        raise TrueNorthError("model output did not account for every candidate")


def _run_opencode_packet(
    *,
    packet_path: Path,
    output_dir: Path,
    models: Sequence[str],
    stage: str,
    timeout_seconds: int,
    opencode_binary: str,
    system_prompt: str | None = None,
    validator: Any | None = None,
    _semantic_retry_remaining: int = 2,
    _semantic_retry_feedback: str | None = None,
    _semantic_retry_number: int = 0,
) -> tuple[dict[str, Any], list[dict[str, Any]], str]:
    packet = _read_json(packet_path)
    packet_sha = _sha256_file(packet_path)
    required_schema = _canonical_json(packet["output_schema"])
    output_dir.mkdir(parents=True, exist_ok=True)
    scratch = output_dir / "scratch"
    scratch.mkdir(exist_ok=True)
    auth_source = Path.home() / ".local" / "share" / "opencode" / "auth.json"
    if not auth_source.is_file():
        raise TrueNorthError("OpenCode authentication material is unavailable")
    receipts: list[dict[str, Any]] = []
    for index, model in enumerate(models, start=1):
        checkpoint = output_dir / f"attempt-{index}.private.jsonl"
        checkpoint_stderr = output_dir / f"attempt-{index}.stderr.private.txt"
        if checkpoint.is_file():
            stdout = checkpoint.read_text(encoding="utf-8")
            stderr = (
                checkpoint_stderr.read_text(encoding="utf-8")
                if checkpoint_stderr.is_file()
                else ""
            )
            answer, finish, stream_count = _parse_opencode_stream(stdout)
            operational, fallback_reason = _is_operational_failure(
                timed_out=False,
                exit_code=0,
                stderr=stderr,
                stdout=stdout,
            )
            receipt = {
                "packet_sha256": packet_sha,
                "provider_model": model,
                "system_prompt_sha256": sha256_text(
                    system_prompt or BASE_SYSTEM_PROMPT
                ),
                "attempt": index * 10 + _semantic_retry_number,
                "operational_failure": operational,
                "fallback_reason": fallback_reason,
                "timed_out": False,
                "exit_code": 0,
                "elapsed_seconds": 0.0,
                "stream_event_count": stream_count,
                "usage": {**_usage(finish), "checkpoint_reuse": True},
                "stderr_sha256": _sha256_bytes(stderr.encode("utf-8")),
            }
            receipts.append(receipt)
            if operational:
                continue
            try:
                output = _decode_json_answer(answer)
                if validator is None:
                    _validate_atomic_output(output, packet)
                else:
                    validator(output, packet)
            except Exception:
                # A prior process may have checkpointed an answer that failed a
                # newer invariant. Re-run the same provider; never fall through
                # to another provider for a semantic/schema failure.
                receipts.pop()
            else:
                output_path = output_dir / "validated.private.json"
                _write_json(output_path, output, immutable=False)
                receipt["output_sha256"] = _sha256_file(output_path)
                return output, receipts, model
        env = os.environ.copy()
        env["OPENCODE_CONFIG_CONTENT"] = _canonical_json(
            _opencode_config(model, stage=stage, system_prompt=system_prompt)
        )
        with tempfile.TemporaryDirectory(
            prefix="pif-true-north-", dir=str(output_dir)
        ) as worker_dir:
            data_root = Path(worker_dir)
            auth_dir = data_root / "opencode"
            auth_dir.mkdir()
            shutil.copy2(auth_source, auth_dir / "auth.json")
            os.chmod(auth_dir / "auth.json", 0o600)
            env["XDG_DATA_HOME"] = str(data_root)
            agent_name = f"pif-true-north-{stage}"
            retry_feedback = (
                " A previous response failed local validation. Correct this exact "
                f"error and re-account for the complete packet: {_semantic_retry_feedback}"
                if _semantic_retry_feedback
                else ""
            )
            # OpenCode's --file attachment is exposed to the model through its
            # file reader.  That reader clips any individual line at roughly
            # 2,000 characters; our immutable JSON packets intentionally use a
            # compact single-line serialization, so attaching them caused the
            # model to see only the packet preamble.  Put the bounded packet in
            # the user message itself.  This keeps the exact frozen bytes and
            # property order visible without granting tools or changing the
            # experimental input.
            packet_payload = packet_path.read_text(encoding="utf-8")
            semantic_invariant = (
                " Invariant: retain/revise require one or more atomic_claims; "
                "hold/reject require atomic_claims to be an empty array."
                if not packet.get("multipass_stage")
                else (
                    " This is multipass stage "
                    f"{packet['multipass_stage']}; do not emit fields owned by "
                    "another stage."
                )
            )
            user_message = (
                "Complete the private structured job below. The packet's "
                "output_schema is mandatory: return exactly its named top-level "
                "fields, no alternative adjudication format, no prose, and no "
                "Markdown. Ignore any remembered or inferred output format. "
                + semantic_invariant
                + " "
                "Your entire response must validate against this exact schema:\n"
                + required_schema
                + retry_feedback
                + "\n\nBEGIN_FROZEN_PACKET\n"
                + packet_payload
                + "END_FROZEN_PACKET"
            )
            command = [
                opencode_binary,
                "run",
                "--pure",
                "--dir",
                str(scratch),
                "--model",
                model,
                "--agent",
                agent_name,
                "--format",
                "json",
                "--title",
                f"pif-true-north-{stage}-{packet_sha[:10]}-{index}",
                user_message,
            ]
            started = time.monotonic()
            timed_out = False
            try:
                completed = subprocess.run(
                    command,
                    cwd=str(PROJECT_ROOT),
                    env=env,
                    text=True,
                    errors="replace",
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    timeout=timeout_seconds,
                    check=False,
                )
                stdout = completed.stdout
                stderr = completed.stderr
                exit_code = completed.returncode
            except subprocess.TimeoutExpired as exc:
                timed_out = True
                stdout = (
                    exc.stdout.decode(errors="replace")
                    if isinstance(exc.stdout, bytes)
                    else exc.stdout or ""
                )
                stderr = (
                    exc.stderr.decode(errors="replace")
                    if isinstance(exc.stderr, bytes)
                    else exc.stderr or ""
                )
                exit_code = None
            elapsed = round(time.monotonic() - started, 3)
            answer, finish, stream_count = _parse_opencode_stream(stdout)
            operational, fallback_reason = _is_operational_failure(
                timed_out=timed_out,
                exit_code=exit_code,
                stderr=stderr,
                stdout=stdout,
            )
            receipt = {
                "packet_sha256": packet_sha,
                "provider_model": model,
                "system_prompt_sha256": sha256_text(
                    system_prompt or BASE_SYSTEM_PROMPT
                ),
                "attempt": index * 10 + _semantic_retry_number,
                "operational_failure": operational,
                "fallback_reason": fallback_reason,
                "timed_out": timed_out,
                "exit_code": exit_code,
                "elapsed_seconds": elapsed,
                "stream_event_count": stream_count,
                "usage": _usage(finish),
                "stderr_sha256": _sha256_bytes(stderr.encode("utf-8")),
            }
            receipts.append(receipt)
            _write_text(
                output_dir / f"attempt-{index}.private.jsonl", stdout
            )
            _write_text(output_dir / f"attempt-{index}.stderr.private.txt", stderr)
            if operational:
                continue
            if exit_code != 0 or timed_out:
                raise TrueNorthError(
                    f"non-operational OpenCode failure from {model}: exit={exit_code}"
                )
            try:
                output = _decode_json_answer(answer)
                if validator is None:
                    _validate_atomic_output(output, packet)
                else:
                    validator(output, packet)
            except Exception as exc:
                # Invalid semantics or schema is deliberately not a provider
                # fallback. Give the same provider one fresh attempt, retaining
                # the invalid response as evidence.
                receipt["semantic_validation_error"] = (
                    f"{type(exc).__name__}: {str(exc)[:4000]}"
                )
                if _semantic_retry_remaining <= 0:
                    raise
                receipt["semantic_retry_triggered"] = True
                failed_stdout = (
                    output_dir / f"semantic-failure-{index}.private.jsonl"
                )
                failed_stderr = (
                    output_dir / f"semantic-failure-{index}.stderr.private.txt"
                )
                checkpoint_path = output_dir / f"attempt-{index}.private.jsonl"
                checkpoint_stderr_path = (
                    output_dir / f"attempt-{index}.stderr.private.txt"
                )
                checkpoint_path.replace(failed_stdout)
                if checkpoint_stderr_path.exists():
                    checkpoint_stderr_path.replace(failed_stderr)
                retry_output, retry_receipts, retry_model = _run_opencode_packet(
                    packet_path=packet_path,
                    output_dir=output_dir,
                    models=models,
                    stage=stage,
                    timeout_seconds=timeout_seconds,
                    opencode_binary=opencode_binary,
                    system_prompt=system_prompt,
                    validator=validator,
                    _semantic_retry_remaining=_semantic_retry_remaining - 1,
                    _semantic_retry_feedback=receipt[
                        "semantic_validation_error"
                    ],
                    _semantic_retry_number=_semantic_retry_number + 1,
                )
                return (
                    retry_output,
                    [receipt, *retry_receipts],
                    retry_model,
                )
            output_path = output_dir / "validated.private.json"
            _write_json(output_path, output, immutable=False)
            receipt["output_sha256"] = _sha256_file(output_path)
            return output, receipts, model
    raise TrueNorthError("all configured providers failed operationally")


def _clone_shadow_database(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        raise TrueNorthError(f"run shadow database already exists: {destination}")
    source_conn = _readonly_connect(source)
    target_conn = sqlite3.connect(destination)
    try:
        source_conn.backup(target_conn)
        target_conn.executescript(TRUE_NORTH_SCHEMA)
        target_conn.commit()
    finally:
        target_conn.close()
        source_conn.close()


def _restore_atomic_checkpoint(
    *,
    suite_root: Path,
    current_run_root: Path,
    job_path: Path,
    output_dir: Path,
    stage: str = "atomic",
) -> bool:
    packet_sha = _sha256_file(job_path)
    for prior_run in sorted(
        (suite_root / "runs").glob("tnrun_*"),
        key=lambda path: path.stat().st_mtime_ns,
        reverse=True,
    ):
        if prior_run == current_run_root:
            continue
        prior_job = prior_run / stage / "jobs" / job_path.name
        prior_output = (
            prior_run
            / stage
            / "outputs"
            / job_path.name.removesuffix(".private.json")
        )
        if (
            not prior_job.is_file()
            or _sha256_file(prior_job) != packet_sha
            or not (prior_output / "validated.private.json").is_file()
            or not (prior_output / "attempt-1.private.jsonl").is_file()
        ):
            continue
        output_dir.mkdir(parents=True, exist_ok=True)
        for source in prior_output.glob("attempt-*.private.jsonl"):
            shutil.copy2(source, output_dir / source.name)
        for source in prior_output.glob("attempt-*.stderr.private.txt"):
            shutil.copy2(source, output_dir / source.name)
        return True
    return False


def _semantic_job(packet: Mapping[str, Any]) -> dict[str, Any]:
    target = str(packet["target"])
    target_instruction = {
        "identities": (
            "Resolve grouped raw speaker mentions to canonical people only when the episode "
            "evidence and frozen episode speaker map support one identity. Use numbered speaker "
            "labels, roles, affiliations, and episode-local aliases from that map together; do "
            "not claim the map is absent when it is supplied. Preserve unknown when ambiguous. Direct speakers "
            "must not be replaced by people merely quoted or discussed. This is an immediately "
            "validated shadow adjudication: use accepted or merged for supported identities, "
            "unknown or rejected when unresolved; never return candidate judgments or people."
        ),
        "claims": (
            "Create stable issue subjects, specific proposition variants, and speaker positions. "
            "Reuse keys within this packet for genuinely shared subjects or propositions. Preserve "
            "scope, conditions, time, polarity, uncertainty, and useful singletons."
        ),
        "relations": (
            "Classify each supplied claim pair as equivalent, supports, contradicts, qualifies, "
            "orthogonal, or incomparable. Topical similarity alone is not equivalence. State the "
            "temporal scope and preserve conditional differences."
        ),
    }[target]
    output_schema = json.loads(
        _canonical_json(semantic_reconcile.output_schema(target))
    )
    if target == "identities":
        identity_properties = output_schema["properties"]["decisions"][
            "properties"
        ]
        identity_properties["people"]["items"]["properties"]["decision"]["enum"] = [
            "accepted",
            "rejected",
            "merged",
        ]
        identity_properties["judgments"]["items"]["properties"]["decision"][
            "enum"
        ] = ["accepted", "rejected", "merged", "unknown"]
    return {
        "schema_version": RUN_SCHEMA_VERSION,
        "suite_id": SUITE_ID,
        "task": f"Perform shadow-only {target} reconciliation.",
        "instructions": [
            target_instruction,
            "Use only the supplied private packet evidence.",
            "Account for every packet item exactly once: either return its decision or include its item_id in abstentions with a concrete reason.",
            "Return one strict JSON object matching output_schema and no Markdown.",
            "Do not use tools or external knowledge.",
        ],
        "output_schema": output_schema,
        "input": {"reconciliation_packet": packet},
    }


def _validate_semantic_output(
    output: Mapping[str, Any], job: Mapping[str, Any]
) -> None:
    packet = job["input"]["reconciliation_packet"]
    # These are transport bindings, not semantic judgments. Bind them from the
    # packet so a model cannot invent scope and need not transcribe long hashes.
    output["target"] = packet["target"]
    output["corpus_release_id"] = packet["corpus_release_id"]
    output["packet_sha256"] = packet["packet_sha256"]
    _validate_schema(job["output_schema"], output, path="$")
    packet_ids = {str(item["item_id"]) for item in packet["items"]}
    def bind_opaque_id(value: Any, allowed: set[str]) -> str:
        text = str(value)
        if text in allowed:
            return text
        ranked = sorted(
            (
                (difflib.SequenceMatcher(a=text, b=candidate).ratio(), candidate)
                for candidate in allowed
            ),
            reverse=True,
        )
        if not ranked or ranked[0][0] < 0.90:
            return text
        if len(ranked) > 1 and ranked[0][0] - ranked[1][0] < 0.05:
            return text
        return ranked[0][1]

    for abstention in output["abstentions"]:
        abstention["item_id"] = bind_opaque_id(abstention["item_id"], packet_ids)
    abstained_ids = {str(item["item_id"]) for item in output["abstentions"]}
    if len(abstained_ids) != len(output["abstentions"]) or not abstained_ids <= packet_ids:
        raise TrueNorthError("semantic abstentions are duplicated or outside packet scope")
    target = str(packet["target"])
    decisions = output["decisions"]
    if target == "identities":
        judgments = list(decisions.get("judgments", []))
        people = list(decisions.get("people", []))
        person_keys = [str(person["person_key"]) for person in people]
        if len(person_keys) != len(set(person_keys)):
            raise TrueNorthError("identity people contain duplicate person_key values")
        for judgment in judgments:
            judgment["item_id"] = bind_opaque_id(judgment["item_id"], packet_ids)
            if (
                str(judgment.get("decision")) in {"accepted", "merged"}
                and str(judgment.get("person_key") or "") not in set(person_keys)
            ):
                raise TrueNorthError(
                    "accepted identity judgment references an unknown person_key: "
                    f"item_id={judgment['item_id']} "
                    f"person_key={judgment.get('person_key')!r}; choose one of "
                    f"{sorted(person_keys)} or mark the item unknown"
                )
        if any(
            str(item.get("decision")) == "candidate"
            for collection in ("people", "judgments")
            for item in decisions.get(collection, [])
        ):
            raise TrueNorthError(
                "accepted identity stage cannot contain candidate decisions"
            )
        decision_id_list = [str(item["item_id"]) for item in judgments]
        if len(decision_id_list) != len(set(decision_id_list)):
            duplicates = sorted(
                item_id
                for item_id in set(decision_id_list)
                if decision_id_list.count(item_id) > 1
            )
            raise TrueNorthError(
                f"identity judgments duplicate exact item_id values: {duplicates}"
            )
        decided_ids = set(decision_id_list)
    elif target == "claims":
        subject_keys = {
            str(item["subject_key"]) for item in decisions.get("subjects", [])
        }
        variant_keys = {
            str(item["variant_key"]) for item in decisions.get("variants", [])
        }
        for variant in decisions.get("variants", []):
            variant["subject_key"] = bind_opaque_id(
                variant["subject_key"], subject_keys
            )
        for position in decisions.get("positions", []):
            position["atomic_claim_id"] = bind_opaque_id(
                position["atomic_claim_id"], packet_ids
            )
            position["subject_key"] = bind_opaque_id(
                position["subject_key"], subject_keys
            )
            position["variant_key"] = bind_opaque_id(
                position["variant_key"], variant_keys
            )
        position_id_list = [
            str(item["atomic_claim_id"])
            for item in decisions.get("positions", [])
        ]
        if len(position_id_list) != len(set(position_id_list)):
            duplicates = sorted(
                item_id
                for item_id in set(position_id_list)
                if position_id_list.count(item_id) > 1
            )
            raise TrueNorthError(
                f"claim positions duplicate atomic_claim_id values: {duplicates}"
            )
        position_ids = set(position_id_list)
        outside = position_ids - packet_ids
        missing_before_abstention = packet_ids - position_ids - abstained_ids
        if len(outside) == 1 and len(missing_before_abstention) == 1:
            outside_id = next(iter(outside))
            missing_id = next(iter(missing_before_abstention))
            for position in decisions.get("positions", []):
                if str(position["atomic_claim_id"]) == outside_id:
                    position["atomic_claim_id"] = missing_id
        decided_ids = {
            str(item["atomic_claim_id"]) for item in decisions.get("positions", [])
        }
    elif target == "relations":
        allowed_claim_ids = {
            str(item["source"]["claim_id"]) for item in packet["items"]
        } | {
            str(item["target"]["claim_id"]) for item in packet["items"]
        }
        for relation in decisions:
            relation["source_claim_id"] = bind_opaque_id(
                relation["source_claim_id"], allowed_claim_ids
            )
            relation["target_claim_id"] = bind_opaque_id(
                relation["target_claim_id"], allowed_claim_ids
            )
        pair_to_item = {
            frozenset(
                (str(item["source"]["claim_id"]), str(item["target"]["claim_id"]))
            ): str(item["item_id"])
            for item in packet["items"]
        }
        relation_id_list = [
            pair_to_item.get(
                frozenset(
                    (str(item["source_claim_id"]), str(item["target_claim_id"]))
                ),
                "",
            )
            for item in decisions
        ]
        nonempty_relation_ids = [value for value in relation_id_list if value]
        if len(nonempty_relation_ids) != len(set(nonempty_relation_ids)):
            duplicates = sorted(
                item_id
                for item_id in set(nonempty_relation_ids)
                if nonempty_relation_ids.count(item_id) > 1
            )
            raise TrueNorthError(
                f"relation decisions duplicate packet item IDs: {duplicates}"
            )
        decided_ids = set(relation_id_list)
        decided_ids.discard("")
    else:
        decided_ids = set()
    if decided_ids & abstained_ids:
        raise TrueNorthError("semantic item is both decided and abstained")
    missing = packet_ids - decided_ids - abstained_ids
    if missing:
        missing_ids = sorted(missing)
        raise TrueNorthError(
            "semantic output left "
            f"{len(missing_ids)} bounded packet items unaccounted; copy these "
            f"exact item_id values into decisions or abstentions: {missing_ids}"
        )


def _record_call_receipts(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    stage: str,
    receipts: Sequence[Mapping[str, Any]],
) -> None:
    created = now_iso()
    for receipt in receipts:
        receipt_id = stable_id(
            run_id,
            stage,
            str(receipt["packet_sha256"]),
            str(receipt["provider_model"]),
            str(receipt["attempt"]),
            prefix="tncr_",
        )
        conn.execute(
            """
            INSERT OR REPLACE INTO true_north_call_receipts
              (id, run_id, stage, packet_sha256, provider_model, attempt,
               operational_failure, timed_out, exit_code, elapsed_seconds,
               usage_json, fallback_reason, output_sha256, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                receipt_id,
                run_id,
                stage,
                receipt["packet_sha256"],
                receipt["provider_model"],
                int(receipt["attempt"]),
                int(bool(receipt["operational_failure"])),
                int(bool(receipt["timed_out"])),
                receipt["exit_code"],
                float(receipt["elapsed_seconds"]),
                _canonical_json(receipt.get("usage") or {}),
                receipt.get("fallback_reason"),
                receipt.get("output_sha256"),
                created,
            ),
        )


def _insert_atomic_ledger(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    bundle: Mapping[str, Any],
    output: Mapping[str, Any],
    producer_model: str,
    atomic_ids_by_candidate: Mapping[str, Sequence[str]],
) -> None:
    episode_id = str(bundle["episode"]["episode_id"])
    created = now_iso()
    for item in output["items"]:
        candidate_id = str(item["candidate_id"])
        disposition = str(item["disposition"])
        if disposition == "reject":
            category = "rejected_junk"
        elif disposition == "hold":
            category = "held_needs_review"
        elif disposition == "revise":
            category = "revised_to_valid"
        else:
            category = "retained_supported_singleton"
        conn.execute(
            """
            INSERT OR REPLACE INTO true_north_stage_ledger
              (run_id, candidate_id, episode_id, stage, category, reason_code,
               producer_model, atomic_claim_ids_json, details_json, created_at)
            VALUES (?, ?, ?, 'atomic', ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                candidate_id,
                episode_id,
                category,
                str(item["reason_code"]),
                producer_model,
                _canonical_json(list(atomic_ids_by_candidate.get(candidate_id, ()))),
                _canonical_json(
                    {
                        "disposition": disposition,
                        "atomic_claim_count": len(item["atomic_claims"]),
                    }
                ),
                created,
            ),
        )


def _atomic_claim_payloads(
    *,
    outputs: Sequence[tuple[Mapping[str, Any], Mapping[str, Any], str]],
    run_id: str,
) -> tuple[list[dict[str, Any]], dict[str, list[str]], dict[str, str]]:
    claims: list[dict[str, Any]] = []
    ids_by_candidate: dict[str, list[str]] = defaultdict(list)
    evidence_units: dict[str, str] = {}
    prompt_sha = sha256_text("\n".join(_atomic_instructions(gold=False)))
    for bundle, output, producer_model in outputs:
        segments = {
            str(row["segment_id"]): str(row["text"]) for row in bundle["segments"]
        }
        evidence_units.update(segments)
        candidates = {
            str(row["candidate_id"]): row for row in bundle["candidates"]
        }
        for item in output["items"]:
            if item["disposition"] not in {"retain", "revise"}:
                continue
            candidate_id = str(item["candidate_id"])
            candidate = candidates[candidate_id]
            for index, atomic in enumerate(item["atomic_claims"]):
                lineage_id = stable_id(
                    run_id, candidate_id, str(index), prefix="aclin_tn_"
                )
                claim_id = stable_id(
                    lineage_id,
                    atomic["claim_text"],
                    atomic["evidence_text"],
                    prefix="ac_tn_",
                )
                ids_by_candidate[candidate_id].append(claim_id)
                claims.append(
                    {
                        "id": claim_id,
                        "claim_lineage_id": lineage_id,
                        "revision": 1,
                        "claim_text": atomic["claim_text"],
                        "claim_type": atomic["claim_type"],
                        "raw_speaker": atomic["raw_speaker"],
                        "stance": atomic["stance"],
                        "certainty": atomic["certainty"],
                        "time_horizon": atomic["time_horizon"],
                        "discourse_event_id": candidate_id,
                        "segment_id": candidate["segment_id"],
                        "source_id": None,
                        "episode_id": bundle["episode"]["episode_id"],
                        "evidence_unit_type": "segment",
                        "evidence_unit_id": candidate["segment_id"],
                        "evidence_text": atomic["evidence_text"],
                        "evidence_start": atomic["evidence_start"],
                        "evidence_end": atomic["evidence_end"],
                        "extractor_model": producer_model,
                        "extractor_schema": WORK_OUTPUT_SCHEMA_VERSION,
                        "extractor_schema_version": "1",
                        "extractor_prompt_sha256": prompt_sha,
                        "source_artifact_sha256": bundle["bundle_sha256"],
                        "provenance": {
                            "suite_id": SUITE_ID,
                            "benchmark_run_id": run_id,
                            "candidate_id": candidate_id,
                            "candidate_disposition": item["disposition"],
                            "atomic_index": index,
                            "reported_actor": atomic["reported_actor"],
                            "subject_hint": atomic["subject_text"],
                            "proposition_hint": atomic["proposition_text"],
                            "workhorse_model": producer_model,
                        },
                        "confidence": atomic["confidence"],
                        "review_status": "accepted",
                        "reviewed_by_model": "true-north-shadow-provisional",
                    }
                )
    return claims, ids_by_candidate, evidence_units


def _seed_raw_speaker_mentions(
    conn: sqlite3.Connection,
    *,
    outputs: Sequence[tuple[Mapping[str, Any], Mapping[str, Any], str]],
) -> int:
    inserted = 0
    created = now_iso()
    for bundle, output, _producer_model in outputs:
        candidates = {
            str(row["candidate_id"]): row for row in bundle["candidates"]
        }
        for item in output["items"]:
            candidate_id = str(item["candidate_id"])
            if item["disposition"] not in {"retain", "revise"}:
                continue
            candidate = candidates[candidate_id]
            speakers = {
                (
                    str(atomic["raw_speaker"]),
                    str(candidate["speaker"].get("role") or "unknown"),
                    str(candidate["speaker"].get("affiliation") or ""),
                )
                for atomic in item["atomic_claims"]
            }
            for surface, role, affiliation in sorted(speakers):
                mention_id = stable_id(
                    bundle["episode"]["episode_id"],
                    candidate_id,
                    surface,
                    role,
                    affiliation,
                    prefix="rsm_tn_",
                )
                before = conn.total_changes
                conn.execute(
                    """
                    INSERT OR IGNORE INTO raw_speaker_mentions
                      (id, episode_id, segment_id, discourse_event_id, surface_name,
                       role, affiliation_surface, resolution_status, confidence,
                       evidence_json, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 'unresolved', ?, ?, ?)
                    """,
                    (
                        mention_id,
                        bundle["episode"]["episode_id"],
                        candidate["segment_id"],
                        candidate_id,
                        surface,
                        role,
                        affiliation or None,
                        max(
                            float(atomic["confidence"])
                            for atomic in item["atomic_claims"]
                            if str(atomic["raw_speaker"]) == surface
                        ),
                        _canonical_json(
                            {
                                "candidate_id": candidate_id,
                                "exact_evidence_text": candidate["evidence_text"],
                            }
                        ),
                        created,
                    ),
                )
                inserted += int(conn.total_changes > before)
    return inserted


def _promote_shadow_release(
    conn: sqlite3.Connection,
    *,
    release_id: str,
    atomic_run_id: str,
) -> dict[str, Any]:
    existing = conn.execute(
        "SELECT * FROM corpus_release_promotions ORDER BY promotion_revision DESC LIMIT 1"
    ).fetchone()
    if existing is not None:
        return dict(existing)
    return intelligence.promote_corpus_release(
        conn,
        release_id,
        pipeline_run_id=atomic_run_id,
        promoted_by="true-north-shadow-orchestrator",
        rationale="Isolated benchmark release; never production authority.",
    )


def _reconcile_stage(
    conn: sqlite3.Connection,
    *,
    release_id: str,
    run_id: str,
    run_root: Path,
    target: str,
    models: Sequence[str],
    timeout_seconds: int,
    opencode_binary: str,
    max_batches: int,
) -> dict[str, Any]:
    batches: list[dict[str, Any]] = []
    abstained_ids: set[str] = set()
    for prior_output in run_root.glob(
        f"{target}/batch-*/candidate-output.private.json"
    ):
        prior = _read_json(prior_output)
        abstained_ids.update(
            str(item["item_id"]) for item in prior.get("abstentions", [])
        )
    for batch_index in range(max_batches):
        batch_root = run_root / target / f"batch-{batch_index + 1:03d}"
        job_path = batch_root / "job.private.json"
        if job_path.is_file():
            job = _read_json(job_path)
            packet = dict(job["input"]["reconciliation_packet"])
            checkpoint_packet_path = batch_root / "packet" / "checkpoint.private.json"
            _write_json(checkpoint_packet_path, packet, immutable=False)
            receipt = {"packet_path": str(checkpoint_packet_path)}
        else:
            batch_limit = {"identities": 50, "claims": 24, "relations": 24}[target]
            receipt = semantic_reconcile.prepare_reconciliation_packet(
                conn,
                target=target,
                release_id=release_id,
                limit=200,
                scope="all",
                output_dir=batch_root / "packet",
            )
            packet = _read_json(Path(receipt["packet_path"]))
            packet["items"] = [
                item
                for item in packet["items"]
                if str(item["item_id"]) not in abstained_ids
            ][:batch_limit]
            packet["item_count"] = len(packet["items"])
            packet["limit"] = batch_limit
            packet.pop("packet_sha256", None)
            packet["packet_sha256"] = sha256_text(dumps_json(packet))
            checkpoint_packet_path = batch_root / "packet" / "checkpoint.private.json"
            _write_json(checkpoint_packet_path, packet, immutable=False)
            receipt = {"packet_path": str(checkpoint_packet_path)}
            job = _semantic_job(packet)
            if target == "identities":
                suite_root = run_root.parent.parent
                manifest = _read_json(suite_root / "manifest.json")
                episode_ids = {
                    str(item["episode"]["id"]) for item in packet["items"]
                }
                contexts = []
                for bundle_record in manifest["bundles"]:
                    if str(bundle_record["episode_id"]) not in episode_ids:
                        continue
                    bundle = _read_json(Path(bundle_record["bundle_path"]))
                    contexts.append(
                        {
                            "episode": bundle["episode"],
                            "speaker_map": bundle["episode_context"].get(
                                "speaker_map"
                            )
                            or [],
                            "entity_seed": bundle["episode_context"].get(
                                "entity_seed"
                            )
                            or {},
                        }
                    )
                job["input"]["frozen_episode_contexts"] = contexts
        if int(packet["item_count"]) == 0:
            return {
                "target": target,
                "complete": True,
                "batch_count": len(batches),
                "processed_items": sum(row["item_count"] for row in batches),
                "batches": batches,
            }
        _write_json(job_path, job, immutable=True)
        output, call_receipts, producer_model = _run_opencode_packet(
            packet_path=job_path,
            output_dir=batch_root / "model",
            models=models,
            stage=target,
            timeout_seconds=timeout_seconds,
            opencode_binary=opencode_binary,
            validator=_validate_semantic_output,
        )
        output_path = batch_root / "candidate-output.private.json"
        _write_json(output_path, output, immutable=False)
        abstained_ids.update(
            str(item["item_id"]) for item in output.get("abstentions", [])
        )
        imported = semantic_reconcile.import_reconciliation_output(
            conn,
            packet_path=receipt["packet_path"],
            output_path=output_path,
            accept=True,
            reviewer="true-north-shadow-provisional",
            producer_model=producer_model,
            producer_model_version=producer_model,
        )
        _record_call_receipts(
            conn,
            run_id=run_id,
            stage=target,
            receipts=call_receipts,
        )
        conn.commit()
        batches.append(
            {
                "batch": batch_index + 1,
                "item_count": int(packet["item_count"]),
                "producer_model": producer_model,
                "pipeline_run_id": imported["pipeline_run_id"],
                "output_count": imported["output_count"],
                "idempotent_replay": imported["idempotent_replay"],
            }
        )
    return {
        "target": target,
        "complete": False,
        "batch_count": len(batches),
        "processed_items": sum(row["item_count"] for row in batches),
        "batches": batches,
        "error": "max_batches_exhausted",
    }


def _finalize_ledger(conn: sqlite3.Connection, *, run_id: str) -> dict[str, int]:
    rows = conn.execute(
        """
        SELECT ledger.candidate_id,
               COUNT(DISTINCT positions.subject_id) AS subject_count,
               COUNT(DISTINCT CASE
                 WHEN peer_claim.episode_id <> claim.episode_id THEN peer_claim.episode_id
               END) AS cross_episode_peers,
               COUNT(DISTINCT relations.id) AS relation_count
        FROM true_north_stage_ledger AS ledger
        LEFT JOIN json_each(ledger.atomic_claim_ids_json) AS ids
        LEFT JOIN atomic_claims AS claim ON claim.id = ids.value
        LEFT JOIN current_accepted_position_observations AS positions
          ON positions.atomic_claim_id = claim.id
        LEFT JOIN current_accepted_position_observations AS peer_positions
          ON peer_positions.subject_id = positions.subject_id
        LEFT JOIN atomic_claims AS peer_claim
          ON peer_claim.id = peer_positions.atomic_claim_id
        LEFT JOIN current_accepted_claim_relations AS relations
          ON relations.source_claim_id = claim.id OR relations.target_claim_id = claim.id
        WHERE ledger.run_id = ? AND ledger.stage = 'atomic'
        GROUP BY ledger.candidate_id
        """,
        (run_id,),
    ).fetchall()
    for row in rows:
        if int(row["cross_episode_peers"] or 0) > 0 or int(row["relation_count"] or 0) > 0:
            conn.execute(
                """
                UPDATE true_north_stage_ledger
                SET category = 'retained_canonical_member'
                WHERE run_id = ? AND candidate_id = ? AND stage = 'atomic'
                  AND category = 'retained_supported_singleton'
                """,
                (run_id, row["candidate_id"]),
            )
    counts = {
        str(row["category"]): int(row["count"])
        for row in conn.execute(
            """
            SELECT category, COUNT(*) AS count
            FROM true_north_stage_ledger
            WHERE run_id = ? AND stage = 'atomic'
            GROUP BY category
            """,
            (run_id,),
        )
    }
    return {category: counts.get(category, 0) for category in LEDGER_CATEGORIES}


def _development_gate_passed(base_shadow: Path) -> bool:
    # Holdout unlock evidence is stored beside the immutable suite, not inferred
    # from the current process or a model's self-assessment.
    gate_path = base_shadow.parent / "development-gate.json"
    if not gate_path.is_file():
        return False
    gate = _read_json(gate_path)
    return bool(
        gate.get("passed")
        and int(gate.get("consecutive_passes") or 0) >= 2
        and gate.get("configuration_sha256")
    )


def _run_utility_schema(
    claim_ids: Sequence[str],
    question_ids: Sequence[str] | None = None,
) -> dict[str, Any]:
    scoped_question_ids = list(question_ids or [row[0] for row in UTILITY_QUESTIONS])
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["schema_version", "items"],
        "properties": {
            "schema_version": {
                "type": "string",
                "const": "pif_true_north_utility_output_v1",
            },
            "items": {
                "type": "array",
                "minItems": len(scoped_question_ids),
                "maxItems": len(scoped_question_ids),
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": [
                        "question_id",
                        "answerable",
                        "answer",
                        "support_atomic_claim_ids",
                    ],
                    "properties": {
                        "question_id": {
                            "type": "string",
                            "enum": scoped_question_ids,
                        },
                        "answerable": {"type": "boolean"},
                        "answer": {"type": "string"},
                        "support_atomic_claim_ids": {
                            "type": "array",
                            "items": {"type": "string", "enum": list(claim_ids)},
                        },
                    },
                },
            },
        },
    }


def _downstream_runtime_sha256() -> str:
    return sha256_text(
        dumps_json(
            {
                "atomic_instructions": _atomic_instructions(gold=False),
                "semantic_jobs": {
                    target: {
                        key: _semantic_job(
                            {
                                "target": target,
                                "corpus_release_id": "release-sentinel",
                                "packet_sha256": "0" * 64,
                            }
                        )[key]
                        for key in ("task", "instructions", "output_schema")
                    }
                    for target in ("identities", "claims", "relations")
                },
                "canonical_map_contract": "global_subject_then_subject_preserving_proposition_v1",
                "relation_selection": "same_global_canonical_subject_all_pairs_v1",
                "utility_schema": _run_utility_schema(["claim-sentinel"]),
                "utility_questions": UTILITY_QUESTIONS,
            }
        )
    )


def _run_utility_packet(
    conn: sqlite3.Connection,
    *,
    release_id: str,
    run_id: str,
    run_root: Path,
    models: Sequence[str],
    timeout_seconds: int,
    opencode_binary: str,
) -> tuple[dict[str, Any], list[dict[str, Any]], str]:
    rows = conn.execute(
        """
        SELECT claims.id, claims.claim_text, claims.raw_speaker, claims.episode_id,
               claims.provenance_json,
               subjects.subject_text, variants.proposition_text,
               positions.position, people.display_name,
               canonical.canonical_subject_key,
               canonical.canonical_proposition_key
        FROM atomic_claims AS claims
        LEFT JOIN accepted_position_observations AS positions
          ON positions.atomic_claim_id = claims.id
         AND positions.review_status = 'accepted'
         AND positions.corpus_release_id = claims.corpus_release_id
        LEFT JOIN accepted_claim_subjects AS subjects
          ON subjects.id = positions.subject_id
        LEFT JOIN accepted_proposition_variants AS variants
          ON variants.id = positions.variant_id
        LEFT JOIN canonical_people AS people
          ON people.id = positions.canonical_person_id
        LEFT JOIN true_north_variant_canonical_map AS canonical
          ON canonical.variant_id = positions.variant_id
         AND canonical.run_id = ?
        WHERE claims.corpus_release_id = ?
          AND claims.review_status = 'accepted'
        ORDER BY claims.episode_id, claims.id
        """,
        (run_id, release_id),
    ).fetchall()
    claims = [
        {
            "atomic_claim_id": row["id"],
            "episode_id": row["episode_id"],
            "claim_text": row["claim_text"],
            "direct_speaker": row["display_name"] or row["raw_speaker"],
            "subject_text": row["canonical_subject_key"] or row["subject_text"],
            "proposition_text": (
                row["canonical_proposition_key"] or row["proposition_text"]
            ),
            "position": row["position"],
            "reported_actor": json.loads(row["provenance_json"] or "{}").get(
                "reported_actor"
            ),
        }
        for row in rows
    ]
    relation_rows = conn.execute(
        """
        SELECT source_claim_id, target_claim_id, relation
        FROM claim_relation_judgments
        WHERE corpus_release_id = ? AND review_status = 'accepted'
        ORDER BY source_claim_id, target_claim_id
        """,
        (release_id,),
    ).fetchall()
    generic_stopwords = {
        "about", "across", "after", "answer", "claims", "does", "episode",
        "episodes", "exact", "from", "identified", "made", "most", "podcast",
        "recurring", "shows", "speakers", "strongest", "supports", "their",
        "these", "what", "which", "with",
    }
    tokens_by_claim = {
        row["atomic_claim_id"]: set(
            _normalized_label(
                " ".join(
                    str(value or "")
                    for value in (
                        row["claim_text"],
                        row["subject_text"],
                        row["proposition_text"],
                        row["direct_speaker"],
                        row["reported_actor"],
                    )
                )
            ).split()
        )
        for row in claims
    }
    by_episode: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in claims:
        by_episode[str(row["episode_id"])].append(row)
    outputs: list[dict[str, Any]] = []
    receipts: list[dict[str, Any]] = []
    producer_models: list[str] = []
    utility_work: list[tuple[str, Path, Path]] = []
    for question_id, question in UTILITY_QUESTIONS:
        question_tokens = {
            token
            for token in _normalized_label(question).split()
            if len(token) > 3 and token not in generic_stopwords
        }
        selected: dict[str, dict[str, Any]] = {}
        for episode_id in sorted(by_episode):
            ranked = sorted(
                by_episode[episode_id],
                key=lambda row: (
                    len(question_tokens & tokens_by_claim[row["atomic_claim_id"]]),
                    row["atomic_claim_id"],
                ),
                reverse=True,
            )
            for row in ranked[:30]:
                selected[str(row["atomic_claim_id"])] = row
        if question_id == "provenance_04":
            for row in claims:
                if row.get("reported_actor"):
                    selected[str(row["atomic_claim_id"])] = row
        selected_items = list(selected.values())[:200]
        selected_ids = {
            str(row["atomic_claim_id"]) for row in selected_items
        }
        schema = _run_utility_schema(sorted(selected_ids), [question_id])
        packet = {
            "schema_version": "pif_true_north_utility_job_v1",
            "suite_id": SUITE_ID,
            "task": "Answer one fixed research-utility question from a deterministic cross-episode retrieval packet.",
            "instructions": [
                "Return exactly one item for the supplied question_id.",
                "Use only the supplied claims and relations; never use external knowledge.",
                "Cite every supporting atomic_claim_id and no unsupported ID.",
                "Preserve direct-speaker identity and visible contradiction or qualification.",
                "Recurring and consensus answers require evidence from at least two episodes.",
                "Mark the question unanswerable if this packet does not substantively answer it.",
                "Return only strict JSON.",
            ],
            "output_schema": schema,
            "input": {
                "questions": [
                    {"question_id": question_id, "question": question}
                ],
                "atomic_claims": selected_items,
                "relations": [
                    dict(row)
                    for row in relation_rows
                    if str(row["source_claim_id"]) in selected_ids
                    and str(row["target_claim_id"]) in selected_ids
                ],
                "retrieval": {
                    "method": "top_30_lexical_per_episode_plus_question_invariants",
                    "complete_corpus_atomic_count": len(claims),
                },
            },
        }
        packet_path = run_root / "utility" / "jobs" / f"{question_id}.private.json"
        _write_json(packet_path, packet, immutable=True)
        utility_work.append(
            (
                question_id,
                packet_path,
                run_root / "utility" / "outputs" / question_id,
            )
        )

    def execute_utility(
        work: tuple[str, Path, Path],
    ) -> tuple[
        str,
        Mapping[str, Any],
        list[dict[str, Any]],
        str,
    ]:
        question_id, packet_path, output_dir = work
        output, packet_receipts, producer_model = _run_opencode_packet(
            packet_path=packet_path,
            output_dir=output_dir,
            models=models,
            stage="utility",
            timeout_seconds=timeout_seconds,
            opencode_binary=opencode_binary,
            validator=_validate_run_utility,
        )
        return question_id, output, packet_receipts, producer_model

    utility_results = []
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=SEMANTIC_WORKERS
    ) as executor:
        futures = [
            executor.submit(execute_utility, work) for work in utility_work
        ]
        for future in concurrent.futures.as_completed(futures):
            utility_results.append(future.result())
    for (
        _question_id,
        output,
        packet_receipts,
        producer_model,
    ) in sorted(utility_results, key=lambda row: row[0]):
        outputs.extend(output["items"])
        receipts.extend(packet_receipts)
        producer_models.append(producer_model)
    combined = {
        "schema_version": "pif_true_north_utility_output_v1",
        "items": sorted(outputs, key=lambda row: row["question_id"]),
    }
    _write_json(
        run_root / "utility" / "validated.private.json",
        combined,
        immutable=False,
    )
    models_used = sorted(set(producer_models))
    return combined, receipts, ",".join(models_used)


def _validate_subject_map_output(
    output: Mapping[str, Any], packet: Mapping[str, Any]
) -> None:
    _validate_scoped_items(output, packet, id_field="subject_id")


def _validate_variant_map_output(
    output: Mapping[str, Any], packet: Mapping[str, Any]
) -> None:
    _validate_scoped_items(output, packet, id_field="variant_id")


def _canonicalize_committed_claims(
    conn: sqlite3.Connection,
    *,
    release_id: str,
    run_id: str,
    run_root: Path,
    models: Sequence[str],
    timeout_seconds: int,
    opencode_binary: str,
) -> dict[str, Any]:
    existing_subject_count = int(
        conn.execute(
            "SELECT COUNT(*) FROM true_north_subject_canonical_map WHERE run_id = ?",
            (run_id,),
        ).fetchone()[0]
    )
    existing_variant_count = int(
        conn.execute(
            "SELECT COUNT(*) FROM true_north_variant_canonical_map WHERE run_id = ?",
            (run_id,),
        ).fetchone()[0]
    )
    if existing_subject_count and existing_variant_count:
        return {
            "target": "canonical-map",
            "complete": True,
            "batch_count": 0,
            "processed_items": existing_subject_count + existing_variant_count,
            "idempotent_replay": True,
        }
    subject_rows = conn.execute(
        """
        SELECT subjects.id, subjects.subject_text, subjects.subject_type,
               subjects.domain, subjects.scope_note,
               claims.claim_text, claims.episode_id
        FROM accepted_claim_subjects AS subjects
        JOIN accepted_position_observations AS positions
          ON positions.subject_id = subjects.id
         AND positions.review_status = 'accepted'
        JOIN atomic_claims AS claims ON claims.id = positions.atomic_claim_id
        WHERE subjects.corpus_release_id = ?
          AND subjects.review_status = 'accepted'
        ORDER BY subjects.id, claims.id
        """,
        (release_id,),
    ).fetchall()
    subjects: dict[str, dict[str, Any]] = {}
    for row in subject_rows:
        subject = subjects.setdefault(
            str(row["id"]),
            {
                "subject_id": str(row["id"]),
                "subject_text": row["subject_text"],
                "subject_type": row["subject_type"],
                "domain": row["domain"],
                "scope_note": row["scope_note"],
                "episode_ids": [],
                "example_claims": [],
            },
        )
        if row["episode_id"] not in subject["episode_ids"]:
            subject["episode_ids"].append(row["episode_id"])
        if len(subject["example_claims"]) < 2:
            subject["example_claims"].append(row["claim_text"])
    subject_items = [subjects[key] for key in sorted(subjects)]
    if not subject_items:
        raise TrueNorthError("canonical mapping requires committed claim subjects")
    subject_schema = _compact_mapping_schema(
        schema_version="pif_true_north_subject_map_v1",
        id_field="subject_id",
        ids=[row["subject_id"] for row in subject_items],
        value_field="canonical_subject_key",
    )
    subject_job = {
        "schema_version": "pif_true_north_subject_map_job_v1",
        "suite_id": SUITE_ID,
        "task": "Reconcile all locally committed claim subjects across the complete benchmark partition.",
        "instructions": [
            "Return exactly one mapping for every subject_id.",
            "Use the same deterministic snake_case canonical_subject_key only for the same stable issue or question across episodes and batches.",
            "Preserve actor, deployment, jurisdiction, time, and scope distinctions.",
            "Do not merge generic safety, governance, risk, agent, or Anthropic mentions.",
            "Keep useful singletons with unique keys. Return only strict JSON.",
        ],
        "output_schema": subject_schema,
        "input": {"items": subject_items},
    }
    subject_job_path = run_root / "canonical-map" / "jobs" / "subjects.private.json"
    _write_json(subject_job_path, subject_job, immutable=True)
    subject_output, subject_receipts, subject_model = _run_opencode_packet(
        packet_path=subject_job_path,
        output_dir=run_root / "canonical-map" / "outputs" / "subjects",
        models=models,
        stage="canonical-map-subjects",
        timeout_seconds=timeout_seconds,
        opencode_binary=opencode_binary,
        validator=_validate_subject_map_output,
    )
    subject_map = {
        str(row["subject_id"]): str(row["canonical_subject_key"])
        for row in subject_output["items"]
    }
    variant_rows = conn.execute(
        """
        SELECT variants.id, variants.subject_id, variants.proposition_text,
               variants.predicate_text, variants.object_text, variants.polarity,
               variants.time_horizon, variants.conditions_json,
               claims.claim_text, claims.episode_id
        FROM accepted_proposition_variants AS variants
        JOIN accepted_position_observations AS positions
          ON positions.variant_id = variants.id
         AND positions.review_status = 'accepted'
        JOIN atomic_claims AS claims ON claims.id = positions.atomic_claim_id
        WHERE variants.corpus_release_id = ?
          AND variants.review_status = 'accepted'
        ORDER BY variants.id, claims.id
        """,
        (release_id,),
    ).fetchall()
    variants: dict[str, dict[str, Any]] = {}
    for row in variant_rows:
        variant = variants.setdefault(
            str(row["id"]),
            {
                "variant_id": str(row["id"]),
                "subject_id": str(row["subject_id"]),
                "canonical_subject_key": subject_map[str(row["subject_id"])],
                "proposition_text": row["proposition_text"],
                "predicate_text": row["predicate_text"],
                "object_text": row["object_text"],
                "polarity": row["polarity"],
                "time_horizon": row["time_horizon"],
                "conditions": json.loads(row["conditions_json"] or "{}"),
                "episode_ids": [],
                "example_claims": [],
            },
        )
        if row["episode_id"] not in variant["episode_ids"]:
            variant["episode_ids"].append(row["episode_id"])
        if len(variant["example_claims"]) < 2:
            variant["example_claims"].append(row["claim_text"])
    by_subject: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for variant in variants.values():
        by_subject[str(variant["canonical_subject_key"])].append(variant)
    variant_chunks: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = []
    for subject_key in sorted(by_subject):
        group = sorted(by_subject[subject_key], key=lambda row: row["variant_id"])
        if current and len(current) + len(group) > 150:
            variant_chunks.append(current)
            current = []
        if len(group) > 150:
            for offset in range(0, len(group), 150):
                if current:
                    variant_chunks.append(current)
                    current = []
                variant_chunks.append(group[offset : offset + 150])
        else:
            current.extend(group)
    if current:
        variant_chunks.append(current)
    variant_outputs: list[dict[str, Any]] = []
    variant_receipts: list[dict[str, Any]] = []
    variant_models: dict[str, str] = {}
    variant_packet_hashes: dict[str, str] = {}
    for index, chunk in enumerate(variant_chunks, start=1):
        ids = [str(row["variant_id"]) for row in chunk]
        schema = _compact_mapping_schema(
            schema_version="pif_true_north_variant_map_v1",
            id_field="variant_id",
            ids=ids,
            value_field="canonical_proposition_key",
        )
        job = {
            "schema_version": "pif_true_north_variant_map_job_v1",
            "suite_id": SUITE_ID,
            "task": "Reconcile proposition variants that already share globally reconciled subjects.",
            "instructions": [
                "Return exactly one mapping for every variant_id.",
                "Use the same deterministic snake_case canonical_proposition_key only when truth conditions match.",
                "Preserve actor, polarity, condition, time, deployment scope, and jurisdiction.",
                "Keep qualifications distinct from equivalence and retain useful singletons.",
                "Return only strict JSON.",
            ],
            "output_schema": schema,
            "input": {"items": chunk},
        }
        name = f"variants-{index:03d}"
        job_path = run_root / "canonical-map" / "jobs" / f"{name}.private.json"
        _write_json(job_path, job, immutable=True)
        output, receipts, model = _run_opencode_packet(
            packet_path=job_path,
            output_dir=run_root / "canonical-map" / "outputs" / name,
            models=models,
            stage="canonical-map-variants",
            timeout_seconds=timeout_seconds,
            opencode_binary=opencode_binary,
            validator=_validate_variant_map_output,
        )
        variant_outputs.extend(output["items"])
        variant_receipts.extend(receipts)
        packet_hash = _sha256_file(job_path)
        for row in output["items"]:
            variant_models[str(row["variant_id"])] = model
            variant_packet_hashes[str(row["variant_id"])] = packet_hash
    created = now_iso()
    conn.execute("SAVEPOINT true_north_canonical_map")
    try:
        subject_packet_hash = _sha256_file(subject_job_path)
        conn.executemany(
            """
            INSERT INTO true_north_subject_canonical_map
              (run_id, subject_id, canonical_subject_key, producer_model,
               packet_sha256, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    run_id,
                    row["subject_id"],
                    row["canonical_subject_key"],
                    subject_model,
                    subject_packet_hash,
                    created,
                )
                for row in subject_output["items"]
            ],
        )
        conn.executemany(
            """
            INSERT INTO true_north_variant_canonical_map
              (run_id, variant_id, subject_id, canonical_subject_key,
               canonical_proposition_key, producer_model, packet_sha256,
               created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    run_id,
                    row["variant_id"],
                    str(variants[str(row["variant_id"])]["subject_id"]),
                    str(
                        variants[str(row["variant_id"])][
                            "canonical_subject_key"
                        ]
                    ),
                    row["canonical_proposition_key"],
                    variant_models[str(row["variant_id"])],
                    variant_packet_hashes[str(row["variant_id"])],
                    created,
                )
                for row in variant_outputs
            ],
        )
        conn.execute("RELEASE SAVEPOINT true_north_canonical_map")
    except Exception:
        conn.execute("ROLLBACK TO SAVEPOINT true_north_canonical_map")
        conn.execute("RELEASE SAVEPOINT true_north_canonical_map")
        raise
    _record_call_receipts(
        conn,
        run_id=run_id,
        stage="canonical-map",
        receipts=[*subject_receipts, *variant_receipts],
    )
    conn.commit()
    final = {
        "schema_version": "pif_true_north_canonical_map_v1",
        "run_id": run_id,
        "subjects": subject_output["items"],
        "variants": sorted(variant_outputs, key=lambda row: row["variant_id"]),
    }
    final["mapping_sha256"] = sha256_text(dumps_json(final))
    _write_json(
        run_root / "canonical-map" / "final.private.json",
        final,
        immutable=False,
    )
    return {
        "target": "canonical-map",
        "complete": True,
        "batch_count": 1 + len(variant_chunks),
        "processed_items": len(subject_items) + len(variant_outputs),
        "producer_model": ",".join(
            sorted({subject_model, *variant_models.values()})
        ),
    }


def _run_canonical_relations(
    conn: sqlite3.Connection,
    *,
    release_id: str,
    run_id: str,
    run_root: Path,
    models: Sequence[str],
    timeout_seconds: int,
    opencode_binary: str,
    max_batches: int,
) -> dict[str, Any]:
    rows = conn.execute(
        """
        SELECT claims.id AS claim_id, claims.claim_text, claims.evidence_text,
               claims.observed_at, positions.canonical_person_id,
               mapping.canonical_subject_key
        FROM accepted_position_observations AS positions
        JOIN atomic_claims AS claims ON claims.id = positions.atomic_claim_id
        JOIN true_north_variant_canonical_map AS mapping
          ON mapping.variant_id = positions.variant_id
         AND mapping.run_id = ?
        WHERE positions.corpus_release_id = ?
          AND positions.review_status = 'accepted'
        ORDER BY mapping.canonical_subject_key, claims.id
        """,
        (run_id, release_id),
    ).fetchall()
    by_subject: dict[str, list[sqlite3.Row]] = defaultdict(list)
    for row in rows:
        by_subject[str(row["canonical_subject_key"])].append(row)
    items: list[dict[str, Any]] = []
    for subject_key in sorted(by_subject):
        claims = by_subject[subject_key]
        for left_index, left in enumerate(claims):
            for right in claims[left_index + 1 :]:
                source_id, target_id = sorted(
                    (str(left["claim_id"]), str(right["claim_id"]))
                )
                source = left if str(left["claim_id"]) == source_id else right
                target = right if source is left else left
                items.append(
                    {
                        "item_id": stable_id(
                            run_id,
                            subject_key,
                            source_id,
                            target_id,
                            prefix="pair_tn_",
                        ),
                        "subject_id": subject_key,
                        "source": {
                            "claim_id": source_id,
                            "claim_text": source["claim_text"],
                            "exact_evidence_text": source["evidence_text"],
                            "canonical_person_id": source[
                                "canonical_person_id"
                            ],
                            "observed_at": source["observed_at"],
                        },
                        "target": {
                            "claim_id": target_id,
                            "claim_text": target["claim_text"],
                            "exact_evidence_text": target["evidence_text"],
                            "canonical_person_id": target[
                                "canonical_person_id"
                            ],
                            "observed_at": target["observed_at"],
                        },
                    }
                )
    if len(items) > max_batches * 24:
        raise TrueNorthError(
            f"canonical relation scope requires {len(items)} pairs, exceeding "
            f"the configured {max_batches * 24} bound"
        )
    relation_work: list[tuple[int, int, Path, Path, Path]] = []
    for offset in range(0, len(items), 24):
        batch_number = offset // 24 + 1
        chunk = items[offset : offset + 24]
        batch_root = run_root / "relations" / f"batch-{batch_number:03d}"
        packet = {
            "schema_version": semantic_reconcile.PACKET_SCHEMA_VERSION,
            "target": "relations",
            "corpus_release_id": release_id,
            "generated_at": now_iso(),
            "as_of": None,
            "scope": "all",
            "scope_as_of": now_iso(),
            "scope_start": None,
            "limit": len(chunk),
            "item_count": len(chunk),
            "semantic_authority": "managed_llm_only",
            "selection_authority": "true_north_global_canonical_subject_only",
            "items": chunk,
        }
        packet["packet_sha256"] = sha256_text(dumps_json(packet))
        packet_path = batch_root / "packet" / "checkpoint.private.json"
        _write_json(packet_path, packet, immutable=False)
        job = _semantic_job(packet)
        job_path = batch_root / "job.private.json"
        _write_json(job_path, job, immutable=True)
        relation_work.append(
            (
                batch_number,
                len(chunk),
                packet_path,
                job_path,
                batch_root,
            )
        )

    def execute_relation(
        work: tuple[int, int, Path, Path, Path],
    ) -> tuple[
        int,
        int,
        Path,
        Path,
        Mapping[str, Any],
        list[dict[str, Any]],
        str,
    ]:
        batch_number, item_count, packet_path, job_path, batch_root = work
        output, receipts, producer_model = _run_opencode_packet(
            packet_path=job_path,
            output_dir=batch_root / "model",
            models=models,
            stage="relations",
            timeout_seconds=timeout_seconds,
            opencode_binary=opencode_binary,
            validator=_validate_semantic_output,
        )
        output_path = batch_root / "candidate-output.private.json"
        _write_json(output_path, output, immutable=False)
        return (
            batch_number,
            item_count,
            packet_path,
            output_path,
            output,
            receipts,
            producer_model,
        )

    relation_results = []
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=SEMANTIC_WORKERS
    ) as executor:
        futures = [
            executor.submit(execute_relation, work)
            for work in relation_work
        ]
        for future in concurrent.futures.as_completed(futures):
            relation_results.append(future.result())
    batches = []
    for (
        batch_number,
        item_count,
        packet_path,
        output_path,
        _output,
        receipts,
        producer_model,
    ) in sorted(relation_results, key=lambda row: row[0]):
        imported = semantic_reconcile.import_reconciliation_output(
            conn,
            packet_path=packet_path,
            output_path=output_path,
            accept=True,
            reviewer="true-north-shadow-provisional",
            producer_model=producer_model,
            producer_model_version=producer_model,
        )
        _record_call_receipts(
            conn, run_id=run_id, stage="relations", receipts=receipts
        )
        conn.commit()
        batches.append(
            {
                "batch": batch_number,
                "item_count": item_count,
                "producer_model": producer_model,
                "pipeline_run_id": imported["pipeline_run_id"],
                "output_count": imported["output_count"],
                "idempotent_replay": imported["idempotent_replay"],
            }
        )
    return {
        "target": "relations",
        "complete": True,
        "batch_count": len(batches),
        "processed_items": len(items),
        "batches": batches,
    }


def run_benchmark(
    *,
    output_root: str | Path | None = None,
    suite: str = SUITE_ID,
    partition: str = "development",
    router: str = DEFAULT_ROUTER,
    timeout_seconds: int = 900,
    opencode_binary: str = "/opt/homebrew/bin/opencode",
    max_batches: int = 100,
    episode_limit: int | None = None,
    episode_ids: Sequence[str] | None = None,
) -> dict[str, Any]:
    if partition not in {"development", "holdout"}:
        raise TrueNorthError("partition must be development or holdout")
    if router not in ROUTERS:
        raise TrueNorthError(f"unknown router: {router}")
    suite_root = _suite_root(output_root, suite)
    manifest = _read_json(suite_root / "manifest.json")
    base_shadow = Path(manifest["shadow_database"])
    if partition == "holdout" and not _development_gate_passed(base_shadow):
        raise TrueNorthError(
            "holdout is sealed until two consecutive development passes share one configuration"
        )
    bundle_records = [
        row for row in manifest["bundles"] if row["partition"] == partition
    ]
    if episode_ids:
        requested = list(dict.fromkeys(str(value) for value in episode_ids))
        available = {str(row["episode_id"]) for row in bundle_records}
        unknown = sorted(set(requested) - available)
        if unknown:
            raise TrueNorthError(
                f"episode selection is outside the {partition} manifest: {unknown}"
            )
        by_id = {str(row["episode_id"]): row for row in bundle_records}
        bundle_records = [by_id[episode_id] for episode_id in requested]
    if episode_limit is not None:
        bundle_records = bundle_records[: max(0, int(episode_limit))]
    if not bundle_records:
        raise TrueNorthError("no benchmark episodes selected")
    configuration = {
        "suite_manifest_sha256": manifest["manifest_sha256"],
        "partition": partition,
        "router": router,
        "models": list(ROUTERS[router]),
        "prompt_version": WORKHORSE_PROMPT_VERSION,
        "output_schema_version": WORK_OUTPUT_SCHEMA_VERSION,
        "downstream_runtime_sha256": _downstream_runtime_sha256(),
        "episode_ids": [row["episode_id"] for row in bundle_records],
    }
    configuration_sha = sha256_text(dumps_json(configuration))
    run_id = stable_id(
        SUITE_ID,
        partition,
        configuration_sha,
        now_iso(),
        prefix="tnrun_",
    )
    run_root = suite_root / "runs" / run_id
    run_db_path = run_root / "shadow.sqlite"
    run_root.mkdir(parents=True, exist_ok=False)
    _clone_shadow_database(base_shadow, run_db_path)
    conn = db.connect(run_db_path)
    started = now_iso()
    all_outputs: list[tuple[Mapping[str, Any], Mapping[str, Any], str]] = []
    initial_atomic_receipts: list[dict[str, Any]] = []
    atomic_audit_receipts: list[dict[str, Any]] = []
    try:
        conn.execute(
            """
            INSERT INTO true_north_runs
              (run_id, schema_version, suite_id, partition_name, router,
               configuration_sha256, status, input_count, output_count,
               failure_count, receipts_json, started_at)
            VALUES (?, ?, ?, ?, ?, ?, 'running', ?, 0, 0, '[]', ?)
            """,
            (
                run_id,
                RUN_SCHEMA_VERSION,
                SUITE_ID,
                partition,
                router,
                configuration_sha,
                sum(int(row["candidate_count"]) for row in bundle_records),
                started,
            ),
        )
        for bundle_record in bundle_records:
            bundle = _read_json(Path(bundle_record["bundle_path"]))
            for job in _segment_jobs(bundle, gold=False):
                segment_id = str(job["input"]["segment"]["segment_id"])
                job_path = run_root / "atomic" / "jobs" / f"{segment_id}.private.json"
                _write_json(job_path, job, immutable=True)
                output_dir = run_root / "atomic" / "outputs" / segment_id
                _restore_atomic_checkpoint(
                    suite_root=suite_root,
                    current_run_root=run_root,
                    job_path=job_path,
                    output_dir=output_dir,
                )
                output, receipts, producer_model = _run_opencode_packet(
                    packet_path=job_path,
                    output_dir=output_dir,
                    models=ROUTERS[router],
                    stage="atomic",
                    timeout_seconds=timeout_seconds,
                    opencode_binary=opencode_binary,
                )
                all_outputs.append((bundle, output, producer_model))
                initial_atomic_receipts.extend(receipts)
        audit_work: list[
            tuple[int, Mapping[str, Any], Path, Path]
        ] = []
        for audit_index, (bundle, job) in enumerate(
            _atomic_audit_jobs(all_outputs),
            start=1,
        ):
            batch_name = f"batch-{audit_index:04d}"
            job_path = (
                run_root
                / "atomic-review"
                / "jobs"
                / f"{batch_name}.private.json"
            )
            _write_json(job_path, job, immutable=True)
            output_dir = run_root / "atomic-review" / "outputs" / batch_name
            _restore_atomic_checkpoint(
                suite_root=suite_root,
                current_run_root=run_root,
                job_path=job_path,
                output_dir=output_dir,
                stage="atomic-review",
            )
            audit_work.append((audit_index, bundle, job_path, output_dir))

        def execute_audit(
            work: tuple[int, Mapping[str, Any], Path, Path],
        ) -> tuple[
            int,
            Mapping[str, Any],
            Mapping[str, Any],
            list[dict[str, Any]],
            str,
        ]:
            audit_index, bundle, job_path, output_dir = work
            output, receipts, producer_model = _run_opencode_packet(
                packet_path=job_path,
                output_dir=output_dir,
                models=ROUTERS[router],
                stage="atomic-review",
                timeout_seconds=timeout_seconds,
                opencode_binary=opencode_binary,
            )
            return audit_index, bundle, output, receipts, producer_model

        audit_results = []
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=ATOMIC_AUDIT_WORKERS
        ) as executor:
            futures = [
                executor.submit(execute_audit, work) for work in audit_work
            ]
            for future in concurrent.futures.as_completed(futures):
                audit_results.append(future.result())
        audited_outputs: list[
            tuple[Mapping[str, Any], Mapping[str, Any], str]
        ] = []
        for (
            _audit_index,
            bundle,
            output,
            receipts,
            producer_model,
        ) in sorted(audit_results, key=lambda row: row[0]):
            audited_outputs.append((bundle, output, producer_model))
            atomic_audit_receipts.extend(receipts)
        all_outputs = audited_outputs
        claims, ids_by_candidate, evidence_units = _atomic_claim_payloads(
            outputs=all_outputs,
            run_id=run_id,
        )
        if not claims:
            raise TrueNorthError("workhorse retained no atomic claims")
        release_id = str(
            conn.execute(
                "SELECT release_id FROM true_north_suites WHERE suite_id = ?",
                (SUITE_ID,),
            ).fetchone()[0]
        )
        atomic_run_id = stable_id(
            release_id, run_id, configuration_sha, prefix="pir_tn_atomic_"
        )
        intelligence.create_pipeline_run(
            conn,
            run_id=atomic_run_id,
            # Use the existing atomic_claim_v1 import boundary so the real
            # transactional validator is exercised. Benchmark identity remains
            # explicit in parameters/receipt; no production allowlist is widened.
            run_type="atomic_claim_import",
            run_schema="atomic_claim_v1",
            run_schema_version="atomic_claim_import_v1",
            corpus_release_id=release_id,
            model="mixed" if len({row[2] for row in all_outputs}) > 1 else all_outputs[0][2],
            model_version="router:" + router,
            prompt_version=WORKHORSE_PROMPT_VERSION,
            status="running",
            parameters={
                "benchmark": True,
                "shadow_only": True,
                "configuration_sha256": configuration_sha,
                "actual_models": sorted({row[2] for row in all_outputs}),
            },
            receipt={"run_id": run_id, "private_artifact_root": str(run_root)},
            input_count=len(claims),
            input_sha256=sha256_text(dumps_json(claims)),
        )
        conn.execute("SAVEPOINT true_north_atomic_commit")
        try:
            atomic_result = intelligence.build_atomic_claims_from_release(
                conn,
                release_id,
                pipeline_run_id=atomic_run_id,
                claims=claims,
                evidence_units=evidence_units,
            )
            mention_count = _seed_raw_speaker_mentions(conn, outputs=all_outputs)
            intelligence.transition_pipeline_run(
                conn,
                atomic_run_id,
                status="succeeded",
                expected_status="running",
                input_count=len(claims),
                output_count=len(claims),
                failure_count=0,
                output_sha256=sha256_text(dumps_json(atomic_result["claim_ids"])),
                metrics={
                    "atomic_claim_count": len(claims),
                    "raw_speaker_mention_count": mention_count,
                },
                receipt={"benchmark_run_id": run_id, "shadow_only": True},
            )
            _promote_shadow_release(
                conn, release_id=release_id, atomic_run_id=atomic_run_id
            )
            conn.execute("RELEASE SAVEPOINT true_north_atomic_commit")
        except Exception:
            conn.execute("ROLLBACK TO SAVEPOINT true_north_atomic_commit")
            conn.execute("RELEASE SAVEPOINT true_north_atomic_commit")
            raise
        for bundle, output, producer_model in all_outputs:
            _insert_atomic_ledger(
                conn,
                run_id=run_id,
                bundle=bundle,
                output=output,
                producer_model=producer_model,
                atomic_ids_by_candidate=ids_by_candidate,
            )
        _record_call_receipts(
            conn,
            run_id=run_id,
            stage="atomic-initial",
            receipts=initial_atomic_receipts,
        )
        _record_call_receipts(
            conn,
            run_id=run_id,
            stage="atomic",
            receipts=atomic_audit_receipts,
        )
        conn.commit()
        stages = []
        for target in ("identities", "claims"):
            result = _reconcile_stage(
                conn,
                release_id=release_id,
                run_id=run_id,
                run_root=run_root,
                target=target,
                models=ROUTERS[router],
                timeout_seconds=timeout_seconds,
                opencode_binary=opencode_binary,
                max_batches=max_batches,
            )
            stages.append(result)
            if not result["complete"]:
                raise TrueNorthError(f"{target} reconciliation did not complete")
        canonical_result = _canonicalize_committed_claims(
            conn,
            release_id=release_id,
            run_id=run_id,
            run_root=run_root,
            models=ROUTERS[router],
            timeout_seconds=timeout_seconds,
            opencode_binary=opencode_binary,
        )
        stages.append(canonical_result)
        relation_result = _run_canonical_relations(
            conn,
            release_id=release_id,
            run_id=run_id,
            run_root=run_root,
            models=ROUTERS[router],
            timeout_seconds=timeout_seconds,
            opencode_binary=opencode_binary,
            max_batches=max_batches,
        )
        stages.append(relation_result)
        utility_output, utility_receipts, utility_model = _run_utility_packet(
            conn,
            release_id=release_id,
            run_id=run_id,
            run_root=run_root,
            models=ROUTERS[router],
            timeout_seconds=timeout_seconds,
            opencode_binary=opencode_binary,
        )
        _record_call_receipts(
            conn, run_id=run_id, stage="utility", receipts=utility_receipts
        )
        stages.append(
            {
                "target": "utility",
                "complete": True,
                "batch_count": len(UTILITY_QUESTIONS),
                "processed_items": len(utility_output["items"]),
                "producer_model": utility_model,
            }
        )
        ledger_counts = _finalize_ledger(conn, run_id=run_id)
        completed = now_iso()
        receipts = [
            {
                "stage": stage["target"],
                "batch_count": stage["batch_count"],
                "processed_items": stage["processed_items"],
            }
            for stage in stages
        ]
        conn.execute(
            """
            UPDATE true_north_runs
            SET status = 'succeeded', output_count = ?, failure_count = 0,
                receipts_json = ?, completed_at = ?
            WHERE run_id = ?
            """,
            (
                len(claims),
                _canonical_json(receipts),
                completed,
                run_id,
            ),
        )
        conn.commit()
        run_receipt = {
            "schema_version": RUN_SCHEMA_VERSION,
            "ok": True,
            "suite_id": SUITE_ID,
            "run_id": run_id,
            "partition": partition,
            "configuration": configuration,
            "configuration_sha256": configuration_sha,
            "run_database": str(run_db_path),
            "atomic_claim_count": len(claims),
            "ledger_counts": ledger_counts,
            "stages": stages,
            "started_at": started,
            "completed_at": completed,
            "production_mutation": False,
            "shadow_mutation": True,
        }
        run_receipt["receipt_sha256"] = sha256_text(dumps_json(run_receipt))
        receipt_path = run_root / "run-receipt.json"
        _write_json(receipt_path, run_receipt, immutable=True)
        return {**run_receipt, "receipt_path": str(receipt_path)}
    except BaseException as exc:
        conn.rollback()
        try:
            conn.execute(
                """
                UPDATE true_north_runs
                SET status = 'failed', failure_count = failure_count + 1,
                    completed_at = ?, error = ?
                WHERE run_id = ?
                """,
                (now_iso(), f"{type(exc).__name__}: {str(exc)[:500]}", run_id),
            )
            conn.commit()
        except Exception:
            conn.rollback()
        raise
    finally:
        conn.close()


def resume_benchmark(
    *,
    run_id: str,
    output_root: str | Path | None = None,
    suite: str = SUITE_ID,
    timeout_seconds: int = 900,
    opencode_binary: str = "/opt/homebrew/bin/opencode",
    max_batches: int = 100,
) -> dict[str, Any]:
    suite_root = _suite_root(output_root, suite)
    run_root = suite_root / "runs" / run_id
    run_db_path = run_root / "shadow.sqlite"
    if not run_db_path.is_file():
        raise TrueNorthError(f"resumable run database is missing: {run_id}")
    conn = db.connect(run_db_path)
    try:
        row = conn.execute(
            "SELECT * FROM true_north_runs WHERE run_id = ?", (run_id,)
        ).fetchone()
        if row is None:
            raise TrueNorthError(f"true-north run is missing: {run_id}")
        if row["status"] == "succeeded":
            receipt_path = run_root / "run-receipt.json"
            result = _read_json(receipt_path)
            return {**result, "receipt_path": str(receipt_path), "idempotent_replay": True}
        router = str(row["router"])
        if router not in ROUTERS:
            raise TrueNorthError(f"run has an unsupported router: {router}")
        release_row = conn.execute(
            "SELECT release_id FROM true_north_suites WHERE suite_id = ?",
            (SUITE_ID,),
        ).fetchone()
        if release_row is None:
            raise TrueNorthError("run shadow database is not bound to the suite")
        release_id = str(release_row[0])
        atomic_count = int(
            conn.execute(
                "SELECT COUNT(*) FROM atomic_claims WHERE corpus_release_id = ?",
                (release_id,),
            ).fetchone()[0]
        )
        if atomic_count == 0:
            raise TrueNorthError(
                "run failed before atomic commit; start a new run instead of resuming"
            )
        conn.execute(
            """
            UPDATE true_north_runs
            SET status = 'running', completed_at = NULL, error = NULL
            WHERE run_id = ?
            """,
            (run_id,),
        )
        conn.commit()
        stages = []
        for target in ("identities", "claims"):
            stage = _reconcile_stage(
                conn,
                release_id=release_id,
                run_id=run_id,
                run_root=run_root,
                target=target,
                models=ROUTERS[router],
                timeout_seconds=timeout_seconds,
                opencode_binary=opencode_binary,
                max_batches=max_batches,
            )
            stages.append(stage)
            if not stage["complete"]:
                raise TrueNorthError(f"{target} reconciliation did not complete")
        canonical_stage = _canonicalize_committed_claims(
            conn,
            release_id=release_id,
            run_id=run_id,
            run_root=run_root,
            models=ROUTERS[router],
            timeout_seconds=timeout_seconds,
            opencode_binary=opencode_binary,
        )
        stages.append(canonical_stage)
        relation_stage = _run_canonical_relations(
            conn,
            release_id=release_id,
            run_id=run_id,
            run_root=run_root,
            models=ROUTERS[router],
            timeout_seconds=timeout_seconds,
            opencode_binary=opencode_binary,
            max_batches=max_batches,
        )
        stages.append(relation_stage)
        utility_output, utility_receipts, utility_model = _run_utility_packet(
            conn,
            release_id=release_id,
            run_id=run_id,
            run_root=run_root,
            models=ROUTERS[router],
            timeout_seconds=timeout_seconds,
            opencode_binary=opencode_binary,
        )
        _record_call_receipts(
            conn, run_id=run_id, stage="utility", receipts=utility_receipts
        )
        stages.append(
            {
                "target": "utility",
                "complete": True,
                "batch_count": len(UTILITY_QUESTIONS),
                "processed_items": len(utility_output["items"]),
                "producer_model": utility_model,
            }
        )
        ledger_counts = _finalize_ledger(conn, run_id=run_id)
        completed = now_iso()
        stage_receipts = [
            {
                "stage": stage["target"],
                "batch_count": stage["batch_count"],
                "processed_items": stage["processed_items"],
            }
            for stage in stages
        ]
        conn.execute(
            """
            UPDATE true_north_runs
            SET status = 'succeeded', output_count = ?, failure_count = 0,
                receipts_json = ?, completed_at = ?, error = NULL
            WHERE run_id = ?
            """,
            (
                atomic_count,
                _canonical_json(stage_receipts),
                completed,
                run_id,
            ),
        )
        conn.commit()
        manifest = _read_json(suite_root / "manifest.json")
        episode_ids = [
            str(item[0])
            for item in conn.execute(
                """
                SELECT DISTINCT episode_id FROM true_north_stage_ledger
                WHERE run_id = ? ORDER BY episode_id
                """,
                (run_id,),
            )
        ]
        configuration = {
            "suite_manifest_sha256": manifest["manifest_sha256"],
            "partition": row["partition_name"],
            "router": router,
            "models": list(ROUTERS[router]),
            "prompt_version": WORKHORSE_PROMPT_VERSION,
            "output_schema_version": WORK_OUTPUT_SCHEMA_VERSION,
            "downstream_runtime_sha256": _downstream_runtime_sha256(),
            "episode_ids": episode_ids,
        }
        run_receipt = {
            "schema_version": RUN_SCHEMA_VERSION,
            "ok": True,
            "suite_id": SUITE_ID,
            "run_id": run_id,
            "partition": row["partition_name"],
            "configuration": configuration,
            "configuration_sha256": row["configuration_sha256"],
            "run_database": str(run_db_path),
            "atomic_claim_count": atomic_count,
            "ledger_counts": ledger_counts,
            "stages": stages,
            "started_at": row["started_at"],
            "completed_at": completed,
            "resumed": True,
            "production_mutation": False,
            "shadow_mutation": True,
        }
        run_receipt["receipt_sha256"] = sha256_text(dumps_json(run_receipt))
        receipt_path = run_root / "run-receipt.json"
        _write_json(receipt_path, run_receipt, immutable=True)
        return {**run_receipt, "receipt_path": str(receipt_path)}
    except BaseException as exc:
        conn.rollback()
        try:
            conn.execute(
                """
                UPDATE true_north_runs
                SET status = 'failed', failure_count = failure_count + 1,
                    completed_at = ?, error = ?
                WHERE run_id = ?
                """,
                (now_iso(), f"{type(exc).__name__}: {str(exc)[:500]}", run_id),
            )
            conn.commit()
        except Exception:
            conn.rollback()
        raise
    finally:
        conn.close()


def _codex_usage_from_jsonl(value: str) -> dict[str, int]:
    totals: Counter[str] = Counter()
    for line in value.splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(event, dict):
            continue
        usage = event.get("usage")
        if not isinstance(usage, Mapping):
            continue
        for key in (
            "input_tokens",
            "cached_input_tokens",
            "output_tokens",
            "reasoning_output_tokens",
            "total_tokens",
        ):
            value_item = usage.get(key)
            if isinstance(value_item, int):
                totals[key] += value_item
    return dict(totals)


def _run_codex_gold_packet(
    *,
    packet_path: Path,
    schema_path: Path,
    output_dir: Path,
    timeout_seconds: int,
    codex_binary: str,
    validator: Any | None = None,
    prompt_prefix: str | None = None,
) -> dict[str, Any]:
    packet = _read_json(packet_path)
    validate = validator or _validate_atomic_output
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "validated.private.json"
    if output_path.exists():
        output = _read_json(output_path)
        validate(output, packet)
        return {
            "ok": True,
            "idempotent_replay": True,
            "output_path": str(output_path),
            "output_sha256": _sha256_file(output_path),
            "model": "gpt-5.6-sol",
        }
    raw_output_path = output_dir / "last-message.private.txt"
    command = [
        codex_binary,
        "exec",
        "-m",
        "gpt-5.6-sol",
        "-c",
        'model_reasoning_effort="high"',
        "-C",
        str(output_dir),
        "--skip-git-repo-check",
        "--ignore-rules",
        "--ephemeral",
        "--sandbox",
        "read-only",
        "--output-schema",
        str(schema_path),
        "--output-last-message",
        str(raw_output_path),
        "--json",
        "-",
    ]
    prompt = (
        (
            prompt_prefix
            or (
                "Create the independent private benchmark gold described in the attached JSON. "
                "Do not use tools or external knowledge. Return only the strict JSON output."
            )
        )
        + "\n\n"
        + _canonical_json(packet)
    )
    started = time.monotonic()
    try:
        completed = subprocess.run(
            command,
            cwd=str(PROJECT_ROOT),
            input=prompt,
            text=True,
            errors="replace",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout_seconds,
            check=False,
        )
        timed_out = False
        exit_code = completed.returncode
        stdout = completed.stdout
        stderr = completed.stderr
    except subprocess.TimeoutExpired as exc:
        timed_out = True
        exit_code = None
        stdout = (
            exc.stdout.decode(errors="replace")
            if isinstance(exc.stdout, bytes)
            else exc.stdout or ""
        )
        stderr = (
            exc.stderr.decode(errors="replace")
            if isinstance(exc.stderr, bytes)
            else exc.stderr or ""
        )
    elapsed = round(time.monotonic() - started, 3)
    _write_text(output_dir / "events.private.jsonl", stdout)
    _write_text(output_dir / "stderr.private.txt", stderr)
    if timed_out or exit_code != 0:
        raise TrueNorthError(
            f"Codex gold pass failed: timed_out={timed_out} exit={exit_code}"
        )
    answer = raw_output_path.read_text(encoding="utf-8") if raw_output_path.exists() else ""
    output = _decode_json_answer(answer)
    validate(output, packet)
    _write_json(output_path, output, immutable=True)
    receipt = {
        "ok": True,
        "idempotent_replay": False,
        "model": "gpt-5.6-sol",
        "effort": "high",
        "elapsed_seconds": elapsed,
        "usage": _codex_usage_from_jsonl(stdout),
        "packet_sha256": _sha256_file(packet_path),
        "output_path": str(output_path),
        "output_sha256": _sha256_file(output_path),
    }
    _write_json(output_dir / "receipt.json", receipt, immutable=True)
    return receipt


def _execute_gold_jobs(
    *,
    jobs: Sequence[Path],
    pass_root: Path,
    timeout_seconds: int,
    codex_binary: str,
    validator: Any | None,
    workers: int,
) -> list[dict[str, Any]]:
    if workers < 1 or workers > 4:
        raise TrueNorthError("gold workers must be between 1 and 4")

    def execute_one(job_path: Path) -> dict[str, Any]:
        name = job_path.name.removesuffix(".private.json")
        return _run_codex_gold_packet(
            packet_path=job_path,
            schema_path=pass_root / "schemas" / f"{name}.json",
            output_dir=pass_root / "outputs" / name,
            timeout_seconds=timeout_seconds,
            codex_binary=codex_binary,
            validator=validator,
        )

    if workers == 1:
        return [execute_one(path) for path in jobs]
    receipts: list[dict[str, Any]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(execute_one, path): path for path in jobs}
        for future in concurrent.futures.as_completed(futures):
            receipts.append(future.result())
    return receipts


def _prepare_adjudication_packets(suite_root: Path, partition: str) -> dict[str, Any]:
    partition_dir = "sealed-holdout" if partition == "holdout" else "development"
    base = suite_root / "gold" / partition_dir
    pass_a = base / "pass-a"
    pass_b = base / "pass-b"
    pass_c = base / "pass-c-adjudication"
    prepared = 0
    agreed = 0
    missing = 0
    for job_path in sorted((pass_a / "jobs").glob("*.private.json")):
        segment_id = job_path.name.removesuffix(".private.json")
        a_path = pass_a / "outputs" / segment_id / "validated.private.json"
        b_path = pass_b / "outputs" / segment_id / "validated.private.json"
        if not a_path.is_file() or not b_path.is_file():
            missing += 1
            continue
        output_a = _read_json(a_path)
        output_b = _read_json(b_path)
        a_items = {str(row["candidate_id"]): row for row in output_a["items"]}
        b_items = {str(row["candidate_id"]): row for row in output_b["items"]}
        disagreement_ids = [
            candidate_id
            for candidate_id in sorted(a_items)
            if _canonical_json(a_items[candidate_id])
            != _canonical_json(b_items[candidate_id])
        ]
        if not disagreement_ids:
            agreed += 1
            final_path = pass_c / "outputs" / segment_id / "validated.private.json"
            _write_json(final_path, output_a, immutable=True)
            continue
        original = _read_json(job_path)
        agreed_items = [
            a_items[candidate_id]
            for candidate_id in sorted(a_items)
            if candidate_id not in set(disagreement_ids)
        ]
        disagreement_candidates = [
            row
            for row in original["input"]["candidates"]
            if str(row["candidate_id"]) in set(disagreement_ids)
        ]
        adjudication = {
            **original,
            "task": "Adjudicate two independent gold annotations for one frozen segment.",
            "instructions": [
                "Resolve only the disagreements between pass_a and pass_b using the original frozen evidence.",
                "Neither prior pass is authoritative. Produce the best complete decision set.",
                *_atomic_instructions(gold=True),
            ],
            "input": {
                **original["input"],
                "candidates": disagreement_candidates,
                "pass_a": {
                    **output_a,
                    "items": [a_items[value] for value in disagreement_ids],
                },
                "pass_b": {
                    **output_b,
                    "items": [b_items[value] for value in disagreement_ids],
                },
            },
            "output_schema": atomic_output_schema(disagreement_ids),
        }
        c_job = pass_c / "jobs" / f"{segment_id}.private.json"
        c_schema = pass_c / "schemas" / f"{segment_id}.json"
        _write_json(c_job, adjudication, immutable=True)
        _write_json(c_schema, adjudication["output_schema"], immutable=True)
        _write_json(
            pass_c / "outputs" / segment_id / "agreed.private.json",
            {"items": agreed_items},
            immutable=True,
        )
        prepared += 1
    return {
        "partition": partition,
        "prepared_disagreements": prepared,
        "agreed_without_adjudication": agreed,
        "missing_prior_outputs": missing,
    }


def _compile_final_gold(suite_root: Path, partition: str) -> dict[str, Any]:
    partition_dir = "sealed-holdout" if partition == "holdout" else "development"
    base = suite_root / "gold" / partition_dir
    pass_c = base / "pass-c-adjudication"
    items: list[dict[str, Any]] = []
    missing: list[str] = []
    for job_path in sorted((base / "pass-a" / "jobs").glob("*.private.json")):
        segment_id = job_path.name.removesuffix(".private.json")
        output_path = pass_c / "outputs" / segment_id / "validated.private.json"
        if not output_path.is_file():
            missing.append(segment_id)
            continue
        original = _read_json(job_path)
        output = _read_json(output_path)
        agreed_path = pass_c / "outputs" / segment_id / "agreed.private.json"
        if agreed_path.is_file():
            output = {
                "schema_version": WORK_OUTPUT_SCHEMA_VERSION,
                "items": [
                    *_read_json(agreed_path)["items"],
                    *output["items"],
                ],
            }
        _validate_atomic_output(output, original)
        episode_id = str(original["input"]["episode"]["episode_id"])
        for item in output["items"]:
            items.append(
                {
                    "episode_id": episode_id,
                    "segment_id": segment_id,
                    **item,
                }
            )
    if missing:
        raise TrueNorthError(
            f"gold cannot be compiled; {len(missing)} segment adjudications are missing"
        )
    items.sort(key=lambda row: (row["episode_id"], row["segment_id"], row["candidate_id"]))
    final = {
        "schema_version": GOLD_SCHEMA_VERSION,
        "suite_id": SUITE_ID,
        "partition": partition,
        "gold_model": "gpt-5.6-sol",
        "adjudication": "independent_a_b_then_c",
        "item_count": len(items),
        "items": items,
    }
    final["gold_sha256"] = sha256_text(dumps_json(final))
    final_path = base / "final" / "gold.private.json"
    _write_json(final_path, final, immutable=True)
    consensus = _compile_consensus_gold(
        suite_root,
        partition,
        preferred_gold=final,
    )
    return {
        "partition": partition,
        "final_path": str(final_path),
        "gold_sha256": final["gold_sha256"],
        "item_count": len(items),
        "consensus_path": consensus["consensus_path"],
        "consensus_sha256": consensus["consensus_sha256"],
        "consensus_counts": consensus["counts"],
    }


def _gold_value_state(disposition: str) -> str:
    if disposition in {"retain", "revise"}:
        return "value"
    if disposition == "reject":
        return "junk"
    if disposition == "hold":
        return "hold"
    raise TrueNorthError(f"unsupported gold disposition: {disposition}")


def _load_independent_atomic_gold(
    base: Path,
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    pass_a: dict[str, dict[str, Any]] = {}
    pass_b: dict[str, dict[str, Any]] = {}
    for job_path in sorted((base / "pass-a" / "jobs").glob("*.private.json")):
        name = job_path.name.removesuffix(".private.json")
        a_path = base / "pass-a" / "outputs" / name / "validated.private.json"
        b_path = base / "pass-b" / "outputs" / name / "validated.private.json"
        if not a_path.is_file() or not b_path.is_file():
            raise TrueNorthError(
                f"consensus gold requires complete independent outputs: {name}"
            )
        for target, path, label in (
            (pass_a, a_path, "pass-a"),
            (pass_b, b_path, "pass-b"),
        ):
            for row in _read_json(path)["items"]:
                candidate_id = str(row["candidate_id"])
                if candidate_id in target:
                    raise TrueNorthError(
                        f"duplicate {label} gold candidate: {candidate_id}"
                    )
                target[candidate_id] = dict(row)
    if not pass_a or set(pass_a) != set(pass_b):
        raise TrueNorthError(
            "independent atomic gold passes have missing or different scopes"
        )
    return pass_a, pass_b


def _atomic_decomposition_record(
    source: str,
    item: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "source": source,
        "disposition": str(item["disposition"]),
        "value_state": _gold_value_state(str(item["disposition"])),
        "atomic_count": len(item["atomic_claims"]),
        "claim_texts": [
            str(row["claim_text"]) for row in item["atomic_claims"]
        ],
    }


def _compile_consensus_gold(
    suite_root: Path,
    partition: str,
    *,
    preferred_gold: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    partition_dir = "sealed-holdout" if partition == "holdout" else "development"
    base = suite_root / "gold" / partition_dir
    preferred_document = (
        dict(preferred_gold)
        if preferred_gold is not None
        else _read_json(base / "final" / "gold.private.json")
    )
    preferred = {
        str(row["candidate_id"]): dict(row)
        for row in preferred_document["items"]
    }
    pass_a, pass_b = _load_independent_atomic_gold(base)
    if set(preferred) != set(pass_a):
        raise TrueNorthError(
            "preferred and independent atomic gold have different scopes"
        )
    counts: Counter[str] = Counter()
    items: list[dict[str, Any]] = []
    for candidate_id in sorted(preferred):
        a = pass_a[candidate_id]
        b = pass_b[candidate_id]
        c = preferred[candidate_id]
        a_state = _gold_value_state(str(a["disposition"]))
        b_state = _gold_value_state(str(b["disposition"]))
        c_state = _gold_value_state(str(c["disposition"]))
        consensus_state = (
            f"consensus_{a_state}" if a_state == b_state else "contested"
        )
        counts[consensus_state] += 1
        observed = (
            _atomic_decomposition_record("pass_a", a),
            _atomic_decomposition_record("pass_b", b),
            _atomic_decomposition_record("adjudicated", c),
        )
        acceptable_states = sorted(
            {str(row["value_state"]) for row in observed}
        )
        acceptable_dispositions = sorted(
            {str(row["disposition"]) for row in observed}
        )
        value_counts = sorted(
            {
                int(row["atomic_count"])
                for row in observed
                if row["value_state"] == "value"
            }
        )
        items.append(
            {
                "candidate_id": candidate_id,
                "episode_id": str(c["episode_id"]),
                "segment_id": str(c["segment_id"]),
                "consensus_state": consensus_state,
                "strictly_scoreable": consensus_state != "contested",
                "stable_exact_disposition": (
                    str(a["disposition"]) == str(b["disposition"])
                ),
                "stable_atomic_count": (
                    len(a["atomic_claims"]) == len(b["atomic_claims"])
                ),
                "acceptable_value_states": acceptable_states,
                "acceptable_dispositions": acceptable_dispositions,
                "acceptable_atomic_counts": value_counts,
                "minimum_atomic_count": min(value_counts) if value_counts else 0,
                "maximum_atomic_count": max(value_counts) if value_counts else 0,
                "preferred_disposition": str(c["disposition"]),
                "preferred_atomic_count": len(c["atomic_claims"]),
                "decompositions": list(observed),
            }
        )
    document = {
        "schema_version": CONSENSUS_GOLD_SCHEMA_VERSION,
        "policy_version": CONSENSUS_GOLD_POLICY_VERSION,
        "suite_id": SUITE_ID,
        "partition": partition,
        "source_gold_sha256": preferred_document["gold_sha256"],
        "policy": {
            "exact_retain_versus_revise_is_diagnostic_only": True,
            "strict_junk_definition": "pass_a_and_pass_b_both_reject",
            "strict_value_definition": (
                "pass_a_and_pass_b_both_retain_or_revise"
            ),
            "contested_items_are_excluded_from_strict_value_and_junk_gates": True,
            "contested_hold_is_always_acceptable": True,
            "atomic_count_rule": (
                "any_integer_between_independently_observed_minimum_and_maximum"
            ),
        },
        "item_count": len(items),
        "counts": dict(sorted(counts.items())),
        "items": items,
    }
    document["consensus_sha256"] = sha256_text(dumps_json(document))
    path = base / "final" / "consensus.private.json"
    _write_json(path, document, immutable=False)
    _write_gold_reliability_diagnostic(
        suite_root,
        partition_dir,
        partition=partition,
    )
    return {
        "partition": partition,
        "consensus_path": str(path),
        "consensus_sha256": document["consensus_sha256"],
        "item_count": len(items),
        "counts": document["counts"],
    }


def _partition_gold_root(suite_root: Path, partition: str) -> Path:
    return (
        suite_root
        / "gold"
        / ("sealed-holdout" if partition == "holdout" else "development")
    )


def _gold_atomics(suite_root: Path, partition: str) -> list[dict[str, Any]]:
    final = _read_json(
        _partition_gold_root(suite_root, partition) / "final" / "gold.private.json"
    )
    atomics: list[dict[str, Any]] = []
    for item in final["items"]:
        for atomic_index, atomic in enumerate(item["atomic_claims"]):
            atomics.append(
                {
                    "gold_atomic_id": stable_id(
                        str(item["candidate_id"]),
                        str(atomic_index),
                        prefix="gac_",
                    ),
                    "episode_id": str(item["episode_id"]),
                    "candidate_id": str(item["candidate_id"]),
                    "candidate_disposition": str(item["disposition"]),
                    "candidate_reason_code": str(item["reason_code"]),
                    "atomic_index": atomic_index,
                    "claim_text": atomic["claim_text"],
                    "raw_speaker": atomic["raw_speaker"],
                    "reported_actor": atomic.get("reported_actor"),
                    "subject_hint": atomic["subject_text"],
                    "subject_type_hint": atomic["subject_type"],
                    "proposition_hint": atomic["proposition_text"],
                    "position_hint": atomic["position"],
                }
            )
    return atomics


def _canonical_gold_schema(atomic_ids: Sequence[str]) -> dict[str, Any]:
    item = {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "gold_atomic_id",
            "subject_key",
            "subject_text",
            "subject_type",
            "proposition_key",
            "proposition_text",
            "direct_speaker",
            "speaker_resolution",
            "canonical_person_name",
            "reported_actor",
            "rationale",
        ],
        "properties": {
            "gold_atomic_id": {"type": "string", "enum": list(atomic_ids)},
            "subject_key": {"type": "string", "minLength": 1},
            "subject_text": {"type": "string", "minLength": 1},
            "subject_type": {"type": "string", "minLength": 1},
            "proposition_key": {"type": "string", "minLength": 1},
            "proposition_text": {"type": "string", "minLength": 1},
            "direct_speaker": {"type": "string", "minLength": 1},
            "speaker_resolution": {
                "type": "string",
                "enum": ["resolved", "unresolved"],
            },
            "canonical_person_name": {"type": ["string", "null"]},
            "reported_actor": {"type": ["string", "null"]},
            "rationale": {"type": "string", "minLength": 1},
        },
    }
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["schema_version", "items"],
        "properties": {
            "schema_version": {
                "type": "string",
                "const": "pif_true_north_canonical_gold_v1",
            },
            "items": {
                "type": "array",
                "minItems": len(atomic_ids),
                "maxItems": len(atomic_ids),
                "items": item,
            },
        },
    }


def _validate_scoped_items(
    output: Mapping[str, Any],
    packet: Mapping[str, Any],
    *,
    id_field: str,
) -> None:
    _validate_schema(packet["output_schema"], output, path="$")
    expected = {
        str(row[id_field]) for row in packet["input"]["items"]
    }
    actual = [str(row[id_field]) for row in output["items"]]
    if len(actual) != len(set(actual)) or set(actual) != expected:
        raise TrueNorthError(f"gold output does not exactly cover {id_field} scope")


def _validate_canonical_gold(
    output: Mapping[str, Any], packet: Mapping[str, Any]
) -> None:
    _validate_scoped_items(
        output, packet, id_field="gold_atomic_id"
    )
    for item in output["items"]:
        resolved = item["speaker_resolution"] == "resolved"
        if resolved != bool(item["canonical_person_name"]):
            raise TrueNorthError(
                "resolved speaker must have a canonical person and unresolved must not"
            )


def _prepare_canonical_gold(suite_root: Path, partition: str) -> dict[str, Any]:
    atomics = _gold_atomics(suite_root, partition)
    if not atomics:
        raise TrueNorthError("canonical gold requires compiled retained atomic gold")
    base = _partition_gold_root(suite_root, partition) / "canonical"
    manifest = _read_json(suite_root / "manifest.json")
    episode_contexts = []
    for bundle_record in manifest["bundles"]:
        if bundle_record["partition"] != partition:
            continue
        bundle = _read_json(Path(bundle_record["bundle_path"]))
        episode_contexts.append(
            {
                "episode": bundle["episode"],
                "speaker_map": bundle["episode_context"].get("speaker_map") or [],
                "entity_seed": bundle["episode_context"].get("entity_seed") or {},
            }
        )
    context_by_episode = {
        str(row["episode"]["episode_id"]): row for row in episode_contexts
    }
    atomics_by_episode: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in atomics:
        atomics_by_episode[str(row["episode_id"])].append(row)
    packet_count = 0
    for episode_id, episode_atomics in sorted(atomics_by_episode.items()):
        for offset in range(0, len(episode_atomics), 150):
            chunk = episode_atomics[offset : offset + 150]
            packet_name = f"{episode_id}-{offset // 150:02d}"
            atomic_ids = [str(row["gold_atomic_id"]) for row in chunk]
            schema = _canonical_gold_schema(atomic_ids)
            task = {
            "schema_version": "pif_true_north_canonical_gold_job_v1",
            "suite_id": SUITE_ID,
            "task": "Independently reconcile speaker identity, canonical subjects, and proposition variants within one complete episode.",
            "instructions": [
                "Return exactly one assignment for every gold_atomic_id.",
                "Use the same subject_key only for claims about the same stable issue or question.",
                "Use the same proposition_key only for semantically equivalent normalized assertions; preserve polarity, conditions, time, actor, and scope.",
                "Do not merge generic mentions merely because they share safety, governance, risk, agent, or Anthropic.",
                "direct_speaker is the person who uttered the evidence, never a quoted or reported actor.",
                "Resolve a canonical person only when the supplied episode context supports it; otherwise mark unresolved.",
                "Keys are episode-local labels. Make them concise, deterministic snake_case; a later independent pass reconciles keys across episodes.",
                "Return only strict JSON.",
            ],
            "output_schema": schema,
                "input": {
                    "episode_contexts": [context_by_episode[episode_id]],
                    "items": chunk,
                },
            }
            for pass_name in ("pass-a", "pass-b"):
                _write_json(
                    base / pass_name / "jobs" / f"{packet_name}.private.json",
                    task,
                    immutable=True,
                )
                _write_json(
                    base / pass_name / "schemas" / f"{packet_name}.json",
                    schema,
                    immutable=True,
                )
            packet_count += 1
    return {
        "phase": "canonical",
        "partition": partition,
        "item_count": len(atomics),
        "packet_count": packet_count,
    }


def _prepare_single_adjudication(
    *,
    base: Path,
    name: str,
    validator: Any,
    id_field: str | None = None,
    scope_field: str = "items",
) -> dict[str, Any]:
    a_job = base / "pass-a" / "jobs" / f"{name}.private.json"
    a_output = base / "pass-a" / "outputs" / name / "validated.private.json"
    b_output = base / "pass-b" / "outputs" / name / "validated.private.json"
    if not a_output.is_file() or not b_output.is_file():
        raise TrueNorthError("adjudication requires complete pass-a and pass-b outputs")
    original = _read_json(a_job)
    output_a = _read_json(a_output)
    output_b = _read_json(b_output)
    validator(output_a, original)
    validator(output_b, original)
    id_field = id_field or (
        "gold_atomic_id"
        if name == "canonical"
        else "question_id"
        if name == "utility"
        else "pair_id"
    )
    a_items = {str(row[id_field]): row for row in output_a["items"]}
    b_items = {str(row[id_field]): row for row in output_b["items"]}

    def semantic(row: Mapping[str, Any]) -> str:
        return _canonical_json(
            {key: value for key, value in row.items() if key != "rationale"}
        )

    disagreement_ids = [
        item_id
        for item_id in sorted(a_items)
        if semantic(a_items[item_id]) != semantic(b_items[item_id])
    ]
    final_path = base / "pass-c-adjudication" / "outputs" / name / "validated.private.json"
    if not disagreement_ids:
        _write_json(final_path, output_a, immutable=True)
        return {"prepared": 0, "agreed": 1}
    narrowed_schema = json.loads(_canonical_json(original["output_schema"]))
    narrowed_schema["properties"]["items"]["minItems"] = len(disagreement_ids)
    narrowed_schema["properties"]["items"]["maxItems"] = len(disagreement_ids)
    narrowed_schema["properties"]["items"]["items"]["properties"][id_field]["enum"] = (
        disagreement_ids
    )
    narrowed_input = dict(original["input"])
    narrowed_input[scope_field] = [
        row
        for row in original["input"][scope_field]
        if str(row[id_field]) in set(disagreement_ids)
    ]
    job = {
        **original,
        "task": "Adjudicate two independent annotations against the original frozen inputs.",
        "instructions": [
            "Neither pass is authoritative. Resolve their disagreements from the frozen input.",
            *list(original["instructions"]),
        ],
        "input": {
            **narrowed_input,
            "pass_a": {
                **output_a,
                "items": [a_items[value] for value in disagreement_ids],
            },
            "pass_b": {
                **output_b,
                "items": [b_items[value] for value in disagreement_ids],
            },
        },
        "output_schema": narrowed_schema,
    }
    root = base / "pass-c-adjudication"
    _write_json(root / "jobs" / f"{name}.private.json", job, immutable=True)
    _write_json(root / "schemas" / f"{name}.json", narrowed_schema, immutable=True)
    _write_json(
        root / "outputs" / name / "agreed.private.json",
        {
            "id_field": id_field,
            "items": [
                a_items[item_id]
                for item_id in sorted(a_items)
                if item_id not in set(disagreement_ids)
            ],
        },
        immutable=True,
    )
    return {"prepared": 1, "agreed": 0}


def _merge_adjudicated_output(
    base: Path,
    name: str,
    output: Mapping[str, Any],
    *,
    schema_version: str,
) -> dict[str, Any]:
    agreed_path = (
        base / "pass-c-adjudication" / "outputs" / name / "agreed.private.json"
    )
    if not agreed_path.is_file():
        return dict(output)
    return {
        "schema_version": schema_version,
        "items": [*_read_json(agreed_path)["items"], *output["items"]],
    }


def _compile_canonical_gold(suite_root: Path, partition: str) -> dict[str, Any]:
    base = _partition_gold_root(suite_root, partition) / "canonical"
    items = []
    missing = []
    for job_path in sorted((base / "pass-a" / "jobs").glob("*.private.json")):
        name = job_path.name.removesuffix(".private.json")
        output_path = (
            base / "pass-c-adjudication" / "outputs" / name / "validated.private.json"
        )
        if not output_path.is_file():
            missing.append(name)
            continue
        packet = _read_json(job_path)
        output = _merge_adjudicated_output(
            base,
            name,
            _read_json(output_path),
            schema_version="pif_true_north_canonical_gold_v1",
        )
        _validate_canonical_gold(output, packet)
        items.extend(output["items"])
    if missing:
        raise TrueNorthError(f"canonical adjudications missing: {len(missing)}")
    final = {
        "schema_version": "pif_true_north_canonical_gold_v1",
        "items": sorted(items, key=lambda row: row["gold_atomic_id"]),
        "suite_id": SUITE_ID,
        "partition": partition,
        "adjudication": "independent_a_b_then_c",
        "key_scope": "episode_local_pending_cross_episode_reconciliation",
    }
    final["gold_sha256"] = sha256_text(dumps_json(final))
    final_path = (
        _partition_gold_root(suite_root, partition)
        / "final"
        / "canonicals.episode.private.json"
    )
    _write_json(final_path, final, immutable=True)
    return {"phase": "canonical", "item_count": len(final["items"]), "final_path": str(final_path)}


def _canonical_key_schema(local_cluster_ids: Sequence[str]) -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["schema_version", "items"],
        "properties": {
            "schema_version": {
                "type": "string",
                "const": "pif_true_north_canonical_key_gold_v1",
            },
            "items": {
                "type": "array",
                "minItems": len(local_cluster_ids),
                "maxItems": len(local_cluster_ids),
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": [
                        "local_cluster_id",
                        "global_subject_key",
                        "global_proposition_key",
                    ],
                    "properties": {
                        "local_cluster_id": {
                            "type": "string",
                            "enum": list(local_cluster_ids),
                        },
                        "global_subject_key": {"type": "string", "minLength": 1},
                        "global_proposition_key": {"type": "string", "minLength": 1},
                    },
                },
            },
        },
    }


def _validate_canonical_key_gold(
    output: Mapping[str, Any], packet: Mapping[str, Any]
) -> None:
    _validate_scoped_items(output, packet, id_field="local_cluster_id")


def _prepare_canonical_key_gold(
    suite_root: Path, partition: str
) -> dict[str, Any]:
    local_path = (
        _partition_gold_root(suite_root, partition)
        / "final"
        / "canonicals.episode.private.json"
    )
    local = _read_json(local_path)
    raw_atomics = {
        row["gold_atomic_id"]: row for row in _gold_atomics(suite_root, partition)
    }
    clusters: dict[str, dict[str, Any]] = {}
    for assignment in local["items"]:
        atomic = raw_atomics[assignment["gold_atomic_id"]]
        local_cluster_id = stable_id(
            str(atomic["episode_id"]),
            str(assignment["subject_key"]),
            str(assignment["proposition_key"]),
            prefix="glocal_",
        )
        cluster = clusters.setdefault(
            local_cluster_id,
            {
                "local_cluster_id": local_cluster_id,
                "episode_id": atomic["episode_id"],
                "subject_key": assignment["subject_key"],
                "subject_text": assignment["subject_text"],
                "subject_type": assignment["subject_type"],
                "proposition_key": assignment["proposition_key"],
                "proposition_text": assignment["proposition_text"],
                "example_claims": [],
            },
        )
        if len(cluster["example_claims"]) < 2:
            cluster["example_claims"].append(atomic["claim_text"])
    items = [clusters[key] for key in sorted(clusters)]
    schema = _canonical_key_schema([row["local_cluster_id"] for row in items])
    job = {
        "schema_version": "pif_true_north_canonical_key_job_v1",
        "suite_id": SUITE_ID,
        "task": "Reconcile compact episode-local canonical keys across the complete development partition.",
        "instructions": [
            "Return exactly one mapping for every local_cluster_id.",
            "Use one global_subject_key only for the same stable issue or question across episodes.",
            "Use one global_proposition_key only when truth conditions match, including actor, polarity, condition, time, deployment scope, and jurisdiction.",
            "Keep useful singletons with unique global keys.",
            "Do not collapse generic safety, governance, risk, agent, or Anthropic mentions.",
            "Return compact deterministic snake_case keys and only strict JSON.",
        ],
        "output_schema": schema,
        "input": {"items": items},
    }
    base = _partition_gold_root(suite_root, partition) / "canonical-keys"
    for pass_name in ("pass-a", "pass-b"):
        _write_json(base / pass_name / "jobs" / "canonical-keys.private.json", job, immutable=True)
        _write_json(base / pass_name / "schemas" / "canonical-keys.json", schema, immutable=True)
    return {
        "phase": "canonical-keys",
        "partition": partition,
        "cluster_count": len(items),
    }


def _compile_canonical_key_gold(
    suite_root: Path, partition: str
) -> dict[str, Any]:
    partition_root = _partition_gold_root(suite_root, partition)
    base = partition_root / "canonical-keys"
    output_path = (
        base
        / "pass-c-adjudication"
        / "outputs"
        / "canonical-keys"
        / "validated.private.json"
    )
    if not output_path.is_file():
        raise TrueNorthError("canonical-key adjudication is incomplete")
    packet = _read_json(base / "pass-a" / "jobs" / "canonical-keys.private.json")
    output = _merge_adjudicated_output(
        base,
        "canonical-keys",
        _read_json(output_path),
        schema_version="pif_true_north_canonical_key_gold_v1",
    )
    _validate_canonical_key_gold(output, packet)
    mapping = {row["local_cluster_id"]: row for row in output["items"]}
    local = _read_json(
        partition_root / "final" / "canonicals.episode.private.json"
    )
    raw_atomics = {
        row["gold_atomic_id"]: row for row in _gold_atomics(suite_root, partition)
    }
    items = []
    for assignment in local["items"]:
        atomic = raw_atomics[assignment["gold_atomic_id"]]
        local_cluster_id = stable_id(
            str(atomic["episode_id"]),
            str(assignment["subject_key"]),
            str(assignment["proposition_key"]),
            prefix="glocal_",
        )
        reconciled = mapping[local_cluster_id]
        items.append(
            {
                **assignment,
                "subject_key": reconciled["global_subject_key"],
                "proposition_key": reconciled["global_proposition_key"],
                "episode_local_subject_key": assignment["subject_key"],
                "episode_local_proposition_key": assignment["proposition_key"],
            }
        )
    final = {
        "schema_version": "pif_true_north_canonical_gold_v1",
        "suite_id": SUITE_ID,
        "partition": partition,
        "adjudication": "episode_a_b_c_then_global_key_a_b_c",
        "key_scope": "partition_global",
        "items": sorted(items, key=lambda row: row["gold_atomic_id"]),
    }
    final["gold_sha256"] = sha256_text(dumps_json(final))
    final_path = partition_root / "final" / "canonicals.private.json"
    _write_json(final_path, final, immutable=True)
    return {
        "phase": "canonical-keys",
        "item_count": len(items),
        "final_path": str(final_path),
    }


def _compact_mapping_schema(
    *,
    schema_version: str,
    id_field: str,
    ids: Sequence[str],
    value_field: str,
) -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["schema_version", "items"],
        "properties": {
            "schema_version": {"type": "string", "const": schema_version},
            "items": {
                "type": "array",
                "minItems": len(ids),
                "maxItems": len(ids),
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": [id_field, value_field],
                    "properties": {
                        id_field: {"type": "string", "enum": list(ids)},
                        value_field: {"type": "string", "minLength": 1},
                    },
                },
            },
        },
    }


def _validate_canonical_subject_gold(
    output: Mapping[str, Any], packet: Mapping[str, Any]
) -> None:
    _validate_scoped_items(output, packet, id_field="local_subject_id")


def _prepare_canonical_subject_gold(
    suite_root: Path, partition: str
) -> dict[str, Any]:
    partition_root = _partition_gold_root(suite_root, partition)
    local = _read_json(
        partition_root / "final" / "canonicals.episode.private.json"
    )
    raw = {row["gold_atomic_id"]: row for row in _gold_atomics(suite_root, partition)}
    subjects: dict[str, dict[str, Any]] = {}
    for assignment in local["items"]:
        atomic = raw[assignment["gold_atomic_id"]]
        local_subject_id = stable_id(
            str(atomic["episode_id"]),
            str(assignment["subject_key"]),
            prefix="gsubject_local_",
        )
        subjects.setdefault(
            local_subject_id,
            {
                "local_subject_id": local_subject_id,
                "episode_id": atomic["episode_id"],
                "subject_key": assignment["subject_key"],
                "subject_text": assignment["subject_text"],
                "subject_type": assignment["subject_type"],
                "example_claim": atomic["claim_text"],
            },
        )
    items = [subjects[key] for key in sorted(subjects)]
    schema = _compact_mapping_schema(
        schema_version="pif_true_north_canonical_subject_gold_v1",
        id_field="local_subject_id",
        ids=[row["local_subject_id"] for row in items],
        value_field="global_subject_key",
    )
    job = {
        "schema_version": "pif_true_north_canonical_subject_job_v1",
        "suite_id": SUITE_ID,
        "task": "Reconcile episode-local subjects across the complete partition.",
        "instructions": [
            "Return exactly one mapping for every local_subject_id.",
            "Use the same deterministic snake_case global_subject_key only for the same stable issue or question.",
            "Preserve actor, deployment, jurisdiction, and scope distinctions.",
            "Do not merge generic safety, governance, risk, agent, or Anthropic mentions.",
            "Keep useful singletons with unique keys. Return only strict JSON.",
        ],
        "output_schema": schema,
        "input": {"items": items},
    }
    base = partition_root / "canonical-subjects"
    for pass_name in ("pass-a", "pass-b"):
        _write_json(base / pass_name / "jobs" / "canonical-subjects.private.json", job, immutable=True)
        _write_json(base / pass_name / "schemas" / "canonical-subjects.json", schema, immutable=True)
    return {"phase": "canonical-subjects", "subject_count": len(items)}


def _compile_canonical_subject_gold(
    suite_root: Path, partition: str
) -> dict[str, Any]:
    partition_root = _partition_gold_root(suite_root, partition)
    base = partition_root / "canonical-subjects"
    output_path = (
        base / "pass-c-adjudication" / "outputs" / "canonical-subjects" / "validated.private.json"
    )
    packet = _read_json(base / "pass-a" / "jobs" / "canonical-subjects.private.json")
    output = _merge_adjudicated_output(
        base,
        "canonical-subjects",
        _read_json(output_path),
        schema_version="pif_true_north_canonical_subject_gold_v1",
    )
    _validate_canonical_subject_gold(output, packet)
    final = {
        **output,
        "suite_id": SUITE_ID,
        "partition": partition,
        "adjudication": "independent_a_b_then_c",
    }
    final["gold_sha256"] = sha256_text(dumps_json(final))
    final_path = partition_root / "final" / "canonical-subjects.private.json"
    _write_json(final_path, final, immutable=True)
    return {"phase": "canonical-subjects", "item_count": len(final["items"]), "final_path": str(final_path)}


def _validate_canonical_proposition_gold(
    output: Mapping[str, Any], packet: Mapping[str, Any]
) -> None:
    _validate_scoped_items(output, packet, id_field="local_cluster_id")


def _prepare_canonical_proposition_gold(
    suite_root: Path, partition: str
) -> dict[str, Any]:
    partition_root = _partition_gold_root(suite_root, partition)
    local = _read_json(partition_root / "final" / "canonicals.episode.private.json")
    subject_map = {
        row["local_subject_id"]: row["global_subject_key"]
        for row in _read_json(partition_root / "final" / "canonical-subjects.private.json")["items"]
    }
    raw = {row["gold_atomic_id"]: row for row in _gold_atomics(suite_root, partition)}
    clusters: dict[str, dict[str, Any]] = {}
    for assignment in local["items"]:
        atomic = raw[assignment["gold_atomic_id"]]
        local_subject_id = stable_id(
            str(atomic["episode_id"]),
            str(assignment["subject_key"]),
            prefix="gsubject_local_",
        )
        local_cluster_id = stable_id(
            str(atomic["episode_id"]),
            str(assignment["subject_key"]),
            str(assignment["proposition_key"]),
            prefix="glocal_",
        )
        clusters.setdefault(
            local_cluster_id,
            {
                "local_cluster_id": local_cluster_id,
                "global_subject_key": subject_map[local_subject_id],
                "proposition_text": assignment["proposition_text"],
                "example_claim": atomic["claim_text"],
            },
        )
    by_subject: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in clusters.values():
        by_subject[row["global_subject_key"]].append(row)
    packets: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = []
    for subject_key in sorted(by_subject):
        group = sorted(by_subject[subject_key], key=lambda row: row["local_cluster_id"])
        if current and len(current) + len(group) > 150:
            packets.append(current)
            current = []
        while len(group) > 150:
            packets.append(group[:150])
            group = group[150:]
        current.extend(group)
    if current:
        packets.append(current)
    base = partition_root / "canonical-propositions"
    for index, items in enumerate(packets):
        name = f"propositions-{index:04d}"
        schema = _compact_mapping_schema(
            schema_version="pif_true_north_canonical_proposition_gold_v1",
            id_field="local_cluster_id",
            ids=[row["local_cluster_id"] for row in items],
            value_field="global_proposition_key",
        )
        job = {
            "schema_version": "pif_true_north_canonical_proposition_job_v1",
            "suite_id": SUITE_ID,
            "task": "Reconcile proposition variants within already-global canonical subjects.",
            "instructions": [
                "Return exactly one mapping for every local_cluster_id.",
                "Use the same global_proposition_key only for matching truth conditions within the supplied global subject.",
                "Preserve actor, polarity, condition, time, deployment scope, and jurisdiction.",
                "Keep useful singletons unique. Return deterministic snake_case keys and only strict JSON.",
            ],
            "output_schema": schema,
            "input": {"items": items},
        }
        for pass_name in ("pass-a", "pass-b"):
            _write_json(base / pass_name / "jobs" / f"{name}.private.json", job, immutable=True)
            _write_json(base / pass_name / "schemas" / f"{name}.json", schema, immutable=True)
    return {"phase": "canonical-propositions", "cluster_count": len(clusters), "packet_count": len(packets)}


def _compile_canonical_proposition_gold(
    suite_root: Path, partition: str
) -> dict[str, Any]:
    partition_root = _partition_gold_root(suite_root, partition)
    base = partition_root / "canonical-propositions"
    proposition_map: dict[str, str] = {}
    for job_path in sorted((base / "pass-a" / "jobs").glob("*.private.json")):
        name = job_path.name.removesuffix(".private.json")
        output = _merge_adjudicated_output(
            base,
            name,
            _read_json(base / "pass-c-adjudication" / "outputs" / name / "validated.private.json"),
            schema_version="pif_true_north_canonical_proposition_gold_v1",
        )
        packet = _read_json(job_path)
        _validate_canonical_proposition_gold(output, packet)
        proposition_map.update(
            {row["local_cluster_id"]: row["global_proposition_key"] for row in output["items"]}
        )
    local = _read_json(partition_root / "final" / "canonicals.episode.private.json")
    subjects = {
        row["local_subject_id"]: row["global_subject_key"]
        for row in _read_json(partition_root / "final" / "canonical-subjects.private.json")["items"]
    }
    raw = {row["gold_atomic_id"]: row for row in _gold_atomics(suite_root, partition)}
    items = []
    for assignment in local["items"]:
        atomic = raw[assignment["gold_atomic_id"]]
        local_subject_id = stable_id(str(atomic["episode_id"]), str(assignment["subject_key"]), prefix="gsubject_local_")
        local_cluster_id = stable_id(
            str(atomic["episode_id"]),
            str(assignment["subject_key"]),
            str(assignment["proposition_key"]),
            prefix="glocal_",
        )
        items.append(
            {
                **assignment,
                "subject_key": subjects[local_subject_id],
                "proposition_key": proposition_map[local_cluster_id],
                "episode_local_subject_key": assignment["subject_key"],
                "episode_local_proposition_key": assignment["proposition_key"],
            }
        )
    final = {
        "schema_version": "pif_true_north_canonical_gold_v1",
        "suite_id": SUITE_ID,
        "partition": partition,
        "adjudication": "episode_a_b_c_then_subject_a_b_c_then_proposition_a_b_c",
        "key_scope": "partition_global",
        "items": sorted(items, key=lambda row: row["gold_atomic_id"]),
    }
    final["gold_sha256"] = sha256_text(dumps_json(final))
    final_path = partition_root / "final" / "canonicals.private.json"
    _write_json(final_path, final, immutable=True)
    return {"phase": "canonical-propositions", "item_count": len(items), "final_path": str(final_path)}


def _relation_gold_schema(pair_ids: Sequence[str]) -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["schema_version", "items"],
        "properties": {
            "schema_version": {
                "type": "string",
                "const": "pif_true_north_relation_gold_v1",
            },
            "items": {
                "type": "array",
                "minItems": len(pair_ids),
                "maxItems": len(pair_ids),
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": [
                        "pair_id",
                        "relation",
                        "scope_note",
                        "rationale",
                    ],
                    "properties": {
                        "pair_id": {"type": "string", "enum": list(pair_ids)},
                        "relation": {"type": "string", "enum": list(RELATIONS)},
                        "scope_note": {"type": ["string", "null"]},
                        "rationale": {"type": "string", "minLength": 1},
                    },
                },
            },
        },
    }


def _validate_relation_gold(
    output: Mapping[str, Any], packet: Mapping[str, Any]
) -> None:
    _validate_scoped_items(output, packet, id_field="pair_id")


def _relation_pair_candidates(
    suite_root: Path, partition: str
) -> list[dict[str, Any]]:
    canonical = _read_json(
        _partition_gold_root(suite_root, partition) / "final" / "canonicals.private.json"
    )
    raw = {row["gold_atomic_id"]: row for row in _gold_atomics(suite_root, partition)}
    assignments = {row["gold_atomic_id"]: row for row in canonical["items"]}
    by_subject: dict[str, list[str]] = defaultdict(list)
    by_proposition: dict[tuple[str, str], list[str]] = defaultdict(list)
    for atomic_id, row in assignments.items():
        by_subject[str(row["subject_key"])].append(str(atomic_id))
        by_proposition[
            (str(row["subject_key"]), str(row["proposition_key"]))
        ].append(str(atomic_id))
    selected: set[tuple[str, str]] = set()

    def add(left: str, right: str) -> None:
        if left == right:
            return
        selected.add(tuple(sorted((left, right))))

    # All canonical-proposition duplicates are always eligible positives.
    for ids in by_proposition.values():
        for index, left in enumerate(ids):
            for right in ids[index + 1 :]:
                add(left, right)
    # Exactly two nearest same-subject hard cases per claim, in addition to all
    # pairs already known to share a canonical proposition.
    for ids in by_subject.values():
        for left in ids:
            ranked = sorted(
                (right for right in ids if right != left),
                key=lambda right: difflib.SequenceMatcher(
                    None,
                    str(raw[left]["claim_text"]).lower(),
                    str(raw[right]["claim_text"]).lower(),
                ).ratio(),
                reverse=True,
            )
            for right in ranked[:2]:
                add(left, right)
    # One lexically closest cross-subject negative per atomic.
    all_ids = sorted(assignments)
    stopwords = {
        "about", "after", "against", "because", "could", "from", "have",
        "into", "model", "models", "should", "that", "their", "there",
        "these", "they", "this", "with", "would",
    }
    tokens_by_id = {
        atomic_id: {
            token
            for token in _normalized_label(raw[atomic_id]["claim_text"]).split()
            if len(token) > 3 and token not in stopwords
        }
        for atomic_id in all_ids
    }
    ids_by_token: dict[str, set[str]] = defaultdict(set)
    for atomic_id, tokens in tokens_by_id.items():
        for token in tokens:
            ids_by_token[token].add(atomic_id)
    for left in all_ids:
        pool: set[str] = set()
        for token in tokens_by_id[left]:
            pool.update(ids_by_token[token])
        candidates = [
            right
            for right in pool
            if right != left
            and assignments[right]["subject_key"]
            != assignments[left]["subject_key"]
        ]
        if len(candidates) > 100:
            left_tokens = tokens_by_id[left]
            candidates = sorted(
                candidates,
                key=lambda right: (
                    len(left_tokens & tokens_by_id[right])
                    / max(1, len(left_tokens | tokens_by_id[right])),
                    right,
                ),
                reverse=True,
            )[:100]
        if not candidates:
            candidates = [
                right
                for right in all_ids
                if assignments[right]["subject_key"]
                != assignments[left]["subject_key"]
            ][:1]
        if candidates:
            right = max(
                candidates,
                key=lambda candidate: difflib.SequenceMatcher(
                    None,
                    str(raw[left]["claim_text"]).lower(),
                    str(raw[candidate]["claim_text"]).lower(),
                ).ratio(),
            )
            add(left, right)
    pairs = []
    for left, right in sorted(selected):
        pair_id = stable_id(left, right, prefix="gpair_")
        pairs.append(
            {
                "pair_id": pair_id,
                "source_gold_atomic_id": left,
                "target_gold_atomic_id": right,
                "same_subject": (
                    assignments[left]["subject_key"]
                    == assignments[right]["subject_key"]
                ),
                "source": {**raw[left], "canonical": assignments[left]},
                "target": {**raw[right], "canonical": assignments[right]},
            }
        )
    return pairs


def _prepare_relation_gold(suite_root: Path, partition: str) -> dict[str, Any]:
    pairs = _relation_pair_candidates(suite_root, partition)
    if not pairs:
        raise TrueNorthError("relation gold has no eligible pairs")
    base = _partition_gold_root(suite_root, partition) / "relations"
    packet_count = 0
    for offset in range(0, len(pairs), 50):
        chunk = pairs[offset : offset + 50]
        name = f"relations-{offset // 50:04d}"
        schema = _relation_gold_schema([row["pair_id"] for row in chunk])
        job = {
            "schema_version": "pif_true_north_relation_gold_job_v1",
            "suite_id": SUITE_ID,
            "task": "Independently classify bounded claim-pair relations.",
            "instructions": [
                "Return exactly one classification for every pair_id.",
                "equivalent means the same truth conditions, including actor, polarity, time, condition, and scope.",
                "supports means one claim materially increases support for the other without being equivalent.",
                "contradicts requires incompatible truth conditions under aligned scope.",
                "qualifies narrows, conditions, limits, or adds a material exception.",
                "orthogonal means the claims share a subject but address different propositions.",
                "incomparable means their subjects or scopes do not support a meaningful relation.",
                "Cross-subject lexical similarity is normally incomparable, not equivalent.",
                "Return only strict JSON.",
            ],
            "output_schema": schema,
            "input": {"items": chunk},
        }
        for pass_name in ("pass-a", "pass-b"):
            _write_json(base / pass_name / "jobs" / f"{name}.private.json", job, immutable=True)
            _write_json(base / pass_name / "schemas" / f"{name}.json", schema, immutable=True)
        packet_count += 1
    return {
        "phase": "relations",
        "partition": partition,
        "pair_count": len(pairs),
        "packet_count": packet_count,
    }


def _prepare_relation_adjudications(
    suite_root: Path, partition: str
) -> dict[str, Any]:
    base = _partition_gold_root(suite_root, partition) / "relations"
    prepared = agreed = 0
    for job_path in sorted((base / "pass-a" / "jobs").glob("*.private.json")):
        result = _prepare_single_adjudication(
            base=base,
            name=job_path.name.removesuffix(".private.json"),
            validator=_validate_relation_gold,
        )
        prepared += result["prepared"]
        agreed += result["agreed"]
    return {"prepared": prepared, "agreed": agreed}


def _compile_relation_gold(suite_root: Path, partition: str) -> dict[str, Any]:
    base = _partition_gold_root(suite_root, partition) / "relations"
    items = []
    missing = []
    for job_path in sorted((base / "pass-a" / "jobs").glob("*.private.json")):
        name = job_path.name.removesuffix(".private.json")
        output_path = (
            base / "pass-c-adjudication" / "outputs" / name / "validated.private.json"
        )
        if not output_path.is_file():
            missing.append(name)
            continue
        packet = _read_json(job_path)
        output = _merge_adjudicated_output(
            base,
            name,
            _read_json(output_path),
            schema_version="pif_true_north_relation_gold_v1",
        )
        _validate_relation_gold(output, packet)
        pair_scope = {row["pair_id"]: row for row in packet["input"]["items"]}
        for row in output["items"]:
            scope = pair_scope[row["pair_id"]]
            items.append(
                {
                    **row,
                    "source_gold_atomic_id": scope["source_gold_atomic_id"],
                    "target_gold_atomic_id": scope["target_gold_atomic_id"],
                }
            )
    if missing:
        raise TrueNorthError(f"relation adjudications missing: {len(missing)}")
    final = {
        "schema_version": "pif_true_north_relation_gold_v1",
        "suite_id": SUITE_ID,
        "partition": partition,
        "items": sorted(items, key=lambda row: row["pair_id"]),
    }
    final["gold_sha256"] = sha256_text(dumps_json(final))
    final_path = _partition_gold_root(suite_root, partition) / "final" / "relations.private.json"
    _write_json(final_path, final, immutable=True)
    return {"phase": "relations", "item_count": len(items), "final_path": str(final_path)}


def _utility_gold_schema(
    question_ids: Sequence[str], atomic_ids: Sequence[str]
) -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["schema_version", "items"],
        "properties": {
            "schema_version": {
                "type": "string",
                "const": "pif_true_north_utility_gold_v1",
            },
            "items": {
                "type": "array",
                "minItems": len(question_ids),
                "maxItems": len(question_ids),
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": [
                        "question_id",
                        "answerable",
                        "answer",
                        "support_gold_atomic_ids",
                        "rationale",
                    ],
                    "properties": {
                        "question_id": {
                            "type": "string",
                            "enum": list(question_ids),
                        },
                        "answerable": {"type": "boolean"},
                        "answer": {"type": "string"},
                        "support_gold_atomic_ids": {
                            "type": "array",
                            "items": {
                                "type": "string",
                                "enum": list(atomic_ids),
                            },
                        },
                        "rationale": {"type": "string", "minLength": 1},
                    },
                },
            },
        },
    }


def _validate_utility_gold(
    output: Mapping[str, Any], packet: Mapping[str, Any]
) -> None:
    _validate_schema(packet["output_schema"], output, path="$")
    expected = {
        str(row["question_id"]) for row in packet["input"]["questions"]
    }
    actual = [str(row["question_id"]) for row in output["items"]]
    if len(actual) != len(set(actual)) or set(actual) != expected:
        raise TrueNorthError("utility gold does not exactly cover question scope")
    for row in output["items"]:
        support_ids = list(row["support_gold_atomic_ids"])
        if len(support_ids) != len(set(support_ids)):
            raise TrueNorthError("utility gold contains duplicate support IDs")
        if not row["answerable"]:
            # Models sometimes cite evidence explaining why the requested
            # proposition is absent. Canonicalize that mechanically: an
            # unanswerable result must never retain affirmative support.
            row["support_gold_atomic_ids"] = []
        if row["answerable"] and (
            not str(row["answer"]).strip() or not row["support_gold_atomic_ids"]
        ):
            raise TrueNorthError("answerable utility gold requires answer and support")


def _validate_run_utility(
    output: Mapping[str, Any], packet: Mapping[str, Any]
) -> None:
    _validate_schema(packet["output_schema"], output, path="$")
    expected = {
        str(row["question_id"]) for row in packet["input"]["questions"]
    }
    actual = [str(row["question_id"]) for row in output["items"]]
    if len(actual) != len(set(actual)) or set(actual) != expected:
        raise TrueNorthError("utility output does not exactly cover question scope")
    for row in output["items"]:
        support_ids = list(row["support_atomic_claim_ids"])
        if len(support_ids) != len(set(support_ids)):
            raise TrueNorthError("utility output contains duplicate support IDs")
        if not row["answerable"]:
            row["support_atomic_claim_ids"] = []
        if row["answerable"] and (
            not str(row["answer"]).strip() or not row["support_atomic_claim_ids"]
        ):
            raise TrueNorthError("answerable utility output requires answer and support")


def _prepare_utility_gold(suite_root: Path, partition: str) -> dict[str, Any]:
    atomics = _gold_atomics(suite_root, partition)
    canonical = _read_json(
        _partition_gold_root(suite_root, partition) / "final" / "canonicals.private.json"
    )
    relations = _read_json(
        _partition_gold_root(suite_root, partition) / "final" / "relations.private.json"
    )
    assignments = {row["gold_atomic_id"]: row for row in canonical["items"]}
    compact = [
        {
            **row,
            "canonical": assignments[row["gold_atomic_id"]],
        }
        for row in atomics
    ]
    questions = [
        {"question_id": question_id, "question": question}
        for question_id, question in UTILITY_QUESTIONS
    ]
    base = _partition_gold_root(suite_root, partition) / "utility"
    generic_stopwords = {
        "about", "across", "after", "answer", "claims", "does", "episode",
        "episodes", "exact", "from", "identified", "made", "most", "podcast",
        "recurring", "shows", "speakers", "strongest", "supports", "their",
        "these", "what", "which", "with",
    }
    tokens_by_atomic = {
        row["gold_atomic_id"]: set(
            _normalized_label(
                " ".join(
                    (
                        str(row["claim_text"]),
                        str(row["canonical"]["subject_text"]),
                        str(row["canonical"]["proposition_text"]),
                        str(row["raw_speaker"]),
                        str(row.get("reported_actor") or ""),
                    )
                )
            ).split()
        )
        for row in compact
    }
    by_episode: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in compact:
        by_episode[str(row["episode_id"])].append(row)
    packet_sizes = []
    for question in questions:
        question_tokens = {
            token
            for token in _normalized_label(question["question"]).split()
            if len(token) > 3 and token not in generic_stopwords
        }
        selected: dict[str, dict[str, Any]] = {}
        for episode_id in sorted(by_episode):
            ranked = sorted(
                by_episode[episode_id],
                key=lambda row: (
                    len(question_tokens & tokens_by_atomic[row["gold_atomic_id"]]),
                    row["gold_atomic_id"],
                ),
                reverse=True,
            )
            for row in ranked[:30]:
                selected[row["gold_atomic_id"]] = row
        if question["question_id"] == "provenance_04":
            for row in compact:
                if row.get("reported_actor"):
                    selected[row["gold_atomic_id"]] = row
        if question["question_id"] == "provenance_05":
            for row in compact:
                if row.get("candidate_disposition") == "revise":
                    selected[row["gold_atomic_id"]] = row
        selected_items = list(selected.values())[:200]
        selected_ids = {row["gold_atomic_id"] for row in selected_items}
        selected_relations = [
            row
            for row in relations["items"]
            if row["source_gold_atomic_id"] in selected_ids
            and row["target_gold_atomic_id"] in selected_ids
        ]
        schema = _utility_gold_schema(
            [question["question_id"]], sorted(selected_ids)
        )
        job = {
            "schema_version": "pif_true_north_utility_gold_job_v1",
            "suite_id": SUITE_ID,
            "task": "Create research-utility answer truth for one fixed question from a deterministic cross-episode retrieval packet.",
            "instructions": [
                "Return exactly one item for the supplied question_id.",
                "Answer only from supplied atomic claims and relations; do not use external knowledge.",
                "Cite every materially supporting gold_atomic_id and no unsupported IDs.",
                "Preserve direct-speaker attribution and visible contradictions or qualifications.",
                "For recurring or consensus questions, require support from at least two episodes.",
                "If the packet does not substantively answer the question, mark it unanswerable.",
                "Return a concise substantial answer as strict JSON.",
            ],
            "output_schema": schema,
            "input": {
                "questions": [question],
                "atomic_claims": selected_items,
                "relations": selected_relations,
                "retrieval": {
                    "method": "top_30_lexical_per_episode_plus_question_invariants",
                    "complete_corpus_atomic_count": len(compact),
                },
            },
        }
        name = str(question["question_id"])
        for pass_name in ("pass-a", "pass-b"):
            _write_json(base / pass_name / "jobs" / f"{name}.private.json", job, immutable=True)
            _write_json(base / pass_name / "schemas" / f"{name}.json", schema, immutable=True)
        packet_sizes.append(len(_canonical_json(job)))
    return {
        "phase": "utility",
        "partition": partition,
        "question_count": len(questions),
        "packet_count": len(questions),
        "max_packet_chars": max(packet_sizes),
    }


def _compile_utility_gold(suite_root: Path, partition: str) -> dict[str, Any]:
    base = _partition_gold_root(suite_root, partition) / "utility"
    items = []
    missing = []
    for job_path in sorted((base / "pass-a" / "jobs").glob("*.private.json")):
        name = job_path.name.removesuffix(".private.json")
        output_path = (
            base / "pass-c-adjudication" / "outputs" / name / "validated.private.json"
        )
        if not output_path.is_file():
            missing.append(name)
            continue
        packet = _read_json(job_path)
        output = _merge_adjudicated_output(
            base,
            name,
            _read_json(output_path),
            schema_version="pif_true_north_utility_gold_v1",
        )
        _validate_utility_gold(output, packet)
        items.extend(output["items"])
    if missing:
        raise TrueNorthError(f"utility adjudications missing: {len(missing)}")
    final = {
        "schema_version": "pif_true_north_utility_gold_v1",
        "items": sorted(items, key=lambda row: row["question_id"]),
        "suite_id": SUITE_ID,
        "partition": partition,
        "adjudication": "independent_a_b_then_c",
    }
    final["gold_sha256"] = sha256_text(dumps_json(final))
    final_path = _partition_gold_root(suite_root, partition) / "final" / "utility.private.json"
    _write_json(final_path, final, immutable=True)
    return {"phase": "utility", "item_count": len(final["items"]), "final_path": str(final_path)}


def execute_gold(
    *,
    output_root: str | Path | None = None,
    suite: str = SUITE_ID,
    partition: str = "development",
    pass_name: str = "pass-a",
    phase: str = "atomic",
    limit: int | None = None,
    timeout_seconds: int = 1200,
    codex_binary: str = "codex",
    opencode_binary: str = "/opt/homebrew/bin/opencode",
    workers: int = 1,
) -> dict[str, Any]:
    if partition not in {"development", "holdout"}:
        raise TrueNorthError("partition must be development or holdout")
    if pass_name not in {"pass-a", "pass-b", "pass-c", "compile"}:
        raise TrueNorthError("gold pass must be pass-a, pass-b, pass-c, or compile")
    if phase not in {
        "atomic",
        "canonical",
        "canonical-keys",
        "canonical-subjects",
        "canonical-propositions",
        "relations",
        "utility",
        "actor-repair",
    }:
        raise TrueNorthError(
            "unsupported gold phase"
        )
    suite_root = _suite_root(output_root, suite)
    if phase == "actor-repair":
        if partition != "development":
            raise TrueNorthError("actor repair is development-only")
        from . import true_north_actor_repair

        if pass_name == "compile":
            return {
                "ok": True,
                "state": "compiled",
                **true_north_actor_repair.compile_actor_repair(suite_root),
            }
        return true_north_actor_repair.execute_actor_pass(
            suite_root,
            pass_name=pass_name,
            workers=workers,
            timeout_seconds=timeout_seconds,
            opencode_binary=opencode_binary,
        )
    if phase in {"canonical-subjects", "canonical-propositions"}:
        is_subject = phase == "canonical-subjects"
        base = _partition_gold_root(suite_root, partition) / phase
        if not any((base / "pass-a" / "jobs").glob("*.private.json")):
            (
                _prepare_canonical_subject_gold
                if is_subject
                else _prepare_canonical_proposition_gold
            )(suite_root, partition)
        if pass_name == "compile":
            result = (
                _compile_canonical_subject_gold
                if is_subject
                else _compile_canonical_proposition_gold
            )(suite_root, partition)
            return {"ok": True, "state": "compiled", **result}
        validator = (
            _validate_canonical_subject_gold
            if is_subject
            else _validate_canonical_proposition_gold
        )
        id_field = "local_subject_id" if is_subject else "local_cluster_id"
        if pass_name == "pass-c":
            prepared = agreed = 0
            for job_path in sorted((base / "pass-a" / "jobs").glob("*.private.json")):
                result = _prepare_single_adjudication(
                    base=base,
                    name=job_path.name.removesuffix(".private.json"),
                    validator=validator,
                    id_field=id_field,
                )
                prepared += result["prepared"]
                agreed += result["agreed"]
            preparation = {"prepared": prepared, "agreed": agreed}
            directory_name = "pass-c-adjudication"
        else:
            preparation = None
            directory_name = pass_name
        pass_root = base / directory_name
        jobs = sorted((pass_root / "jobs").glob("*.private.json"))
        receipts = _execute_gold_jobs(
            jobs=jobs,
            pass_root=pass_root,
            timeout_seconds=timeout_seconds,
            codex_binary=codex_binary,
            validator=validator,
            workers=workers,
        )
        return {
            "ok": True,
            "suite_id": SUITE_ID,
            "state": "gold_pass_completed",
            "phase": phase,
            "partition": partition,
            "pass": pass_name,
            "executed_count": len(receipts),
            "idempotent_replays": sum(bool(row["idempotent_replay"]) for row in receipts),
            "preparation": preparation,
            "model_execution_attempted": bool(receipts),
        }
    if phase == "canonical-keys":
        base = _partition_gold_root(suite_root, partition) / "canonical-keys"
        if not (base / "pass-a" / "jobs" / "canonical-keys.private.json").is_file():
            _prepare_canonical_key_gold(suite_root, partition)
        if pass_name == "compile":
            return {
                "ok": True,
                "state": "compiled",
                **_compile_canonical_key_gold(suite_root, partition),
            }
        if pass_name == "pass-c":
            preparation = _prepare_single_adjudication(
                base=base,
                name="canonical-keys",
                validator=_validate_canonical_key_gold,
                id_field="local_cluster_id",
            )
            directory_name = "pass-c-adjudication"
        else:
            preparation = None
            directory_name = pass_name
        pass_root = base / directory_name
        jobs = sorted((pass_root / "jobs").glob("*.private.json"))
        receipts = _execute_gold_jobs(
            jobs=jobs,
            pass_root=pass_root,
            timeout_seconds=timeout_seconds,
            codex_binary=codex_binary,
            validator=_validate_canonical_key_gold,
            workers=workers,
        )
        return {
            "ok": True,
            "suite_id": SUITE_ID,
            "state": "gold_pass_completed",
            "phase": phase,
            "partition": partition,
            "pass": pass_name,
            "executed_count": len(receipts),
            "idempotent_replays": sum(bool(row["idempotent_replay"]) for row in receipts),
            "preparation": preparation,
            "model_execution_attempted": bool(receipts),
        }
    if phase == "utility":
        base = _partition_gold_root(suite_root, partition) / "utility"
        if not any((base / "pass-a" / "jobs").glob("*.private.json")):
            _prepare_utility_gold(suite_root, partition)
        if pass_name == "compile":
            return {"ok": True, "state": "compiled", **_compile_utility_gold(suite_root, partition)}
        if pass_name == "pass-c":
            prepared = 0
            agreed = 0
            for job_path in sorted((base / "pass-a" / "jobs").glob("*.private.json")):
                name = job_path.name.removesuffix(".private.json")
                result = _prepare_single_adjudication(
                    base=base,
                    name=name,
                    validator=_validate_utility_gold,
                    id_field="question_id",
                    scope_field="questions",
                )
                prepared += int(result["prepared"])
                agreed += int(result["agreed"])
            preparation = {
                "packet_count": prepared + agreed,
                "prepared": prepared,
                "agreed": agreed,
            }
            directory_name = "pass-c-adjudication"
        else:
            preparation = None
            directory_name = pass_name
        pass_root = base / directory_name
        jobs = sorted((pass_root / "jobs").glob("*.private.json"))
        if limit is not None:
            jobs = jobs[: max(0, int(limit))]
        receipts = _execute_gold_jobs(
            jobs=jobs,
            pass_root=pass_root,
            timeout_seconds=timeout_seconds,
            codex_binary=codex_binary,
            validator=_validate_utility_gold,
            workers=workers,
        )
        return {
            "ok": True,
            "suite_id": SUITE_ID,
            "state": "gold_pass_completed",
            "phase": phase,
            "partition": partition,
            "pass": pass_name,
            "executed_count": len(receipts),
            "idempotent_replays": sum(bool(row["idempotent_replay"]) for row in receipts),
            "preparation": preparation,
            "model_execution_attempted": bool(receipts),
        }
    if phase == "relations":
        base = _partition_gold_root(suite_root, partition) / "relations"
        if not any((base / "pass-a" / "jobs").glob("*.private.json")):
            _prepare_relation_gold(suite_root, partition)
        if pass_name == "compile":
            return {"ok": True, "state": "compiled", **_compile_relation_gold(suite_root, partition)}
        if pass_name == "pass-c":
            preparation = _prepare_relation_adjudications(suite_root, partition)
            directory_name = "pass-c-adjudication"
        else:
            preparation = None
            directory_name = pass_name
        pass_root = base / directory_name
        jobs = sorted((pass_root / "jobs").glob("*.private.json"))
        if limit is not None:
            jobs = jobs[: max(0, int(limit))]
        receipts = _execute_gold_jobs(
            jobs=jobs,
            pass_root=pass_root,
            timeout_seconds=timeout_seconds,
            codex_binary=codex_binary,
            validator=_validate_relation_gold,
            workers=workers,
        )
        return {
            "ok": True,
            "suite_id": SUITE_ID,
            "state": "gold_pass_completed",
            "phase": phase,
            "partition": partition,
            "pass": pass_name,
            "executed_count": len(receipts),
            "idempotent_replays": sum(bool(row["idempotent_replay"]) for row in receipts),
            "preparation": preparation,
            "model_execution_attempted": bool(receipts),
        }
    if phase == "canonical":
        base = _partition_gold_root(suite_root, partition) / "canonical"
        if not any((base / "pass-a" / "jobs").glob("*.private.json")):
            _prepare_canonical_gold(suite_root, partition)
        if pass_name == "compile":
            return {"ok": True, "state": "compiled", **_compile_canonical_gold(suite_root, partition)}
        if pass_name == "pass-c":
            prepared = agreed = 0
            for job_path in sorted((base / "pass-a" / "jobs").glob("*.private.json")):
                result = _prepare_single_adjudication(
                    base=base,
                    name=job_path.name.removesuffix(".private.json"),
                    validator=_validate_canonical_gold,
                    id_field="gold_atomic_id",
                )
                prepared += result["prepared"]
                agreed += result["agreed"]
            preparation = {"prepared": prepared, "agreed": agreed}
            directory_name = "pass-c-adjudication"
        else:
            preparation = None
            directory_name = pass_name
        pass_root = base / directory_name
        jobs = sorted((pass_root / "jobs").glob("*.private.json"))
        if limit is not None:
            jobs = jobs[: max(0, int(limit))]
        receipts = _execute_gold_jobs(
            jobs=jobs,
            pass_root=pass_root,
            timeout_seconds=timeout_seconds,
            codex_binary=codex_binary,
            validator=_validate_canonical_gold,
            workers=workers,
        )
        return {
            "ok": True,
            "suite_id": SUITE_ID,
            "state": "gold_pass_completed",
            "phase": phase,
            "partition": partition,
            "pass": pass_name,
            "executed_count": len(receipts),
            "idempotent_replays": sum(bool(row["idempotent_replay"]) for row in receipts),
            "preparation": preparation,
            "model_execution_attempted": bool(receipts),
        }
    if pass_name == "compile":
        return {"ok": True, "state": "compiled", **_compile_final_gold(suite_root, partition)}
    if pass_name == "pass-c":
        preparation = _prepare_adjudication_packets(suite_root, partition)
        if preparation["missing_prior_outputs"]:
            raise TrueNorthError("pass-c requires complete pass-a and pass-b outputs")
        directory_name = "pass-c-adjudication"
    else:
        preparation = None
        directory_name = pass_name
    partition_dir = "sealed-holdout" if partition == "holdout" else "development"
    pass_root = suite_root / "gold" / partition_dir / directory_name
    jobs = sorted((pass_root / "jobs").glob("*.private.json"))
    if limit is not None:
        jobs = jobs[: max(0, int(limit))]
    receipts = _execute_gold_jobs(
        jobs=jobs,
        pass_root=pass_root,
        timeout_seconds=timeout_seconds,
        codex_binary=codex_binary,
        validator=None,
        workers=workers,
    )
    return {
        "ok": True,
        "suite_id": SUITE_ID,
        "state": "gold_pass_completed",
        "partition": partition,
        "phase": phase,
        "pass": pass_name,
        "executed_count": len(receipts),
        "idempotent_replays": sum(bool(row["idempotent_replay"]) for row in receipts),
        "preparation": preparation,
        "model_execution_attempted": bool(receipts),
    }


def execute_development_gold_pipeline(
    *,
    output_root: str | Path | None = None,
    suite: str = SUITE_ID,
    timeout_seconds: int = 1200,
    codex_binary: str = "codex",
    workers: int = 2,
) -> dict[str, Any]:
    """Run the restart-safe development gold dependency graph in order."""
    stages = []
    for phase in (
        "atomic",
        "canonical",
        "canonical-subjects",
        "canonical-propositions",
        "relations",
        "utility",
    ):
        for pass_name in ("pass-a", "pass-b", "pass-c", "compile"):
            result = execute_gold(
                output_root=output_root,
                suite=suite,
                partition="development",
                phase=phase,
                pass_name=pass_name,
                timeout_seconds=timeout_seconds,
                codex_binary=codex_binary,
                workers=workers,
            )
            stages.append(
                {
                    "phase": phase,
                    "pass": pass_name,
                    "state": result.get("state"),
                    "executed_count": result.get("executed_count"),
                    "idempotent_replays": result.get("idempotent_replays"),
                }
            )
    return {
        "ok": True,
        "suite_id": SUITE_ID,
        "partition": "development",
        "state": "development_gold_complete",
        "stages": stages,
        "holdout_opened": False,
    }


def _normalized_label(value: Any) -> str:
    return " ".join(
        re.sub(r"[^a-z0-9]+", " ", str(value or "").lower()).split()
    )


def _macro_f1(
    gold: Mapping[str, str],
    predicted: Mapping[str, str],
    labels: Sequence[str],
) -> tuple[float, dict[str, dict[str, float]]]:
    detail: dict[str, dict[str, float]] = {}
    scores = []
    for label in labels:
        tp = sum(gold[key] == label and predicted.get(key) == label for key in gold)
        fp = sum(gold[key] != label and predicted.get(key) == label for key in gold)
        fn = sum(gold[key] == label and predicted.get(key) != label for key in gold)
        precision = tp / (tp + fp) if tp + fp else 1.0
        recall = tp / (tp + fn) if tp + fn else 1.0
        f1 = (
            2 * precision * recall / (precision + recall)
            if precision + recall
            else 0.0
        )
        scores.append(f1)
        detail[label] = {
            "precision": round(precision, 6),
            "recall": round(recall, 6),
            "f1": round(f1, 6),
            "tp": tp,
            "fp": fp,
            "fn": fn,
        }
    return round(sum(scores) / len(scores), 6), detail


def _pairwise_cluster_metrics(
    gold: Mapping[str, str], predicted: Mapping[str, str]
) -> dict[str, float]:
    keys = sorted(set(gold) & set(predicted))
    tp = fp = fn = 0
    for left_index, left in enumerate(keys):
        for right in keys[left_index + 1 :]:
            gold_same = gold[left] == gold[right]
            predicted_same = predicted[left] == predicted[right]
            if gold_same and predicted_same:
                tp += 1
            elif not gold_same and predicted_same:
                fp += 1
            elif gold_same and not predicted_same:
                fn += 1
    precision = tp / (tp + fp) if tp + fp else 1.0
    recall = tp / (tp + fn) if tp + fn else 1.0
    f1 = (
        2 * precision * recall / (precision + recall)
        if precision + recall
        else 0.0
    )
    return {
        "precision": round(precision, 6),
        "recall": round(recall, 6),
        "f1": round(f1, 6),
        "false_merge_rate": round(fp / (tp + fp), 6) if tp + fp else 0.0,
        "true_positive_pairs": tp,
        "false_positive_pairs": fp,
        "false_negative_pairs": fn,
    }


def _gold_interannotator_reliability(
    suite_root: Path,
    partition_dir: str,
) -> dict[str, Any]:
    base = suite_root / "gold" / partition_dir
    pass_a = base / "pass-a"
    pass_b = base / "pass-b"
    a_items: dict[str, dict[str, Any]] = {}
    b_items: dict[str, dict[str, Any]] = {}
    for job_path in sorted((pass_a / "jobs").glob("*.private.json")):
        name = job_path.name.removesuffix(".private.json")
        a_path = pass_a / "outputs" / name / "validated.private.json"
        b_path = pass_b / "outputs" / name / "validated.private.json"
        if not a_path.is_file() or not b_path.is_file():
            continue
        for row in _read_json(a_path)["items"]:
            candidate_id = str(row["candidate_id"])
            if candidate_id in a_items:
                raise TrueNorthError(
                    f"duplicate pass-a gold candidate: {candidate_id}"
                )
            a_items[candidate_id] = dict(row)
        for row in _read_json(b_path)["items"]:
            candidate_id = str(row["candidate_id"])
            if candidate_id in b_items:
                raise TrueNorthError(
                    f"duplicate pass-b gold candidate: {candidate_id}"
                )
            b_items[candidate_id] = dict(row)
    raw_shared = sorted(set(a_items) & set(b_items))
    if not raw_shared:
        raise TrueNorthError(
            f"independent gold passes are unavailable for {partition_dir}"
        )
    if set(a_items) != set(b_items):
        raise TrueNorthError(
            "independent gold passes have different candidate scopes"
        )

    consensus_path = base / "final" / "consensus.private.json"
    contested_ids: set[str] = set()
    if consensus_path.is_file():
        consensus = _read_json(consensus_path)
        consensus_items = {
            str(row["candidate_id"]): row
            for row in consensus.get("items", [])
        }
        if set(consensus_items) != set(raw_shared):
            raise TrueNorthError(
                "consensus and independent gold have different candidate scopes"
            )
        contested_ids = {
            candidate_id
            for candidate_id, row in consensus_items.items()
            if not bool(row.get("strictly_scoreable"))
        }
    shared = [key for key in raw_shared if key not in contested_ids]
    if not shared:
        raise TrueNorthError(
            f"no strictly scoreable independent gold remains for {partition_dir}"
        )

    def exact_disposition_agreement(keys: Sequence[str]) -> float:
        return sum(
            a_items[key]["disposition"] == b_items[key]["disposition"]
            for key in keys
        ) / len(keys)

    def exact_atomic_count_agreement(keys: Sequence[str]) -> float:
        return sum(
            len(a_items[key]["atomic_claims"])
            == len(b_items[key]["atomic_claims"])
            for key in keys
        ) / len(keys)

    def exact_joint_agreement(keys: Sequence[str]) -> float:
        return sum(
            a_items[key]["disposition"] == b_items[key]["disposition"]
            and len(a_items[key]["atomic_claims"])
            == len(b_items[key]["atomic_claims"])
            for key in keys
        ) / len(keys)

    raw_disposition_agreement = exact_disposition_agreement(raw_shared)
    raw_atomic_count_agreement = exact_atomic_count_agreement(raw_shared)
    raw_joint_agreement = exact_joint_agreement(raw_shared)
    disposition_agreement = exact_disposition_agreement(shared)
    atomic_count_agreement = exact_atomic_count_agreement(shared)
    joint_agreement = exact_joint_agreement(shared)
    value_state_agreement = sum(
        _gold_value_state(str(a_items[key]["disposition"]))
        == _gold_value_state(str(b_items[key]["disposition"]))
        for key in shared
    ) / len(shared)
    rejects_a = {
        key for key in shared if a_items[key]["disposition"] == "reject"
    }
    rejects_b = {
        key for key in shared if b_items[key]["disposition"] == "reject"
    }
    reject_union = rejects_a | rejects_b
    reject_jaccard = (
        len(rejects_a & rejects_b) / len(reject_union)
        if reject_union
        else 1.0
    )
    threshold = 0.90
    return {
        "raw_item_count": len(raw_shared),
        "item_count": len(shared),
        "contested_excluded_count": len(contested_ids),
        "raw_disposition_agreement": raw_disposition_agreement,
        "raw_atomic_count_agreement": raw_atomic_count_agreement,
        "raw_joint_agreement": raw_joint_agreement,
        "disposition_agreement": disposition_agreement,
        "atomic_count_agreement": atomic_count_agreement,
        "joint_agreement": joint_agreement,
        "value_state_agreement": value_state_agreement,
        "pass_a_reject_count": len(rejects_a),
        "pass_b_reject_count": len(rejects_b),
        "reject_intersection_count": len(rejects_a & rejects_b),
        "reject_union_count": len(reject_union),
        "reject_jaccard": reject_jaccard,
        "threshold": threshold,
        "pass_gate": (
            value_state_agreement >= threshold
            and reject_jaccard >= threshold
        ),
    }


def _write_gold_reliability_diagnostic(
    suite_root: Path,
    partition_dir: str,
    *,
    partition: str,
) -> Path:
    reliability = _gold_interannotator_reliability(
        suite_root,
        partition_dir,
    )
    document = {
        "schema_version": "pif.true-north.gold-reliability.v2",
        "suite_id": SUITE_ID,
        "partition": partition,
        **reliability,
    }
    path = suite_root / "diagnostics" / "gold-reliability.json"
    _write_json(path, document, immutable=False)
    return path


def _predicted_atomic_outputs(run_root: Path) -> dict[str, dict[str, Any]]:
    items: dict[str, dict[str, Any]] = {}
    review_paths = sorted(
        (run_root / "atomic-review" / "outputs").glob(
            "*/validated.private.json"
        )
    )
    paths = review_paths or sorted(
        (run_root / "atomic" / "outputs").glob("*/validated.private.json")
    )
    for path in paths:
        output = _read_json(path)
        for item in output["items"]:
            candidate_id = str(item["candidate_id"])
            if candidate_id in items:
                raise TrueNorthError(
                    f"candidate appears in multiple run outputs: {candidate_id}"
                )
            items[candidate_id] = dict(item)
    return items


def _metric(
    name: str,
    value: float | None,
    *,
    threshold: float | None = None,
    comparison: str = ">=",
    details: Mapping[str, Any] | None = None,
    gate: bool = True,
    gate_group: str = "quality",
) -> dict[str, Any]:
    if not gate or value is None or threshold is None:
        passed = None
    elif comparison == ">=":
        passed = value >= threshold
    elif comparison == "<=":
        passed = value <= threshold
    elif comparison == "==":
        passed = value == threshold
    else:
        raise TrueNorthError(f"unsupported metric comparison: {comparison}")
    return {
        "metric": name,
        "value": value,
        "threshold": threshold,
        "comparison": comparison,
        "passed": passed,
        "gate": gate,
        "gate_group": gate_group,
        "details": dict(details or {}),
    }


def _source_unchanged(manifest: Mapping[str, Any]) -> bool:
    source_record = manifest["source_database"]
    path = Path(source_record["path"])
    if not path.is_file():
        return False
    stat = path.stat()
    if (
        stat.st_size != int(source_record["size_bytes"])
        or stat.st_mtime_ns != int(source_record["mtime_ns"])
    ):
        return False
    if source_record.get("sha256"):
        return _sha256_file(path) == source_record["sha256"]
    return True


def _consensus_atomic_metrics(
    consensus_document: Mapping[str, Any],
    predicted: Mapping[str, Mapping[str, Any]],
    *,
    require_complete_scope: bool = True,
) -> list[dict[str, Any]]:
    consensus = {
        str(row["candidate_id"]): row
        for row in consensus_document["items"]
    }
    if require_complete_scope and set(consensus) != set(predicted):
        missing = sorted(set(consensus) - set(predicted))
        extra = sorted(set(predicted) - set(consensus))
        raise TrueNorthError(
            "consensus/run candidate scope differs: "
            f"missing={len(missing)} extra={len(extra)}"
        )
    if not set(predicted) <= set(consensus):
        raise TrueNorthError(
            "atomic diagnostic contains candidates outside consensus gold"
        )
    if not require_complete_scope:
        consensus = {
            key: consensus[key] for key in sorted(predicted)
        }
    strict = {
        key: row
        for key, row in consensus.items()
        if bool(row["strictly_scoreable"])
    }
    gold_states = {
        key: str(row["consensus_state"]).removeprefix("consensus_")
        for key, row in strict.items()
    }
    predicted_states = {
        key: _gold_value_state(str(predicted[key]["disposition"]))
        for key in strict
    }
    state_f1, state_detail = _macro_f1(
        gold_states,
        predicted_states,
        ("value", "junk", "hold"),
    )
    consensus_value = {
        key
        for key, row in strict.items()
        if row["consensus_state"] == "consensus_value"
    }
    retained_value = {
        key
        for key in consensus_value
        if predicted_states[key] == "value"
    }
    retained_value_recall = (
        len(retained_value) / len(consensus_value)
        if consensus_value
        else 1.0
    )
    consensus_junk = {
        key
        for key, row in strict.items()
        if row["consensus_state"] == "consensus_junk"
    }
    escaped_junk = {
        key
        for key in consensus_junk
        if predicted_states[key] == "value"
    }
    junk_escape_rate = (
        len(escaped_junk) / len(consensus_junk)
        if consensus_junk
        else 0.0
    )
    count_correct = 0
    count_scored = 0
    count_detail: dict[str, Any] = {}
    for candidate_id in sorted(consensus_value):
        if predicted_states[candidate_id] != "value":
            continue
        count_scored += 1
        predicted_count = len(predicted[candidate_id]["atomic_claims"])
        row = consensus[candidate_id]
        minimum = int(row["minimum_atomic_count"])
        maximum = int(row["maximum_atomic_count"])
        correct = minimum <= predicted_count <= maximum
        count_correct += int(correct)
        if not correct:
            count_detail[candidate_id] = {
                "predicted": predicted_count,
                "minimum": minimum,
                "maximum": maximum,
            }
    atomic_range_accuracy = (
        count_correct / count_scored if count_scored else 0.0
    )
    contested = {
        key: row
        for key, row in consensus.items()
        if row["consensus_state"] == "contested"
    }
    contested_acceptable = 0
    contested_detail: dict[str, Any] = {}
    for candidate_id, row in contested.items():
        predicted_state = _gold_value_state(
            str(predicted[candidate_id]["disposition"])
        )
        acceptable = set(row["acceptable_value_states"]) | {"hold"}
        correct = predicted_state in acceptable
        contested_acceptable += int(correct)
        if not correct:
            contested_detail[candidate_id] = {
                "predicted": predicted_state,
                "acceptable": sorted(acceptable),
            }
    contested_safe_rate = (
        contested_acceptable / len(contested) if contested else 1.0
    )
    return [
        _metric(
            "consensus_candidate_state_macro_f1",
            state_f1,
            threshold=0.90,
            details={
                **state_detail,
                "strict_item_count": len(strict),
            },
            gate_group="core_value",
        ),
        _metric(
            "retained_value_recall",
            retained_value_recall,
            threshold=0.90,
            details={
                "retained": len(retained_value),
                "consensus_value": len(consensus_value),
            },
            gate_group="core_value",
        ),
        _metric(
            "consensus_junk_escape_rate",
            junk_escape_rate,
            threshold=0.02,
            comparison="<=",
            details={
                "escaped": len(escaped_junk),
                "consensus_junk": len(consensus_junk),
            },
            gate_group="safety",
        ),
        _metric(
            "acceptable_atomic_count_rate",
            atomic_range_accuracy,
            threshold=0.90,
            details={
                "correct": count_correct,
                "scored_retained_value": count_scored,
                "out_of_range": count_detail,
            },
            gate_group="core_value",
        ),
        _metric(
            "contested_safe_handling_rate",
            contested_safe_rate,
            threshold=0.90,
            details={
                "acceptable": contested_acceptable,
                "contested": len(contested),
                "failures": contested_detail,
            },
            gate_group="core_value",
        ),
    ]


def diagnose_atomic_run(
    *,
    run_id: str,
    output_root: str | Path | None = None,
    suite: str = SUITE_ID,
) -> dict[str, Any]:
    suite_root = _suite_root(output_root, suite)
    run_root = suite_root / "runs" / run_id
    if not run_root.is_dir():
        raise TrueNorthError(f"atomic diagnostic run is missing: {run_id}")
    predicted = _predicted_atomic_outputs(run_root)
    if not predicted:
        raise TrueNorthError(
            f"run has no validated atomic outputs: {run_id}"
        )
    consensus_path = (
        suite_root
        / "gold"
        / "development"
        / "final"
        / "consensus.private.json"
    )
    if not consensus_path.is_file():
        raise TrueNorthError(
            "development consensus-aware atomic gold is not compiled"
        )
    consensus_document = _read_json(consensus_path)
    metrics = _consensus_atomic_metrics(
        consensus_document,
        predicted,
        require_complete_scope=False,
    )
    gate_metrics = [row for row in metrics if row["gate"]]
    result = {
        "schema_version": "pif_true_north_atomic_diagnostic_v1",
        "suite_id": SUITE_ID,
        "run_id": run_id,
        "candidate_count": len(predicted),
        "consensus_gold_sha256": consensus_document["consensus_sha256"],
        "passed": all(row["passed"] is True for row in gate_metrics),
        "metrics": metrics,
        "production_mutation": False,
        "holdout_opened": False,
        "diagnosed_at": now_iso(),
    }
    result["diagnostic_sha256"] = sha256_text(dumps_json(result))
    path = run_root / "atomic-diagnostic.json"
    _write_json(path, result, immutable=False)
    return {**result, "diagnostic_path": str(path)}


def run_system_prompt_arm(
    *,
    arm: str,
    output_root: str | Path | None = None,
    suite: str = SUITE_ID,
    episode_ids: Sequence[str] | None = None,
    workers: int = 4,
    timeout_seconds: int = 900,
    opencode_binary: str = "/opt/homebrew/bin/opencode",
) -> dict[str, Any]:
    """Run one system-prompt-only arm against frozen development packets."""
    if arm not in SYSTEM_PROMPT_ARMS:
        raise TrueNorthError(f"unknown system prompt arm: {arm}")
    if workers < 1 or workers > 4:
        raise TrueNorthError("prompt optimization workers must be between 1 and 4")
    suite_root = _suite_root(output_root, suite)
    manifest = _read_json(suite_root / "manifest.json")
    consensus_path = (
        suite_root
        / "gold"
        / "development"
        / "final"
        / "consensus.private.json"
    )
    if not consensus_path.is_file():
        raise TrueNorthError("development consensus gold is unavailable")
    consensus = _read_json(consensus_path)
    development = {
        str(row["episode_id"]): row
        for row in manifest["bundles"]
        if row["partition"] == "development"
    }
    requested = (
        list(dict.fromkeys(str(value) for value in episode_ids))
        if episode_ids
        else sorted(development)
    )
    unknown = sorted(set(requested) - set(development))
    if unknown:
        raise TrueNorthError(
            f"prompt optimization cannot access non-development episodes: {unknown}"
        )
    experiment_root = (
        suite_root / "prompt-optimization" / PROMPT_OPTIMIZATION_SCHEMA_VERSION
    )
    jobs_root = experiment_root / "jobs"
    job_records: list[dict[str, Any]] = []
    work: list[tuple[str, str, Path, Path]] = []
    for episode_id in requested:
        bundle = _read_json(Path(development[episode_id]["bundle_path"]))
        for job in _segment_jobs(bundle, gold=False):
            segment_id = str(job["input"]["segment"]["segment_id"])
            job_path = jobs_root / episode_id / f"{segment_id}.private.json"
            _write_json(job_path, job, immutable=True)
            job_records.append(
                {
                    "episode_id": episode_id,
                    "segment_id": segment_id,
                    "path": str(job_path),
                    "sha256": _sha256_file(job_path),
                }
            )
            work.append(
                (
                    episode_id,
                    segment_id,
                    job_path,
                    experiment_root
                    / "arms"
                    / arm
                    / "outputs"
                    / episode_id
                    / segment_id,
                )
            )
    invariant = {
        "schema_version": PROMPT_OPTIMIZATION_SCHEMA_VERSION,
        "suite_id": SUITE_ID,
        "suite_manifest_sha256": manifest["manifest_sha256"],
        "consensus_gold_sha256": consensus["consensus_sha256"],
        "model": PROMPT_OPTIMIZATION_MODEL,
        "temperature": 0.1,
        "steps": 12,
        "stage": "prompt-optimization",
        "packet_count": len(job_records),
        "packets": job_records,
        "system_prompt_is_only_arm_variable": True,
        "holdout_access_allowed": False,
    }
    invariant["invariant_sha256"] = sha256_text(dumps_json(invariant))
    invariant_path = experiment_root / "invariants" / (
        sha256_text(dumps_json(requested)) + ".json"
    )
    _write_json(invariant_path, invariant, immutable=True)
    prompt = SYSTEM_PROMPT_ARMS[arm]
    prompt_path = experiment_root / "system-prompts" / f"{arm}.txt"
    _write_text(prompt_path, prompt + "\n", immutable=True)

    def execute(
        item: tuple[str, str, Path, Path],
    ) -> tuple[str, str, dict[str, Any], list[dict[str, Any]], str]:
        episode_id, segment_id, job_path, output_dir = item
        output, receipts, model = _run_opencode_packet(
            packet_path=job_path,
            output_dir=output_dir,
            models=(PROMPT_OPTIMIZATION_MODEL,),
            stage="prompt-optimization",
            timeout_seconds=timeout_seconds,
            opencode_binary=opencode_binary,
            system_prompt=prompt,
        )
        return episode_id, segment_id, output, receipts, model

    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(execute, item) for item in work]
        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())
    predicted: dict[str, dict[str, Any]] = {}
    receipts: list[dict[str, Any]] = []
    candidate_episode: dict[str, str] = {}
    models_used: set[str] = set()
    for episode_id, _segment_id, output, packet_receipts, model in results:
        models_used.add(model)
        receipts.extend(packet_receipts)
        for item in output["items"]:
            candidate_id = str(item["candidate_id"])
            if candidate_id in predicted:
                raise TrueNorthError(
                    f"prompt arm emitted duplicate candidate: {candidate_id}"
                )
            predicted[candidate_id] = dict(item)
            candidate_episode[candidate_id] = episode_id
    metrics = _consensus_atomic_metrics(
        consensus,
        predicted,
        require_complete_scope=False,
    )
    by_episode: dict[str, list[dict[str, Any]]] = {}
    for episode_id in requested:
        episode_predictions = {
            candidate_id: item
            for candidate_id, item in predicted.items()
            if candidate_episode[candidate_id] == episode_id
        }
        by_episode[episode_id] = _consensus_atomic_metrics(
            consensus,
            episode_predictions,
            require_complete_scope=False,
        )
    elapsed_values = [
        float(row["elapsed_seconds"])
        for row in receipts
        if not row.get("usage", {}).get("checkpoint_reuse")
    ]
    score = {
        "schema_version": PROMPT_OPTIMIZATION_SCHEMA_VERSION,
        "suite_id": SUITE_ID,
        "arm": arm,
        "system_prompt_sha256": sha256_text(prompt),
        "invariant_sha256": invariant["invariant_sha256"],
        "model": PROMPT_OPTIMIZATION_MODEL,
        "models_used": sorted(models_used),
        "episode_ids": requested,
        "candidate_count": len(predicted),
        "packet_count": len(work),
        "metrics": metrics,
        "episode_metrics": by_episode,
        "latency": {
            "call_count": len(elapsed_values),
            "total_seconds": round(sum(elapsed_values), 3),
            "mean_seconds": (
                round(sum(elapsed_values) / len(elapsed_values), 3)
                if elapsed_values
                else 0.0
            ),
            "maximum_seconds": max(elapsed_values, default=0.0),
        },
        "receipts": receipts,
        "holdout_opened": False,
        "production_mutation": False,
        "completed_at": now_iso(),
    }
    score["score_sha256"] = sha256_text(dumps_json(score))
    score_path = experiment_root / "arms" / arm / (
        "score-" + sha256_text(dumps_json(requested))[:16] + ".private.json"
    )
    _write_json(score_path, score, immutable=False)
    return {
        **score,
        "receipts": len(receipts),
        "score_path": str(score_path),
        "invariant_path": str(invariant_path),
        "system_prompt_path": str(prompt_path),
    }


def _multipass_usage(receipts: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    tokens = 0
    elapsed = 0.0
    calls = 0
    for receipt in receipts:
        usage = receipt.get("usage", {})
        checkpoint_reuse = (
            bool(usage.get("checkpoint_reuse"))
            if isinstance(usage, Mapping)
            else False
        )
        if not checkpoint_reuse:
            calls += 1
        total = usage.get("total_tokens") if isinstance(usage, Mapping) else None
        if isinstance(total, (int, float)) and not checkpoint_reuse:
            tokens += int(total)
        if not checkpoint_reuse:
            elapsed += float(receipt.get("elapsed_seconds") or 0.0)
    return {
        "calls": calls,
        "tokens": tokens,
        "receipt_elapsed_seconds": round(elapsed, 3),
    }


def _multipass_state_write(path: Path, state: Mapping[str, Any]) -> None:
    value = {key: item for key, item in state.items() if key != "state_sha256"}
    value["state_sha256"] = sha256_text(dumps_json(value))
    _write_json(path, value, immutable=False)


def _multipass_check_budget(
    state: Mapping[str, Any],
    *,
    pending_calls: int,
    reserved_tokens_per_call: int = 32_000,
) -> None:
    budget = state["budget"]
    usage = state["usage"]
    if int(usage["calls"]) + pending_calls > int(budget["max_calls"]):
        raise TrueNorthError("multipass call budget cannot cover the next stage")
    if (
        int(usage["tokens"])
        + pending_calls * reserved_tokens_per_call
        > int(budget["max_tokens"])
    ):
        raise TrueNorthError("multipass token budget cannot cover the next stage")
    if float(usage["wall_seconds"]) >= float(budget["max_wall_seconds"]):
        raise TrueNorthError("multipass wall-time budget is exhausted")


def _multipass_execute_stage(
    *,
    run_root: Path,
    stage: str,
    jobs: Sequence[tuple[str, str, Mapping[str, Any]]],
    state: dict[str, Any],
    workers: int,
    timeout_seconds: int,
    opencode_binary: str,
    runner: Any | None,
) -> dict[tuple[str, str], dict[str, Any]]:
    if stage not in MULTIPASS_ALL_STAGES:
        raise TrueNorthError(f"unknown multipass stage: {stage}")
    validators = {
        "disposition": validate_multipass_disposition,
        "decomposition": validate_multipass_decomposition,
        "adjudication": validate_multipass_adjudication,
        "attribution": validate_multipass_attribution,
    }
    validate = validators[stage]
    outputs: dict[tuple[str, str], dict[str, Any]] = {}
    pending: list[tuple[str, str, Path, Path]] = []
    for episode_id, segment_id, packet in jobs:
        packet_path = (
            run_root
            / "packets"
            / stage
            / episode_id
            / f"{segment_id}.private.json"
        )
        output_dir = run_root / "outputs" / stage / episode_id / segment_id
        _write_json(packet_path, packet, immutable=True)
        validated = output_dir / "validated.private.json"
        if validated.is_file():
            output = _read_json(validated)
            validate(output, packet)
            outputs[(episode_id, segment_id)] = output
            continue
        pending.append((episode_id, segment_id, packet_path, output_dir))
    # A process can fail after the provider returned an invalid answer but
    # before _run_opencode_packet can return a receipt. Account those paid
    # attempts from the preserved OpenCode stream before allowing a resume.
    newly_accounted_failures = 0
    newly_accounted_tokens = 0
    for _episode_id, _segment_id, _packet_path, output_dir in pending:
        for attempt_path in sorted(output_dir.glob("attempt-*.private.jsonl")):
            attempt_sha = _sha256_file(attempt_path)
            marker = output_dir / f"failed-attempt-{attempt_sha}.accounted.json"
            if marker.is_file():
                continue
            _answer, finish, _events = _parse_opencode_stream(
                attempt_path.read_text(encoding="utf-8")
            )
            usage = _usage(finish)
            tokens = usage.get("total_tokens")
            token_count = int(tokens) if isinstance(tokens, (int, float)) else 32_000
            accounting = {
                "schema_version": MULTIPASS_SCHEMA_VERSION,
                "stage": stage,
                "attempt_sha256": attempt_sha,
                "usage": usage,
                "conservative_tokens_used": token_count,
                "reason": "provider_answer_failed_local_validation",
            }
            _write_json(marker, accounting, immutable=True)
            newly_accounted_failures += 1
            newly_accounted_tokens += token_count
    if newly_accounted_failures:
        state["usage"]["calls"] += newly_accounted_failures
        state["usage"]["tokens"] += newly_accounted_tokens
        _multipass_state_write(run_root / "state.json", state)
    _multipass_check_budget(state, pending_calls=len(pending))
    actual_runner = runner or _run_opencode_packet

    def execute(
        row: tuple[str, str, Path, Path],
    ) -> tuple[str, str, dict[str, Any], list[dict[str, Any]], str]:
        episode_id, segment_id, packet_path, output_dir = row
        kwargs = {
            "packet_path": packet_path,
            "output_dir": output_dir,
            "models": (MULTIPASS_MODEL,),
            "stage": f"multipass-{stage}",
            "timeout_seconds": timeout_seconds,
            "opencode_binary": opencode_binary,
            "system_prompt": MULTIPASS_SYSTEM_PROMPTS[stage],
            "validator": validate,
        }
        if runner is None:
            # Retry capacity is a global campaign resource. The orchestrator
            # performs one paid attempt per stage packet so 120 is a real hard
            # ceiling; a failed packet remains resumable and never advances.
            kwargs["_semantic_retry_remaining"] = 0
        output, receipts, model = actual_runner(**kwargs)
        if model != MULTIPASS_MODEL:
            raise TrueNorthError("multipass stage used an undeclared model")
        validate(output, _read_json(packet_path))
        return episode_id, segment_id, output, receipts, model

    started = time.monotonic()
    results = []
    errors: list[Exception] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(execute, row) for row in pending]
        for future in concurrent.futures.as_completed(futures):
            try:
                results.append(future.result())
            except Exception as exc:
                errors.append(exc)
    stage_wall = round(time.monotonic() - started, 3)
    for episode_id, segment_id, output, receipts, model in results:
        output_dir = run_root / "outputs" / stage / episode_id / segment_id
        _write_json(
            output_dir / "validated.private.json",
            output,
            immutable=False,
        )
        usage = _multipass_usage(receipts)
        record = {
            "schema_version": MULTIPASS_SCHEMA_VERSION,
            "stage": stage,
            "episode_id": episode_id,
            "segment_id": segment_id,
            "provider_model": model,
            "system_prompt_sha256": sha256_text(
                MULTIPASS_SYSTEM_PROMPTS[stage]
            ),
            "output_sha256": sha256_text(dumps_json(output)),
            "usage": usage,
            "receipts": receipts,
        }
        record["receipt_sha256"] = sha256_text(dumps_json(record))
        _write_json(output_dir / "receipt.json", record, immutable=True)
        outputs[(episode_id, segment_id)] = output
        state["completed"].append(f"{stage}/{episode_id}/{segment_id}")
        state["usage"]["calls"] += int(usage["calls"])
        state["usage"]["tokens"] += int(usage["tokens"])
    state["completed"] = sorted(set(state["completed"]))
    state["usage"]["wall_seconds"] = round(
        float(state["usage"]["wall_seconds"]) + stage_wall,
        3,
    )
    if (
        int(state["usage"]["calls"]) > int(state["budget"]["max_calls"])
        or int(state["usage"]["tokens"]) > int(state["budget"]["max_tokens"])
        or float(state["usage"]["wall_seconds"])
        > float(state["budget"]["max_wall_seconds"])
    ):
        raise TrueNorthError("persisted multipass usage exceeded campaign budget")
    _multipass_state_write(run_root / "state.json", state)
    if errors:
        raise errors[0]
    return outputs


def run_multipass(
    *,
    output_root: str | Path | None = None,
    suite: str = SUITE_ID,
    episode_ids: Sequence[str] | None = None,
    workers: int = 4,
    timeout_seconds: int = 900,
    opencode_binary: str = "/opt/homebrew/bin/opencode",
    run_id: str | None = None,
    resume_run_id: str | None = None,
    max_packets: int | None = None,
    runner: Any | None = None,
    budget: Mapping[str, Any] | None = None,
    stage_b_mode: str = DEFAULT_MULTIPASS_STAGE_B_MODE,
) -> dict[str, Any]:
    """Run the bounded disposition -> stage B -> attribution stack.

    ``stage_b_mode`` selects the stage-B contract and defaults to the committed
    ``decomposition`` behaviour.  Because the mode is carried by ``stages`` and
    ``system_prompt_sha256`` rather than a new configuration key, a default run
    hashes exactly as it did before this option existed and in-flight runs stay
    resumable.
    """
    if workers < 1 or workers > 4:
        raise TrueNorthError("multipass workers must be between 1 and 4")
    if stage_b_mode not in MULTIPASS_STAGE_B_MODES:
        raise TrueNorthError(f"unknown multipass stage-B mode: {stage_b_mode}")
    # The mode names are the stage names, so no separate mapping is needed.
    stage_sequence = (
        MULTIPASS_STAGES
        if stage_b_mode == "decomposition"
        else MULTIPASS_ADJUDICATION_STAGES
    )
    build_stage_b_packet = (
        build_multipass_decomposition_packet
        if stage_b_mode == "decomposition"
        else build_multipass_adjudication_packet
    )
    if run_id and resume_run_id:
        raise TrueNorthError("choose run_id or resume_run_id, not both")
    suite_root = _suite_root(output_root, suite)
    manifest = _read_json(suite_root / "manifest.json")
    development = {
        str(row["episode_id"]): row
        for row in manifest["bundles"]
        if row["partition"] == "development"
    }
    requested = list(
        dict.fromkeys(
            str(value)
            for value in (
                episode_ids or MULTIPASS_OPEN_DEVELOPMENT_EPISODES
            )
        )
    )
    forbidden = sorted(
        set(requested) - set(MULTIPASS_OPEN_DEVELOPMENT_EPISODES)
    )
    if forbidden:
        raise TrueNorthError(
            f"multipass development run cannot open sealed episodes: {forbidden}"
        )
    missing = sorted(set(requested) - set(development))
    if missing:
        raise TrueNorthError(f"multipass episodes are not in manifest: {missing}")
    effective_budget = {
        **MULTIPASS_DEFAULT_BUDGET,
        **dict(budget or {}),
    }
    configuration = {
        "schema_version": MULTIPASS_SCHEMA_VERSION,
        "suite_id": suite,
        "suite_manifest_sha256": manifest["manifest_sha256"],
        "episode_ids": requested,
        "model": MULTIPASS_MODEL,
        "stages": list(stage_sequence),
        "system_prompt_sha256": {
            stage: sha256_text(MULTIPASS_SYSTEM_PROMPTS[stage])
            for stage in stage_sequence
        },
        "budget": effective_budget,
        "holdout_access_allowed": False,
        "production_database_open_allowed": False,
    }
    configuration["configuration_sha256"] = sha256_text(
        dumps_json(configuration)
    )
    resolved_run_id = resume_run_id or run_id or (
        "multipass-"
        + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        + "-"
        + configuration["configuration_sha256"][:8]
    )
    run_root = suite_root / "multipass" / "runs" / resolved_run_id
    config_path = run_root / "configuration.json"
    state_path = run_root / "state.json"
    if config_path.is_file():
        existing = _read_json(config_path)
        if existing != configuration:
            raise TrueNorthError(
                "multipass resume configuration differs from frozen run"
            )
    else:
        _write_json(config_path, configuration, immutable=True)
    if state_path.is_file():
        state = _read_json(state_path)
        expected_hash = state.get("state_sha256")
        observed = sha256_text(
            dumps_json(
                {
                    key: value
                    for key, value in state.items()
                    if key != "state_sha256"
                }
            )
        )
        if expected_hash != observed:
            raise TrueNorthError("multipass state hash mismatch")
    else:
        state = {
            "schema_version": MULTIPASS_SCHEMA_VERSION,
            "run_id": resolved_run_id,
            "configuration_sha256": configuration["configuration_sha256"],
            "budget": effective_budget,
            "completed": [],
            "usage": {"calls": 0, "tokens": 0, "wall_seconds": 0.0},
        }
        _multipass_state_write(state_path, state)
    base_jobs: dict[tuple[str, str], dict[str, Any]] = {}
    for episode_id in requested:
        bundle = _read_json(Path(development[episode_id]["bundle_path"]))
        for job in _segment_jobs(bundle, gold=False):
            segment_id = str(job["input"]["segment"]["segment_id"])
            base_jobs[(episode_id, segment_id)] = job
    ordered_keys = sorted(base_jobs)
    if max_packets is not None:
        ordered_keys = ordered_keys[:max_packets]
    stage_a_jobs = [
        (
            episode_id,
            segment_id,
            build_multipass_disposition_packet(
                base_jobs[(episode_id, segment_id)]
            ),
        )
        for episode_id, segment_id in ordered_keys
    ]
    stage_a = _multipass_execute_stage(
        run_root=run_root,
        stage="disposition",
        jobs=stage_a_jobs,
        state=state,
        workers=workers,
        timeout_seconds=timeout_seconds,
        opencode_binary=opencode_binary,
        runner=runner,
    )
    stage_b_jobs = []
    for key in ordered_keys:
        packet = build_stage_b_packet(base_jobs[key], stage_a[key])
        if packet is not None:
            stage_b_jobs.append((*key, packet))
    stage_b = _multipass_execute_stage(
        run_root=run_root,
        stage=stage_b_mode,
        jobs=stage_b_jobs,
        state=state,
        workers=workers,
        timeout_seconds=timeout_seconds,
        opencode_binary=opencode_binary,
        runner=runner,
    )
    stage_c_jobs = []
    for key in ordered_keys:
        decomposition = stage_b.get(key)
        if decomposition is None:
            continue
        packet = build_multipass_attribution_packet(
            base_jobs[key], decomposition
        )
        if packet is not None:
            stage_c_jobs.append((*key, packet))
    stage_c = _multipass_execute_stage(
        run_root=run_root,
        stage="attribution",
        jobs=stage_c_jobs,
        state=state,
        workers=workers,
        timeout_seconds=timeout_seconds,
        opencode_binary=opencode_binary,
        runner=runner,
    )
    composed_count = 0
    for key in ordered_keys:
        episode_id, segment_id = key
        output = compose_multipass_output(
            base_jobs[key],
            stage_a[key],
            stage_b.get(key),
            stage_c.get(key),
            stage_b_mode=stage_b_mode,
        )
        destination = (
            run_root
            / "outputs"
            / "composed"
            / episode_id
            / segment_id
            / "validated.private.json"
        )
        _write_json(destination, output, immutable=True)
        composed_count += 1
    state["complete"] = composed_count == len(ordered_keys)
    state["packet_count"] = len(ordered_keys)
    state["candidate_count"] = sum(
        len(base_jobs[key]["input"]["candidates"]) for key in ordered_keys
    )
    _multipass_state_write(state_path, state)
    return {
        "ok": True,
        "schema_version": MULTIPASS_SCHEMA_VERSION,
        "run_id": resolved_run_id,
        "configuration_sha256": configuration["configuration_sha256"],
        "episode_ids": requested,
        "stage_b_mode": stage_b_mode,
        "packet_count": len(ordered_keys),
        "candidate_count": state["candidate_count"],
        "usage": state["usage"],
        "complete": state["complete"],
        "run_root": str(run_root),
        "holdout_opened": False,
        "production_mutation": False,
    }


def score_multipass_run(
    *,
    run_id: str,
    output_root: str | Path | None = None,
    suite: str = SUITE_ID,
) -> dict[str, Any]:
    from .true_north_semantic_scoring import score_campaign as semantic_score

    suite_root = _suite_root(output_root, suite)
    run_root = suite_root / "multipass" / "runs" / run_id
    configuration = _read_json(run_root / "configuration.json")
    state = _read_json(run_root / "state.json")
    if not state.get("complete"):
        raise TrueNorthError("multipass run is incomplete")
    consensus_document = _read_json(
        suite_root
        / "gold"
        / "development"
        / "final"
        / "consensus.private.json"
    )
    preferred_document = _read_json(
        suite_root
        / "gold"
        / "development"
        / "final"
        / "gold.private.json"
    )
    predictions: list[dict[str, Any]] = []
    speaker_maps: dict[str, Any] = {}
    manifest = _read_json(suite_root / "manifest.json")
    bundles = {
        str(row["episode_id"]): row for row in manifest["bundles"]
    }
    for episode_id in configuration["episode_ids"]:
        bundle = _read_json(Path(bundles[episode_id]["bundle_path"]))
        candidate_ids = {
            str(row["candidate_id"]) for row in bundle["candidates"]
        }
        for candidate_id in candidate_ids:
            speaker_maps[candidate_id] = bundle["episode_context"].get(
                "speaker_map", []
            )
        for path in sorted(
            (
                run_root / "outputs" / "composed" / episode_id
            ).glob("*/validated.private.json")
        ):
            predictions.extend(_read_json(path)["items"])
    ids = {str(row["candidate_id"]) for row in predictions}
    consensus = [
        row
        for row in consensus_document["items"]
        if str(row["candidate_id"]) in ids
    ]
    preferred = [
        row
        for row in preferred_document["items"]
        if str(row["candidate_id"]) in ids
    ]
    score = dict(
        semantic_score(
            predictions,
            consensus,
            preferred,
            speaker_maps_by_candidate=speaker_maps,
        )
    )
    score["aggregate"]["schema_parse_success_rate"] = 1.0
    core = _consensus_atomic_metrics(
        consensus_document,
        {str(row["candidate_id"]): row for row in predictions},
        require_complete_scope=False,
    )
    score["aggregate"].update(
        {str(row["metric"]): row["value"] for row in core}
    )
    gates = dict(APPROVED_GATE_POLICY)
    checks = {
        metric: (
            float(score["aggregate"][metric]) >= threshold
            if comparison == ">="
            else float(score["aggregate"][metric]) <= threshold
        )
        for metric, (comparison, threshold) in gates.items()
    }
    document = {
        "schema_version": MULTIPASS_SCHEMA_VERSION,
        "run_id": run_id,
        "configuration_sha256": configuration["configuration_sha256"],
        "candidate_count": len(predictions),
        "aggregate": score["aggregate"],
        "gate_policy": gates,
        "gate_checks": checks,
        "passed_gate_count": sum(checks.values()),
        "passed": all(checks.values()),
        "private_candidate_scores": score["candidates"],
        "usage": state["usage"],
        "holdout_opened": False,
        "production_mutation": False,
    }
    document["score_sha256"] = sha256_text(dumps_json(document))
    path = run_root / "score.private.json"
    _write_json(path, document, immutable=False)
    return {
        **{key: value for key, value in document.items() if key != "private_candidate_scores"},
        "score_path": str(path),
    }


def _phase_c_preferred_document(suite_root: Path) -> dict[str, Any]:
    return _read_json(
        suite_root
        / "gold"
        / "development"
        / "final"
        / "gold.private.json"
    )


def _score_phase_c_dispositions(
    consensus_document: Mapping[str, Any],
    predictions: Mapping[str, Mapping[str, Any]],
    preferred_document: Mapping[str, Any],
) -> dict[str, Any]:
    from .true_north_relational_merge import is_relational_junk_reason

    consensus = {
        str(row["candidate_id"]): row
        for row in consensus_document["items"]
        if str(row["candidate_id"]) in predictions
    }
    if set(consensus) != set(predictions):
        raise TrueNorthError(
            "phase-C disposition predictions lack consensus coverage"
        )
    strict = {
        key: row
        for key, row in consensus.items()
        if bool(row["strictly_scoreable"])
    }
    gold_states = {
        key: str(row["consensus_state"]).removeprefix("consensus_")
        for key, row in strict.items()
    }
    predicted_states = {
        key: _gold_value_state(str(predictions[key]["disposition"]))
        for key in strict
    }
    macro_f1, detail = _macro_f1(
        gold_states, predicted_states, ("value", "junk", "hold")
    )
    gold_value = {
        key for key, value in gold_states.items() if value == "value"
    }
    gold_junk = {
        key for key, value in gold_states.items() if value == "junk"
    }
    false_rejects = sorted(
        key
        for key in gold_value
        if predicted_states[key] != "value"
    )
    all_junk_escapes = sorted(
        key
        for key in gold_junk
        if predicted_states[key] == "value"
    )
    reject_reasons = {
        str(row["candidate_id"]): str(row.get("reason_code") or "")
        for row in preferred_document["items"]
        if str(row.get("disposition")) == "reject"
    }
    missing_reasons = sorted(gold_junk - set(reject_reasons))
    if missing_reasons:
        raise TrueNorthError(
            "phase-C intrinsic-junk gate lacks preferred-gold reason codes: "
            + ", ".join(missing_reasons)
        )
    relational_junk_escapes = sorted(
        key
        for key in all_junk_escapes
        if is_relational_junk_reason(reject_reasons[key])
    )
    intrinsic_junk_escapes = sorted(
        set(all_junk_escapes) - set(relational_junk_escapes)
    )
    retained_value_recall = (
        (len(gold_value) - len(false_rejects)) / len(gold_value)
        if gold_value
        else 1.0
    )
    junk_escape_rate = (
        len(intrinsic_junk_escapes) / len(gold_junk)
        if gold_junk
        else 0.0
    )
    old_acceptance = {
        "junk_escapes_zero": len(all_junk_escapes) == 0,
        "false_rejects_at_most_10": len(false_rejects) <= 10,
        "retained_value_recall_at_least_0_95": (
            retained_value_recall >= 0.95
        ),
    }
    acceptance = {
        "intrinsic_junk_escapes_zero": (
            len(intrinsic_junk_escapes) == 0
        ),
        "false_rejects_at_most_10": len(false_rejects) <= 10,
        "retained_value_recall_at_least_0_95": (
            retained_value_recall >= 0.95
        ),
    }
    return {
        "candidate_count": len(predictions),
        "strict_candidate_count": len(strict),
        "consensus_candidate_state_macro_f1": macro_f1,
        "retained_value_recall": retained_value_recall,
        "consensus_junk_escape_rate": junk_escape_rate,
        "false_reject_count": len(false_rejects),
        "junk_escape_count": len(intrinsic_junk_escapes),
        "intrinsic_junk_escape_count": len(
            intrinsic_junk_escapes
        ),
        "relational_junk_escape_count": len(
            relational_junk_escapes
        ),
        "all_junk_escape_count": len(all_junk_escapes),
        "false_reject_candidate_ids": false_rejects,
        "junk_escape_candidate_ids": intrinsic_junk_escapes,
        "intrinsic_junk_escape_candidate_ids": (
            intrinsic_junk_escapes
        ),
        "relational_junk_escape_candidate_ids": (
            relational_junk_escapes
        ),
        "all_junk_escape_candidate_ids": all_junk_escapes,
        "measurement_contract": {
            "version": "pif_true_north_phase_c_option2_v1",
            "old_gate": {
                "scope": "all_gold_junk",
                "acceptance": old_acceptance,
                "passed": all(old_acceptance.values()),
            },
            "new_gate": {
                "scope": "intrinsic_junk_only",
                "relational_reason_families": [
                    "non_useful_repetition*",
                    "nonasserted_question_frame",
                ],
                "relational_acceptance_is_conditional_on": (
                    "certification.relational_merge_contamination_count == 0"
                ),
            },
        },
        "classification_detail": detail,
        "acceptance": acceptance,
        "passed": all(acceptance.values()),
    }


def run_phase_c_disposition(
    *,
    output_root: str | Path | None = None,
    suite: str = SUITE_ID,
    workers: int = 4,
    timeout_seconds: int = 900,
    opencode_binary: str = "/opt/homebrew/bin/opencode",
    run_id: str | None = None,
    runner: Any | None = None,
) -> dict[str, Any]:
    """Run only the approved Phase-C disposition stage on the Search fold."""

    if workers < 1 or workers > 4:
        raise TrueNorthError("phase-C workers must be between 1 and 4")
    suite_root = _suite_root(output_root, suite)
    verification = verify_suite(output_root=output_root, suite=suite)
    if not verification["ok"]:
        raise TrueNorthError("suite verification failed before Phase C")
    manifest = _read_json(suite_root / "manifest.json")
    episode_ids = (
        "ep_90c3b5c995bce501c9aef55c",
        "ep_7ec9f808a3955c720aeb94ff",
    )
    development = {
        str(row["episode_id"]): row
        for row in manifest["bundles"]
        if row["partition"] == "development"
    }
    consensus = _read_json(
        suite_root
        / "gold"
        / "development"
        / "final"
        / "consensus.private.json"
    )
    budget = {
        "max_calls": 25,
        "max_tokens": 800_000,
        "max_wall_seconds": 3600,
    }
    configuration = {
        "schema_version": MULTIPASS_SCHEMA_VERSION,
        "phase": "phase_c_disposition",
        "suite_id": suite,
        "suite_manifest_sha256": manifest["manifest_sha256"],
        "consensus_sha256": consensus["consensus_sha256"],
        "episode_ids": list(episode_ids),
        "model": MULTIPASS_MODEL,
        "system_prompt_sha256": sha256_text(
            MULTIPASS_SYSTEM_PROMPTS["disposition"]
        ),
        "gate_policy_version": APPROVED_GATE_POLICY_VERSION,
        "budget": budget,
        "holdout_access_allowed": False,
        "production_database_open_allowed": False,
    }
    configuration["configuration_sha256"] = sha256_text(
        dumps_json(configuration)
    )
    resolved_run_id = run_id or (
        "phase-c-disposition-"
        + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        + "-"
        + configuration["configuration_sha256"][:8]
    )
    run_root = suite_root / "phase-c" / "runs" / resolved_run_id
    config_path = run_root / "configuration.json"
    if config_path.is_file():
        if _read_json(config_path) != configuration:
            raise TrueNorthError(
                "phase-C disposition resume configuration differs"
            )
    else:
        _write_json(config_path, configuration, immutable=True)
    state_path = run_root / "state.json"
    if state_path.is_file():
        state = _read_json(state_path)
    else:
        state = {
            "schema_version": MULTIPASS_SCHEMA_VERSION,
            "run_id": resolved_run_id,
            "configuration_sha256": configuration[
                "configuration_sha256"
            ],
            "budget": budget,
            "completed": [],
            "usage": {"calls": 0, "tokens": 0, "wall_seconds": 0.0},
        }
        _multipass_state_write(state_path, state)
    jobs: list[tuple[str, str, dict[str, Any]]] = []
    for episode_id in episode_ids:
        bundle = _read_json(Path(development[episode_id]["bundle_path"]))
        for base_job in _segment_jobs(bundle, gold=False):
            segment_id = str(
                base_job["input"]["segment"]["segment_id"]
            )
            jobs.append(
                (
                    episode_id,
                    segment_id,
                    build_multipass_disposition_packet(base_job),
                )
            )
    if len(jobs) > budget["max_calls"]:
        raise TrueNorthError(
            "phase-C disposition packet count exceeds declared budget"
        )
    outputs = _multipass_execute_stage(
        run_root=run_root,
        stage="disposition",
        jobs=jobs,
        state=state,
        workers=workers,
        timeout_seconds=timeout_seconds,
        opencode_binary=opencode_binary,
        runner=runner,
    )
    predictions: dict[str, dict[str, Any]] = {}
    for output in outputs.values():
        for item in output["items"]:
            candidate_id = str(item["candidate_id"])
            if candidate_id in predictions:
                raise TrueNorthError(
                    f"duplicate Phase-C disposition: {candidate_id}"
                )
            predictions[candidate_id] = dict(item)
    score = _score_phase_c_dispositions(
        consensus,
        predictions,
        _phase_c_preferred_document(suite_root),
    )
    state = _read_json(state_path)
    state["complete"] = True
    state["candidate_count"] = len(predictions)
    state["packet_count"] = len(jobs)
    _multipass_state_write(state_path, state)
    document = {
        "schema_version": MULTIPASS_SCHEMA_VERSION,
        "phase": "phase_c_disposition",
        "run_id": resolved_run_id,
        "configuration_sha256": configuration["configuration_sha256"],
        "score": score,
        "usage": state["usage"],
        "packet_count": len(jobs),
        "production_mutation": False,
        "holdout_opened": False,
    }
    document["score_sha256"] = sha256_text(dumps_json(document))
    score_path = run_root / "score.private.json"
    _write_json(score_path, document, immutable=False)
    return {
        **document,
        "score_path": str(score_path),
    }


def _phase_c_disposition_outputs(run_root: Path) -> dict[str, dict[str, Any]]:
    predictions: dict[str, dict[str, Any]] = {}
    for path in sorted(
        (run_root / "outputs" / "disposition").glob(
            "*/*/validated.private.json"
        )
    ):
        for item in _read_json(path)["items"]:
            candidate_id = str(item["candidate_id"])
            if candidate_id in predictions:
                raise TrueNorthError(
                    f"duplicate disposition output: {candidate_id}"
                )
            predictions[candidate_id] = dict(item)
    return predictions


def select_phase_c_disposition_conflicts(
    first: Mapping[str, Mapping[str, Any]],
    second: Mapping[str, Mapping[str, Any]],
    candidates: Mapping[str, Mapping[str, Any]],
) -> list[str]:
    """Select a bounded, gold-blind Spark conflict set."""

    if set(first) != set(second) or set(second) != set(candidates):
        raise TrueNorthError(
            "disposition escalation inputs have different candidate scopes"
        )
    selected = {
        candidate_id
        for candidate_id, decision in second.items()
        if str(decision["disposition"]) in {"reject", "hold"}
    }
    risk_flags = {
        "question_frame_not_asserted_claim",
        "intro_framing",
    }
    for candidate_id in sorted(second):
        earlier = first[candidate_id]
        current = second[candidate_id]
        if not (
            str(earlier["disposition"]) == "reject"
            and str(current["disposition"]) in {"retain", "revise"}
        ):
            continue
        candidate_flags = set(
            candidates[candidate_id].get("flags", {}).get("quality", [])
        )
        if (
            candidate_flags & risk_flags
            or str(earlier.get("junk_reason")) == "banter"
        ):
            selected.add(candidate_id)
    ordered = sorted(selected)
    if len(ordered) > 25:
        raise TrueNorthError(
            "gold-blind disposition conflict set exceeds Spark budget"
        )
    return ordered


def combine_phase_c_disposition_votes(
    first: Mapping[str, Mapping[str, Any]],
    second: Mapping[str, Mapping[str, Any]],
    escalated: Mapping[str, Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Combine two GLM passes with Spark as a true disagreement tiebreaker."""

    if set(first) != set(second):
        raise TrueNorthError(
            "disposition vote inputs have different candidate scopes"
        )
    if not set(escalated).issubset(second):
        raise TrueNorthError(
            "escalated dispositions contain an out-of-scope candidate"
        )
    combined: dict[str, dict[str, Any]] = {}
    for candidate_id in sorted(second):
        earlier = first[candidate_id]
        current = second[candidate_id]
        earlier_state = _gold_value_state(
            str(earlier["disposition"])
        )
        current_state = _gold_value_state(
            str(current["disposition"])
        )
        if earlier_state == current_state:
            # Two independent passes already form a majority. Preserve the
            # newer pass so its schema-valid reason text remains intact.
            combined[candidate_id] = dict(current)
            continue
        # Only frozen-risk disagreements enter the bounded escalation set.
        # For other disagreements, the approved second GLM pass remains the
        # decision instead of silently broadening Spark's scope.
        combined[candidate_id] = dict(
            escalated.get(candidate_id, current)
        )
    return combined


def _phase_c_junk_tokens(text: str) -> set[str]:
    return set(re.findall(r"[a-z0-9]+", text.casefold()))


def _phase_c_token_jaccard(left: str, right: str) -> float:
    left_tokens = _phase_c_junk_tokens(left)
    right_tokens = _phase_c_junk_tokens(right)
    union = left_tokens | right_tokens
    return len(left_tokens & right_tokens) / len(union) if union else 0.0


PHASE_C_MARGINAL_STEM_STOPWORDS = frozenset(
    {
        "about",
        "after",
        "again",
        "also",
        "although",
        "another",
        "anything",
        "around",
        "because",
        "before",
        "being",
        "between",
        "both",
        "could",
        "different",
        "does",
        "doing",
        "down",
        "during",
        "each",
        "even",
        "everything",
        "from",
        "further",
        "going",
        "have",
        "having",
        "here",
        "hers",
        "herself",
        "himself",
        "into",
        "itself",
        "just",
        "know",
        "like",
        "look",
        "made",
        "make",
        "many",
        "more",
        "most",
        "much",
        "myself",
        "other",
        "ours",
        "ourselves",
        "over",
        "people",
        "person",
        "really",
        "right",
        "said",
        "same",
        "says",
        "should",
        "some",
        "something",
        "source",
        "sources",
        "speaker",
        "such",
        "than",
        "that",
        "their",
        "theirs",
        "them",
        "themselves",
        "then",
        "there",
        "these",
        "they",
        "this",
        "thing",
        "things",
        "think",
        "those",
        "thought",
        "through",
        "told",
        "under",
        "until",
        "very",
        "want",
        "well",
        "were",
        "what",
        "when",
        "where",
        "which",
        "while",
        "with",
        "would",
        "yeah",
        "your",
        "yours",
        "yourself",
        "yourselves",
    }
)


def _phase_c_simple_stem(token: str) -> str:
    for suffix, replacement in (
        ("ations", ""),
        ("ation", ""),
        ("ingly", ""),
        ("edly", ""),
        ("ies", "y"),
        ("ing", ""),
        ("ed", ""),
        ("es", ""),
        ("s", ""),
    ):
        if token.endswith(suffix):
            stem = token[: -len(suffix)] + replacement
            if len(stem) >= 4:
                return stem
    return token


def _phase_c_informative_stems(text: str) -> set[str]:
    stems: set[str] = set()
    for token in re.findall(r"[a-z0-9]+", text.casefold()):
        if (
            len(token) < 4
            or token in PHASE_C_MARGINAL_STEM_STOPWORDS
        ):
            continue
        stems.add(_phase_c_simple_stem(token))
    return stems


def _phase_c_has_finite_verb(text: str) -> bool:
    tokens = re.findall(r"[A-Za-z]+(?:'[A-Za-z]+)?", text.casefold())
    finite_words = {
        "am",
        "is",
        "are",
        "was",
        "were",
        "be",
        "been",
        "have",
        "has",
        "had",
        "do",
        "does",
        "did",
        "can",
        "could",
        "will",
        "would",
        "shall",
        "should",
        "may",
        "might",
        "must",
        "say",
        "says",
        "said",
        "make",
        "makes",
        "made",
        "go",
        "goes",
        "went",
        "think",
        "thinks",
        "thought",
        "believe",
        "believes",
        "argue",
        "argues",
        "predict",
        "predicts",
        "need",
        "needs",
        "want",
        "wants",
    }
    return any(
        token in finite_words
        or (
            len(token) > 4
            and (token.endswith("ed") or token.endswith("ing"))
        )
        for token in tokens
    )


def _phase_c_bare_mention_or_question(
    claim_text: str, evidence_text: str
) -> bool:
    claim_has_verb = _phase_c_has_finite_verb(claim_text)
    if not claim_has_verb:
        return True
    if "?" not in evidence_text:
        return False
    continuation = evidence_text.rsplit("?", 1)[-1].strip()
    return not continuation or not _phase_c_has_finite_verb(continuation)


def screen_phase_c_junk_candidates(
    episode_candidates: Mapping[str, Sequence[Mapping[str, Any]]],
    composed: Mapping[str, Mapping[str, Any]],
    ensemble_members: Sequence[Mapping[str, Mapping[str, Any]]],
) -> dict[str, dict[str, Any]]:
    """Apply the frozen, gold-blind Task-4b screen to retained candidates."""

    screened: dict[str, dict[str, Any]] = {}
    observed: set[str] = set()
    for episode_id, candidates in episode_candidates.items():
        earlier_texts: list[str] = []
        for candidate in candidates:
            candidate_id = str(candidate["candidate_id"])
            if candidate_id in observed:
                raise TrueNorthError(
                    f"duplicate junk-screen candidate: {candidate_id}"
                )
            observed.add(candidate_id)
            if candidate_id not in composed:
                raise TrueNorthError(
                    f"junk-screen candidate lacks disposition: {candidate_id}"
                )
            claim_text = str(candidate.get("claim_text") or "")
            evidence_text = str(candidate.get("evidence_text") or "")
            comparison_text = f"{claim_text} {evidence_text}".strip()
            maximum_jaccard = max(
                (
                    _phase_c_token_jaccard(comparison_text, earlier)
                    for earlier in earlier_texts
                ),
                default=0.0,
            )
            earlier_texts.append(comparison_text)
            if _gold_value_state(
                str(composed[candidate_id]["disposition"])
            ) != "value":
                continue
            classes: list[str] = []
            rejectors = [
                index
                for index, member in enumerate(ensemble_members)
                if candidate_id in member
                and _gold_value_state(
                    str(member[candidate_id]["disposition"])
                )
                != "value"
            ]
            if rejectors:
                classes.append("ensemble_disagreement")
            if maximum_jaccard >= PHASE_C_JUNK_VERIFY_JACCARD:
                classes.append("repetition")
            terminal_text = evidence_text.rstrip().rstrip(
                "\"'”’)]}"
            )
            if (
                len(evidence_text.strip())
                < PHASE_C_JUNK_VERIFY_FRAGMENT_MAX_CHARS
                or not terminal_text
                or terminal_text[-1] not in ".!?"
            ):
                classes.append("fragment")
            if _phase_c_bare_mention_or_question(
                claim_text, evidence_text
            ):
                classes.append("bare_mention_question")
            if classes:
                screened[candidate_id] = {
                    "candidate_id": candidate_id,
                    "episode_id": str(episode_id),
                    "screen_classes": classes,
                    "ensemble_rejectors": rejectors,
                    "maximum_prior_token_jaccard": round(
                        maximum_jaccard, 6
                    ),
                }
    if observed != set(composed):
        raise TrueNorthError(
            "junk screen and disposition scopes differ"
        )
    return screened


def _phase_c_screen_reason_matches(
    screen_classes: Sequence[str], junk_reason: str
) -> bool:
    if junk_reason == "repetition":
        return "repetition" in screen_classes
    if junk_reason == "fragment":
        return "fragment" in screen_classes
    if junk_reason in {"bare_mention", "question_or_setup"}:
        return "bare_mention_question" in screen_classes
    return False


def compose_phase_c_junk_verification(
    composed: Mapping[str, Mapping[str, Any]],
    screened: Mapping[str, Mapping[str, Any]],
    verifier: Mapping[str, Mapping[str, Any]],
    spark: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    """Apply Task-4b's asymmetric flip rule without consulting gold."""

    if not set(verifier).issubset(screened):
        raise TrueNorthError(
            "junk verifier attempted to decide an unscreened candidate"
        )
    if set(verifier) != set(screened):
        raise TrueNorthError(
            "junk verifier did not account for every screened candidate"
        )
    predictions = {
        candidate_id: dict(decision)
        for candidate_id, decision in composed.items()
    }
    escalation_ids: list[str] = []
    flipped: list[str] = []
    for candidate_id in sorted(screened):
        decision = verifier[candidate_id]
        if decision["verdict"] == "confirm_retain":
            continue
        reason = str(decision["junk_reason"])
        screen = screened[candidate_id]
        corroborated = bool(screen["ensemble_rejectors"]) or (
            _phase_c_screen_reason_matches(
                list(screen["screen_classes"]), reason
            )
        )
        final_reject = corroborated
        if not corroborated:
            escalation_ids.append(candidate_id)
            if candidate_id in spark:
                final_reject = (
                    spark[candidate_id]["verdict"] == "reject"
                )
        if final_reject:
            predictions[candidate_id] = {
                "candidate_id": candidate_id,
                "disposition": "reject",
                "junk_reason": (
                    str(spark[candidate_id]["junk_reason"])
                    if not corroborated
                    and candidate_id in spark
                    and spark[candidate_id]["verdict"] == "reject"
                    else reason
                ),
            }
            flipped.append(candidate_id)
    if not set(spark).issubset(escalation_ids):
        raise TrueNorthError(
            "Spark junk verdict exists without an uncorroborated reject"
        )
    return {
        "predictions": predictions,
        "spark_escalation_candidate_ids": escalation_ids,
        "flipped_candidate_ids": flipped,
    }


def _phase_c_is_pure_interrogative(text: str) -> bool:
    stripped = text.strip()
    if not stripped:
        return False
    sentences = [
        sentence.strip()
        for sentence in re.split(r"(?<=[.!?])\s+", stripped)
        if sentence.strip()
    ]
    return bool(sentences) and all(
        sentence.rstrip("\"'”’)]}").endswith("?")
        for sentence in sentences
    )


def _phase_c_is_reference_only_claim(text: str) -> bool:
    match = re.search(
        r"\b(references?|mentions?|cites?|names?|points?\s+to)\b",
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        return False
    remainder = text[match.end() :].strip(" .,:;!?")
    if not remainder:
        return True
    return not bool(
        re.search(
            r"[;:]|\b(that|because|while|although|but|which)\b|"
            r"\b(and|or)\s+\w+(?:ed|ing|s)\b",
            remainder,
            flags=re.IGNORECASE,
        )
    )


def screen_phase_c_marginal_candidates(
    episode_candidates: Mapping[str, Sequence[Mapping[str, Any]]],
    composed: Mapping[str, Mapping[str, Any]],
    ensemble_members: Sequence[Mapping[str, Mapping[str, Any]]],
) -> dict[str, dict[str, Any]]:
    """Screen retained candidates using only frozen relational features."""

    screened: dict[str, dict[str, Any]] = {}
    observed: set[str] = set()
    for episode_id, candidates in episode_candidates.items():
        retained = [
            candidate
            for candidate in candidates
            if _gold_value_state(
                str(
                    composed[str(candidate["candidate_id"])][
                        "disposition"
                    ]
                )
            )
            == "value"
        ]
        comparison_text = {
            str(candidate["candidate_id"]): (
                f"{candidate.get('claim_text') or ''} "
                f"{candidate.get('evidence_text') or ''}"
            ).strip()
            for candidate in retained
        }
        informative_stems = {
            candidate_id: _phase_c_informative_stems(text)
            for candidate_id, text in comparison_text.items()
        }
        for candidate in candidates:
            candidate_id = str(candidate["candidate_id"])
            if candidate_id in observed:
                raise TrueNorthError(
                    f"duplicate marginal-screen candidate: {candidate_id}"
                )
            observed.add(candidate_id)
            if candidate_id not in composed:
                raise TrueNorthError(
                    "marginal-screen candidate lacks disposition: "
                    f"{candidate_id}"
                )
            if _gold_value_state(
                str(composed[candidate_id]["disposition"])
            ) != "value":
                continue
            neighbor_scores = sorted(
                (
                    {
                        "candidate_id": neighbor_id,
                        "similarity": round(
                            _phase_c_token_jaccard(
                                comparison_text[candidate_id],
                                neighbor_text,
                            ),
                            6,
                        ),
                    }
                    for neighbor_id, neighbor_text in comparison_text.items()
                    if neighbor_id != candidate_id
                ),
                key=lambda row: (
                    -float(row["similarity"]),
                    str(row["candidate_id"]),
                ),
            )
            top_neighbors = neighbor_scores[:3]
            score_by_neighbor = {
                str(row["candidate_id"]): float(row["similarity"])
                for row in neighbor_scores
            }
            local_proximity_neighbors: list[dict[str, Any]] = []
            segment_id = candidate.get("segment_id")
            evidence_start = candidate.get("evidence_start")
            if (
                segment_id is not None
                and isinstance(evidence_start, (int, float))
            ):
                for neighbor in retained:
                    neighbor_id = str(neighbor["candidate_id"])
                    neighbor_start = neighbor.get("evidence_start")
                    if (
                        neighbor_id == candidate_id
                        or neighbor.get("segment_id") != segment_id
                        or not isinstance(
                            neighbor_start, (int, float)
                        )
                    ):
                        continue
                    distance = abs(
                        int(evidence_start) - int(neighbor_start)
                    )
                    if distance > PHASE_C_MARGINAL_LOCAL_DISTANCE:
                        continue
                    shared_stems = sorted(
                        informative_stems[candidate_id]
                        & informative_stems[neighbor_id]
                    )
                    if (
                        len(shared_stems)
                        < PHASE_C_MARGINAL_LOCAL_MIN_STEMS
                    ):
                        continue
                    local_proximity_neighbors.append(
                        {
                            "candidate_id": neighbor_id,
                            "similarity": score_by_neighbor.get(
                                neighbor_id, 0.0
                            ),
                            "evidence_distance": distance,
                            "shared_informative_stems": shared_stems,
                        }
                    )
            local_proximity_neighbors.sort(
                key=lambda row: (
                    -len(row["shared_informative_stems"]),
                    int(row["evidence_distance"]),
                    -float(row["similarity"]),
                    str(row["candidate_id"]),
                )
            )
            maximum_similarity = (
                float(top_neighbors[0]["similarity"])
                if top_neighbors
                else 0.0
            )
            evidence_text = str(candidate.get("evidence_text") or "")
            claim_text = str(candidate.get("claim_text") or "")
            classes: list[str] = []
            rejectors = [
                index
                for index, member in enumerate(ensemble_members)
                if candidate_id in member
                and _gold_value_state(
                    str(member[candidate_id]["disposition"])
                )
                != "value"
            ]
            if maximum_similarity >= PHASE_C_MARGINAL_JACCARD:
                classes.append("repetition")
            if local_proximity_neighbors:
                classes.append("local_paraphrase_repetition")
            if re.search(r"\[[^\]\r\n]+\]", evidence_text) or (
                _phase_c_is_reference_only_claim(claim_text)
            ):
                classes.append("chrome_bare_mention")
            if _phase_c_is_pure_interrogative(evidence_text):
                classes.append("pure_interrogative")
            stripped_evidence = evidence_text.strip()
            terminal = stripped_evidence.rstrip("\"'”’)]}")
            if (
                bool(stripped_evidence)
                and len(stripped_evidence) < 160
                and stripped_evidence[0].islower()
                and (
                    not terminal
                    or terminal[-1] not in ".!?"
                )
            ):
                classes.append("dangling_fragment")
            if rejectors:
                classes.append("ensemble_disagreement")
            if classes:
                screened[candidate_id] = {
                    "candidate_id": candidate_id,
                    "episode_id": str(episode_id),
                    "screen_classes": classes,
                    "ensemble_rejectors": rejectors,
                    "maximum_neighbor_similarity": maximum_similarity,
                    "top_neighbors": top_neighbors,
                    "local_proximity_neighbors": (
                        local_proximity_neighbors
                    ),
                }
    if observed != set(composed):
        raise TrueNorthError(
            "marginal screen and disposition scopes differ"
        )
    return screened


def build_phase_c_marginal_packet(
    *,
    suite: str,
    candidate_ids: Sequence[str],
    candidate_by_id: Mapping[str, Mapping[str, Any]],
    segment_by_id: Mapping[str, Mapping[str, Any]],
    screened: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    prepared: dict[str, dict[str, Any]] = {}
    intervals_by_segment: dict[
        str, list[tuple[int, int, str]]
    ] = defaultdict(list)
    neighbor_catalog: dict[str, dict[str, Any]] = {}
    neighbors_by_candidate: dict[str, list[str]] = {}
    for candidate_id in candidate_ids:
        candidate = candidate_by_id[candidate_id]
        segment_id = str(candidate["segment_id"])
        segment_text = str(segment_by_id[segment_id]["text"])
        evidence_start = int(candidate["evidence_start"])
        evidence_end = int(candidate["evidence_end"])
        evidence_text = str(candidate["evidence_text"])
        if (
            evidence_start < 0
            or evidence_end > len(segment_text)
            or evidence_start >= evidence_end
            or segment_text[evidence_start:evidence_end]
            != evidence_text
        ):
            raise TrueNorthError(
                "marginal packet evidence offsets do not bind exactly: "
                f"{candidate_id}"
            )
        crop_start = max(
            0, evidence_start - PHASE_C_MARGINAL_SEGMENT_RADIUS
        )
        crop_end = min(
            len(segment_text),
            evidence_end + PHASE_C_MARGINAL_SEGMENT_RADIUS,
        )
        intervals_by_segment[segment_id].append(
            (crop_start, crop_end, candidate_id)
        )
        neighbor_rows: list[Mapping[str, Any]] = list(
            screened[candidate_id]["top_neighbors"]
        )
        included_neighbor_ids = {
            str(row["candidate_id"]) for row in neighbor_rows
        }
        for local_row in screened[candidate_id].get(
            "local_proximity_neighbors", []
        )[:1]:
            if str(local_row["candidate_id"]) not in included_neighbor_ids:
                neighbor_rows.append(local_row)
                included_neighbor_ids.add(
                    str(local_row["candidate_id"])
                )
        neighbors = []
        local_ids = {
            str(row["candidate_id"])
            for row in screened[candidate_id].get(
                "local_proximity_neighbors", []
            )[:1]
        }
        for neighbor_row in neighbor_rows:
            neighbor_id = str(neighbor_row["candidate_id"])
            neighbor = candidate_by_id[neighbor_id]
            neighbor_catalog[neighbor_id] = {
                "candidate_id": neighbor_id,
                "claim_text": str(neighbor["claim_text"]),
                "evidence_text": str(neighbor["evidence_text"]),
            }
            neighbors.append(
                {
                    "candidate_id": neighbor_id,
                    "similarity": float(neighbor_row["similarity"]),
                    "local_proximity_match": neighbor_id in local_ids,
                }
            )
        neighbors_by_candidate[candidate_id] = [
            str(row["candidate_id"]) for row in neighbors
        ]
        prepared[candidate_id] = {
            "candidate_id": candidate_id,
            "claim_text": str(candidate["claim_text"]),
            "evidence_text": evidence_text,
            "evidence_start": evidence_start,
            "evidence_end": evidence_end,
            "screen_classes": list(
                screened[candidate_id]["screen_classes"]
            ),
            "segment_id": segment_id,
            "crop_start": crop_start,
            "crop_end": crop_end,
            "neighbors": neighbors,
        }
    context_catalog: list[dict[str, Any]] = []
    context_by_candidate: dict[str, dict[str, Any]] = {}
    for segment_id in sorted(intervals_by_segment):
        segment_text = str(segment_by_id[segment_id]["text"])
        intervals = sorted(intervals_by_segment[segment_id])
        groups: list[dict[str, Any]] = []
        for start, end, candidate_id in intervals:
            if not groups or start > int(groups[-1]["end"]):
                groups.append(
                    {
                        "start": start,
                        "end": end,
                        "candidate_ids": [candidate_id],
                    }
                )
            else:
                groups[-1]["end"] = max(
                    int(groups[-1]["end"]), end
                )
                groups[-1]["candidate_ids"].append(candidate_id)
        for index, group in enumerate(groups, start=1):
            start = int(group["start"])
            end = int(group["end"])
            context_id = f"{segment_id}:crop:{index}"
            context_catalog.append(
                {
                    "context_id": context_id,
                    "segment_id": segment_id,
                    "crop_start": start,
                    "crop_end": end,
                    "crop_text": segment_text[start:end],
                }
            )
            for candidate_id in group["candidate_ids"]:
                row = prepared[candidate_id]
                context_by_candidate[candidate_id] = {
                    "context_id": context_id,
                    "segment_id": segment_id,
                    "crop_start": row["crop_start"],
                    "crop_end": row["crop_end"],
                    "catalog_start": start,
                    "catalog_end": end,
                    "evidence_start": row["evidence_start"],
                    "evidence_end": row["evidence_end"],
                    "evidence_start_in_catalog": (
                        row["evidence_start"] - start
                    ),
                    "evidence_end_in_catalog": (
                        row["evidence_end"] - start
                    ),
                }
    packet_candidates = [
        {
            "candidate_id": candidate_id,
            "claim_text": prepared[candidate_id]["claim_text"],
            "evidence_text": prepared[candidate_id][
                "evidence_text"
            ],
            "evidence_start": prepared[candidate_id][
                "evidence_start"
            ],
            "evidence_end": prepared[candidate_id]["evidence_end"],
            "screen_classes": prepared[candidate_id][
                "screen_classes"
            ],
            "segment_context": context_by_candidate[candidate_id],
            "neighbors": prepared[candidate_id]["neighbors"],
        }
        for candidate_id in candidate_ids
    ]
    return {
        "schema_version": MULTIPASS_SCHEMA_VERSION,
        "suite_id": suite,
        "multipass_stage": "marginal_junk_verify",
        "task": (
            "Decide whether each provisionally retained candidate adds a "
            "proposition not carried by its shown retained neighbors."
        ),
        "instructions": [
            "Return one verdict for every candidate and no others.",
            "Resolve segment_context.context_id in segment_context_catalog and each neighbor candidate_id in neighbor_catalog.",
            "A repetition reject must name duplicate_of from the shown neighbors.",
            "Every non-repetition reject requires a verbatim deficiency_quote from evidence_text.",
            "A confirmed retention requires null junk_reason, duplicate_of, and deficiency_quote.",
        ],
        "output_schema": phase_c_marginal_schema(
            candidate_ids, neighbors_by_candidate
        ),
        "input": {
            "candidates": packet_candidates,
            "segment_context_catalog": context_catalog,
            "neighbor_catalog": [
                neighbor_catalog[neighbor_id]
                for neighbor_id in sorted(neighbor_catalog)
            ],
        },
    }


def _phase_c_marginal_reason_matches(
    screen_classes: Sequence[str], junk_reason: str
) -> bool:
    if junk_reason == "repetition":
        return bool(
            {"repetition", "local_paraphrase_repetition"}
            & set(screen_classes)
        )
    if junk_reason in {"bare_mention", "metadata"}:
        return "chrome_bare_mention" in screen_classes
    if junk_reason == "question_or_setup":
        return "pure_interrogative" in screen_classes
    if junk_reason == "fragment":
        return "dangling_fragment" in screen_classes
    return False


def compose_phase_c_marginal_verification(
    composed: Mapping[str, Mapping[str, Any]],
    screened: Mapping[str, Mapping[str, Any]],
    verifier: Mapping[str, Mapping[str, Any]],
    spark: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    """Apply Task-4c's asymmetric relational-junk composition."""

    if set(verifier) != set(screened):
        if not set(verifier).issubset(screened):
            raise TrueNorthError(
                "marginal verifier attempted an unscreened candidate"
            )
        raise TrueNorthError(
            "marginal verifier did not cover every screened candidate"
        )
    predictions = {
        candidate_id: dict(decision)
        for candidate_id, decision in composed.items()
    }
    escalation_reasons: dict[str, str] = {}
    flipped: list[str] = []
    for candidate_id in sorted(screened):
        decision = verifier[candidate_id]
        screen = screened[candidate_id]
        maximum_similarity = float(
            screen["maximum_neighbor_similarity"]
            if "maximum_neighbor_similarity" in screen
            else max(
                (
                    float(row["similarity"])
                    for row in screen["top_neighbors"]
                ),
                default=0.0,
            )
        )
        if decision["verdict"] == "confirm_retain":
            if (
                "repetition" in screen["screen_classes"]
                and maximum_similarity
                >= PHASE_C_MARGINAL_SPARK_CONFIRM_JACCARD
            ):
                escalation_reasons[candidate_id] = (
                    "high_similarity_confirm"
                )
            continue
        reason = str(decision["junk_reason"])
        duplicate_of = decision.get("duplicate_of")
        duplicate_similarity = next(
            (
                float(row["similarity"])
                for row in screen["top_neighbors"]
                if str(row["candidate_id"]) == str(duplicate_of)
            ),
            0.0,
        )
        corroborated = (
            bool(screen["ensemble_rejectors"])
            or _phase_c_marginal_reason_matches(
                list(screen["screen_classes"]), reason
            )
            or (
                reason == "repetition"
                and duplicate_of is not None
                and duplicate_similarity
                >= PHASE_C_MARGINAL_JACCARD
            )
        )
        final_reject = corroborated
        if not corroborated:
            escalation_reasons[candidate_id] = (
                "uncorroborated_reject"
            )
            if candidate_id in spark:
                final_reject = (
                    spark[candidate_id]["verdict"] == "reject"
                )
        if final_reject:
            predictions[candidate_id] = {
                "candidate_id": candidate_id,
                "disposition": "reject",
                "junk_reason": reason,
            }
            flipped.append(candidate_id)
    if not set(spark).issubset(escalation_reasons):
        raise TrueNorthError(
            "Spark marginal verdict exists outside escalation scope"
        )
    return {
        "predictions": predictions,
        "spark_escalation_candidate_ids": sorted(escalation_reasons),
        "spark_escalation_reasons": escalation_reasons,
        "flipped_candidate_ids": flipped,
    }


def _load_phase_c_task4b_inputs(
    suite_root: Path,
) -> tuple[
    dict[str, list[dict[str, Any]]],
    dict[str, dict[str, Any]],
    tuple[dict[str, dict[str, Any]], ...],
]:
    phase_root = suite_root / "phase-c" / "runs"
    first = _phase_c_disposition_outputs(
        phase_root / "phase-c-disposition-20260728-v1"
    )
    second = _phase_c_disposition_outputs(
        phase_root / "phase-c-disposition-20260728-v2"
    )
    spark_document = _read_json(
        phase_root
        / "phase-c-disposition-20260728-v2-spark-conflicts-v1"
        / "outputs"
        / "disposition"
        / "validated.private.json"
    )
    spark = {
        str(item["candidate_id"]): dict(item)
        for item in spark_document["items"]
    }
    composed = combine_phase_c_disposition_votes(
        first, second, spark
    )
    search_episode_ids = {
        "ep_90c3b5c995bce501c9aef55c",
        "ep_7ec9f808a3955c720aeb94ff",
    }
    manifest = _read_json(suite_root / "manifest.json")
    episodes: dict[str, list[dict[str, Any]]] = {}
    for row in manifest["bundles"]:
        episode_id = str(row["episode_id"])
        if episode_id not in search_episode_ids:
            continue
        bundle = _read_json(Path(row["bundle_path"]))
        episodes[episode_id] = [
            dict(candidate) for candidate in bundle["candidates"]
        ]
    if set(episodes) != search_episode_ids:
        raise TrueNorthError(
            "Task-4b Search-fold episode scope is incomplete"
        )
    return episodes, composed, (first, second, spark)


def dry_run_phase_c_junk_screen(
    *,
    output_root: str | Path | None = None,
    suite: str = SUITE_ID,
) -> dict[str, Any]:
    suite_root = _suite_root(output_root, suite)
    if not verify_suite(output_root=output_root, suite=suite)["ok"]:
        raise TrueNorthError(
            "suite verification failed before Task-4b screen"
        )
    episodes, composed, ensemble = _load_phase_c_task4b_inputs(
        suite_root
    )
    screened = screen_phase_c_junk_candidates(
        episodes, composed, ensemble
    )
    required_known_escapes = {
        "dev_c094b91406c9222943a29eba",
        "dev_d7f6bd87ab720be875111f97",
    }
    missing = sorted(required_known_escapes - set(screened))
    if missing:
        raise TrueNorthError(
            "Task-4b screen missed a required known escape: "
            + ", ".join(missing)
        )
    class_counts = Counter(
        screen_class
        for row in screened.values()
        for screen_class in row["screen_classes"]
    )
    retained_count = sum(
        _gold_value_state(str(row["disposition"])) == "value"
        for row in composed.values()
    )
    packet_count = (
        len(screened) + PHASE_C_JUNK_VERIFY_BATCH_SIZE - 1
    ) // PHASE_C_JUNK_VERIFY_BATCH_SIZE
    if packet_count > PHASE_C_JUNK_VERIFY_MAX_GLM_CALLS:
        raise TrueNorthError(
            "Task-4b deterministic screen exceeds GLM call budget"
        )
    report = {
        "schema_version": MULTIPASS_SCHEMA_VERSION,
        "phase": "phase_c_junk_verify_screen",
        "suite_id": suite,
        "source_disposition_result_sha256": (
            "f1f5ebda6e8c88ddc845767bdeab7f6fdf45a2e9c7aabd434a0a9e4c48a06fcd"
        ),
        "thresholds": {
            "repetition_token_jaccard": PHASE_C_JUNK_VERIFY_JACCARD,
            "fragment_max_characters": (
                PHASE_C_JUNK_VERIFY_FRAGMENT_MAX_CHARS
            ),
        },
        "retained_count": retained_count,
        "screened_count": len(screened),
        "screen_class_counts": dict(sorted(class_counts.items())),
        "packet_count": packet_count,
        "known_escapes_screened": sorted(required_known_escapes),
        "screened": [
            screened[candidate_id]
            for candidate_id in sorted(screened)
        ],
        "gold_accessed": False,
        "model_calls_made": 0,
        "holdout_opened": False,
        "production_mutation": False,
    }
    report["screen_sha256"] = sha256_text(dumps_json(report))
    return report


def _phase_c_junk_verify_packet(
    *,
    suite: str,
    candidates: Sequence[Mapping[str, Any]],
    screened: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    compact_candidates = [
        {
            "candidate_id": str(candidate["candidate_id"]),
            "claim_text": str(candidate["claim_text"]),
            "evidence_text": str(candidate["evidence_text"]),
            "screen_classes": list(
                screened[str(candidate["candidate_id"])][
                    "screen_classes"
                ]
            ),
        }
        for candidate in candidates
    ]
    candidate_ids = [
        str(candidate["candidate_id"])
        for candidate in compact_candidates
    ]
    return {
        "schema_version": MULTIPASS_SCHEMA_VERSION,
        "suite_id": suite,
        "multipass_stage": "junk_verify",
        "task": (
            "Audit provisionally retained candidates for the closed junk "
            "classes without rewriting any claim."
        ),
        "instructions": [
            "Return one verdict for every candidate and no others.",
            "A reject requires a closed junk_reason and a verbatim deficiency_quote from evidence_text.",
            "A confirmed retention requires null junk_reason and null deficiency_quote.",
            "Screen classes explain why an item was audited; they are not verdicts.",
        ],
        "output_schema": phase_c_junk_verify_schema(candidate_ids),
        "input": {"candidates": compact_candidates},
    }


def _phase_c_junk_outputs(
    output_root: Path,
) -> dict[str, dict[str, Any]]:
    results: dict[str, dict[str, Any]] = {}
    for path in sorted(output_root.glob("*/validated.private.json")):
        for item in _read_json(path)["items"]:
            candidate_id = str(item["candidate_id"])
            if candidate_id in results:
                raise TrueNorthError(
                    f"duplicate junk-verifier output: {candidate_id}"
                )
            results[candidate_id] = dict(item)
    return results


def _phase_c_paid_attempt_receipts(
    run_root: Path,
) -> list[dict[str, Any]]:
    receipts: list[dict[str, Any]] = []
    paths = list(
        (run_root / "failed-attempts").glob("*.private.jsonl")
    )
    paths.extend(
        (run_root / "outputs").glob(
            "**/attempt-*.private.jsonl"
        )
    )
    for path in sorted(paths):
        stdout = path.read_text(encoding="utf-8")
        _, finish, stream_count = _parse_opencode_stream(stdout)
        timestamps = [
            float(event["timestamp"])
            for line in stdout.splitlines()
            if line.strip()
            for event in [json.loads(line)]
            if isinstance(event.get("timestamp"), (int, float))
        ]
        elapsed_seconds = (
            (max(timestamps) - min(timestamps)) / 1000
            if timestamps
            else 0.0
        )
        receipts.append(
            {
                "provider_model": MULTIPASS_MODEL,
                "attempt_artifact": str(path),
                "semantic_validation_failure": (
                    "failed-attempts" in path.parts
                ),
                "elapsed_seconds": elapsed_seconds,
                "stream_event_count": stream_count,
                "usage": _usage(finish),
            }
        )
    return receipts


def run_phase_c_junk_verify(
    *,
    output_root: str | Path | None = None,
    suite: str = SUITE_ID,
    timeout_seconds: int = 900,
    opencode_binary: str = "/opt/homebrew/bin/opencode",
    run_id: str = "phase-c-junk-verify-20260728-v1",
    runner: Any | None = None,
) -> dict[str, Any]:
    """Run the bounded Task-4b asymmetric verifier on the Search fold."""

    suite_root = _suite_root(output_root, suite)
    screen_report = dry_run_phase_c_junk_screen(
        output_root=output_root, suite=suite
    )
    episodes, composed, ensemble = _load_phase_c_task4b_inputs(
        suite_root
    )
    screened = screen_phase_c_junk_candidates(
        episodes, composed, ensemble
    )
    candidate_by_id = {
        str(candidate["candidate_id"]): candidate
        for candidates in episodes.values()
        for candidate in candidates
    }
    selected_ids = sorted(screened)
    batches = [
        selected_ids[index : index + PHASE_C_JUNK_VERIFY_BATCH_SIZE]
        for index in range(
            0, len(selected_ids), PHASE_C_JUNK_VERIFY_BATCH_SIZE
        )
    ]
    if len(batches) > PHASE_C_JUNK_VERIFY_MAX_GLM_CALLS:
        raise TrueNorthError(
            "Task-4b batch count exceeds declared GLM budget"
        )
    run_root = suite_root / "phase-c" / "runs" / run_id
    budget = {
        "max_glm_calls": PHASE_C_JUNK_VERIFY_MAX_GLM_CALLS,
        "max_spark_calls": PHASE_C_JUNK_VERIFY_MAX_SPARK_CALLS,
        "max_tokens": PHASE_C_JUNK_VERIFY_MAX_TOKENS,
    }
    configuration = {
        "schema_version": MULTIPASS_SCHEMA_VERSION,
        "phase": "phase_c_junk_verify",
        "suite_id": suite,
        "suite_manifest_sha256": _read_json(
            suite_root / "manifest.json"
        )["manifest_sha256"],
        "source_disposition_result_sha256": screen_report[
            "source_disposition_result_sha256"
        ],
        "screen_sha256": screen_report["screen_sha256"],
        "system_prompt_sha256": sha256_text(
            PHASE_C_JUNK_VERIFY_SYSTEM_PROMPT
        ),
        "model": MULTIPASS_MODEL,
        "spark_model": "openai/gpt-5.3-codex-spark",
        "batch_size": PHASE_C_JUNK_VERIFY_BATCH_SIZE,
        "budget": budget,
        "holdout_access_allowed": False,
        "production_database_open_allowed": False,
    }
    configuration["configuration_sha256"] = sha256_text(
        dumps_json(configuration)
    )
    config_path = run_root / "configuration.json"
    if config_path.is_file():
        if _read_json(config_path) != configuration:
            raise TrueNorthError(
                "Task-4b resume configuration differs"
            )
    else:
        _write_json(config_path, configuration, immutable=True)
    _write_json(
        run_root / "screen.private.json",
        screen_report,
        immutable=True,
    )
    actual_runner = runner or _run_opencode_packet
    all_receipts = _phase_c_paid_attempt_receipts(run_root)
    for index, candidate_ids in enumerate(batches, start=1):
        packet = _phase_c_junk_verify_packet(
            suite=suite,
            candidates=[
                candidate_by_id[candidate_id]
                for candidate_id in candidate_ids
            ],
            screened=screened,
        )
        packet_path = (
            run_root
            / "packets"
            / "glm"
            / f"batch-{index:03d}.private.json"
        )
        _write_json(packet_path, packet, immutable=True)
        kwargs = {
            "packet_path": packet_path,
            "output_dir": (
                run_root
                / "outputs"
                / "glm"
                / f"batch-{index:03d}"
            ),
            "models": (MULTIPASS_MODEL,),
            "stage": "phase-c-junk-verify",
            "timeout_seconds": timeout_seconds,
            "opencode_binary": opencode_binary,
            "system_prompt": PHASE_C_JUNK_VERIFY_SYSTEM_PROMPT,
            "validator": validate_phase_c_junk_verify,
        }
        if runner is None:
            kwargs["_semantic_retry_remaining"] = 0
        _, receipts, _ = actual_runner(**kwargs)
        all_receipts = _phase_c_paid_attempt_receipts(run_root)
        usage = _multipass_usage(all_receipts)
        if (
            usage["calls"] > PHASE_C_JUNK_VERIFY_MAX_GLM_CALLS
            or usage["tokens"] > PHASE_C_JUNK_VERIFY_MAX_TOKENS
        ):
            raise TrueNorthError(
                "Task-4b GLM usage exceeded its declared budget"
            )
    verifier = _phase_c_junk_outputs(
        run_root / "outputs" / "glm"
    )
    preliminary = compose_phase_c_junk_verification(
        composed, screened, verifier, {}
    )
    escalation_ids = preliminary[
        "spark_escalation_candidate_ids"
    ]
    if len(escalation_ids) > PHASE_C_JUNK_VERIFY_MAX_SPARK_CALLS:
        raise TrueNorthError(
            "Task-4b needs more Spark escalations than authorized"
        )
    spark_results: dict[str, dict[str, Any]] = {}
    for candidate_id in escalation_ids:
        packet = _phase_c_junk_verify_packet(
            suite=suite,
            candidates=[candidate_by_id[candidate_id]],
            screened=screened,
        )
        packet_path = (
            run_root
            / "packets"
            / "spark"
            / f"{candidate_id}.private.json"
        )
        _write_json(packet_path, packet, immutable=True)
        kwargs = {
            "packet_path": packet_path,
            "output_dir": (
                run_root / "outputs" / "spark" / candidate_id
            ),
            "models": ("openai/gpt-5.3-codex-spark",),
            "stage": "phase-c-junk-verify-spark",
            "timeout_seconds": timeout_seconds,
            "opencode_binary": opencode_binary,
            "system_prompt": PHASE_C_JUNK_VERIFY_SYSTEM_PROMPT,
            "validator": validate_phase_c_junk_verify,
        }
        if runner is None:
            kwargs["_semantic_retry_remaining"] = 0
        output, receipts, _ = actual_runner(**kwargs)
        all_receipts = _phase_c_paid_attempt_receipts(run_root)
        spark_results[candidate_id] = dict(output["items"][0])
        usage = _multipass_usage(all_receipts)
        if usage["tokens"] > PHASE_C_JUNK_VERIFY_MAX_TOKENS:
            raise TrueNorthError(
                "Task-4b total token usage exceeded its declared budget"
            )
    composition = compose_phase_c_junk_verification(
        composed, screened, verifier, spark_results
    )
    consensus = _read_json(
        suite_root
        / "gold"
        / "development"
        / "final"
        / "consensus.private.json"
    )
    score = _score_phase_c_dispositions(
        consensus,
        composition["predictions"],
        _phase_c_preferred_document(suite_root),
    )
    usage = _multipass_usage(all_receipts)
    result = {
        "schema_version": MULTIPASS_SCHEMA_VERSION,
        "phase": "phase_c_junk_verify",
        "run_id": run_id,
        "configuration_sha256": configuration[
            "configuration_sha256"
        ],
        "screen_sha256": screen_report["screen_sha256"],
        "screened_count": len(screened),
        "screen_class_counts": screen_report[
            "screen_class_counts"
        ],
        "glm_packet_count": len(batches),
        "spark_escalation_count": len(escalation_ids),
        "flipped_candidate_ids": composition[
            "flipped_candidate_ids"
        ],
        "score": score,
        "usage": usage,
        "budget": budget,
        "holdout_opened": False,
        "production_mutation": False,
    }
    result["result_sha256"] = sha256_text(dumps_json(result))
    _write_json(
        run_root / "outputs" / "combined.private.json",
        {
            "items": [
                composition["predictions"][candidate_id]
                for candidate_id in sorted(
                    composition["predictions"]
                )
            ]
        },
        immutable=True,
    )
    _write_json(
        run_root / "result.private.json", result, immutable=True
    )
    return result


def _load_phase_c_task4c_inputs(
    suite_root: Path,
) -> tuple[
    dict[str, list[dict[str, Any]]],
    dict[str, dict[str, Any]],
    tuple[dict[str, dict[str, Any]], ...],
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
]:
    episodes, composed, ensemble = _load_phase_c_task4b_inputs(
        suite_root
    )
    manifest = _read_json(suite_root / "manifest.json")
    search_episode_ids = set(episodes)
    candidate_by_id: dict[str, dict[str, Any]] = {}
    segment_by_id: dict[str, dict[str, Any]] = {}
    for row in manifest["bundles"]:
        if str(row["episode_id"]) not in search_episode_ids:
            continue
        bundle = _read_json(Path(row["bundle_path"]))
        for candidate in bundle["candidates"]:
            candidate_by_id[str(candidate["candidate_id"])] = dict(
                candidate
            )
        for segment in bundle["segments"]:
            segment_id = str(segment["segment_id"])
            if segment_id in segment_by_id:
                raise TrueNorthError(
                    f"duplicate Task-4c segment ID: {segment_id}"
                )
            segment_by_id[segment_id] = dict(segment)
    if set(candidate_by_id) != set(composed):
        raise TrueNorthError(
            "Task-4c bundle and disposition scopes differ"
        )
    return (
        episodes,
        composed,
        ensemble,
        candidate_by_id,
        segment_by_id,
    )


def dry_run_phase_c_marginal_screen(
    *,
    output_root: str | Path | None = None,
    suite: str = SUITE_ID,
) -> dict[str, Any]:
    suite_root = _suite_root(output_root, suite)
    if not verify_suite(output_root=output_root, suite=suite)["ok"]:
        raise TrueNorthError(
            "suite verification failed before Task-4c screen"
        )
    (
        episodes,
        composed,
        ensemble,
        candidate_by_id,
        segment_by_id,
    ) = _load_phase_c_task4c_inputs(suite_root)
    screened = screen_phase_c_marginal_candidates(
        episodes, composed, ensemble
    )
    selected_ids = sorted(
        screened,
        key=lambda candidate_id: (
            screened[candidate_id]["episode_id"],
            candidate_by_id[candidate_id]["segment_id"],
            int(candidate_by_id[candidate_id]["evidence_start"]),
            candidate_id,
        ),
    )
    batches = [
        selected_ids[index : index + PHASE_C_MARGINAL_BATCH_SIZE]
        for index in range(
            0, len(selected_ids), PHASE_C_MARGINAL_BATCH_SIZE
        )
    ]
    if len(batches) > PHASE_C_MARGINAL_MAX_GLM_CALLS:
        raise TrueNorthError(
            "Task-4c screen exceeds declared GLM call budget"
        )
    packet_hashes: list[str] = []
    packet_bytes: list[int] = []
    for candidate_ids in batches:
        packet = build_phase_c_marginal_packet(
            suite=suite,
            candidate_ids=candidate_ids,
            candidate_by_id=candidate_by_id,
            segment_by_id=segment_by_id,
            screened=screened,
        )
        rendered_packet = dumps_json(packet)
        packet_hashes.append(sha256_text(rendered_packet))
        packet_bytes.append(len(rendered_packet.encode("utf-8")))
    estimated_input_tokens = (
        sum(packet_bytes)
        + PHASE_C_MARGINAL_PREFLIGHT_CHARS_PER_TOKEN
        - 1
    ) // PHASE_C_MARGINAL_PREFLIGHT_CHARS_PER_TOKEN
    estimated_total_tokens = estimated_input_tokens + (
        len(batches)
        * PHASE_C_MARGINAL_PREFLIGHT_NONINPUT_TOKENS_PER_CALL
    )
    class_counts = Counter(
        screen_class
        for row in screened.values()
        for screen_class in row["screen_classes"]
    )
    retained_count = sum(
        _gold_value_state(str(row["disposition"])) == "value"
        for row in composed.values()
    )
    report = {
        "schema_version": MULTIPASS_SCHEMA_VERSION,
        "phase": "phase_c_marginal_screen",
        "suite_id": suite,
        "source_disposition_result_sha256": (
            "f1f5ebda6e8c88ddc845767bdeab7f6fdf45a2e9c7aabd434a0a9e4c48a06fcd"
        ),
        "thresholds": {
            "repetition_token_jaccard": PHASE_C_MARGINAL_JACCARD,
            "local_proximity_characters": (
                PHASE_C_MARGINAL_LOCAL_DISTANCE
            ),
            "local_minimum_informative_stems": (
                PHASE_C_MARGINAL_LOCAL_MIN_STEMS
            ),
            "spark_confirm_token_jaccard": (
                PHASE_C_MARGINAL_SPARK_CONFIRM_JACCARD
            ),
            "segment_context_radius": (
                PHASE_C_MARGINAL_SEGMENT_RADIUS
            ),
        },
        "retained_count": retained_count,
        "screened_count": len(screened),
        "screen_class_counts": dict(sorted(class_counts.items())),
        "packet_count": len(batches),
        "packet_hashes": packet_hashes,
        "budget_preflight": {
            "packet_bytes": packet_bytes,
            "estimated_input_tokens": estimated_input_tokens,
            "reserved_noninput_tokens": (
                len(batches)
                * PHASE_C_MARGINAL_PREFLIGHT_NONINPUT_TOKENS_PER_CALL
            ),
            "estimated_total_tokens": estimated_total_tokens,
            "max_tokens": PHASE_C_MARGINAL_MAX_TOKENS,
            "fits": estimated_total_tokens
            <= PHASE_C_MARGINAL_MAX_TOKENS,
        },
        "screened": [
            screened[candidate_id]
            for candidate_id in selected_ids
        ],
        "gold_accessed": False,
        "model_calls_made": 0,
        "holdout_opened": False,
        "production_mutation": False,
    }
    report["screen_sha256"] = sha256_text(dumps_json(report))
    return report


def _phase_c_marginal_outputs(
    output_root: Path,
) -> dict[str, dict[str, Any]]:
    results: dict[str, dict[str, Any]] = {}
    for path in sorted(output_root.glob("*/validated.private.json")):
        for item in _read_json(path)["items"]:
            candidate_id = str(item["candidate_id"])
            if candidate_id in results:
                raise TrueNorthError(
                    f"duplicate marginal-verifier output: {candidate_id}"
                )
            results[candidate_id] = dict(item)
    return results


def run_phase_c_marginal_verify(
    *,
    output_root: str | Path | None = None,
    suite: str = SUITE_ID,
    timeout_seconds: int = 900,
    opencode_binary: str = "/opt/homebrew/bin/opencode",
    run_id: str = "phase-c-marginal-verify-20260728-v1",
    runner: Any | None = None,
) -> dict[str, Any]:
    """Run Task-4c once with neighbor and segment context."""

    suite_root = _suite_root(output_root, suite)
    screen_report = dry_run_phase_c_marginal_screen(
        output_root=output_root, suite=suite
    )
    if not screen_report["budget_preflight"]["fits"]:
        raise TrueNorthError(
            "Task-4c packet plan cannot fit the declared token budget; "
            "stopped before provider calls"
        )
    (
        episodes,
        composed,
        ensemble,
        candidate_by_id,
        segment_by_id,
    ) = _load_phase_c_task4c_inputs(suite_root)
    screened = screen_phase_c_marginal_candidates(
        episodes, composed, ensemble
    )
    selected_ids = sorted(
        screened,
        key=lambda candidate_id: (
            screened[candidate_id]["episode_id"],
            candidate_by_id[candidate_id]["segment_id"],
            int(candidate_by_id[candidate_id]["evidence_start"]),
            candidate_id,
        ),
    )
    batches = [
        selected_ids[index : index + PHASE_C_MARGINAL_BATCH_SIZE]
        for index in range(
            0, len(selected_ids), PHASE_C_MARGINAL_BATCH_SIZE
        )
    ]
    run_root = suite_root / "phase-c" / "runs" / run_id
    budget = {
        "max_glm_calls": PHASE_C_MARGINAL_MAX_GLM_CALLS,
        "max_spark_calls": PHASE_C_MARGINAL_MAX_SPARK_CALLS,
        "max_total_calls": (
            PHASE_C_MARGINAL_MAX_GLM_CALLS
            + PHASE_C_MARGINAL_MAX_SPARK_CALLS
        ),
        "max_tokens": PHASE_C_MARGINAL_MAX_TOKENS,
    }
    configuration = {
        "schema_version": MULTIPASS_SCHEMA_VERSION,
        "phase": "phase_c_marginal_verify",
        "suite_id": suite,
        "suite_manifest_sha256": _read_json(
            suite_root / "manifest.json"
        )["manifest_sha256"],
        "source_disposition_result_sha256": screen_report[
            "source_disposition_result_sha256"
        ],
        "screen_sha256": screen_report["screen_sha256"],
        "system_prompt_sha256": sha256_text(
            PHASE_C_MARGINAL_SYSTEM_PROMPT
        ),
        "model": MULTIPASS_MODEL,
        "spark_model": "openai/gpt-5.3-codex-spark",
        "batch_size": PHASE_C_MARGINAL_BATCH_SIZE,
        "budget": budget,
        "single_run_no_iteration": True,
        "holdout_access_allowed": False,
        "production_database_open_allowed": False,
    }
    configuration["configuration_sha256"] = sha256_text(
        dumps_json(configuration)
    )
    config_path = run_root / "configuration.json"
    if config_path.is_file():
        if _read_json(config_path) != configuration:
            raise TrueNorthError(
                "Task-4c resume configuration differs"
            )
    else:
        _write_json(config_path, configuration, immutable=True)
    _write_json(
        run_root / "screen.private.json",
        screen_report,
        immutable=True,
    )
    actual_runner = runner or _run_opencode_packet
    for index, candidate_ids in enumerate(batches, start=1):
        packet = build_phase_c_marginal_packet(
            suite=suite,
            candidate_ids=candidate_ids,
            candidate_by_id=candidate_by_id,
            segment_by_id=segment_by_id,
            screened=screened,
        )
        packet_path = (
            run_root
            / "packets"
            / "glm"
            / f"batch-{index:03d}.private.json"
        )
        _write_json(packet_path, packet, immutable=True)
        kwargs = {
            "packet_path": packet_path,
            "output_dir": (
                run_root
                / "outputs"
                / "glm"
                / f"batch-{index:03d}"
            ),
            "models": (MULTIPASS_MODEL,),
            "stage": "phase-c-marginal-verify",
            "timeout_seconds": timeout_seconds,
            "opencode_binary": opencode_binary,
            "system_prompt": PHASE_C_MARGINAL_SYSTEM_PROMPT,
            "validator": validate_phase_c_marginal_verify,
        }
        if runner is None:
            kwargs["_semantic_retry_remaining"] = 0
        actual_runner(**kwargs)
        usage = _multipass_usage(
            _phase_c_paid_attempt_receipts(run_root)
        )
        if (
            usage["calls"] > PHASE_C_MARGINAL_MAX_GLM_CALLS
            or usage["tokens"] > PHASE_C_MARGINAL_MAX_TOKENS
        ):
            raise TrueNorthError(
                "Task-4c GLM usage exceeded declared budget"
            )
    verifier = _phase_c_marginal_outputs(
        run_root / "outputs" / "glm"
    )
    preliminary = compose_phase_c_marginal_verification(
        composed, screened, verifier, {}
    )
    escalation_ids = preliminary[
        "spark_escalation_candidate_ids"
    ]
    if len(escalation_ids) > PHASE_C_MARGINAL_MAX_SPARK_CALLS:
        raise TrueNorthError(
            "Task-4c needs more Spark calls than authorized"
        )
    spark_results: dict[str, dict[str, Any]] = {}
    for candidate_id in escalation_ids:
        packet = build_phase_c_marginal_packet(
            suite=suite,
            candidate_ids=[candidate_id],
            candidate_by_id=candidate_by_id,
            segment_by_id=segment_by_id,
            screened=screened,
        )
        packet_path = (
            run_root
            / "packets"
            / "spark"
            / f"{candidate_id}.private.json"
        )
        _write_json(packet_path, packet, immutable=True)
        kwargs = {
            "packet_path": packet_path,
            "output_dir": (
                run_root / "outputs" / "spark" / candidate_id
            ),
            "models": ("openai/gpt-5.3-codex-spark",),
            "stage": "phase-c-marginal-verify-spark",
            "timeout_seconds": timeout_seconds,
            "opencode_binary": opencode_binary,
            "system_prompt": PHASE_C_MARGINAL_SYSTEM_PROMPT,
            "validator": validate_phase_c_marginal_verify,
        }
        if runner is None:
            kwargs["_semantic_retry_remaining"] = 0
        output, _, _ = actual_runner(**kwargs)
        spark_results[candidate_id] = dict(output["items"][0])
        usage = _multipass_usage(
            _phase_c_paid_attempt_receipts(run_root)
        )
        if (
            usage["calls"] > budget["max_total_calls"]
            or usage["tokens"] > PHASE_C_MARGINAL_MAX_TOKENS
        ):
            raise TrueNorthError(
                "Task-4c total usage exceeded declared budget"
            )
    composition = compose_phase_c_marginal_verification(
        composed, screened, verifier, spark_results
    )
    consensus = _read_json(
        suite_root
        / "gold"
        / "development"
        / "final"
        / "consensus.private.json"
    )
    score = _score_phase_c_dispositions(
        consensus,
        composition["predictions"],
        _phase_c_preferred_document(suite_root),
    )
    usage = _multipass_usage(
        _phase_c_paid_attempt_receipts(run_root)
    )
    result = {
        "schema_version": MULTIPASS_SCHEMA_VERSION,
        "phase": "phase_c_marginal_verify",
        "run_id": run_id,
        "configuration_sha256": configuration[
            "configuration_sha256"
        ],
        "screen_sha256": screen_report["screen_sha256"],
        "screened_count": len(screened),
        "screen_class_counts": screen_report[
            "screen_class_counts"
        ],
        "glm_packet_count": len(batches),
        "spark_escalation_count": len(escalation_ids),
        "spark_escalation_reasons": composition[
            "spark_escalation_reasons"
        ],
        "flipped_candidate_ids": composition[
            "flipped_candidate_ids"
        ],
        "score": score,
        "usage": usage,
        "budget": budget,
        "holdout_opened": False,
        "production_mutation": False,
    }
    result["result_sha256"] = sha256_text(dumps_json(result))
    _write_json(
        run_root / "outputs" / "combined.private.json",
        {
            "items": [
                composition["predictions"][candidate_id]
                for candidate_id in sorted(
                    composition["predictions"]
                )
            ]
        },
        immutable=True,
    )
    _write_json(
        run_root / "result.private.json", result, immutable=True
    )
    return result


def run_phase_c_disposition_escalation(
    *,
    first_run_id: str,
    second_run_id: str,
    output_root: str | Path | None = None,
    suite: str = SUITE_ID,
    timeout_seconds: int = 900,
    opencode_binary: str = "/opt/homebrew/bin/opencode",
    runner: Any | None = None,
) -> dict[str, Any]:
    """Use one bounded Spark call to adjudicate detected GLM conflicts."""

    suite_root = _suite_root(output_root, suite)
    if not verify_suite(output_root=output_root, suite=suite)["ok"]:
        raise TrueNorthError("suite verification failed before escalation")
    phase_root = suite_root / "phase-c" / "runs"
    first = _phase_c_disposition_outputs(phase_root / first_run_id)
    second = _phase_c_disposition_outputs(phase_root / second_run_id)
    manifest = _read_json(suite_root / "manifest.json")
    candidate_by_id: dict[str, dict[str, Any]] = {}
    for bundle_row in manifest["bundles"]:
        if str(bundle_row["episode_id"]) not in {
            "ep_90c3b5c995bce501c9aef55c",
            "ep_7ec9f808a3955c720aeb94ff",
        }:
            continue
        bundle = _read_json(Path(bundle_row["bundle_path"]))
        for candidate in bundle["candidates"]:
            candidate_by_id[str(candidate["candidate_id"])] = dict(
                candidate
            )
    selected = select_phase_c_disposition_conflicts(
        first, second, candidate_by_id
    )
    packet = {
        "schema_version": MULTIPASS_SCHEMA_VERSION,
        "suite_id": suite,
        "multipass_stage": "disposition",
        "task": (
            "Resolve bounded disposition conflicts from two independent GLM "
            "passes without changing any non-disposition field."
        ),
        "instructions": [
            "Read the exact evidence; neither GLM decision nor candidate wording is authoritative.",
            "Return one disposition for every selected candidate and no others.",
            "Apply the same substantive-assertion versus junk boundary as the approved disposition prompt.",
        ],
        "output_schema": multipass_disposition_schema(selected),
        "input": {
            "candidates": [
                candidate_by_id[candidate_id]
                for candidate_id in selected
            ],
            "first_pass": [
                first[candidate_id] for candidate_id in selected
            ],
            "second_pass": [
                second[candidate_id] for candidate_id in selected
            ],
        },
    }
    selection = {
        "schema_version": MULTIPASS_SCHEMA_VERSION,
        "selection_contract": (
            "all_second_pass_nonvalue_plus_changed_to_value_with_frozen_"
            "question_frame_intro_or_banter_risk"
        ),
        "gold_accessed": False,
        "first_run_id": first_run_id,
        "second_run_id": second_run_id,
        "selected_candidate_ids": selected,
        "selected_count": len(selected),
        "max_spark_calls": 25,
        "planned_spark_calls": 1,
    }
    selection["selection_sha256"] = sha256_text(dumps_json(selection))
    run_id = f"{second_run_id}-spark-conflicts-v1"
    run_root = phase_root / run_id
    packet_path = run_root / "packets" / "disposition.private.json"
    _write_json(packet_path, packet, immutable=True)
    _write_json(
        run_root / "selection.json", selection, immutable=True
    )
    actual_runner = runner or _run_opencode_packet
    kwargs = {
        "packet_path": packet_path,
        "output_dir": run_root / "outputs" / "disposition",
        "models": ("openai/gpt-5.3-codex-spark",),
        "stage": "phase-c-disposition-escalation",
        "timeout_seconds": timeout_seconds,
        "opencode_binary": opencode_binary,
        "system_prompt": (
            "You are the final disposition conflict adjudicator for a private "
            "podcast research corpus. Do not use tools. Read only exact evidence. "
            + MULTIPASS_SYSTEM_PROMPTS["disposition"]
        ),
        "validator": validate_multipass_disposition,
    }
    if runner is None:
        kwargs["_semantic_retry_remaining"] = 0
    output, receipts, model = actual_runner(**kwargs)
    validate_multipass_disposition(output, packet)
    escalated = {
        str(item["candidate_id"]): dict(item)
        for item in output["items"]
    }
    combined = combine_phase_c_disposition_votes(
        first, second, escalated
    )
    consensus = _read_json(
        suite_root
        / "gold"
        / "development"
        / "final"
        / "consensus.private.json"
    )
    score = _score_phase_c_dispositions(
        consensus,
        combined,
        _phase_c_preferred_document(suite_root),
    )
    usage = _multipass_usage(receipts)
    result = {
        "schema_version": MULTIPASS_SCHEMA_VERSION,
        "phase": "phase_c_disposition_escalation",
        "run_id": run_id,
        "selection_sha256": selection["selection_sha256"],
        "provider_model": model,
        "score": score,
        "usage": usage,
        "spark_call_ceiling": 25,
        "production_mutation": False,
        "holdout_opened": False,
    }
    result["result_sha256"] = sha256_text(dumps_json(result))
    _write_json(run_root / "result.private.json", result, immutable=True)
    _write_json(
        run_root / "outputs" / "combined.private.json",
        {"items": [combined[key] for key in sorted(combined)]},
        immutable=True,
    )
    return result


def score_run(
    *,
    run_id: str,
    output_root: str | Path | None = None,
    suite: str = SUITE_ID,
) -> dict[str, Any]:
    suite_root = _suite_root(output_root, suite)
    run_root = suite_root / "runs" / run_id
    receipt = _read_json(run_root / "run-receipt.json")
    current_manifest = _read_json(suite_root / "manifest.json")
    if (
        receipt["configuration"]["suite_manifest_sha256"]
        != current_manifest["manifest_sha256"]
    ):
        raise TrueNorthError(
            "run predates the current frozen suite interface and cannot be scored"
        )
    partition = str(receipt["partition"])
    partition_dir = "sealed-holdout" if partition == "holdout" else "development"
    gold_path = (
        suite_root / "gold" / partition_dir / "final" / "gold.private.json"
    )
    if not gold_path.is_file():
        raise TrueNorthError(
            f"frozen {partition} gold is not compiled; score is unavailable"
        )
    gold_document = _read_json(gold_path)
    gold = {str(row["candidate_id"]): row for row in gold_document["items"]}
    predicted = _predicted_atomic_outputs(run_root)
    if set(gold) != set(predicted):
        missing = sorted(set(gold) - set(predicted))
        extra = sorted(set(predicted) - set(gold))
        raise TrueNorthError(
            f"run/gold candidate scope differs: missing={len(missing)} extra={len(extra)}"
        )
    consensus_path = (
        suite_root
        / "gold"
        / partition_dir
        / "final"
        / "consensus.private.json"
    )
    if not consensus_path.is_file():
        raise TrueNorthError(
            "consensus-aware atomic gold is not compiled"
        )
    consensus_document = _read_json(consensus_path)
    if consensus_document.get("source_gold_sha256") != gold_document["gold_sha256"]:
        raise TrueNorthError(
            "consensus-aware gold is not bound to the current preferred gold"
        )
    metrics: list[dict[str, Any]] = []
    gold_reliability = _gold_interannotator_reliability(
        suite_root,
        partition_dir,
    )
    metrics.extend(
        [
            _metric(
                "gold_disposition_interannotator_agreement",
                gold_reliability["disposition_agreement"],
                threshold=None,
                details={"item_count": gold_reliability["item_count"]},
                gate=False,
                gate_group="diagnostic",
            ),
            _metric(
                "gold_atomic_count_interannotator_agreement",
                gold_reliability["atomic_count_agreement"],
                threshold=None,
                details={"item_count": gold_reliability["item_count"]},
                gate=False,
                gate_group="diagnostic",
            ),
            _metric(
                "gold_reject_interannotator_jaccard",
                gold_reliability["reject_jaccard"],
                threshold=None,
                details={
                    "pass_a_reject_count": gold_reliability[
                        "pass_a_reject_count"
                    ],
                    "pass_b_reject_count": gold_reliability[
                        "pass_b_reject_count"
                    ],
                    "reject_intersection_count": gold_reliability[
                        "reject_intersection_count"
                    ],
                    "reject_union_count": gold_reliability[
                        "reject_union_count"
                    ],
                },
                gate=False,
                gate_group="diagnostic",
            ),
        ]
    )
    metrics.extend(
        _consensus_atomic_metrics(consensus_document, predicted)
    )
    gold_disposition = {key: str(row["disposition"]) for key, row in gold.items()}
    predicted_disposition = {
        key: str(row["disposition"]) for key, row in predicted.items()
    }
    macro_f1, disposition_detail = _macro_f1(
        gold_disposition, predicted_disposition, DISPOSITIONS
    )
    metrics.append(
        _metric(
            "adjudicated_exact_disposition_macro_f1",
            macro_f1,
            threshold=None,
            details=disposition_detail,
            gate=False,
            gate_group="diagnostic",
        )
    )
    valuable_gold = {
        key for key, value in gold_disposition.items() if value in {"retain", "revise"}
    }
    retained_predicted = {
        key
        for key, value in predicted_disposition.items()
        if value in {"retain", "revise"}
    }
    retained_recall = (
        len(valuable_gold & retained_predicted) / len(valuable_gold)
        if valuable_gold
        else 1.0
    )
    metrics.append(
        _metric(
            "adjudicated_retained_value_recall",
            retained_recall,
            threshold=None,
            gate=False,
            gate_group="diagnostic",
        )
    )
    gold_junk = {key for key, value in gold_disposition.items() if value == "reject"}
    junk_escape = (
        len(gold_junk & retained_predicted) / len(gold_junk) if gold_junk else 0.0
    )
    metrics.append(
        _metric(
            "adjudicated_junk_escape_rate",
            junk_escape,
            threshold=None,
            comparison="<=",
            gate=False,
            gate_group="diagnostic",
        )
    )
    gold_atomic_count = sum(len(row["atomic_claims"]) for row in gold.values())
    predicted_atomic_count = sum(
        len(row["atomic_claims"]) for row in predicted.values()
    )
    shared_atomic_boundaries = sum(
        min(len(gold[key]["atomic_claims"]), len(predicted[key]["atomic_claims"]))
        for key in gold
    )
    boundary_precision = (
        shared_atomic_boundaries / predicted_atomic_count
        if predicted_atomic_count
        else (1.0 if not gold_atomic_count else 0.0)
    )
    boundary_recall = (
        shared_atomic_boundaries / gold_atomic_count
        if gold_atomic_count
        else (1.0 if not predicted_atomic_count else 0.0)
    )
    boundary_f1 = (
        2
        * boundary_precision
        * boundary_recall
        / (boundary_precision + boundary_recall)
        if boundary_precision + boundary_recall
        else 0.0
    )
    metrics.append(
        _metric(
            "adjudicated_exact_atomic_boundary_f1",
            boundary_f1,
            threshold=None,
            details={
                "precision": boundary_precision,
                "recall": boundary_recall,
                "gold_atomic_count": gold_atomic_count,
                "predicted_atomic_count": predicted_atomic_count,
            },
            gate=False,
            gate_group="diagnostic",
        )
    )
    canonical_gold_path = (
        suite_root / "gold" / partition_dir / "final" / "canonicals.private.json"
    )
    if not canonical_gold_path.is_file():
        raise TrueNorthError("frozen canonical gold is not compiled")
    canonical_gold_doc = _read_json(canonical_gold_path)
    canonical_gold = {
        str(row["gold_atomic_id"]): row for row in canonical_gold_doc["items"]
    }
    gold_candidate_by_atomic: dict[str, str] = {}
    for candidate_id, row in gold.items():
        for atomic_index, _atomic in enumerate(row["atomic_claims"]):
            gold_candidate_by_atomic[
                stable_id(candidate_id, str(atomic_index), prefix="gac_")
            ] = candidate_id
    speaker_total = 0
    speaker_correct = 0
    confidently_wrong_speakers = 0
    unresolved_gold_total = 0
    unresolved_gold_correct = 0
    reported_actor_predictions = 0
    reported_actor_correct = 0
    gold_clusters: dict[str, str] = {}
    predicted_atomic_keys: set[str] = set()
    for candidate_id in sorted(gold):
        gold_atomics = list(gold[candidate_id]["atomic_claims"])
        predicted_atomics = list(predicted[candidate_id]["atomic_claims"])
        for index in range(min(len(gold_atomics), len(predicted_atomics))):
            atomic_key = f"{candidate_id}:{index}"
            gold_atomic_id = stable_id(candidate_id, str(index), prefix="gac_")
            assignment = canonical_gold[gold_atomic_id]
            gold_clusters[atomic_key] = "|".join(
                (str(assignment["subject_key"]), str(assignment["proposition_key"]))
            )
            predicted_atomic_keys.add(atomic_key)
    run_db = db.connect(Path(receipt["run_database"]))
    try:
        atomic_rows = int(
            run_db.execute("SELECT COUNT(*) FROM atomic_claims").fetchone()[0]
        )
        invalid_offsets = int(
            run_db.execute(
                """
                SELECT COUNT(*)
                FROM atomic_claims AS claims
                JOIN discourse_events AS events ON events.id = claims.discourse_event_id
                WHERE claims.evidence_text <> events.evidence_text
                   OR claims.evidence_start <> events.evidence_start
                   OR claims.evidence_end <> events.evidence_end
                """
            ).fetchone()[0]
        )
        orphan_claims = int(
            run_db.execute(
                """
                SELECT COUNT(*) FROM atomic_claims AS claims
                LEFT JOIN corpus_releases AS releases
                  ON releases.id = claims.corpus_release_id
                LEFT JOIN pipeline_runs AS runs ON runs.id = claims.pipeline_run_id
                WHERE releases.id IS NULL OR runs.id IS NULL
                """
            ).fetchone()[0]
        )
        committed_rows = run_db.execute(
            """
            SELECT claims.id, claims.raw_speaker, claims.provenance_json,
                   positions.subject_id, positions.variant_id,
                   people.display_name,
                   canonical.canonical_subject_key,
                   canonical.canonical_proposition_key
            FROM atomic_claims AS claims
            LEFT JOIN accepted_position_observations AS positions
              ON positions.atomic_claim_id = claims.id
             AND positions.review_status = 'accepted'
            LEFT JOIN canonical_people AS people
              ON people.id = positions.canonical_person_id
            LEFT JOIN true_north_variant_canonical_map AS canonical
              ON canonical.variant_id = positions.variant_id
             AND canonical.run_id = ?
            WHERE claims.review_status = 'accepted'
            ORDER BY claims.id
            """,
            (run_id,),
        ).fetchall()
        predicted_clusters: dict[str, str] = {}
        run_claim_by_gold_atomic: dict[str, str] = {}
        run_candidate_by_claim: dict[str, str] = {}
        run_claims_by_candidate: dict[str, list[str]] = defaultdict(list)
        for row in committed_rows:
            provenance = json.loads(row["provenance_json"] or "{}")
            candidate_id = str(provenance.get("candidate_id") or "")
            atomic_index = provenance.get("atomic_index")
            if not candidate_id or not isinstance(atomic_index, int):
                continue
            atomic_key = f"{candidate_id}:{atomic_index}"
            gold_atomic_id = stable_id(candidate_id, str(atomic_index), prefix="gac_")
            run_claim_by_gold_atomic[gold_atomic_id] = str(row["id"])
            run_candidate_by_claim[str(row["id"])] = candidate_id
            run_claims_by_candidate[candidate_id].append(str(row["id"]))
            if row["subject_id"] and row["variant_id"]:
                predicted_clusters[atomic_key] = "|".join(
                    (
                        str(
                            row["canonical_subject_key"]
                            or row["subject_id"]
                        ),
                        str(
                            row["canonical_proposition_key"]
                            or row["variant_id"]
                        ),
                    )
                )
            if gold_atomic_id in canonical_gold:
                gold_assignment = canonical_gold[gold_atomic_id]
                speaker_total += 1
                predicted_speaker = row["display_name"] or row["raw_speaker"]
                speaker_matches = (
                    _normalized_label(gold_assignment["direct_speaker"])
                    == _normalized_label(predicted_speaker)
                )
                speaker_correct += int(speaker_matches)
                confidently_wrong_speakers += int(
                    bool(row["display_name"]) and not speaker_matches
                )
                if gold_assignment["speaker_resolution"] == "unresolved":
                    unresolved_gold_total += 1
                    unresolved_gold_correct += int(not bool(row["display_name"]))
                predicted_actor = provenance.get("reported_actor")
                if predicted_actor:
                    reported_actor_predictions += 1
                    reported_actor_correct += int(
                        _normalized_label(predicted_actor)
                        == _normalized_label(gold_assignment.get("reported_actor"))
                    )
        accepted_relation_rows = run_db.execute(
            """
            SELECT source_claim_id, target_claim_id, relation
            FROM claim_relation_judgments
            WHERE review_status = 'accepted'
            """
        ).fetchall()
        predicted_relations = {
            tuple(sorted((str(row["source_claim_id"]), str(row["target_claim_id"])))): str(
                row["relation"]
            )
            for row in accepted_relation_rows
        }
    finally:
        run_db.close()
    speaker_accuracy = speaker_correct / speaker_total if speaker_total else 0.0
    for atomic_key in gold_clusters:
        predicted_clusters.setdefault(atomic_key, f"__missing__:{atomic_key}")
    metrics.append(
        _metric(
            "primary_speaker_accuracy",
            speaker_accuracy,
            threshold=0.97,
            details={"correct": speaker_correct, "total": speaker_total},
        )
    )
    metrics.extend(
        (
            _metric(
                "reported_actor_precision",
                (
                    reported_actor_correct / reported_actor_predictions
                    if reported_actor_predictions
                    else 1.0
                ),
                threshold=0.95,
                details={
                    "correct": reported_actor_correct,
                    "predicted": reported_actor_predictions,
                },
                gate_group="safety",
            ),
            _metric(
                "confidently_wrong_speaker_rate",
                (
                    confidently_wrong_speakers / speaker_total
                    if speaker_total
                    else 0.0
                ),
                threshold=0.01,
                comparison="<=",
                gate_group="safety",
            ),
            _metric(
                "unresolved_speaker_recall",
                (
                    unresolved_gold_correct / unresolved_gold_total
                    if unresolved_gold_total
                    else 1.0
                ),
                threshold=0.90,
                details={
                    "correct": unresolved_gold_correct,
                    "gold_unresolved": unresolved_gold_total,
                },
            ),
        )
    )
    cluster = _pairwise_cluster_metrics(gold_clusters, predicted_clusters)
    metrics.extend(
        (
            _metric(
                "canonical_cluster_precision",
                cluster["precision"],
                threshold=0.95,
                details=cluster,
            ),
            _metric(
                "canonical_cluster_recall",
                cluster["recall"],
                threshold=0.85,
                details=cluster,
            ),
            _metric(
                "canonical_false_merge_rate",
                cluster["false_merge_rate"],
                threshold=0.02,
                comparison="<=",
                details=cluster,
                gate_group="safety",
            ),
        )
    )
    metrics.extend(
        (
            _metric(
                "exact_evidence_commit_rate",
                1.0 - (invalid_offsets / atomic_rows if atomic_rows else 0.0),
                threshold=1.0,
                gate_group="safety",
            ),
            _metric(
                "orphan_commit_count",
                float(orphan_claims),
                threshold=0.0,
                comparison="==",
                gate_group="safety",
            ),
        )
    )
    manifest = current_manifest
    metrics.append(
        _metric(
            "production_source_unchanged",
            1.0 if _source_unchanged(manifest) else 0.0,
            threshold=1.0,
            comparison="==",
            gate_group="safety",
        )
    )
    relation_gold = suite_root / "gold" / partition_dir / "final" / "relations.private.json"
    utility_gold = suite_root / "gold" / partition_dir / "final" / "utility.private.json"
    metrics.append(
        _metric(
            "relation_gold_complete",
            1.0 if relation_gold.is_file() else 0.0,
            threshold=1.0,
            comparison="==",
        )
    )
    metrics.append(
        _metric(
            "utility_gold_complete",
            1.0 if utility_gold.is_file() else 0.0,
            threshold=1.0,
            comparison="==",
        )
    )
    if relation_gold.is_file():
        relation_document = _read_json(relation_gold)
        gold_relation_labels: dict[str, str] = {}
        predicted_relation_labels: dict[str, str] = {}
        for row in relation_document["items"]:
            pair_id = str(row["pair_id"])
            expected_relation = str(row["relation"])
            gold_relation_labels[pair_id] = expected_relation
            left_candidate = gold_candidate_by_atomic.get(
                str(row["source_gold_atomic_id"])
            )
            right_candidate = gold_candidate_by_atomic.get(
                str(row["target_gold_atomic_id"])
            )
            observed_labels: set[str] = set()
            if left_candidate and right_candidate:
                for left in run_claims_by_candidate.get(left_candidate, ()):
                    for right in run_claims_by_candidate.get(
                        right_candidate, ()
                    ):
                        if left == right:
                            continue
                        observed = predicted_relations.get(
                            tuple(sorted((left, right)))
                        )
                        if observed:
                            observed_labels.add(observed)
            predicted_relation_labels[pair_id] = (
                expected_relation
                if expected_relation in observed_labels
                else sorted(observed_labels)[0]
                if len(observed_labels) == 1
                else "incomparable"
            )
        relation_macro_f1, relation_detail = _macro_f1(
            gold_relation_labels, predicted_relation_labels, RELATIONS
        )
        metrics.append(
            _metric(
                "relation_macro_f1",
                relation_macro_f1,
                threshold=0.85,
                details=relation_detail,
            )
        )
        for label in ("equivalent", "contradicts"):
            metrics.append(
                _metric(
                    f"relation_{label}_precision",
                    relation_detail[label]["precision"],
                    threshold=0.95,
                    details=relation_detail[label],
                )
            )
    if utility_gold.is_file():
        predicted_utility_path = run_root / "utility" / "validated.private.json"
        legacy_predicted_utility_path = (
            run_root / "utility" / "outputs" / "utility" / "validated.private.json"
        )
        if not predicted_utility_path.is_file() and legacy_predicted_utility_path.is_file():
            predicted_utility_path = legacy_predicted_utility_path
        if not predicted_utility_path.is_file():
            metrics.append(
                _metric(
                    "research_utility_correct_questions",
                    0.0,
                    threshold=22.0,
                    gate_group="primary",
                )
            )
        else:
            gold_utility = {
                str(row["question_id"]): row
                for row in _read_json(utility_gold)["items"]
            }
            predicted_utility = {
                str(row["question_id"]): row
                for row in _read_json(predicted_utility_path)["items"]
            }
            correct = 0
            detail = {}
            unsupported = 0
            for question_id, gold_row in gold_utility.items():
                predicted_row = predicted_utility.get(question_id)
                is_correct = False
                if predicted_row is not None:
                    answerability_matches = bool(predicted_row["answerable"]) == bool(
                        gold_row["answerable"]
                    )
                    if not gold_row["answerable"]:
                        is_correct = answerability_matches
                    elif answerability_matches:
                        predicted_support = {
                            run_candidate_by_claim.get(str(value), "")
                            for value in predicted_row["support_atomic_claim_ids"]
                        }
                        predicted_support.discard("")
                        gold_support = {
                            gold_candidate_by_atomic.get(str(value), "")
                            for value in gold_row["support_gold_atomic_ids"]
                        }
                        gold_support.discard("")
                        overlap = len(predicted_support & gold_support)
                        precision = (
                            overlap / len(predicted_support)
                            if predicted_support
                            else 0.0
                        )
                        recall = overlap / len(gold_support) if gold_support else 1.0
                        support_f1 = (
                            2 * precision * recall / (precision + recall)
                            if precision + recall
                            else 0.0
                        )
                        is_correct = support_f1 >= 0.5
                        detail[question_id] = {
                            "support_precision": round(precision, 6),
                            "support_recall": round(recall, 6),
                            "support_f1": round(support_f1, 6),
                        }
                    if predicted_row["answerable"] and not gold_row["answerable"]:
                        unsupported += 1
                correct += int(is_correct)
                detail.setdefault(question_id, {})["correct"] = is_correct
            metrics.extend(
                (
                    _metric(
                        "research_utility_correct_questions",
                        float(correct),
                        threshold=22.0,
                        details=detail,
                        gate_group="primary",
                    ),
                    _metric(
                        "unsupported_research_answers",
                        float(unsupported),
                        threshold=0.0,
                        comparison="==",
                        gate_group="safety",
                    ),
                )
            )
    gate_metrics = [row for row in metrics if row["gate"]]
    primary_metrics = [
        row for row in gate_metrics if row["gate_group"] == "primary"
    ]
    if not primary_metrics:
        raise TrueNorthError(
            "research utility primary certification gate is missing"
        )
    passed = all(row["passed"] is True for row in gate_metrics)
    score_document = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "suite_id": SUITE_ID,
        "run_id": run_id,
        "partition": partition,
        "configuration_sha256": receipt["configuration_sha256"],
        "gold_sha256": gold_document["gold_sha256"],
        "consensus_gold_sha256": consensus_document["consensus_sha256"],
        "gold_interannotator_reliability": gold_reliability,
        "certification_policy": {
            "policy_version": CONSENSUS_GOLD_POLICY_VERSION,
            "measurement_contract_sha256": (
                measurement_contract.get("contract_sha256")
                if isinstance(measurement_contract, Mapping)
                else None
            ),
            "primary_gate": "research_utility_correct_questions",
            "primary_gate_passed": all(
                row["passed"] is True for row in primary_metrics
            ),
            "gate_count": len(gate_metrics),
            "diagnostic_count": len(metrics) - len(gate_metrics),
        },
        "relational_merge_certification": (
            relational_certification
        ),
        "passed": passed,
        "metrics": metrics,
        "scored_at": now_iso(),
    }
    score_document["score_sha256"] = sha256_text(dumps_json(score_document))
    score_path = run_root / "score.json"
    _write_json(score_path, score_document, immutable=True)
    conn = db.connect(Path(receipt["run_database"]))
    try:
        for row in metrics:
            conn.execute(
                """
                INSERT OR REPLACE INTO true_north_scores
                  (run_id, metric, value, passed, details_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    row["metric"],
                    row["value"],
                    None if row["passed"] is None else int(row["passed"]),
                    _canonical_json(row["details"]),
                    score_document["scored_at"],
                ),
            )
        conn.commit()
    finally:
        conn.close()
    if partition == "development":
        _update_development_gate(suite_root, score_document)
    return {**score_document, "score_path": str(score_path)}


def _update_development_gate(
    suite_root: Path, score: Mapping[str, Any]
) -> None:
    gate_path = suite_root / "development-gate.json"
    history: list[dict[str, Any]] = []
    if gate_path.is_file():
        current = _read_json(gate_path)
        history = list(current.get("history") or [])
    history.append(
        {
            "run_id": score["run_id"],
            "configuration_sha256": score["configuration_sha256"],
            "passed": bool(score["passed"]),
            "score_sha256": score["score_sha256"],
            "scored_at": score["scored_at"],
        }
    )
    history = history[-20:]
    consecutive = 0
    expected_config = None
    for row in reversed(history):
        if not row["passed"]:
            break
        if expected_config is None:
            expected_config = row["configuration_sha256"]
        if row["configuration_sha256"] != expected_config:
            break
        consecutive += 1
    gate = {
        "schema_version": "pif_true_north_development_gate_v1",
        "suite_id": SUITE_ID,
        "passed": consecutive >= 2,
        "consecutive_passes": consecutive,
        "configuration_sha256": expected_config,
        "history": history,
        "updated_at": now_iso(),
    }
    _write_json(gate_path, gate)


def render_report(
    *,
    run_id: str,
    output_root: str | Path | None = None,
    suite: str = SUITE_ID,
) -> dict[str, Any]:
    suite_root = _suite_root(output_root, suite)
    run_root = suite_root / "runs" / run_id
    receipt = _read_json(run_root / "run-receipt.json")
    score_path = run_root / "score.json"
    score = _read_json(score_path) if score_path.is_file() else None
    rows = []
    if score is not None:
        for metric in score["metrics"]:
            value = metric["value"]
            rendered_value = "not scored" if value is None else f"{float(value):.4f}"
            status = (
                "PASS"
                if metric["passed"] is True
                else "FAIL"
                if metric["passed"] is False
                else "DIAGNOSTIC"
            )
            rows.append(
                "<tr>"
                f"<td>{html.escape(metric['metric'])}</td>"
                f"<td>{html.escape(rendered_value)}</td>"
                f"<td>{html.escape(str(metric.get('gate_group', 'quality')))}</td>"
                f"<td>{html.escape(str(metric['comparison']))} "
                f"{html.escape(str(metric['threshold']))}</td>"
                f"<td>{status}</td>"
                "</tr>"
            )
    ledger_rows = "".join(
        f"<li>{html.escape(key)}: {int(value)}</li>"
        for key, value in receipt["ledger_counts"].items()
    )
    report_html = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>PIF AI-Safety True-North Report</title>
<style>
body{{font-family:system-ui,sans-serif;max-width:1050px;margin:2rem auto;padding:0 1rem;line-height:1.45}}
table{{border-collapse:collapse;width:100%}}th,td{{text-align:left;padding:.55rem;border-bottom:1px solid #999}}
code{{overflow-wrap:anywhere}}.note{{color:#555}}
</style>
</head>
<body>
<h1>AI-Safety True-North Downstream Benchmark</h1>
<p><strong>Run:</strong> <code>{html.escape(run_id)}</code><br>
<strong>Partition:</strong> {html.escape(receipt['partition'])}<br>
<strong>Result:</strong> {html.escape('PASS' if score and score['passed'] else 'NOT PASSED')}</p>
<h2>Stage accounting</h2><ul>{ledger_rows}</ul>
<h2>Quality gates</h2>
<table><thead><tr><th>Metric</th><th>Value</th><th>Group</th><th>Gate</th><th>Status</th></tr></thead>
<tbody>{''.join(rows)}</tbody></table>
<p class="note">This report contains operational metrics and identifiers only. Raw transcript text is intentionally omitted.</p>
</body></html>
"""
    html_path = run_root / "report.html"
    _write_text(html_path, report_html, immutable=True)
    report_json = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "suite_id": SUITE_ID,
        "run_id": run_id,
        "partition": receipt["partition"],
        "passed": bool(score and score["passed"]),
        "ledger_counts": receipt["ledger_counts"],
        "score_path": str(score_path) if score else None,
        "html_path": str(html_path),
        "raw_transcript_included": False,
        "generated_at": now_iso(),
    }
    report_path = run_root / "report.json"
    _write_json(report_path, report_json, immutable=True)
    return {**report_json, "report_path": str(report_path)}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pif lab true-north",
        description="Isolated AI-safety downstream benchmark.",
    )
    parser.add_argument(
        "--source-db",
        default=str(db_path()),
        help="Authoritative read-only factory database.",
    )
    parser.add_argument("--output-root", default=str(DEFAULT_PRIVATE_ROOT))
    parser.add_argument("--suite", default=SUITE_ID, choices=(SUITE_ID,))
    sub = parser.add_subparsers(dest="command", required=True)

    build = sub.add_parser("build", help="Freeze bundles and create the shadow database.")
    build.add_argument(
        "--skip-source-file-hash",
        action="store_true",
        help="Use source size, mtime, and canonical counts without hashing the full database.",
    )
    build.add_argument(
        "--rebuild",
        action="store_true",
        help="Replace the private lab freeze after an intentional interface change.",
    )
    sub.add_parser("verify", help="Verify manifest, bundle, transcript, and source integrity.")

    gold = sub.add_parser("gold", help="Prepare or execute independent Codex gold passes.")
    gold.add_argument(
        "--execute",
        action="store_true",
        help="Execute one selected gold pass; preparation alone never starts a model.",
    )
    gold.add_argument(
        "--all-development",
        action="store_true",
        help="Restart-safely execute and compile every development gold phase in dependency order.",
    )
    gold.add_argument(
        "--partition",
        choices=("development", "holdout"),
        default="development",
    )
    gold.add_argument(
        "--pass",
        dest="pass_name",
        choices=("pass-a", "pass-b", "pass-c", "compile"),
        default="pass-a",
    )
    gold.add_argument(
        "--phase",
        choices=(
            "atomic",
            "canonical",
            "canonical-subjects",
            "canonical-propositions",
            "relations",
            "utility",
            "actor-repair",
        ),
        default="atomic",
        help="Gold layer to execute. Canonical begins after compiled atomic gold.",
    )
    gold.add_argument("--limit", type=int)
    gold.add_argument("--timeout-seconds", type=int, default=1200)
    gold.add_argument("--codex-binary", default="codex")
    gold.add_argument(
        "--opencode-binary", default="/opt/homebrew/bin/opencode"
    )
    gold.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Parallel gold calls (1-4); two is the recommended subscription-safe maximum.",
    )

    run = sub.add_parser("run", help="Run workhorse and downstream shadow reconciliation.")
    run.add_argument("--router", choices=tuple(ROUTERS), default=DEFAULT_ROUTER)
    run.add_argument(
        "--partition",
        choices=("development", "holdout"),
        default="development",
    )
    run.add_argument("--timeout-seconds", type=int, default=900)
    run.add_argument("--opencode-binary", default="/opt/homebrew/bin/opencode")
    run.add_argument("--max-batches", type=int, default=100)
    run.add_argument("--episode-limit", type=int)
    run.add_argument(
        "--resume-run-id",
        help="Resume a failed run after its atomic shadow commit.",
    )
    run.add_argument(
        "--episode-id",
        action="append",
        dest="episode_ids",
        help="Run only a manifest-bound episode; repeat to select multiple episodes.",
    )

    score = sub.add_parser("score", help="Score one completed run against frozen gold.")
    score.add_argument("--run-id", required=True)
    diagnose_atomic = sub.add_parser(
        "diagnose-atomic",
        help="Score validated atomic outputs without running downstream stages.",
    )
    diagnose_atomic.add_argument("--run-id", required=True)
    prompt_optimize = sub.add_parser(
        "prompt-optimize",
        help="Run one GLM system-prompt-only optimization arm on development episodes.",
    )
    prompt_optimize.add_argument(
        "--arm", required=True, choices=tuple(SYSTEM_PROMPT_ARMS)
    )
    prompt_optimize.add_argument(
        "--episode-id",
        action="append",
        dest="episode_ids",
        help="Development episode fold; repeat to select multiple episodes.",
    )
    prompt_optimize.add_argument("--workers", type=int, default=4)
    prompt_optimize.add_argument("--timeout-seconds", type=int, default=900)
    prompt_optimize.add_argument(
        "--opencode-binary", default="/opt/homebrew/bin/opencode"
    )
    multipass = sub.add_parser(
        "multipass-run",
        help="Run the bounded GLM disposition/decomposition/attribution stack.",
    )
    multipass.add_argument(
        "--episode-id",
        action="append",
        dest="episode_ids",
        help="Opened development episode; repeat to select multiple episodes.",
    )
    multipass.add_argument("--workers", type=int, default=4)
    multipass.add_argument("--timeout-seconds", type=int, default=900)
    multipass.add_argument(
        "--opencode-binary", default="/opt/homebrew/bin/opencode"
    )
    multipass.add_argument("--run-id")
    multipass.add_argument("--resume-run-id")
    multipass.add_argument("--max-packets", type=int)
    multipass_score = sub.add_parser(
        "multipass-score",
        help="Score a completed multipass extraction run.",
    )
    multipass_score.add_argument("--run-id", required=True)
    phase_c_disposition = sub.add_parser(
        "phase-c-disposition",
        help="Run the approved bounded Search-fold disposition stage.",
    )
    phase_c_disposition.add_argument("--workers", type=int, default=4)
    phase_c_disposition.add_argument(
        "--timeout-seconds", type=int, default=900
    )
    phase_c_disposition.add_argument(
        "--opencode-binary", default="/opt/homebrew/bin/opencode"
    )
    phase_c_disposition.add_argument("--run-id")
    phase_c_junk_verify = sub.add_parser(
        "phase-c-junk-verify",
        help="Run the approved asymmetric Task-4b junk verifier.",
    )
    phase_c_junk_verify.add_argument(
        "--timeout-seconds", type=int, default=900
    )
    phase_c_junk_verify.add_argument(
        "--opencode-binary", default="/opt/homebrew/bin/opencode"
    )
    phase_c_junk_verify.add_argument(
        "--run-id", default="phase-c-junk-verify-20260728-v1"
    )
    phase_c_junk_verify.add_argument(
        "--dry-run", action="store_true"
    )
    phase_c_marginal_verify = sub.add_parser(
        "phase-c-marginal-verify",
        help="Run the approved relational Task-4c junk verifier.",
    )
    phase_c_marginal_verify.add_argument(
        "--timeout-seconds", type=int, default=900
    )
    phase_c_marginal_verify.add_argument(
        "--opencode-binary", default="/opt/homebrew/bin/opencode"
    )
    phase_c_marginal_verify.add_argument(
        "--run-id",
        default="phase-c-marginal-verify-20260728-v1",
    )
    phase_c_marginal_verify.add_argument(
        "--dry-run", action="store_true"
    )
    report = sub.add_parser("report", help="Render sanitized JSON and HTML reports.")
    report.add_argument("--run-id", required=True)
    for command_parser in (
        build,
        gold,
        run,
        score,
        diagnose_atomic,
        prompt_optimize,
        multipass,
        multipass_score,
        phase_c_disposition,
        phase_c_junk_verify,
        phase_c_marginal_verify,
        report,
    ):
        command_parser.add_argument(
            "--suite", choices=(SUITE_ID,), default=argparse.SUPPRESS
        )
        command_parser.add_argument("--output-root", default=argparse.SUPPRESS)
    verify = sub.choices["verify"]
    verify.add_argument("--suite", choices=(SUITE_ID,), default=argparse.SUPPRESS)
    verify.add_argument("--output-root", default=argparse.SUPPRESS)
    return parser


def _print(value: Mapping[str, Any], *, stream: Any = sys.stdout) -> None:
    print(json.dumps(dict(value), ensure_ascii=True, indent=2, sort_keys=True), file=stream)


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    try:
        common = {
            "output_root": args.output_root,
            "suite": args.suite,
        }
        if args.command == "build":
            result = build_suite(
                source_db=args.source_db,
                include_source_db_hash=not args.skip_source_file_hash,
                rebuild=args.rebuild,
                **common,
            )
        elif args.command == "verify":
            result = verify_suite(**common)
        elif args.command == "gold":
            if args.all_development:
                result = execute_development_gold_pipeline(
                    timeout_seconds=args.timeout_seconds,
                    codex_binary=args.codex_binary,
                    opencode_binary=args.opencode_binary,
                    workers=args.workers,
                    **common,
                )
            elif args.execute:
                result = execute_gold(
                    partition=args.partition,
                    pass_name=args.pass_name,
                    phase=args.phase,
                    limit=args.limit,
                    timeout_seconds=args.timeout_seconds,
                    codex_binary=args.codex_binary,
                    workers=args.workers,
                    **common,
                )
            else:
                result = prepare_gold(**common)
        elif args.command == "run":
            if args.resume_run_id:
                result = resume_benchmark(
                    run_id=args.resume_run_id,
                    timeout_seconds=args.timeout_seconds,
                    opencode_binary=args.opencode_binary,
                    max_batches=args.max_batches,
                    **common,
                )
            else:
                result = run_benchmark(
                    partition=args.partition,
                    router=args.router,
                    timeout_seconds=args.timeout_seconds,
                    opencode_binary=args.opencode_binary,
                    max_batches=args.max_batches,
                    episode_limit=args.episode_limit,
                    episode_ids=args.episode_ids,
                    **common,
                )
        elif args.command == "score":
            result = score_run(run_id=args.run_id, **common)
        elif args.command == "diagnose-atomic":
            result = diagnose_atomic_run(run_id=args.run_id, **common)
        elif args.command == "prompt-optimize":
            result = run_system_prompt_arm(
                arm=args.arm,
                episode_ids=args.episode_ids,
                workers=args.workers,
                timeout_seconds=args.timeout_seconds,
                opencode_binary=args.opencode_binary,
                **common,
            )
        elif args.command == "multipass-run":
            result = run_multipass(
                episode_ids=args.episode_ids,
                workers=args.workers,
                timeout_seconds=args.timeout_seconds,
                opencode_binary=args.opencode_binary,
                run_id=args.run_id,
                resume_run_id=args.resume_run_id,
                max_packets=args.max_packets,
                **common,
            )
        elif args.command == "multipass-score":
            result = score_multipass_run(
                run_id=args.run_id,
                **common,
            )
        elif args.command == "phase-c-disposition":
            result = run_phase_c_disposition(
                workers=args.workers,
                timeout_seconds=args.timeout_seconds,
                opencode_binary=args.opencode_binary,
                run_id=args.run_id,
                **common,
            )
        elif args.command == "phase-c-junk-verify":
            if args.dry_run:
                result = dry_run_phase_c_junk_screen(**common)
            else:
                result = run_phase_c_junk_verify(
                    timeout_seconds=args.timeout_seconds,
                    opencode_binary=args.opencode_binary,
                    run_id=args.run_id,
                    **common,
                )
        elif args.command == "phase-c-marginal-verify":
            if args.dry_run:
                result = dry_run_phase_c_marginal_screen(**common)
            else:
                result = run_phase_c_marginal_verify(
                    timeout_seconds=args.timeout_seconds,
                    opencode_binary=args.opencode_binary,
                    run_id=args.run_id,
                    **common,
                )
        elif args.command == "report":
            result = render_report(run_id=args.run_id, **common)
        else:
            raise TrueNorthError(f"unknown command: {args.command}")
        _print(result)
        return 0 if result.get("ok", True) else 1
    except (
        TrueNorthError,
        intelligence.IntelligenceValidationError,
        semantic_reconcile.SemanticReconciliationError,
        ValidationError,
        OSError,
        sqlite3.Error,
        subprocess.SubprocessError,
        json.JSONDecodeError,
    ) as exc:
        _print(
            {
                "ok": False,
                "error": type(exc).__name__,
                "message": str(exc),
                "canonical_mutation": False,
            },
            stream=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
