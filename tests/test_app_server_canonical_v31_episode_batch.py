from __future__ import annotations

import asyncio
import copy
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from research_factory import app_server_canonical_v31_episode_batch as adapter
from research_factory import codex_app_server
from research_factory.efficient_backtest import build_windowed_segment_packet
from research_factory.labels import validate_label_output
from research_factory.util import sha256_text


@pytest.fixture(autouse=True)
def _restore_main_thread_event_loop() -> None:
    """Keep Python 3.9 tests from leaking ``asyncio.run``'s closed loop."""

    yield
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())
    else:
        if loop.is_closed():
            asyncio.set_event_loop(asyncio.new_event_loop())


def _quality(words: int) -> dict:
    return {
        "artifact_type": "dialogue_transcript",
        "boilerplate_risk": "low",
        "substantive_word_count": words,
        "transcript_preparation_id": "prep_fixture_v1",
    }


def _segment(segment_id: str, text: str, *, density: str) -> dict:
    return {
        "segment_id": segment_id,
        "segment_text": text,
        "segment_quality": _quality(len(text.split())),
        "density_stratum": density,
        "boundaries": [
            {
                "window_id": 0,
                "chunk_index": 0,
                "owner_start": 0,
                "owner_end": len(text),
                "extract_start": 0,
                "extract_end": len(text),
            }
        ],
    }


def _episode(*, segment_count: int = 2, suffix: str = "one") -> dict:
    signal = _segment(
        f"seg_signal_{suffix}",
        "Host: New Atlas cuts inference latency by 40 percent.\n"
        "Guest: That makes on-device adoption feasible.\n",
        density="coded",
    )
    no_signal = _segment(
        f"seg_setup_{suffix}",
        "Host: Welcome to the show.\nGuest: Thanks for having me.\n",
        density="no_signal",
    )
    segments = [signal, no_signal]
    for index in range(2, segment_count):
        segments.append(
            _segment(
                f"seg_extra_{index}_{suffix}",
                f"Host: This is neutral setup number {index}.\n",
                density="no_signal",
            )
        )
    return {
        "episode_id": f"ep_fixture_{suffix}",
        "source_name": "Fixture Podcast",
        "episode_title": "A real-shaped coded and no-signal packet",
        "context_summary": "The guest discusses an inference system after show setup.",
        "speaker_map": [
            {"name": "Host", "role": "host"},
            {"name": "Guest", "role": "guest", "affiliation": "New Atlas"},
        ],
        "section_map": [
            {"section_id": "discussion", "segment_ids": [signal["segment_id"]]},
            {"section_id": "setup", "segment_ids": [no_signal["segment_id"]]},
        ],
        "entity_seed": {"organizations": ["New Atlas"]},
        "concept_seed": ["inference latency", "on-device adoption"],
        "extraction_guidance": "Code grounded technology propositions, not show setup.",
        "excluded_source_context": ["sponsor reads and page chrome when present"],
        "segments": segments,
    }


def _event(start_unit: str, end_unit: str, *, ordinal: int = 0) -> dict:
    event_type = "capability_claim" if ordinal == 0 else "adoption_signal"
    claim = (
        "New Atlas cuts inference latency by 40 percent."
        if ordinal == 0
        else "Lower latency makes on-device adoption feasible."
    )
    metric = (
        {
            "value": "40",
            "unit": "percent",
            "comparator": None,
            "direction": "decrease",
            "raw_text": "40 percent",
        }
        if ordinal == 0
        else {
            "value": None,
            "unit": None,
            "comparator": None,
            "direction": "not_applicable",
            "raw_text": None,
        }
    )
    return {
        "event_type": event_type,
        "event_subtype": "inference_performance" if ordinal == 0 else "edge_adoption",
        "actor": {
            "name": "New Atlas",
            "actor_type": "organization",
            "affiliation": None,
            "role": "system developer",
        },
        "speaker_context": {
            "name": "Guest",
            "role": "guest",
            "affiliation": "New Atlas",
            "confidence": 0.94,
        },
        "reported_actor": {
            "name": "",
            "actor_type": "none",
            "affiliation": None,
            "confidence": 1.0,
        },
        "source_context": {
            "kind": "substantive_dialogue",
            "confidence": 0.98,
            "rationale": "The speakers directly discuss the system and its consequence.",
        },
        "target": {
            "raw_target": "inference latency" if ordinal == 0 else "on-device adoption",
            "candidate_concept": "inference efficiency" if ordinal == 0 else "edge adoption",
            "canonical_concept": None,
            "concept_confidence": 0.87,
        },
        "surface_terms": ["inference latency"] if ordinal == 0 else ["on-device adoption"],
        "frames": ["performance improvement"] if ordinal == 0 else ["deployment feasibility"],
        "model_names": [],
        "product_names": ["New Atlas"],
        "organizations": ["New Atlas"],
        "people": [],
        "stance": "supportive",
        "claim_text": claim,
        "claim_type": "comparative" if ordinal == 0 else "causal",
        "certainty": "high",
        "temporal_horizon": "present",
        "causal_mechanism": "" if ordinal == 0 else "Reduced latency enables local execution.",
        "counterclaim": "",
        "metric": metric,
        "signal_reason": (
            "The source directly states a quantified reduction in inference latency."
            if ordinal == 0
            else "The source directly links the latency result to feasible on-device adoption."
        ),
        "exclusion_flags": [],
        "quality_flags": [],
        "evidence_start_unit_id": start_unit,
        "evidence_end_unit_id": end_unit,
        "confidence": 0.91,
        "audit_notes": "All material fields are grounded in the selected source unit.",
    }


def _candidate(unit_id: str) -> dict:
    return {
        "candidate": "edge adoption feasibility",
        "surface_terms": ["on-device adoption"],
        "rationale": "The source connects improved latency with feasible local deployment.",
        "usefulness_score": 0.86,
        "evidence_start_unit_id": unit_id,
        "evidence_end_unit_id": unit_id,
        "confidence": 0.9,
    }


def _receipts(units: list[dict], events: list[dict], candidates: list[dict]) -> list[dict]:
    return [
        {
            "unit_id": unit["unit_id"],
            "reviewed": True,
            "grounded_event_count": sum(
                event["evidence_start_unit_id"] == unit["unit_id"] for event in events
            ),
            "grounded_concept_candidate_count": sum(
                candidate["evidence_start_unit_id"] == unit["unit_id"]
                for candidate in candidates
            ),
            "unresolved_count": 0,
        }
        for unit in units
    ]


def _raw_segment(
    source: dict,
    *,
    events: list[dict],
    candidates: list[dict],
    status: str,
) -> dict:
    no_signal = status != "coded"
    return {
        "segment_id": source["segment_id"],
        "extraction_status": status,
        "segment_source_context": {
            "kind": "show_setup" if no_signal else "substantive_dialogue",
            "confidence": 0.99,
            "rationale": (
                "This segment contains only greetings and show setup."
                if no_signal
                else "This segment contains a substantive technical claim."
            ),
        },
        "discourse_events": events,
        "concept_candidates": candidates,
        "rejected_candidates": (
            [
                {
                    "text": "Welcome to the show",
                    "reason": "show_setup",
                    "source_context_kind": "show_setup",
                }
            ]
            if no_signal
            else []
        ),
        "no_signal_reason": (
            "The segment is show setup and contains no grounded discourse event."
            if no_signal
            else None
        ),
        "overall_confidence": 0.99 if no_signal else 0.92,
        "needs_review": False,
        "review_reason": None,
        "unit_receipts": _receipts(source["units"], events, candidates),
        "coverage_audit": {
            "all_source_units_reviewed": True,
            "unresolved_count": 0,
        },
    }


def _coded_and_no_signal_output(request: dict, *, two_events: bool = False) -> dict:
    source_segments = request["private_input"]["segments"]
    segments = []
    for index, source in enumerate(source_segments):
        if index == 0 and source["density_stratum"] == "coded":
            units = source["units"]
            events = [_event(units[0]["unit_id"], units[0]["unit_id"])]
            if two_events:
                events.append(_event(units[1]["unit_id"], units[1]["unit_id"], ordinal=1))
            candidates = [_candidate(units[1]["unit_id"])]
            segments.append(
                _raw_segment(
                    source,
                    events=events,
                    candidates=candidates,
                    status="coded",
                )
            )
        else:
            segments.append(
                _raw_segment(source, events=[], candidates=[], status="no_signal")
            )
    return {"episode_id": request["episode_id"], "segments": segments}


def _all_no_signal_output_from_prompt(prompt: str) -> dict:
    packet = json.loads(prompt.split("\n", 1)[1])
    return {
        "episode_id": packet["episode_id"],
        "segments": [
            {
                "segment_id": segment["segment_id"],
                "extraction_status": "no_signal",
                "segment_source_context": {
                    "kind": "show_setup",
                    "confidence": 1.0,
                    "rationale": "Fixture client classifies this packet as no signal.",
                },
                "discourse_events": [],
                "concept_candidates": [],
                "rejected_candidates": [],
                "no_signal_reason": "Fixture output contains no grounded discourse event.",
                "overall_confidence": 1.0,
                "needs_review": False,
                "review_reason": None,
                "unit_receipts": [
                    {
                        "unit_id": unit["unit_id"],
                        "reviewed": True,
                        "grounded_event_count": 0,
                        "grounded_concept_candidate_count": 0,
                        "unresolved_count": 0,
                    }
                    for unit in segment["source_units"]
                ],
                "coverage_audit": {
                    "all_source_units_reviewed": True,
                    "unresolved_count": 0,
                },
            }
            for segment in packet["segments"]
        ],
    }


def _full_boundary(text: str) -> list[dict]:
    return [
        {
            "window_id": 0,
            "chunk_index": 0,
            "owner_start": 0,
            "owner_end": len(text),
            "extract_start": 0,
            "extract_end": len(text),
        }
    ]


def _episode_for_segment(segment: dict, *, suffix: str) -> dict:
    return {
        "episode_id": f"ep_chunk_{suffix}",
        "source_name": "Chunk Fixture Podcast",
        "episode_title": "Exact source-unit representability fixture",
        "context_summary": "One segment exercises deterministic source-unit projection.",
        "speaker_map": [],
        "section_map": [
            {"section_id": "source", "segment_ids": [segment["segment_id"]]}
        ],
        "entity_seed": {},
        "concept_seed": [],
        "extraction_guidance": "Read every supplied source unit.",
        "excluded_source_context": [],
        "segments": [segment],
    }


def _assert_exact_unit_partition(
    text: str, units: list[dict], boundaries: list[dict]
) -> None:
    assert units
    assert adapter.SOURCE_UNIT_MAX_CHARS == 900
    assert adapter.SOURCE_UNIT_SAFETY_MARGIN_CHARS == 100
    assert (
        adapter.SOURCE_UNIT_MAX_CHARS
        < adapter.CANONICAL_EVIDENCE_MAX_CHARS
    )
    cursor = 0
    for unit in units:
        start = unit["start_char"]
        end = unit["end_char"]
        assert cursor <= start < end <= len(text)
        # Anything not represented as a unit is an intentional structural
        # delimiter: indentation, inter-unit whitespace, or a line ending.
        assert not text[cursor:start].strip()
        assert text[start:end] == unit["text"]
        assert unit["text"].strip()
        assert len(unit["text"]) <= adapter.SOURCE_UNIT_MAX_CHARS
        owners = [
            boundary
            for boundary in boundaries
            if boundary["owner_start"] <= start < boundary["owner_end"]
        ]
        assert len(owners) == 1
        assert unit["window_id"] == owners[0]["window_id"]
        assert end <= owners[0]["owner_end"]
        cursor = end
    assert not text[cursor:].strip()


def _unit_manifest_sha256(units: list[dict]) -> str:
    manifest = [
        {
            "unit_id": unit["unit_id"],
            "start_char": unit["start_char"],
            "end_char": unit["end_char"],
            "window_id": unit["window_id"],
            "text_sha256": sha256_text(unit["text"]),
        }
        for unit in units
    ]
    return sha256_text(adapter._canonical_json(manifest))


@pytest.mark.parametrize(
    ("length", "expected_lengths"),
    [
        (adapter.SOURCE_UNIT_MAX_CHARS - 1, [899]),
        (adapter.SOURCE_UNIT_MAX_CHARS, [900]),
        (adapter.SOURCE_UNIT_MAX_CHARS + 1, [900, 1]),
        (adapter.SOURCE_UNIT_MAX_CHARS * 2, [900, 900]),
    ],
)
def test_source_unit_character_boundaries_are_exact_stable_and_bounded(
    length: int, expected_lengths: list[int]
) -> None:
    text = "x" * length
    boundaries = _full_boundary(text)
    first = adapter._source_units(text, boundaries, segment_position=7)
    second = adapter._source_units(text, boundaries, segment_position=7)
    assert first == second
    assert [len(unit["text"]) for unit in first] == expected_lengths
    assert [unit["unit_id"] for unit in first] == [
        f"S0007U{index:04d}" for index in range(len(first))
    ]
    _assert_exact_unit_partition(text, first, boundaries)


def test_source_units_split_at_owner_windows_and_anchor_indented_lines() -> None:
    left = "a" * 950
    right = "b" * 950
    text = f"   {left}  {right}\r\n"
    left_start = 3
    left_end = left_start + len(left)
    right_start = left_end + 2
    right_end = right_start + len(right)
    boundaries = [
        {
            "window_id": 0,
            "chunk_index": 0,
            "owner_start": left_start,
            "owner_end": left_end,
            "extract_start": 0,
            "extract_end": len(text),
        },
        {
            "window_id": 1,
            "chunk_index": 1,
            "owner_start": right_start,
            "owner_end": right_end,
            "extract_start": 0,
            "extract_end": len(text),
        },
    ]
    segment = _segment("seg_indented_windows", text, density="coded")
    segment["boundaries"] = boundaries
    prepared = adapter._prepare_segment(segment, segment_position=3)
    units = prepared["units"]
    assert units[0]["start_char"] == left_start
    assert [unit["window_id"] for unit in units] == [0, 0, 1, 1]
    assert [len(unit["text"]) for unit in units] == [900, 50, 900, 50]
    _assert_exact_unit_partition(text, units, boundaries)


def test_overlapping_owner_windows_fail_before_unit_projection() -> None:
    text = "x" * 800
    boundaries = [
        {
            "window_id": 0,
            "owner_start": 0,
            "owner_end": 800,
            "extract_start": 0,
            "extract_end": 800,
        },
        {
            "window_id": 1,
            "owner_start": 500,
            "owner_end": 800,
            "extract_start": 0,
            "extract_end": 800,
        },
    ]
    with pytest.raises(
        adapter.CanonicalV31EpisodeBatchError,
        match="source unit owner windows overlap",
    ):
        adapter._source_units(text, boundaries, segment_position=0)


@pytest.mark.parametrize(
    (
        "segment_id",
        "text_chars",
        "maximum_line_chars",
        "text_sha256",
        "unit_count",
        "unit_manifest_sha256",
        "windowed_unit_count",
        "windowed_unit_manifest_sha256",
    ),
    [
        (
            "seg_97711589d307982a162be74a",
            4335,
            1146,
            "028797abc279c62a87643a307a08791651e22457db71014da377cb8f8d86f67d",
            23,
            "85390e514d6e26755493d08107655e33c7b64e9aa16a0f53d18a8f83dca06b22",
            25,
            "c7d60cd38a568892e67a3457cfcd027e2b65d5847e33773a24fd1d488f9c0732",
        ),
        (
            "seg_000089c8dcc003738ce1a95c",
            4609,
            1366,
            "98ad8d0c95986539f4f167dd29159a7c685e08459e9b1f0e373ba8932e28e901",
            25,
            "aec78702cb7d820fe5b8358bb5a8fcae97fa7d18c1ae9ebae66016c72571c0e3",
            26,
            "3cf40b7a8aa5d9909bfd011f9b2dfa3edfcc53336d66c81ff561dd0addc10aed",
        ),
        (
            "seg_0000e6f408875538416532a1",
            4383,
            1431,
            "1374b055afdce5d037fd4eb77f9915ebc216032613a5939b85c8e0e2d20d728f",
            19,
            "99b49e22f62436e7c60cc8dc7658d69bbf61d2d4e2094e92ae2719c5ed16d090",
            22,
            "d1942b405ef9067c9564bdfee189d39c3bc415d5ddfb9fe65c0d6d28aefe2036",
        ),
        (
            "seg_00176426423cdda8d1156063",
            4198,
            4198,
            "9a225c6f3f439ef2365cb39dc94a5f8f2b5e7b5d7788087c0c1510e48ec2ec1b",
            5,
            "e9379364568b2e5ff9a34e5a62214a88a27bd90233967c19a632d78797188937",
            8,
            "12d22e30b2a75c190d83bd4a08b603767dbe149f30e27269cfd6abb4ca8d4d6a",
        ),
        (
            "seg_001b0973c3debe82e0d5666e",
            4313,
            1778,
            "7a91ef7a7789e93fed3feb7242969b33c2f4db3e52e4f467358a8182e714137f",
            8,
            "8fc73f9fd8ab1951629506bf45056dcdb354d3930efc3d3b913daaacaabbeb06",
            11,
            "cae21948b4e23ff41881f909feae701cde01d23a1ddc45c4c6e15c03976ed51e",
        ),
    ],
)
def test_real_corpus_long_lines_have_stable_exact_bounded_unit_manifests(
    segment_id: str,
    text_chars: int,
    maximum_line_chars: int,
    text_sha256: str,
    unit_count: int,
    unit_manifest_sha256: str,
    windowed_unit_count: int,
    windowed_unit_manifest_sha256: str,
) -> None:
    path = adapter.PROJECT_ROOT / "corpus" / "segments" / f"{segment_id}.txt"
    assert path.is_file()
    text = path.read_text(encoding="utf-8")
    assert len(text) == text_chars
    assert max(len(line) for line in text.splitlines()) == maximum_line_chars
    assert sha256_text(text) == text_sha256
    boundaries = _full_boundary(text)
    units = adapter._source_units(text, boundaries, segment_position=0)
    assert units == adapter._source_units(text, boundaries, segment_position=0)
    assert len(units) == unit_count
    assert max(len(unit["text"]) for unit in units) <= adapter.SOURCE_UNIT_MAX_CHARS
    assert _unit_manifest_sha256(units) == unit_manifest_sha256
    _assert_exact_unit_partition(text, units, boundaries)

    _windows, windowed_boundaries = build_windowed_segment_packet(
        text, window_count=4, context_chars=900
    )
    windowed_units = adapter._source_units(
        text, windowed_boundaries, segment_position=0
    )
    assert windowed_units == adapter._source_units(
        text, windowed_boundaries, segment_position=0
    )
    assert len(windowed_units) == windowed_unit_count
    assert (
        _unit_manifest_sha256(windowed_units)
        == windowed_unit_manifest_sha256
    )
    _assert_exact_unit_partition(text, windowed_units, windowed_boundaries)


def test_real_long_line_single_unit_event_projects_and_validates() -> None:
    segment_id = "seg_00176426423cdda8d1156063"
    text = (
        adapter.PROJECT_ROOT / "corpus" / "segments" / f"{segment_id}.txt"
    ).read_text(encoding="utf-8")
    segment = _segment(segment_id, text, density="coded")
    request = adapter.prepare_episode_batches(
        _episode_for_segment(segment, suffix="real_long_line"),
        batch_size=3,
        thread_mode="new_thread",
    )[0]
    source = request["private_input"]["segments"][0]
    unit = source["units"][0]
    event = _event(unit["unit_id"], unit["unit_id"])
    event["metric"] = {
        "value": None,
        "unit": None,
        "comparator": None,
        "direction": "not_applicable",
        "raw_text": None,
    }
    output = {
        "episode_id": request["episode_id"],
        "segments": [
            _raw_segment(source, events=[event], candidates=[], status="coded")
        ],
    }
    projected = adapter.validate_and_project_output(request, output)
    label = projected["labels"][0]
    projected_event = label["discourse_events"][0]
    assert projected_event["evidence"] == unit["text"]
    assert projected_event["evidence_start"] == unit["start_char"]
    assert projected_event["evidence_end"] == unit["end_char"]
    assert len(projected_event["evidence"]) <= adapter.SOURCE_UNIT_MAX_CHARS
    validate_label_output("ai_discourse_v3_1", label, segment_text=text)


def test_multi_unit_evidence_reconstructs_exact_source_delimiters() -> None:
    text = "  alpha source text.\r\n\tbeta source text.  \n"
    segment = _segment("seg_exact_delimiters", text, density="coded")
    request = adapter.prepare_episode_batches(
        _episode_for_segment(segment, suffix="exact_delimiters"),
        batch_size=3,
        thread_mode="new_thread",
    )[0]
    source = request["private_input"]["segments"][0]
    assert len(source["units"]) == 2
    first, second = source["units"]
    event = _event(first["unit_id"], second["unit_id"])
    event["metric"] = {
        "value": None,
        "unit": None,
        "comparator": None,
        "direction": "not_applicable",
        "raw_text": None,
    }
    output = {
        "episode_id": request["episode_id"],
        "segments": [
            _raw_segment(source, events=[event], candidates=[], status="coded")
        ],
    }
    projected = adapter.validate_and_project_output(request, output)
    projected_event = projected["labels"][0]["discourse_events"][0]
    expected = text[first["start_char"] : second["end_char"]]
    assert expected == "alpha source text.\r\n\tbeta source text."
    assert projected_event["evidence"] == expected
    assert projected_event["evidence_start"] == first["start_char"]
    assert projected_event["evidence_end"] == second["end_char"]


def test_overlength_contiguous_unit_span_fails_without_truncation() -> None:
    text = "x" * (adapter.CANONICAL_EVIDENCE_MAX_CHARS + 1)
    segment = _segment("seg_overlength_span", text, density="coded")
    request = adapter.prepare_episode_batches(
        _episode_for_segment(segment, suffix="overlength_span"),
        batch_size=3,
        thread_mode="new_thread",
    )[0]
    source = request["private_input"]["segments"][0]
    assert [len(unit["text"]) for unit in source["units"]] == [900, 101]
    event = _event(
        source["units"][0]["unit_id"], source["units"][-1]["unit_id"]
    )
    event["metric"] = {
        "value": None,
        "unit": None,
        "comparator": None,
        "direction": "not_applicable",
        "raw_text": None,
    }
    output = {
        "episode_id": request["episode_id"],
        "segments": [
            _raw_segment(source, events=[event], candidates=[], status="coded")
        ],
    }
    untouched = copy.deepcopy(output)
    reconstructed = text[
        source["units"][0]["start_char"] : source["units"][-1]["end_char"]
    ]
    assert reconstructed == text
    assert len(reconstructed) == adapter.CANONICAL_EVIDENCE_MAX_CHARS + 1
    with pytest.raises(
        adapter.CanonicalV31OutputError,
        match=r"projected evidence exceeds canonical maxLength=1000",
    ):
        adapter.validate_and_project_output(request, output)
    assert output == untouched


def test_schema_contains_every_v31_semantic_field_and_no_event_cap() -> None:
    request = adapter.prepare_episode_batches(
        _episode(), batch_size=3, thread_mode="new_thread"
    )[0]
    canonical = adapter.canonical_label_schema()
    raw_segment = request["output_schema"]["properties"]["segments"]["items"]
    expected_top = set(canonical["required"]) - {
        "schema_version",
        "segment_id",
        "episode_id",
        "segment_quality",
    }
    assert expected_top <= set(raw_segment["required"])
    raw_events = raw_segment["properties"]["discourse_events"]
    canonical_events = canonical["properties"]["discourse_events"]
    assert "maxItems" not in raw_events
    assert set(raw_events["items"]["properties"]) == (
        set(canonical_events["items"]["properties"])
        - {"evidence", "evidence_start", "evidence_end"}
        | {"evidence_start_unit_id", "evidence_end_unit_id"}
    )
    for nested in ("actor", "speaker_context", "reported_actor", "source_context", "target", "metric"):
        assert raw_events["items"]["properties"][nested] == canonical_events["items"]["properties"][nested]
    assert len(adapter.model_semantic_field_paths()) == 60


def test_receipts_partition_model_semantics_from_deterministic_provenance() -> None:
    final_fields = set(adapter.canonical_final_field_paths())
    model_fields = set(adapter.model_semantic_field_paths())
    provenance_fields = set(adapter.deterministic_provenance_field_paths())
    assert model_fields.isdisjoint(provenance_fields)
    assert model_fields | provenance_fields == final_fields
    assert {
        "$.discourse_events[].evidence",
        "$.discourse_events[].evidence_start",
        "$.discourse_events[].evidence_end",
        "$.concept_candidates[].evidence",
        "$.concept_candidates[].evidence_start",
        "$.concept_candidates[].evidence_end",
        "$.segment_id",
        "$.episode_id",
        "$.segment_quality.artifact_type",
    } <= provenance_fields
    contract = adapter.semantic_integrity_contract()
    assert contract["model_emits_every_model_owned_semantic_field"] is True
    assert contract["model_selects_exact_evidence_unit_span_ownership"] is True
    assert contract["model_emits_deterministic_provenance_fields"] is False
    assert contract["raw_schema_contains_every_canonical_final_leaf"] is False
    binding = adapter.build_six_arm_matrix_binding()
    assert binding["model_semantic_field_paths"] == sorted(model_fields)
    assert binding["deterministic_provenance_field_paths"] == sorted(provenance_fields)
    assert binding["canonical_final_field_paths"] == sorted(final_fields)


def test_real_packet_projects_exact_event_and_candidate_evidence_and_validates() -> None:
    request = adapter.prepare_episode_batches(
        _episode(), batch_size=3, thread_mode="new_thread"
    )[0]
    output = _coded_and_no_signal_output(request)
    projected = adapter.validate_and_project_output(request, output)
    assert len(projected["labels"]) == 2
    coded, no_signal = projected["labels"]
    assert coded["segment_quality"] == request["private_input"]["segments"][0]["segment_quality"]
    event = coded["discourse_events"][0]
    event_unit = request["private_input"]["segments"][0]["units"][0]
    assert event["evidence"] == event_unit["text"]
    assert event["evidence_start"] == event_unit["start_char"]
    assert event["evidence_end"] == event_unit["end_char"]
    candidate = coded["concept_candidates"][0]
    candidate_unit = request["private_input"]["segments"][0]["units"][1]
    assert candidate["evidence"] == candidate_unit["text"]
    assert candidate["evidence_start"] == candidate_unit["start_char"]
    assert candidate["evidence_end"] == candidate_unit["end_char"]
    assert no_signal["extraction_status"] == "no_signal"
    assert no_signal["discourse_events"] == []
    for label, source in zip(projected["labels"], request["private_input"]["segments"]):
        validate_label_output("ai_discourse_v3_1", label, segment_text=source["segment_text"])
    assert projected["provenance"]["unit_ids_removed_from_canonical_labels"] is True
    assert "evidence_start_unit_id" not in json.dumps(projected["labels"])


def test_event_order_count_and_every_emitted_semantic_value_are_preserved() -> None:
    request = adapter.prepare_episode_batches(
        _episode(), batch_size=3, thread_mode="same_thread"
    )[0]
    output = _coded_and_no_signal_output(request, two_events=True)
    original = copy.deepcopy(output)
    projected = adapter.validate_and_project_output(request, output)
    events = projected["labels"][0]["discourse_events"]
    assert [event["event_type"] for event in events] == ["capability_claim", "adoption_signal"]
    assert projected["fidelity"]["emitted_event_count"] == 2
    assert projected["fidelity"]["projected_event_count"] == 2
    assert projected["fidelity"]["all_emitted_semantic_values_preserved"] is True
    assert projected["fidelity"]["emitted_semantics_sha256"] == projected["fidelity"]["projected_emitted_semantics_sha256"]
    assert output == original
    for raw, event in zip(original["segments"][0]["discourse_events"], events):
        for field, value in raw.items():
            if field not in {"evidence_start_unit_id", "evidence_end_unit_id"}:
                assert event[field] == value


@pytest.mark.parametrize(
    "mutate",
    [
        lambda output, _request: output["segments"][0].pop("overall_confidence"),
        lambda output, _request: output["segments"][0]["discourse_events"][0].__setitem__("claim_text", "x" * 701),
        lambda output, _request: output["segments"][0]["discourse_events"][0].__setitem__("stance", "bullish"),
        lambda output, _request: output["segments"][0]["discourse_events"][0]["metric"].__setitem__("raw_text", "25 percent"),
        lambda output, request: output["segments"][0]["discourse_events"][0].__setitem__(
            "evidence_start_unit_id", request["private_input"]["segments"][1]["units"][0]["unit_id"]
        ),
        lambda output, _request: output["segments"][0]["unit_receipts"][0].__setitem__("grounded_event_count", 0),
    ],
    ids=["missing", "overlength", "enum", "metric", "evidence", "coverage"],
)
def test_invalid_output_rejects_whole_batch_without_deletion(mutate) -> None:
    request = adapter.prepare_episode_batches(
        _episode(), batch_size=3, thread_mode="new_thread"
    )[0]
    output = _coded_and_no_signal_output(request)
    mutate(output, request)
    before = copy.deepcopy(output)
    with pytest.raises(adapter.CanonicalV31OutputError):
        adapter.validate_and_project_output(request, output)
    assert output == before


@pytest.mark.parametrize(
    ("status", "keep_event"),
    [("coded", False), ("low_signal", True), ("excluded_source_context", True)],
)
def test_coded_iff_grounded_events_remain(status: str, keep_event: bool) -> None:
    request = adapter.prepare_episode_batches(
        _episode(), batch_size=3, thread_mode="new_thread"
    )[0]
    output = _coded_and_no_signal_output(request)
    first = output["segments"][0]
    first["extraction_status"] = status
    if not keep_event:
        first["discourse_events"] = []
        first["unit_receipts"][0]["grounded_event_count"] = 0
    else:
        first["no_signal_reason"] = "A non-coded status cannot retain an event."
    with pytest.raises(adapter.CanonicalV31OutputError, match="coded iff"):
        adapter.validate_and_project_output(request, output)


def test_no_deterministic_semantic_defaults_or_pruning_contract_exists() -> None:
    contract = adapter.semantic_integrity_contract()
    assert contract["deterministic_semantic_defaults"] == {}
    for field in (
        "deterministic_semantic_pruning",
        "deterministic_sponsor_pruning",
        "deterministic_keyword_pruning",
        "deterministic_regex_pruning",
        "deterministic_truncation",
        "deterministic_padding",
        "deterministic_deduplication",
        "deterministic_relabeling",
    ):
        assert contract[field] is False
    assert "Do not omit a field and do not rely on downstream defaults" in adapter.BASE_INSTRUCTIONS_TEMPLATE


def test_corrected_six_arm_matrix_binds_full_canonical_surface() -> None:
    binding = adapter.build_six_arm_matrix_binding()
    assert binding["arm_count"] == 6
    assert {(row["batch_size"], row["thread_mode"]) for row in binding["arms"]} == {
        (size, mode) for size in (3, 5, 8) for mode in ("new_thread", "same_thread")
    }
    assert binding["canonical_label_schema"]["sha256"] == adapter.CANONICAL_LABEL_SCHEMA_SHA256
    assert binding["candidate_system_id"] == adapter.CANDIDATE_SYSTEM_ID
    assert binding["project_instruction_content_byte_budget"] == 0
    assert binding["complete_sidecar_usage_required"] is True
    assert binding["managed_chatgpt_plan_type"] == "pro"
    assert binding["instruction_source_contract"] == (
        adapter.expected_instruction_source_contract()
    )
    assert binding["instruction_source_contract_sha256"] == sha256_text(
        adapter._canonical_json(adapter.expected_instruction_source_contract())
    )
    assert "exploratory_evidence_only" in binding["epoch6_artifact_policy"]


def test_instruction_source_contract_binds_exact_paths_hash_count_and_zero_bytes() -> None:
    contract = adapter.expected_instruction_source_contract()
    assert contract["effective_instruction_source_paths"] == list(
        adapter.EXPECTED_INSTRUCTION_SOURCE_PATHS
    )
    assert contract["effective_instruction_sources_count"] == (
        adapter.EXPECTED_INSTRUCTION_SOURCES_COUNT
    )
    assert contract["effective_instruction_sources_sha256"] == (
        adapter.EXPECTED_INSTRUCTION_SOURCES_SHA256
    )
    assert contract["project_instruction_content_byte_budget"] == 0
    assert contract["project_instruction_content_included"] is False


class _FixtureClient:
    def __init__(self) -> None:
        self.account_summary = {"type": "chatgpt", "plan_type": "pro"}
        self.instruction_sources_sha256 = adapter.EXPECTED_INSTRUCTION_SOURCES_SHA256
        self.instruction_sources_count = adapter.EXPECTED_INSTRUCTION_SOURCES_COUNT
        self.sidecar_plan_type = "pro"
        self.sidecar_instruction_sources_sha256: str | None = None
        self.sidecar_instruction_sources_count: int | None = None
        self.thread_count = 0
        self.turn_count = 0
        self.model_calls = 0
        self.thread_totals: dict[str, dict[str, int]] = {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    async def start_thread(self, *, model: str, base_instructions: str, cwd: Path, ephemeral: bool):
        assert model == adapter.MODEL
        assert cwd == adapter.PROJECT_ROOT
        assert ephemeral is True
        self.thread_count += 1
        return codex_app_server.AppServerThread(
            thread_id=f"fixture-thread-{self.thread_count}",
            model=model,
            cwd=str(cwd),
            ephemeral=True,
            instruction_sources_sha256=self.instruction_sources_sha256,
            instruction_sources_count=self.instruction_sources_count,
            base_instructions_sha256=sha256_text(base_instructions),
            base_instructions_bytes=len(base_instructions.encode("utf-8")),
        )

    async def run_structured_turn(self, **kwargs):
        self.model_calls += 1
        self.turn_count += 1
        thread = kwargs["thread"]
        output = _all_no_signal_output_from_prompt(kwargs["prompt"])
        output_text = json.dumps(output, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        output_path = Path(kwargs["output_path"])
        sidecar_path = Path(kwargs["sidecar_path"])
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(output_text + "\n", encoding="utf-8")
        usage = {
            "input_tokens": 1000,
            "cached_input_tokens": 100 if self.thread_totals.get(thread.thread_id) else 0,
            "output_tokens": 200,
            "reasoning_output_tokens": 50,
            "total_tokens": 1200,
        }
        total = self.thread_totals.setdefault(
            thread.thread_id, {field: 0 for field in adapter.USAGE_FIELDS}
        )
        for field in adapter.USAGE_FIELDS:
            total[field] += usage[field]
        schema_json = adapter._canonical_json(kwargs["output_schema"])
        sidecar = {
            "schema_version": codex_app_server.TURN_SIDECAR_SCHEMA_VERSION,
            "state": "completed",
            "started_at": "2026-07-18T00:00:00+00:00",
            "finished_at": "2026-07-18T00:00:01+00:00",
            "client_version": codex_app_server.APP_SERVER_CLIENT_VERSION,
            "cli_version": codex_app_server.PINNED_CODEX_CLI_VERSION,
            "app_server_user_agent": "pif-fixture-app-server",
            "protocol_schema_sha256": adapter._sha256_file(codex_app_server.PROTOCOL_SCHEMA_PATH),
            "transport": "stdio",
            "max_message_bytes": 32 * 1024 * 1024,
            "synthetic_debug_errors": False,
            "status": "completed",
            "usage_status": "measured",
            "usage_complete": True,
            "usage": usage,
            "thread_total_usage": copy.deepcopy(total),
            "auth_type": "chatgpt",
            "plan_type": self.sidecar_plan_type,
            "thread_id": thread.thread_id,
            "turn_id": f"fixture-turn-{self.turn_count}",
            "model": thread.model,
            "effort": kwargs["effort"],
            "thread_mode": kwargs["thread_mode"],
            "batch_size": kwargs["batch_size"],
            "error_class": None,
            "wall_elapsed_seconds": 1.25,
            "prompt_sha256": sha256_text(kwargs["prompt"]),
            "prompt_bytes": len(kwargs["prompt"].encode("utf-8")),
            "base_instructions_sha256": thread.base_instructions_sha256,
            "base_instructions_bytes": thread.base_instructions_bytes,
            "instruction_sources_sha256": (
                self.sidecar_instruction_sources_sha256
                if self.sidecar_instruction_sources_sha256 is not None
                else thread.instruction_sources_sha256
            ),
            "instruction_sources_count": (
                self.sidecar_instruction_sources_count
                if self.sidecar_instruction_sources_count is not None
                else thread.instruction_sources_count
            ),
            "output_schema_sha256": sha256_text(schema_json),
            "output_schema_bytes": len(schema_json.encode("utf-8")),
            "output_sha256": sha256_text(output_text),
            "output_path": str(output_path.resolve()),
            "stderr_sha256": "0" * 64,
            "stderr_bytes": 0,
            "recovery_reran_model": False,
        }
        sidecar_path.write_text(json.dumps(sidecar, sort_keys=True) + "\n", encoding="utf-8")
        return SimpleNamespace(
            status_ok=True,
            status="completed",
            output=output,
            error_class=None,
            thread_id=thread.thread_id,
            turn_id=sidecar["turn_id"],
        )


class _FixtureFactory:
    def __init__(self) -> None:
        self.client = _FixtureClient()

    def __call__(self):
        return self.client


@pytest.mark.parametrize(
    ("thread_mode", "expected_threads", "expected_cached"),
    [("new_thread", 3, 0), ("same_thread", 1, 200)],
)
def test_offline_run_has_explicit_new_same_lifecycle_and_complete_sidecars(
    tmp_path: Path, thread_mode: str, expected_threads: int, expected_cached: int
) -> None:
    factory = _FixtureFactory()
    report = asyncio.run(
        adapter.run_episode_batch_arm(
            [_episode(segment_count=7)],
            output_dir=tmp_path / thread_mode,
            batch_size=3,
            thread_mode=thread_mode,
            client_factory=factory,
        )
    )
    assert report["state"] == "passed"
    assert report["requested_calls"] == 3
    assert report["attempted_calls"] == 3
    assert report["validated_calls"] == 3
    assert report["usage_status"] == "complete"
    assert report["cached_input_tokens"] == expected_cached
    assert report["reasoning_output_tokens"] == 150
    assert report["observed_thread_count"] == expected_threads
    assert report["observed_turn_count"] == 3
    assert report["thread_lineage_valid"] is True
    assert report["turn_ids_unique"] is True
    assert report["retry_count"] == 0
    assert report["ambiguous_retry_count"] == 0
    assert report["managed_chatgpt_plan_type"] == "pro"
    assert report["instruction_source_contract"] == (
        adapter.expected_instruction_source_contract()
    )
    assert factory.client.thread_count == expected_threads
    assert factory.client.model_calls == 3
    assert len(list((tmp_path / thread_mode).glob("batches/*/sidecar.json"))) == 3
    run_spec = json.loads((tmp_path / thread_mode / "run-spec.json").read_text())
    assert run_spec["managed_chatgpt_plan_type"] == "pro"
    assert run_spec["instruction_source_contract"] == (
        adapter.expected_instruction_source_contract()
    )
    for path in (tmp_path / thread_mode).glob("batches/*/semantic-call-attempt.json"):
        preflight = json.loads(path.read_text())["preflight"]
        assert preflight["instruction_sources_sha256"] == (
            adapter.EXPECTED_INSTRUCTION_SOURCES_SHA256
        )
        assert preflight["instruction_sources_count"] == (
            adapter.EXPECTED_INSTRUCTION_SOURCES_COUNT
        )


@pytest.mark.parametrize(
    ("attribute", "value"),
    [
        ("instruction_sources_sha256", "f" * 64),
        ("instruction_sources_count", 2),
    ],
)
def test_started_thread_instruction_source_drift_fails_before_turn(
    tmp_path: Path, attribute: str, value: object
) -> None:
    factory = _FixtureFactory()
    setattr(factory.client, attribute, value)
    report = asyncio.run(
        adapter.run_episode_batch_arm(
            [_episode()],
            output_dir=tmp_path / attribute,
            batch_size=3,
            thread_mode="new_thread",
            client_factory=factory,
        )
    )
    assert report["state"] == "not_eligible"
    assert report["failed_calls"] == 1
    assert report["attempted_calls"] == 0
    assert factory.client.model_calls == 0


def test_non_pro_account_fails_before_thread_or_turn(tmp_path: Path) -> None:
    factory = _FixtureFactory()
    factory.client.account_summary = {"type": "chatgpt", "plan_type": "plus"}
    with pytest.raises(codex_app_server.AppServerAuthError, match="Pro"):
        asyncio.run(
            adapter.run_episode_batch_arm(
                [_episode()],
                output_dir=tmp_path / "non-pro",
                batch_size=3,
                thread_mode="new_thread",
                client_factory=factory,
            )
        )
    assert factory.client.thread_count == 0
    assert factory.client.model_calls == 0


@pytest.mark.parametrize(
    ("attribute", "value"),
    [
        ("sidecar_plan_type", "plus"),
        ("sidecar_instruction_sources_sha256", "e" * 64),
        ("sidecar_instruction_sources_count", 2),
    ],
)
def test_sidecar_auth_or_instruction_source_drift_fails_closed(
    tmp_path: Path, attribute: str, value: object
) -> None:
    factory = _FixtureFactory()
    setattr(factory.client, attribute, value)
    report = asyncio.run(
        adapter.run_episode_batch_arm(
            [_episode()],
            output_dir=tmp_path / attribute,
            batch_size=3,
            thread_mode="new_thread",
            client_factory=factory,
        )
    )
    assert report["state"] == "not_eligible"
    assert report["failed_calls"] == 1
    assert report["validated_calls"] == 0
    assert factory.client.model_calls == 1


def test_nonzero_ambient_project_instruction_budget_is_rejected(monkeypatch) -> None:
    monkeypatch.setitem(adapter.CONTEXT_CONTROL_OVERLAY, "project_doc_max_bytes", 1)
    with pytest.raises(adapter.CanonicalV31EpisodeBatchError, match="zero-byte"):
        adapter.verified_context_control_overlay()
    with pytest.raises(adapter.CanonicalV31EpisodeBatchError):
        adapter.expected_instruction_source_contract()


def test_zero_byte_overlay_is_bound_to_official_persistent_client() -> None:
    overlay = adapter.verified_context_control_overlay()
    assert overlay["project_doc_max_bytes"] == 0
    assert overlay["include_environment_context"] is False
    assert overlay["include_permissions_instructions"] is False
    client = adapter._client_factory()
    assert isinstance(client, adapter.CanonicalV31CodexAppServerClient)
    assert client.command == [
        str(adapter.PINNED_CODEX),
        "app-server",
        "--stdio",
        "--strict-config",
    ]
    assert client._canonical_v31_config_overlay == overlay
