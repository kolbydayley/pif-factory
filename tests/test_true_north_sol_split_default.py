from __future__ import annotations

from research_factory import true_north_sol_split_default as probe


def test_pack_preserves_19_packets_in_16_envelopes() -> None:
    source = [
        {
            "packet_key": (
                f"episode-a/segment-{index:02d}"
                if index < 10
                else f"episode-b/segment-{index:02d}"
            ),
            "packet_sha256": str(index),
            "candidate_count": (index % 5) + 1,
            "packet": {"index": index},
        }
        for index in range(19)
    ]

    packed = probe.pack_provider_envelopes(source)

    assert len(packed) == 16
    assert sum(len(envelope) == 2 for envelope in packed) == 3
    assert sorted(
        row["packet_key"] for envelope in packed for row in envelope
    ) == sorted(row["packet_key"] for row in source)
    assert all(
        len(
            {
                row["packet_key"].split("/", 1)[0]
                for row in envelope
            }
        )
        == 1
        for envelope in packed
    )


def test_reserved_whole_run_fits_declared_ceiling() -> None:
    assert (
        probe.MAX_CALLS * probe.RESERVED_TOKENS_PER_CALL
        <= probe.MAX_TOKENS
    )
    assert probe.MODEL == "gpt-5.6-sol"
    assert probe.MODEL_LANE == "codex_subscription_ephemeral"


def test_atomic_error_directions_use_gold_range() -> None:
    consensus = {
        "items": [
            {
                "candidate_id": "ok",
                "minimum_atomic_count": 1,
                "maximum_atomic_count": 2,
            },
            {
                "candidate_id": "under",
                "minimum_atomic_count": 2,
                "maximum_atomic_count": 3,
            },
            {
                "candidate_id": "over",
                "minimum_atomic_count": 1,
                "maximum_atomic_count": 1,
            },
        ]
    }
    predictions = {
        "ok": {"atomic_claims": [{}, {}]},
        "under": {"atomic_claims": [{}]},
        "over": {"atomic_claims": [{}, {}]},
    }

    assert probe._atomic_error_directions(
        consensus=consensus,
        predictions=predictions,
        candidate_ids=set(predictions),
    ) == {"acceptable": 1, "under": 1, "over": 1}
