from __future__ import annotations

import asyncio
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from research_factory.app_server_capacity import AppServerCapacityError
from research_factory.app_server_capacity_reserve import (
    RESERVE_CAPACITY_CHECKPOINT_VERSION,
    RESERVE_CAPACITY_POLICY_VERSION,
    ReserveCapacityError,
    ReserveCapacityGatedCodexAppServerClient,
    evaluate_reserve_capacity,
    load_reserve_capacity_policy,
)


def _rate_limit(used: int, reached=None) -> dict:
    return {
        "rateLimitsByLimitId": {
            "codex": {
                "limitId": "codex",
                "primary": {"usedPercent": used, "resetsAt": 9999999999},
                "rateLimitReachedType": reached,
            }
        }
    }


def _write_policy(root: Path, *, turns=2, quota_rate=10, maximum_tokens=100000) -> Path:
    audit_path = root.parent / "audit.json"
    audit_path.write_text(
        json.dumps(
            {
                "schema_version": "pif_app_server_capacity_policy_audit_v20",
                "production_mutation_performed": False,
            }
        ),
        encoding="utf-8",
    )
    audit_bytes = audit_path.read_bytes()
    policy = {
        "schema_version": RESERVE_CAPACITY_POLICY_VERSION,
        "phase_id": "test_phase",
        "semantic_output_root": str(root.resolve()),
        "ordered_turn_names": ["turn_%02d" % index for index in range(turns)],
        "minimum_remaining_reserve_percent": 20,
        "quota_points_per_million_tokens": quota_rate,
        "maximum_total_tokens_per_turn": maximum_tokens,
        "phase_total_token_bound": turns * maximum_tokens,
        "projected_phase_quota_points": (turns * maximum_tokens * quota_rate + 999999)
        // 1000000,
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "rate_limit_reached_type_must_be_null": True,
        "retry_count_per_turn": 0,
        "unknown_usage_hard_stop": True,
        "production_mutation_allowed": False,
        "audit": {
            "path": str(audit_path.resolve()),
            "sha256": hashlib.sha256(audit_bytes).hexdigest(),
            "size_bytes": len(audit_bytes),
        },
    }
    path = root.parent / "policy.json"
    path.write_text(json.dumps(policy), encoding="utf-8")
    return path


class FakeInner:
    def __init__(self, responses, *, totals=None):
        self.account_summary = {"type": "chatgpt", "plan_type": "pro"}
        self.responses = list(responses)
        self.totals = list(totals or [50000] * len(responses))
        self.probe_calls = 0
        self.run_calls = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    async def _request(self, method, params):
        self.probe_calls += 1
        if method != "account/rateLimits/read" or params != {}:
            raise AssertionError("unexpected probe")
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response

    async def run_ephemeral_structured_turn(self, **kwargs):
        self.run_calls += 1
        total = self.totals.pop(0)
        sidecar = Path(kwargs["sidecar_path"])
        output = Path(kwargs["output_path"])
        sidecar.parent.mkdir(parents=True, exist_ok=True)
        usage = None if total is None else {"total_tokens": total}
        sidecar.write_text(
            json.dumps(
                {
                    "state": "completed" if total is not None else "failed",
                    "status": "completed" if total is not None else "failed",
                    "usage_complete": total is not None,
                    "usage_status": "measured" if total is not None else "unknown",
                    "usage": usage,
                }
            ),
            encoding="utf-8",
        )
        output.write_text("{}", encoding="utf-8")
        return SimpleNamespace(
            status_ok=total is not None,
            usage=(SimpleNamespace(total_tokens=total) if total is not None else None),
        )


class ReserveCapacityArithmeticTests(unittest.TestCase):
    def test_used_and_remaining_semantics_preserve_twenty_point_reserve(self):
        policy = {
            "ordered_turn_names": ["a"] * 12,
            "minimum_remaining_reserve_percent": 20,
            "maximum_total_tokens_per_turn": 102000,
            "quota_points_per_million_tokens": 17,
        }
        clear = evaluate_reserve_capacity(
            {"primary_used_percent": 29, "rate_limit_reached_type": None},
            policy=policy,
            remaining_turn_count=12,
        )
        self.assertTrue(clear["cleared_for_semantic_turn"])
        self.assertEqual(clear["projected_remaining_quota_points"], 21)
        self.assertEqual(clear["projected_terminal_remaining_percent"], 50)
        blocked = evaluate_reserve_capacity(
            {"primary_used_percent": 60, "rate_limit_reached_type": None},
            policy=policy,
            remaining_turn_count=12,
        )
        self.assertFalse(blocked["cleared_for_semantic_turn"])

    def test_provider_reached_type_always_blocks(self):
        policy = {
            "ordered_turn_names": ["a"],
            "minimum_remaining_reserve_percent": 20,
            "maximum_total_tokens_per_turn": 1000,
            "quota_points_per_million_tokens": 1,
        }
        result = evaluate_reserve_capacity(
            {
                "primary_used_percent": 0,
                "rate_limit_reached_type": "rate_limit_reached",
            },
            policy=policy,
            remaining_turn_count=1,
        )
        self.assertFalse(result["cleared_for_semantic_turn"])

    def test_policy_loader_rejects_embedded_audit_drift(self):
        with tempfile.TemporaryDirectory() as directory:
            semantic = Path(directory) / "semantic"
            policy = _write_policy(semantic)
            load_reserve_capacity_policy(policy)
            (Path(directory) / "audit.json").write_text(
                '{"schema_version":"changed"}', encoding="utf-8"
            )
            with self.assertRaisesRegex(ReserveCapacityError, "audit record drifted"):
                load_reserve_capacity_policy(policy)


class ReserveCapacityClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_no_semantic_thread_or_turn_before_clearance(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            semantic = base / "semantic"
            policy = _write_policy(semantic)
            inner = FakeInner([_rate_limit(79)])
            client = ReserveCapacityGatedCodexAppServerClient(
                policy_path=policy, inner_factory=lambda: inner
            )
            checkpoint = semantic / "turns/turn-00/capacity.json"
            async with client:
                with self.assertRaises(ReserveCapacityError):
                    await client.run_ephemeral_structured_turn(
                        capacity_checkpoint_path=checkpoint,
                        sidecar_path=checkpoint.with_name("sidecar.json"),
                        output_path=checkpoint.with_name("output.private.json"),
                    )
            self.assertEqual(inner.probe_calls, 1)
            self.assertEqual(inner.run_calls, 0)
            self.assertFalse(checkpoint.exists())

    async def test_reprobes_each_turn_and_stops_when_reserve_is_lost(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            semantic = base / "semantic"
            policy = _write_policy(semantic)
            inner = FakeInner([_rate_limit(29), _rate_limit(80)])
            client = ReserveCapacityGatedCodexAppServerClient(
                policy_path=policy, inner_factory=lambda: inner
            )
            first = semantic / "turns/turn-00/capacity.json"
            second = semantic / "turns/turn-01/capacity.json"
            async with client:
                await client.run_ephemeral_structured_turn(
                    capacity_checkpoint_path=first,
                    sidecar_path=first.with_name("sidecar.json"),
                    output_path=first.with_name("output.private.json"),
                )
                with self.assertRaises(ReserveCapacityError):
                    await client.run_ephemeral_structured_turn(
                        capacity_checkpoint_path=second,
                        sidecar_path=second.with_name("sidecar.json"),
                        output_path=second.with_name("output.private.json"),
                    )
            self.assertEqual(inner.probe_calls, 2)
            self.assertEqual(inner.run_calls, 1)
            checkpoint = json.loads(first.read_text(encoding="utf-8"))
            self.assertEqual(
                checkpoint["schema_version"], RESERVE_CAPACITY_CHECKPOINT_VERSION
            )
            self.assertFalse(second.exists())

    async def test_probe_exception_is_sticky_without_second_request(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            semantic = base / "semantic"
            policy = _write_policy(semantic)
            inner = FakeInner([RuntimeError("request failed"), _rate_limit(0)])
            client = ReserveCapacityGatedCodexAppServerClient(
                policy_path=policy, inner_factory=lambda: inner
            )
            async with client:
                with self.assertRaisesRegex(RuntimeError, "request failed"):
                    await client.wait_for_semantic_capacity()
                with self.assertRaises(ReserveCapacityError):
                    await client.wait_for_semantic_capacity()
            self.assertEqual(inner.probe_calls, 1)
            self.assertEqual(inner.run_calls, 0)

    async def test_malformed_probe_is_sticky_without_second_request(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            semantic = base / "semantic"
            policy = _write_policy(semantic)
            inner = FakeInner([{}, _rate_limit(0)])
            client = ReserveCapacityGatedCodexAppServerClient(
                policy_path=policy, inner_factory=lambda: inner
            )
            async with client:
                with self.assertRaises(AppServerCapacityError):
                    await client.wait_for_semantic_capacity()
                with self.assertRaises(ReserveCapacityError):
                    await client.wait_for_semantic_capacity()
            self.assertEqual(inner.probe_calls, 1)
            self.assertEqual(inner.run_calls, 0)

    async def test_cancelled_probe_is_sticky_without_second_request(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            semantic = base / "semantic"
            policy = _write_policy(semantic)
            inner = FakeInner([asyncio.CancelledError(), _rate_limit(0)])
            client = ReserveCapacityGatedCodexAppServerClient(
                policy_path=policy, inner_factory=lambda: inner
            )
            async with client:
                with self.assertRaises(asyncio.CancelledError):
                    await client.wait_for_semantic_capacity()
                with self.assertRaises(ReserveCapacityError):
                    await client.wait_for_semantic_capacity()
            self.assertEqual(inner.probe_calls, 1)
            self.assertEqual(inner.run_calls, 0)

    async def test_existing_checkpoint_reuse_is_rejected_before_probe(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            semantic = base / "semantic"
            policy = _write_policy(semantic)
            inner = FakeInner([_rate_limit(0)])
            checkpoint = semantic / "turns/turn-00/capacity.json"
            checkpoint.parent.mkdir(parents=True, exist_ok=True)
            checkpoint.write_text("{}", encoding="utf-8")
            client = ReserveCapacityGatedCodexAppServerClient(
                policy_path=policy, inner_factory=lambda: inner
            )
            async with client:
                with self.assertRaisesRegex(ReserveCapacityError, "retry is prohibited"):
                    await client.run_ephemeral_structured_turn(
                        capacity_checkpoint_path=checkpoint,
                        sidecar_path=checkpoint.with_name("sidecar.json"),
                        output_path=checkpoint.with_name("output.private.json"),
                    )
            self.assertEqual(inner.probe_calls, 0)
            self.assertEqual(inner.run_calls, 0)

    async def test_skipped_prior_turn_is_rejected_before_probe(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            semantic = base / "semantic"
            policy = _write_policy(semantic)
            inner = FakeInner([_rate_limit(0)])
            checkpoint = semantic / "turns/turn-01/capacity.json"
            client = ReserveCapacityGatedCodexAppServerClient(
                policy_path=policy, inner_factory=lambda: inner
            )
            async with client:
                with self.assertRaisesRegex(
                    ReserveCapacityError, "prior frozen turn"
                ):
                    await client.run_ephemeral_structured_turn(
                        capacity_checkpoint_path=checkpoint,
                        sidecar_path=checkpoint.with_name("sidecar.json"),
                        output_path=checkpoint.with_name("output.private.json"),
                    )
            self.assertEqual(inner.probe_calls, 0)
            self.assertEqual(inner.run_calls, 0)

    async def test_unknown_usage_hard_stops_without_second_probe(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            semantic = base / "semantic"
            policy = _write_policy(semantic)
            inner = FakeInner([_rate_limit(29), _rate_limit(29)], totals=[None, 10])
            client = ReserveCapacityGatedCodexAppServerClient(
                policy_path=policy, inner_factory=lambda: inner
            )
            first = semantic / "turns/turn-00/capacity.json"
            second = semantic / "turns/turn-01/capacity.json"
            async with client:
                with self.assertRaisesRegex(ReserveCapacityError, "unknown usage"):
                    await client.run_ephemeral_structured_turn(
                        capacity_checkpoint_path=first,
                        sidecar_path=first.with_name("sidecar.json"),
                        output_path=first.with_name("output.private.json"),
                    )
                with self.assertRaises(ReserveCapacityError):
                    await client.run_ephemeral_structured_turn(
                        capacity_checkpoint_path=second,
                        sidecar_path=second.with_name("sidecar.json"),
                        output_path=second.with_name("output.private.json"),
                    )
            self.assertEqual(inner.probe_calls, 1)
            self.assertEqual(inner.run_calls, 1)
