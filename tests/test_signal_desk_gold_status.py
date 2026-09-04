import json

from research_factory.signal_desk_gold_status import resolve_gold_campaign_status


def test_status_prefers_final_split_receipts_over_stale_ephemeral_status(tmp_path):
    paths = {
        "development": tmp_path / "results/development/gold-development-receipt.json",
        "validation": tmp_path / "sealed-gold-results/validation/gold-validation-receipt.json",
        "sealed_holdout": tmp_path / "sealed-gold-results/sealed_holdout/gold-sealed_holdout-receipt.json",
    }
    for split, path in paths.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({
            "complete": True, "split_windows": 1, "quarantined_windows": 0,
            "receipt_sha256": split,
        }))
    stale = tmp_path / "sealed-gold-results/validation/gold-validation-status.json"
    stale.write_text(json.dumps({"status": "deferred", "complete": False}))
    status = resolve_gold_campaign_status(tmp_path)
    assert status["complete"] is True
    assert status["status"] == "complete"
    assert status["supersedes_ephemeral_split_status_files"] is True
