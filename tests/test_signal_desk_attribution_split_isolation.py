import json

from research_factory.signal_desk_attribution_gate import audit_gold_c, repair_gold_c


def test_development_audit_never_opens_protected_text_or_outputs(tmp_path):
    root = tmp_path / "results"
    (root / "C").mkdir(parents=True)
    windows = []
    for split in ("validation", "sealed_holdout"):
        windows.append({"window_id": split, "split": split,
                        "transcript_path": "must-not-open.txt", "start_char": 0, "end_char": 10})
        # Invalid JSON catches an accidental protected-output read too.
        (root / "C" / f"{split}.json").write_text("must not parse")
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"windows": windows}))
    audit_gold_c(manifest_path=manifest, result_root=root, project_root=tmp_path)
    repair_gold_c(manifest_path=manifest, result_root=root,
                  output_root=tmp_path / "repaired", project_root=tmp_path)
    assert not list((tmp_path / "repaired" / "C").glob("*.json"))


def test_partial_audit_checks_output_presence_before_source_access(tmp_path):
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"windows": [{"window_id": "absent", "split": "development",
                                                "transcript_path": "absent.txt"}]}))
    audit_gold_c(manifest_path=manifest, result_root=tmp_path / "results", project_root=tmp_path)
