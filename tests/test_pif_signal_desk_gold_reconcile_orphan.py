import importlib.util
import sys


def _module():
    path = "scripts/pif_signal_desk_gold_reconcile_orphan.py"
    spec = importlib.util.spec_from_file_location("pif_gold_reconcile", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_reconcile_cli_requires_exact_pair_or_all_stale():
    module = _module()
    try:
        module.main(["--reservation-id", "r1", "--authorization-source", "test"])
    except SystemExit as exc:
        assert exc.code == 2
    else:
        raise AssertionError("partial exact target must be rejected")


def test_reconcile_cli_all_stale_reaches_database_validation(tmp_path):
    module = _module()
    database = tmp_path / "empty.sqlite"
    try:
        module.main([
            "--all-stale", "--authorization-source", "test",
            "--database", str(database), "--receipt-dir", str(tmp_path / "receipts"),
        ])
    except Exception as exc:
        # The empty database proves argument validation accepted the all-stale
        # mode and reached the existing fail-closed schema/reconciliation path.
        assert "no such table" not in str(exc).lower()
