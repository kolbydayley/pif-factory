#!/usr/bin/env python3
"""Write the canonical Gold-authoring campaign status."""

import json
import sys
from pathlib import Path

PIF_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIF_ROOT))

from research_factory.signal_desk_gold_status import resolve_gold_campaign_status
from research_factory.util import write_text_atomic


def main() -> int:
    root = PIF_ROOT / "work/signal-desk-rebuild/gold-authoring-v2"
    status = resolve_gold_campaign_status(root)
    output = root / "artifacts/gold-campaign-status.json"
    write_text_atomic(output, json.dumps(status, indent=2, sort_keys=True) + "\n")
    print(json.dumps(status, indent=2, sort_keys=True))
    return 0 if status["complete"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
