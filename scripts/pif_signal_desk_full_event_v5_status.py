#!/usr/bin/env python3
"""Read-only v5 progress, keeping context separate from claims and voices."""
import argparse
import json
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from scripts.pif_signal_desk_full_event_v4_status import summarize as base_summary
from scripts.pif_signal_desk_full_event_v5_run import OUT,BASE,contract


def summarize(root=OUT,base=BASE):
    # The bundle conversion reads the unchanged dimensions only; v5 validates
    # context separately before conversion. Context is never a voice candidate.
    return base_summary(root,base,output_validator=contract.validate)


if __name__=="__main__":
    parser=argparse.ArgumentParser();parser.add_argument("--calls",action="store_true");args=parser.parse_args()
    result=summarize()
    if not args.calls:result.pop("calls")
    print(json.dumps(result,sort_keys=True))
