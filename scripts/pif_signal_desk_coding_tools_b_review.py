"""Full-source diagnosis of all fourteen held B records; no automatic repairs."""
import argparse
import asyncio
import fcntl
import json
from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import pif_signal_desk_lineage_qualification as run
from scripts.pif_signal_desk_attribution_probe_review import execute
from research_factory.signal_desk_full_event_v4_review import packets
from research_factory.signal_desk_rubric_reference_packets import digest

WID = 'sdw_4117ea30ae4ec3f116ef'
RAW = '963f113a43b839e6907ace5164e94c878c7c67c0a5427156fe307a019957670a'
PACKET = '1b3bb17af4f5e75fa64758cd84d9616649a3b6127ec7c460c8ff44cdd8dd8a00'
OUT = run.OUT / 'coding-tools-b-source-diagnosis-v1'
SYSTEM = run.previous.review.SYSTEM + """\nHELD B SOURCE DIAGNOSIS:
These fourteen records are unaccepted. Diagnose each assigned record independently
against the complete transcript, not another author's repaired answer. Some quoted
text inserts punctuation absent from the source, and many supplied offsets drift.
Never treat the candidate's reconstructed source text as authoritative. Verify the
actual supplied transcript. Separate exact-source defects from semantic defects.
Do not add currencies that the source does not name, infer a company name from
garbled ASR, overstate supplier relationships, or merge Shawn/Swyx using external
knowledge. Preserve hedges and disagreements. Distinguish intelligible hearsay
with uncertain external accuracy from unintelligible source requiring recovery.
The current evidence-role validator only allows non-none needs on a quarantined
research_limitation; flag a genuine contract defect rather than guessing content
to satisfy it. It also lacks a question speech-act enum: report that limitation
if an inquiry is incorrectly labeled as an assertion. Do not conceal errors by
dropping candidates or granting blanket quarantine. All fourteen original records
remain in scope. Specify source-grounded corrections or unresolved limitations.
This diagnostic does not approve a proposed correction or accept gold.
"""


def prepare(*, write=True):
    import tiktoken
    plan = json.loads((run.OUT / 'plan.json').read_text())
    source = json.loads((run.previous.BASE / f"{plan['source_packets'][WID]}.packet.json").read_text())
    p = run.packet(source, 'B', {})
    directory = run.OUT / 'calls' / WID / 'B'
    side = run.verify_provider(directory, p)
    if p['packet_sha256'] != PACKET:
        raise ValueError('original B request changed')
    raw = json.loads((directory / f'{PACKET}.output.json').read_text())
    def inspected(value, *, source, window_id):
        if window_id != WID or source != p['transcript_window'] or digest(value) != RAW or len(value['events']) != 14:
            raise ValueError('not the inspected held B')
    enc = tiktoken.get_encoding('o200k_base')
    ps = packets(raw, source=p['transcript_window'], window_id=WID,
        token_count=lambda s: len(enc.encode(s)), system=SYSTEM,
        schema_for_packet=run.previous.review.schema, output_validator=inspected)
    if [e['event_id'] for q in ps for e in q['candidates']] != [e['event_id'] for e in raw['events']]:
        raise ValueError('diagnostic population changed')
    if write:
        OUT.mkdir(parents=True, exist_ok=True, mode=0o700)
        for q in ps:
            run.immutable_json(OUT / f"{q['packet_sha256']}.packet.json", q)
        run.immutable_json(OUT / 'plan.json', {'packets': [q['packet_sha256'] for q in ps],
            'source_packet_sha256': PACKET, 'original_output_sha256': RAW, 'sidecar_sha256': digest(side),
            'records': 14, 'diagnosis_only': True, 'applied': False, 'gold_accepted': False})
    return ps


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    with (run.previous.OUT / 'runner.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        ps = prepare()
        if args.execute:
            review = run.previous.review
            raise SystemExit(asyncio.run(execute(ps, output_root=OUT, task_prefix='coding-tools-b-source-diagnosis-v1',
                system=SYSTEM, schema_for_packet=review.schema, validator=review.validate_review,
                packet_id=lambda p: p['packet_sha256'], verdict_rows=lambda v: v['decisions'])))


if __name__ == '__main__':
    main()
