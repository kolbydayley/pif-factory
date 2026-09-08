import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from research_factory.signal_desk_omission_review import packets
from research_factory.signal_desk_omission_recovery import repair_packets
from scripts import pif_signal_desk_omission_recovery as runner


class RunnerTests(unittest.TestCase):
    def test_collect_waits_then_recomputes(self):
        p = packets(window_id="dev", source="Demand fell.", structure="flattened", cases=[{"case_id":"x","kind":"candidate"}],
            current=[], provenance={}, token_count=lambda _:1)[0]
        row = {"case_id":"x","verdict":"supported","utility":"consequential","coverage":"missing",
            "current_C_ids":[],"correction":"not_applicable","source_quotes":["Demand declined."],"rationale":"Explicit demand decline."}
        raw = {"decisions":[row]}; part, selected = repair_packets(p, raw, token_count=lambda _:1)
        plan = {"inventory":{p["packet_sha256"]:{"partition":part,"packets":[selected[0]["packet_sha256"]]}}}
        with tempfile.TemporaryDirectory() as tmp:
            original = Path(tmp)/"original"; out = Path(tmp)/"recovery"; original.mkdir(); out.mkdir()
            (original/f"{p['packet_sha256']}.output.json").write_text(json.dumps(raw))
            with patch.object(runner,"ORIGINAL",original), patch.object(runner,"OUT",out):
                outputs, receipts, unresolved = runner.collect([p],selected,plan)
                self.assertEqual(outputs,{})
                self.assertEqual(unresolved,[p["packet_sha256"]])
                fixed = {"decisions":[dict(row,source_quotes=["Demand fell."])]}
                (out/f"{selected[0]['packet_sha256']}.review.json").write_text(json.dumps(fixed))
                outputs, receipts, unresolved = runner.collect([p],selected,plan,write=True)
                self.assertEqual(outputs[p["packet_sha256"]],fixed)
                self.assertEqual(unresolved,[])
                self.assertFalse(receipts[p["packet_sha256"]]["gold_accepted"])
                # A saved assembled output cannot override independently replayed repairs.
                (out/"reconciled"/f"{p['packet_sha256']}.review.json").write_text('{}')
                self.assertEqual(runner.collect([p],selected,plan)[0][p["packet_sha256"]],fixed)
                (original/f"{p['packet_sha256']}.output.json").write_text(json.dumps(fixed))
                with self.assertRaises(ValueError): runner.collect([p],selected,plan)


if __name__ == "__main__": unittest.main()
