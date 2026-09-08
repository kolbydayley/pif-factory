"""Verify actual independent review bytes, not a dispatch-success assertion."""
import hashlib
import json
from .signal_desk_rubric_reference_packets import digest


def verify(directory, packet, *, system, validator):
    sha=packet['packet_sha256'];h=lambda s:hashlib.sha256(s.encode()).hexdigest()
    if digest({k:v for k,v in packet.items() if k!='packet_sha256'})!=sha:
        raise ValueError('review packet hash changed')
    side=json.loads((directory/f'{sha}.sidecar.json').read_text())
    if (side.get('state')!='completed' or side.get('error_class') or side.get('model')!='gpt-5.5'
        or side.get('effort')!='high' or side.get('base_instructions_sha256')!=h(system)
        or side.get('prompt_sha256')!=h(json.dumps(packet,ensure_ascii=False))):
        raise ValueError('actual review provider/request mismatch')
    if json.loads((directory/f'{sha}.packet.json').read_text())!=packet:
        raise ValueError('saved review packet changed')
    value=json.loads((directory/f'{sha}.review.json').read_text())
    if json.loads((directory/f'{sha}.output.json').read_text())!=value:
        raise ValueError('review output projection changed')
    validator(value,packet)
    return value,{'packet_sha256':sha,'sidecar_sha256':digest(side),'review_sha256':digest(value)}
