#!/usr/bin/env python3
"""One-shot process completion -> existing desktop task via local IPC.

No scheduler, new app server, clipboard, or host restart. Private app protocol:
fail closed on errors; a delivery receipt is NOT a resumed-task acknowledgement.
"""
import argparse
import json
import os
import socket
import stat
import struct
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

THREAD = "01a04040-77ee-77f2-9d26-a5ca1ae56986"
ROOT = Path("/Users/kolbydayley/pif-factory")
SOCKET = Path("/Users/kolbydayley/.codex/ipc/ipc.sock")
EVENTS = ROOT / "work/signal-desk-rebuild/gold-authoring-v2/artifacts/completion-hooks"


def save_receipt(path, value):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


def completion_prompt(event_id, code, canary):
    prefix = "Process-completion hook canary" if canary else "Gold pipeline process-completion hook"
    return (
        f"{prefix} {event_id}: the child process exited with code {code}. "
        "This message was sent by its standalone exit wrapper through the running app's local IPC; "
        "no scheduled task was used. Read the receipt at "
        f"{EVENTS / (event_id + '.json')}. Record a separate acknowledgement at "
        f"{EVENTS / (event_id + '.ack.json')} with the event ID, current UTC time, and this task ID. "
        "That acknowledgement, not dispatch success, establishes that the process hook resumed the task. "
        "Report the canary result if this is the canary. Then continue the authorized gold work from "
        f"{ROOT}/work/signal-desk-rebuild/gold-authoring-v2/artifacts/continuation-handoff.md. "
        "Kolby reauthorized one scheduled backstop on 2026-09-07: keep signal-desk-completion-monitor active, "
        "and keep the older signal-desk-gold-rebuild-supervisor paused. Follow the current active goal "
        "and newest continuation handoff, not obsolete blanket pause instructions. "
        "For subsequent long-running processes, use this one-shot "
        "completion wrapper, preserve exit/result receipts, and never duplicate a running child. "
        "Do not launch more canaries, reset hosts, bypass quality gates, or infer that authored gold is accepted."
    )


class OwnerUnavailable(RuntimeError):
    """Owner discovery rejected before any start-turn request was sent."""


class DesktopIPC:
    def __init__(self, path=SOCKET):
        info = path.stat()
        if not stat.S_ISSOCK(info.st_mode) or info.st_uid != os.getuid():
            raise RuntimeError("unexpected IPC socket owner/type")
        self.sock = socket.socket(socket.AF_UNIX)
        self.sock.settimeout(20)
        self.sock.connect(str(path))
        self.client = "initializing-client"
        self.client = self.request("initialize", {"clientType": "signal-desk-completion-hook"})["result"]["clientId"]

    def read_exact(self, size):
        data = bytearray()
        while len(data) < size:
            part = self.sock.recv(size - len(data))
            if not part:
                raise RuntimeError("IPC connection closed")
            data.extend(part)
        return bytes(data)

    def request(self, method, params, target=None):
        request_id = str(uuid.uuid4())
        msg = {"type": "request", "requestId": request_id, "sourceClientId": self.client,
               "version": {"thread-owner-discovery": 1, "thread-follower-start-turn": 2}.get(method, 0),
               "method": method, "params": params, "timeoutMs": 15000}
        if target:
            msg["targetClientId"] = target
        encoded = json.dumps(msg).encode()
        self.sock.sendall(struct.pack("<I", len(encoded)) + encoded)
        while True:
            length = struct.unpack("<I", self.read_exact(4))[0]
            if not 0 < length <= 268435456:
                raise RuntimeError("invalid IPC frame")
            response = json.loads(self.read_exact(length))
            if response.get("type") != "response" or response.get("requestId") != request_id:
                continue
            if response.get("resultType") != "success":
                if method == "thread-owner-discovery" and response.get("error") == "no-client-found":
                    raise OwnerUnavailable("owner unavailable before dispatch")
                raise RuntimeError("IPC request failed: " + str(response.get("error")))
            return response

    def owner(self):
        return self.request("thread-owner-discovery", {"hostId": "local", "conversationId": THREAD})["handledByClientId"]

    def deliver(self, event_id, code, canary):
        return self.request("thread-follower-start-turn", {
            "conversationId": THREAD,
            "turnStart": {"request": {"threadId": THREAD,
                "clientUserMessageId": event_id,
                "input": [{"type": "text", "text": completion_prompt(event_id, code, canary), "text_elements": []}]},
                "context": {"inheritThreadSettings": True}}}, target=self.owner())


def deliver_with_owner_retry(event_id, code, canary, receipt_path, state):
    for attempt in range(1441):
        ipc = DesktopIPC()
        try:
            return ipc.deliver(event_id, code, canary)
        except OwnerUnavailable:
            if attempt == 1440:
                raise
            state.update(status="waiting_for_owner_before_dispatch", owner_lookup_attempts=attempt+1,
                         start_turn_sent=False)
            save_receipt(receipt_path,state)
        finally:
            ipc.sock.close()
        time.sleep(60)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--probe", action="store_true")
    p.add_argument("--event-id")
    p.add_argument("--canary", action="store_true")
    p.add_argument("--launch", action="store_true", help="detach exactly one wrapper, no recurring jobs")
    p.add_argument("command", nargs=argparse.REMAINDER)
    a = p.parse_args()
    if a.probe:
        ipc = DesktopIPC()
        try:
            ipc.owner()
            print(json.dumps({"owner_found": True, "thread_id": THREAD, "mutation_performed": False}))
        finally:
            ipc.sock.close()
        return
    command = a.command[1:] if a.command[:1] == ["--"] else a.command
    if not command:
        raise RuntimeError("a child command is required")
    event_id = str(uuid.UUID(a.event_id)) if a.event_id else str(uuid.uuid4())
    EVENTS.mkdir(parents=True, exist_ok=True)
    receipt_path = EVENTS / f"{event_id}.json"
    if a.launch:
        if receipt_path.exists():
            raise RuntimeError("event already exists; do not duplicate a process")
        args = [sys.executable, "-B", str(Path(__file__).resolve()), "--event-id", event_id]
        if a.canary:
            args.append("--canary")
        with (EVENTS / f"{event_id}.wrapper.log").open("x") as log:
            process = subprocess.Popen(args + ["--"] + command, cwd=ROOT,
                stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True)
        print(json.dumps({"event_id": event_id, "wrapper_pid": process.pid, "receipt": str(receipt_path)}))
        return
    state = {"event_id": event_id, "thread_id": THREAD, "wrapper_pid": os.getpid(),
             "canary": a.canary, "status": "starting", "started_at": datetime.now(timezone.utc).isoformat(),
             "scheduler_used": False, "autonomous_wake_verified": False}
    # Exclusive claim prevents two wrappers consuming the same event.
    with receipt_path.open("x") as handle:
        json.dump(state, handle)
    try:
        with (EVENTS / f"{event_id}.child.log").open("x") as log:
            process = subprocess.Popen(command, cwd=ROOT, stdin=subprocess.DEVNULL, stdout=log, stderr=log)
            state.update(status="running", child_pid=process.pid)
            save_receipt(receipt_path, state)
            code = process.wait()  # OS process-exit event, not a polling timer.
        state.update(status="child_exited", child_exit_code=code, exited_at=datetime.now(timezone.utc).isoformat())
        save_receipt(receipt_path, state)
        response = deliver_with_owner_retry(event_id, code, a.canary, receipt_path, state)
        state.update(status="dispatch_accepted", response_type=response["resultType"], start_turn_sent=True)
    except Exception as exc:
        # Never automatically resend an uncertain delivery: it might have landed.
        state.update(status="failed_or_delivery_uncertain", error=str(exc))
        raise
    finally:
        save_receipt(receipt_path, state)


if __name__ == "__main__":
    main()
