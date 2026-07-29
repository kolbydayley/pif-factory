# SDK pipeline controller prototype

This is an intentionally non-activated controller skeleton for replacing repeated
`codex exec resume` supervision with one launchd-owned process and one persistent
official Python Codex SDK client. Its prospective default is a fresh bounded SDK
thread supplied with a compact sanitized recovery capsule. Reusing the historical
thread is available only as explicit `--thread-strategy legacy_exact_resume`
migration mode.

## Safety state

- The LaunchAgent file is a template and has `Disabled = true`; it is not installed or loaded.
- The template omits `--allow-model-turns`.
- A model turn requires both `--allow-model-turns` and a valid, unexpired activation file.
- Before resuming a thread, the transport calls the SDK account method and accepts only
  account type `chatgpt`. API-key and other account types fail closed.
- Active external turns are observed and never interrupted.
- Observation uses the watchdog's deadline-bounded helper, so a slow local provider
  cannot wedge the controller indefinitely.
- A fresh-thread recovery capsule contains only bounded phases, hashes, safe relative
  artifact paths, exit classes/codes, prior numeric usage, and fixed constraints. It
  contains no prior chat prose or raw podcast text.
- As soon as app-server accepts a turn, the controller atomically persists its fresh
  thread ID and turn ID as `pending_turn`. A process crash or transport disconnect
  leaves that receipt ambiguous; restart performs read-only thread-history
  reconciliation and never blindly resends.
- The ledger records hashes, lifecycle state, and numeric usage only. It does not store
  prompts, final responses, session tokens, or raw podcast text.

## Verified SDK facts (2026-07-14)

The official OpenAI repository documents `pip install openai-codex` and says the
SDK reuses existing Codex authentication. An isolated install succeeded with
`openai-codex==0.1.0b3`; the package depends on `openai-codex-cli-bin==0.137.0a4`.
The host CLI was `0.144.3` and `codex login status` reported ChatGPT auth.

The pinned-runtime gap is the reason this prototype remains non-activated. A
non-semantic compatibility smoke did pass on 2026-07-14: the pinned app-server
initialized, reported account type `chatgpt`, listed one saved thread, and shut
down cleanly. No thread was started or resumed and no model turn was made. The
generated account union is a root wrapper, so the controller deliberately reads
`account.root.type` when present before applying its ChatGPT-only auth gate.

Before activation, the remaining checks must use a disposable thread and disposable
controller state, never the live evaluation thread:

1. Start one fresh bounded thread, verify callback ordering (`thread/start`,
   `turn/start`, persisted receipt, terminal notification), then verify the next unit
   starts a different fresh thread with only the recovery capsule as continuity.
2. Kill the controller immediately before `turn/start`; restart may create another
   empty fresh thread because no semantic turn was accepted.
3. Kill it immediately after `turn/start` returns but before the first streamed event;
   restart must preserve `pending_turn`, read history, and submit nothing.
4. Repeat the post-accept kill while the turn is still `inProgress`; reconciliation
   must stay read-only and retry with backoff.
5. Repeat with terminal `completed`, `failed`, and `interrupted` histories; each must
   clear ambiguity in a reconciliation-only tick and wait before any next turn.
6. Make history return no matching turn, an unknown status, malformed data, transport
   closure, and auth loss; every case must preserve the pending receipt and fail closed.
7. Prove a SIGKILL between receipt persistence and ledger append cannot cause a
   resend. Pending thread/turn receipts are checkpointed atomically before their
   audit event is appended, so the checkpoint is the recovery authority.
8. Run two controller processes simultaneously and prove the second exits on the
   lease without opening an SDK client.
9. Run from launchd's real environment and working directory, sleep/wake the Mac, and
   prove one persistent process remains owner without catch-up turn bursts.
10. Shadow alongside the existing watchdog, prove only one mechanism can own model
    turns, then disable the watchdog before enabling the controller's model-turn flag.

Legacy exact-resume compatibility is a migration-only test after the fresh path passes;
it is not the prospective production default.

## Read-only dry check

This command does not import the SDK and cannot start a model turn:

```bash
/opt/homebrew/bin/python3 -m research_factory.sdk_pipeline_controller \
  --checkpoint /tmp/pif-sdk-controller/checkpoint.json \
  --ledger /tmp/pif-sdk-controller/ledger.jsonl \
  --lock /tmp/pif-sdk-controller/controller.lock \
  once
```

## Deliberate activation contract

Activation is a later operator step, after compatibility and integration tests.
It requires an activation JSON document with this shape:

```json
{
  "schema_version": "pif_sdk_pipeline_controller_activation_v1",
  "enabled": true,
  "project_root": "/Users/kolbydayley/Documents/Codex/podcast-intelligence-factory",
  "expires_at": "2026-07-15T00:00:00-04:00"
}
```

The activated LaunchAgent copy must also add `--allow-model-turns`. Keep the
activation short-lived for the initial shadow run. Do not add a thread-strategy flag:
the safe default is `fresh_bounded`. Only after the disposable lifecycle chaos suite
proves single ownership, managed ChatGPT auth, exact accepted-turn reconciliation,
complete telemetry, and clean shutdown should this replace the existing watchdog.
