# Kimi Code Workhorse

Status: **lab-only shadow adapter**. Kimi Code is available to Codex as a
bounded labeling workhorse, but it is not part of the production extractor or
the authoritative queue.

Verification note (2026-07-22): the adapter's automated tests and status checks
remain local-only. Separately, an owner-run `kimi login` reached Kimi but its
membership-backed model provisioning failed; no hosted Kimi model call was
performed. Current account-side model availability, quota behavior, and the
actual model served by Kimi therefore remain unverified.

## Architecture

```text
Codex orchestrator
  -> pif lab kimi-workhorse
  -> immutable local job manifest
  -> isolated Kimi Code 0.29.0 process
  -> stream-json parsing
  -> local schema and evidence validation
  -> atomic private shadow output
```

The adapter never opens or mutates the canonical PIF SQLite database. It writes
only the output named by the shadow manifest, and it refuses to overwrite an
existing output. A persistent receipt beside each output also prevents repeat
or concurrent dispatch for that output. Codex owns job selection, comparison,
review, and promotion; Kimi owns only the bounded inference call.

## Installed profile

- CLI package: `@moonshot-ai/kimi-code@0.29.0`
- Executable: `/opt/homebrew/bin/kimi`
- Dedicated state: `/Users/kolbydayley/Library/Application Support/Podcast Intelligence Factory/kimi-code`
- Private job spool: `/Users/kolbydayley/Library/Caches/Podcast Intelligence Factory/kimi-code-jobs`
- Local authorization receipt: `config/kimi_code_automation_authorization.json`

The adapter requires the exact CLI version above. It does not silently accept
a newer Kimi release because tool names, permissions, output events, and model
aliases may change.

## Setup, status, and owner login

Run commands from the repository root:

```bash
cd /Users/kolbydayley/Documents/Codex/podcast-intelligence-factory
python3 -m research_factory.pif_cli lab kimi-workhorse setup
python3 -m research_factory.pif_cli lab kimi-workhorse status
```

`setup` creates the private Kimi home, empty skills directory, and restrictive
configuration, then runs only `kimi --version` and `kimi doctor config`.
`status` reports sanitized readiness and credential presence. It does not trust
credential-file existence alone: Kimi writes the OAuth artifact before its
membership-backed model check, so a failed login may leave that artifact. The
status probe also runs plain `kimi provider list` and requires the locally
provisioned `managed:kimi-code` OAuth provider with a positive model count.
That command reads local configuration only; it makes no network or model call
and does not prove that an older membership or token remains valid today.
Accordingly, `ready_for_live_call` means only that the local prerequisites and
a prior successful login provisioning are present, making the profile eligible
to attempt a call. The status output leaves current authentication and
membership validity unknown. `accepted_model_ids` is the wrapper's allowlist,
not a claim that the account is entitled to every listed model. Neither
`setup` nor `status` authenticates or sends a prompt.

Each local CLI preflight has a 15-second hard deadline. Kimi Code normally
spends several seconds starting even for local commands, so this budget allows
ordinary cold-start variance while still killing a genuinely stalled process.
Timeout errors identify whether the version, configuration, or provider-profile
stage stalled. A timeout is reported as an inconclusive local probe failure,
not as evidence that the profile setup is invalid.

Authentication is an explicit interactive owner step:

```bash
KIMI_CODE_HOME='/Users/kolbydayley/Library/Application Support/Podcast Intelligence Factory/kimi-code' \
  /opt/homebrew/bin/kimi login
```

After login, rerun `status`. Do not inspect or copy credential contents into the
repository, logs, manifests, or Codex messages.

If login reports HTTP 402 or that membership benefits cannot be verified,
confirm that the Kimi Code subscription is active on the same account used for
checkout, wait briefly, and retry the owner login. If it persists, check the
subscription state in Kimi settings/console and contact Kimi support. Until a
login reaches managed provider/model provisioning, `status` reports
`membership_or_login_provisioning_required`, exits nonzero, and `--execute`
fails locally before claiming an attempt receipt or starting inference.

## Shadow manifest contract

A job uses `pif_kimi_workhorse_manifest_v1`:

```json
{
  "schema_version": "pif_kimi_workhorse_manifest_v1",
  "job_id": "holdout-episode-001",
  "privacy_tier": "full_text_allowed",
  "model": "k3",
  "prompt_path": "/Users/kolbydayley/Library/Application Support/Podcast Intelligence Factory/kimi-workhorse-shadow/prompts/holdout-episode-001.md",
  "prompt_sha256": "<lowercase SHA-256 of prompt_path>",
  "schema_path": "/Users/kolbydayley/Documents/Codex/podcast-intelligence-factory/label_packs/ai_discourse_v3_1/schema.json",
  "schema_sha256": "<lowercase SHA-256 of schema_path>",
  "output_path": "/Users/kolbydayley/Library/Application Support/Podcast Intelligence Factory/kimi-workhorse-shadow/outputs/holdout-episode-001.json",
  "label_pack": "ai_discourse_v3_1",
  "segment_text_path": "/Users/kolbydayley/Library/Application Support/Podcast Intelligence Factory/kimi-workhorse-shadow/segments/holdout-episode-001.txt",
  "segment_text_sha256": "<lowercase SHA-256 of segment_text_path>"
}
```

All manifest fields shown above are required. In particular, every job must
name a PIF `label_pack` and a hash-bound `segment_text_path`; the generic
schema-only mode is not supported. The declared schema must exactly match the
named pack, and successful model output must pass that pack's local schema and
exact-evidence validation against the declared segment text.

Paths may be absolute or relative to the manifest directory, but private
prompts, segments, manifests, and outputs should remain outside the repository
under the external shadow root:
`/Users/kolbydayley/Library/Application Support/Podcast Intelligence Factory/kimi-workhorse-shadow/`.
The pack schema remains the repository's versioned validation authority.

Only `privacy_tier: "full_text_allowed"` may execute, because the prompt may
contain private transcript text that will be sent to Kimi's hosted service.
Treat a manifest as immutable after its first live execution attempt. Before
dispatch, the adapter atomically creates a persistent receipt beside the output
(for this example, `/Users/kolbydayley/Library/Application Support/Podcast Intelligence Factory/kimi-workhorse-shadow/outputs/.holdout-episode-001.json.kimi-attempt.json`).
That exclusive create blocks both concurrent and later dispatches targeting the
same output, including after a failed or uncertain call. Every retry requires a
new `job_id`, manifest, and output path. Existing outputs and attempt receipts
are never silently replaced.

Create the manifest and its hashes with the local freezer instead of calculating
them by hand:

```bash
python3 -m research_factory.pif_cli lab kimi-workhorse freeze \
  --manifest '/Users/kolbydayley/Library/Application Support/Podcast Intelligence Factory/kimi-workhorse-shadow/jobs/holdout-episode-001.json' \
  --job-id holdout-episode-001 \
  --model k3 \
  --prompt '/Users/kolbydayley/Library/Application Support/Podcast Intelligence Factory/kimi-workhorse-shadow/prompts/holdout-episode-001.md' \
  --label-pack ai_discourse_v3_1 \
  --segment-text '/Users/kolbydayley/Library/Application Support/Podcast Intelligence Factory/kimi-workhorse-shadow/segments/holdout-episode-001.txt' \
  --output '/Users/kolbydayley/Library/Application Support/Podcast Intelligence Factory/kimi-workhorse-shadow/outputs/holdout-episode-001.json'
```

`freeze` selects the named pack's repository schema, hashes the prompt, schema,
and segment, and makes no model call.

Preview a job without calling Kimi:

```bash
python3 -m research_factory.pif_cli lab kimi-workhorse run \
  --manifest '/Users/kolbydayley/Library/Application Support/Podcast Intelligence Factory/kimi-workhorse-shadow/jobs/holdout-episode-001.json'
```

Execute the exact manifest only with the explicit gate:

```bash
python3 -m research_factory.pif_cli lab kimi-workhorse run \
  --manifest '/Users/kolbydayley/Library/Application Support/Podcast Intelligence Factory/kimi-workhorse-shadow/jobs/holdout-episode-001.json' \
  --execute
```

Dry-run is the default. The command emits sanitized operational metadata, not
prompt text, transcript text, model output, or credentials. A successful live
run publishes the model response only after JSON parsing and local validation.

## Model selection

The only accepted public manifest model IDs and their native Kimi Code `-m`
mapping are:

| Manifest `model` | Native CLI model selector |
| --- | --- |
| `k3` | `kimi-code/k3` |
| `kimi-for-coding` | `kimi-code/kimi-for-coding` |
| `kimi-for-coding-highspeed` | `kimi-code/kimi-for-coding-highspeed` |

No other public model ID is accepted. Every manifest pins one of these values,
the adapter deterministically adds the `kimi-code/` prefix, and it never falls
back silently to another requested model. Availability is still version-,
account-, and plan-dependent.

Kimi Code 0.29.0's stream does not provide model identity evidence that this
adapter can verify. A completed result therefore records the pinned
`requested_model_id` and `cli_model_alias`, but reports
`actual_model_verified: false` and `actual_model_id: null`. Those fields prove
what was requested, not which server-side model actually answered.

## Security, privacy, and persistence

- The wrapper passes subprocess arguments directly, never through a shell.
- Transcript and prompt content live in a mode-0700 temporary job directory;
  the command line contains only a fixed instruction to read `job.json`.
- The one-shot adapter sends the prompt, complete authoritative segment text,
  and output schema as ordered bounded string chunks inside `job.json`. Kimi is
  instructed to concatenate each chunk array before labeling.
- The complete serialized job is rejected above 80 KiB, 900 lines, or the safe
  per-line size instead of letting Kimi silently see truncated input. A prompt
  file is capped at 64 KiB. The separate 8 MiB segment-file read ceiling is not
  dispatch capacity: if the chunked request does not fit the tighter job
  envelope, it fails locally before Kimi starts. Split larger work into
  independently grounded shadow cases.
- Kimi may read only that job file. Write/edit, shell, web, MCP, agent, skill,
  cron, media, search, and background-work tools are denied.
- Telemetry, automatic updates, cron, generic skill discovery, and broad agent
  concurrency are disabled in the dedicated profile.
- Raw stdout and stderr stay inside the private per-job spool and are removed
  after successful or failed execution by default. Caller-visible failures use
  bounded hashes and counts rather than raw model text.
- Kimi Code persists sessions and related local state under `KIMI_CODE_HOME`.
  That state may contain prompts and responses even after the per-job spool is
  removed. Keep it on the encrypted local disk, restrict access, and perform
  retention cleanup only while no Kimi job is active.
- Local isolation does not make inference local: authorized prompt content is
  transmitted to Kimi's hosted service. Do not submit material outside the
  approved PIF scope or the manifest's privacy classification.

## Latency and concurrency

Begin with one live request at a time. Successful command results and completed
attempt receipts record `duration_ms` and `retry_count`; failure receipts record
duration when the transport supplied it. `retry_count` counts Kimi stream events
with `role: "meta"` and `type: "turn.step.retrying"`. It is a provider/CLI retry
metric, not permission to redispatch the manifest. Any application-level retry
still requires a new job and output.

Record latency, provider retries, validation failures, requested-model fields,
and the unresolved actual-model identity before increasing concurrency. Use
`kimi-for-coding-highspeed` only for latency-sensitive shadow trials, and compare
its quality and quota use against `k3` and the standard coding lane.

Batch related labels only when one schema preserves an unambiguous item ID,
exact per-item evidence, and independent validation. Do not build wide
concurrency around remote latency: it can amplify rate limits and invalid
retries. A persistent ACP/warm-process transport is a later optimization, only
after one-shot CLI measurements establish that startup overhead is material and
the same isolation guarantees can be preserved.

## Promotion boundary

The production GPT-5.5 extractor and canonical queue remain unchanged. Kimi
outputs are shadow evidence only. Promotion requires a representative held-out
comparison demonstrating non-inferior label quality and evidence grounding,
zero schema/offset failures, acceptable failure and retry rates, measured
latency and cost, and a separate explicit production-enablement decision. The
existing authorization receipt deliberately has `production_enabled: false`;
provider permission to run the lab does not itself authorize production
cutover.
