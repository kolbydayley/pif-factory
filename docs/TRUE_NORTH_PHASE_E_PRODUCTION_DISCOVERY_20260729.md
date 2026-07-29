# True-North Phase E Production-Truth Discovery

Date: 2026-07-29

Status: **target identified; current corpus metrics unavailable without a
separately authorized read**

## Finding

The live podcast corpus is **not hosted on Railway**. Repository contracts and
the production runner identify the canonical legacy podcast/queue authority as
the Mac-local file-backed SQLite database:

`data/factory.sqlite`

The default resolves through `research_factory.paths.db_path()`, optionally
overridden by `RESEARCH_FACTORY_DB`. The production runner requires an explicit
file-backed SQLite `--database`, opens `mode=ro` when `--execute` is absent,
sets `PRAGMA query_only=ON`, and verifies the queue and episode-context
authorities share the same device/inode/path.

That exact local file is currently a 4,052,508,672-byte APFS dataless
placeholder. It must be hydrated from its file-provider backing before a
read-only trial can proceed.

## What Railway contains

Railway project `podcast-intelligence-observer` contains:

- `observer-ui`: sanitized snapshots only;
- `mcp-broker`: remote-control/broker snapshots, events, and work-package
  metadata;
- managed `Postgres`: durable storage for the MCP broker.

The Postgres service has a public connection endpoint configured and a ready
volume, but application code binds it through `PIF_MCP_DATABASE_URL` for
`BrokerStore`. It is not the transcript, episode, label, or canonical claim
store. Neither Railway application service has a volume mount, worker, cron,
or extraction/model path. No database or production API query was made during
this discovery.

## Count and recency

Current episode count and latest publication date are **unknown under the
zero-query constraint**. Configuration metadata cannot establish either.

The latest repository-recorded consistent snapshot is explicitly historical:

- snapshot date: 2026-07-11
- episodes: 464
- labels: 4,966
- discourse events: 82,977
- sources: 28

Those are not represented as current. The dataless SQLite inode records a file
mtime of 2026-07-20, but that timestamp does not reveal corpus recency.

## Safe read-only trial path

Once the local SQLite file is hydrated, a shadow trial is feasible without
mutation:

1. verify file identity and readable SQLite integrity;
2. open `file:<absolute-path>?mode=ro` and set `PRAGMA query_only=ON`;
3. read only the three selected episode dependencies and existing Codex
   outputs;
4. write all replay outputs to the isolated Phase-E shadow store under
   `~/Library/Application Support/Podcast Intelligence Factory/`;
5. compare file identity, size, mtime, critical table counts, and release/queue
   counts before and after.

Railway Postgres should not be used for this trial. A separate authorization
is still required to hydrate and query the local corpus and establish current
episode count/recency.
