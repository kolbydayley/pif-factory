# Codex app-server protocol snapshot

Generated from `codex-cli 0.147.0` with:

```sh
codex app-server generate-json-schema --experimental --out research_factory/protocol/codex_app_server_0_147_0
```

The rebuild protocol canary uses `codex_app_server_protocol.v2.schemas.json`.
Its pinned SHA-256 is:

```text
ff10829cd75b67297019b39ab508ac699198574663579aa18336b7dc55ea178f
```

Promotion to the default client pin requires the frozen ten-window receipt to
pass schema validity, deterministic envelope shape, complete token accounting,
and compatible error-class checks.
