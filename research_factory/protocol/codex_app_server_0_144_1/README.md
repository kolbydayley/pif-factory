# Codex app-server protocol snapshot

This private development snapshot was generated from `codex-cli 0.144.1` with:

```sh
codex app-server generate-json-schema --experimental --out research_factory/protocol/codex_app_server_0_144_1
```

The client uses `codex_app_server_protocol.v2.schemas.json`. Its pinned SHA-256 is:

```text
312b90372fd7a03423df7f46c60d623ada3ed066abcc5af2e6144bfa83b62026
```

`research_factory.codex_app_server` verifies both the installed CLI version and this schema hash before starting a real worker. Any drift fails closed and requires regenerating the snapshot plus reviewing the protocol fields and tests before updating the pin.
