#!/bin/zsh
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PIPELINE_ROOT="$REPO_ROOT/work/app-server-development-v2/unattended-pipeline-v4"
RUNTIME_LOCK="$REPO_ROOT/work/app-server-development-v2/unattended-runtime-lock-v5.json"
REUSE_CONTRACT="$PIPELINE_ROOT/reuse-contract-v3.json"

cd "$REPO_ROOT"
unset OPENAI_API_KEY

if [[ -e "$PIPELINE_ROOT/STOP" ]]; then
  print -u2 -- "A pipeline-v4 STOP sentinel is present; refusing to start."
  exit 2
fi

/usr/bin/python3 -m research_factory.app_server_runtime_lock \
  --repo-root "$REPO_ROOT" \
  --manifest "$RUNTIME_LOCK"

/usr/bin/python3 -m research_factory.app_server_v2_reuse \
  --output "$REUSE_CONTRACT" \
  --verify-only

/usr/bin/python3 -m research_factory.app_server_runtime_lock \
  --repo-root "$REPO_ROOT" \
  --manifest "$RUNTIME_LOCK"

exec /usr/bin/python3 -m research_factory.unattended_app_server_pipeline_v4 \
  --repo-root "$REPO_ROOT" \
  --pipeline-root "$PIPELINE_ROOT"
