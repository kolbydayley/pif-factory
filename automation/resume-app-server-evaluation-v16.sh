#!/bin/zsh
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PIPELINE_ROOT="$REPO_ROOT/work/app-server-development-v2/unattended-pipeline-v5"
SOURCE_ROOT="$PIPELINE_ROOT/fixture-truth-audit-gpt55-v2"
RECOVERY_ROOT="$PIPELINE_ROOT/fixture-truth-audit-gpt55-v3"
RUNTIME_LOCK="$REPO_ROOT/work/app-server-development-v2/unattended-runtime-lock-v15.json"

cd "$REPO_ROOT"
unset OPENAI_API_KEY

if [[ -e "$PIPELINE_ROOT/STOP" || -e "$RECOVERY_ROOT/STOP" ]]; then
  print -u2 -- "A pipeline-v5 fixture-audit recovery STOP sentinel is present; refusing to start."
  exit 2
fi

/usr/bin/python3 -m research_factory.app_server_runtime_lock_v15 \
  --repo-root "$REPO_ROOT" \
  --manifest "$RUNTIME_LOCK"

/usr/bin/python3 -m research_factory.app_server_runtime_lock \
  --repo-root "$REPO_ROOT" \
  --manifest "$RUNTIME_LOCK"

exec /usr/bin/python3 -m research_factory.app_server_judge_v5_fixture_audit_recovery \
  --v2-root "$SOURCE_ROOT" \
  --output-dir "$RECOVERY_ROOT" \
  --model gpt-5.5 \
  --reasoning-effort high \
  --timeout-seconds 1200
