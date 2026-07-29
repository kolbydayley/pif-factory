#!/bin/zsh
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PIPELINE_ROOT="$REPO_ROOT/work/app-server-development-v2/unattended-pipeline-v5"
V2_ROOT="$PIPELINE_ROOT/fixture-truth-audit-gpt55-v2"
V3_ROOT="$PIPELINE_ROOT/fixture-truth-audit-gpt55-v3"
REFERENCE_ROOT="$PIPELINE_ROOT/fixture-reference-adjudication-luna-v4"
RUNTIME_LOCK="$REPO_ROOT/work/app-server-development-v2/unattended-runtime-lock-v16.json"

cd "$REPO_ROOT"
unset OPENAI_API_KEY

if [[ -e "$PIPELINE_ROOT/STOP" || -e "$REFERENCE_ROOT/STOP" ]]; then
  print -u2 -- "A pipeline-v5 fixture-reference STOP sentinel is present; refusing to start."
  exit 2
fi

/usr/bin/python3 -m research_factory.app_server_runtime_lock_v16 \
  --repo-root "$REPO_ROOT" \
  --manifest "$RUNTIME_LOCK"

/usr/bin/python3 -m research_factory.app_server_runtime_lock \
  --repo-root "$REPO_ROOT" \
  --manifest "$RUNTIME_LOCK"

exec /usr/bin/python3 -m research_factory.app_server_judge_v5_reference_adjudication \
  --v2-root "$V2_ROOT" \
  --v3-root "$V3_ROOT" \
  --output-dir "$REFERENCE_ROOT" \
  --model gpt-5.6-luna \
  --reasoning-effort high \
  --timeout-seconds 1200
