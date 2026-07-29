#!/bin/zsh
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PIPELINE_ROOT="$REPO_ROOT/work/app-server-development-v2/unattended-pipeline-v5"
DIAGNOSTIC_ROOT="$PIPELINE_ROOT/judge-diagnostic-v3"
RUNTIME_LOCK="$REPO_ROOT/work/app-server-development-v2/unattended-runtime-lock-v8.json"
REUSE_CONTRACT="$PIPELINE_ROOT/reuse-contract-v6.json"
ATTEMPT_AUDIT_RECEIPT="$PIPELINE_ROOT/diagnostic-v2-attempt-audit-receipt-v1.json"

cd "$REPO_ROOT"
unset OPENAI_API_KEY

if [[ -e "$PIPELINE_ROOT/STOP" || -e "$DIAGNOSTIC_ROOT/STOP" ]]; then
  print -u2 -- "A pipeline-v5 diagnostic-v3 STOP sentinel is present; refusing to start."
  exit 2
fi

/usr/bin/python3 -m research_factory.app_server_runtime_lock \
  --repo-root "$REPO_ROOT" \
  --manifest "$RUNTIME_LOCK"

/usr/bin/python3 -m research_factory.app_server_v5_diagnostic_v3_reuse \
  --output "$REUSE_CONTRACT" \
  --verify-only

/usr/bin/python3 -m research_factory.app_server_runtime_lock \
  --repo-root "$REPO_ROOT" \
  --manifest "$RUNTIME_LOCK"

exec /usr/bin/python3 -m research_factory.app_server_judge_v5_diagnostic \
  --output-dir "$DIAGNOSTIC_ROOT" \
  --reuse-contract "$REUSE_CONTRACT" \
  --fixture-audit-receipt "$ATTEMPT_AUDIT_RECEIPT" \
  --model gpt-5.6-sol \
  --reasoning-effort high \
  --timeout-seconds 1200
