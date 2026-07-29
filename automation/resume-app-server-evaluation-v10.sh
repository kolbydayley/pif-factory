#!/bin/zsh
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PIPELINE_ROOT="$REPO_ROOT/work/app-server-development-v2/unattended-pipeline-v5"
DIAGNOSTIC_ROOT="$PIPELINE_ROOT/judge-diagnostic-v4"
RUNTIME_LOCK="$REPO_ROOT/work/app-server-development-v2/unattended-runtime-lock-v9.json"
REUSE_CONTRACT="$PIPELINE_ROOT/reuse-contract-v7.json"
QUALITY_AUDIT_RECEIPT="$PIPELINE_ROOT/diagnostic-v3-quality-audit-receipt-v1.json"

cd "$REPO_ROOT"
unset OPENAI_API_KEY

if [[ -e "$PIPELINE_ROOT/STOP" || -e "$DIAGNOSTIC_ROOT/STOP" ]]; then
  print -u2 -- "A pipeline-v5 diagnostic-v4 STOP sentinel is present; refusing to start."
  exit 2
fi

/usr/bin/python3 -m research_factory.app_server_runtime_lock \
  --repo-root "$REPO_ROOT" \
  --manifest "$RUNTIME_LOCK"

/usr/bin/python3 -m research_factory.app_server_v5_diagnostic_v4_reuse \
  --output "$REUSE_CONTRACT" \
  --verify-only

/usr/bin/python3 -m research_factory.app_server_runtime_lock \
  --repo-root "$REPO_ROOT" \
  --manifest "$RUNTIME_LOCK"

exec /usr/bin/python3 -m research_factory.app_server_judge_v5_diagnostic \
  --output-dir "$DIAGNOSTIC_ROOT" \
  --reuse-contract "$REUSE_CONTRACT" \
  --fixture-audit-receipt "$QUALITY_AUDIT_RECEIPT" \
  --model gpt-5.6-sol \
  --reasoning-effort high \
  --timeout-seconds 1200
