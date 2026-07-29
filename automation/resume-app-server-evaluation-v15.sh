#!/bin/zsh
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PIPELINE_ROOT="$REPO_ROOT/work/app-server-development-v2/unattended-pipeline-v5"
AUDIT_ROOT="$PIPELINE_ROOT/fixture-truth-audit-gpt55-v2"
RUNTIME_LOCK="$REPO_ROOT/work/app-server-development-v2/unattended-runtime-lock-v14.json"
JUDGE_FREEZE_RECEIPT="$PIPELINE_ROOT/judge-v5_4-freeze-receipt-v1.json"
V1_AUDIT_RECEIPT="$PIPELINE_ROOT/calibration-v1-failure-audit-receipt-v1.json"
FIXTURE_AUDIT_RECEIPT="$PIPELINE_ROOT/fixture-truth-audit-receipt-v1.json"

cd "$REPO_ROOT"
unset OPENAI_API_KEY

if [[ -e "$PIPELINE_ROOT/STOP" || -e "$AUDIT_ROOT/STOP" ]]; then
  print -u2 -- "A pipeline-v5 fixture-truth-audit STOP sentinel is present; refusing to start."
  exit 2
fi

/usr/bin/python3 -m research_factory.app_server_runtime_lock_v14 \
  --repo-root "$REPO_ROOT" \
  --manifest "$RUNTIME_LOCK" \
  --judge-freeze-receipt "$JUDGE_FREEZE_RECEIPT" \
  --v1-audit-receipt "$V1_AUDIT_RECEIPT" \
  --fixture-truth-audit-receipt "$FIXTURE_AUDIT_RECEIPT"

/usr/bin/python3 -m research_factory.app_server_runtime_lock \
  --repo-root "$REPO_ROOT" \
  --manifest "$RUNTIME_LOCK"

exec /usr/bin/python3 -m research_factory.app_server_judge_v5_calibration_runner \
  --output-dir "$AUDIT_ROOT" \
  --judge-freeze-receipt "$JUDGE_FREEZE_RECEIPT" \
  --v1-audit-receipt "$V1_AUDIT_RECEIPT" \
  --fixture-truth-audit-receipt "$FIXTURE_AUDIT_RECEIPT" \
  --execution-purpose fixture_truth_audit \
  --model gpt-5.5 \
  --reasoning-effort high \
  --timeout-seconds 1200
