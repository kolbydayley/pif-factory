#!/bin/zsh
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CONTROL_ROOT="$REPO_ROOT/work/app-server-development-v2/unattended-control-v22"
PIPELINE_ROOT="$REPO_ROOT/work/app-server-development-v2/unattended-pipeline-v5"
OUTPUT_ROOT="$PIPELINE_ROOT/judge-calibration-v5_4-reference-v2-capacity-v22"
REFERENCE_ROOT="$PIPELINE_ROOT/fixture-reference-adjudication-luna-v6-capacity-v21"
POLICY="$CONTROL_ROOT/capacity-policy-v22.json"
RUNTIME_LOCK="$REPO_ROOT/work/app-server-development-v2/unattended-runtime-lock-v22.json"
LAUNCH_RECEIPT="$CONTROL_ROOT/launch-receipt-v22.json"

cd "$REPO_ROOT"
unset OPENAI_API_KEY

if [[ -e "$PIPELINE_ROOT/STOP" || -e "$OUTPUT_ROOT/STOP" || -e "$CONTROL_ROOT/STOP" ]]; then
  print -u2 -- "A pipeline-v5 v22 STOP sentinel is present; refusing to start."
  exit 2
fi

/usr/bin/python3 -m research_factory.app_server_runtime_lock_v22 \
  --repo-root "$REPO_ROOT" \
  --manifest "$RUNTIME_LOCK" \
  --verify-only

/usr/bin/python3 -m research_factory.app_server_capacity_policy_v22 launch-receipt \
  --policy "$POLICY" \
  --runtime-lock "$RUNTIME_LOCK" \
  --output "$LAUNCH_RECEIPT"

exec /usr/bin/python3 -m research_factory.app_server_judge_v5_fresh_calibration_v22 \
  --policy "$POLICY" \
  --output-dir "$OUTPUT_ROOT" \
  --reference-root "$REFERENCE_ROOT"
