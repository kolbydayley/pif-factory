#!/bin/zsh
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CONTROL_ROOT="$REPO_ROOT/work/app-server-development-v2/unattended-control-v23"
PIPELINE_ROOT="$REPO_ROOT/work/app-server-development-v2/unattended-pipeline-v5"
OUTPUT_ROOT="$PIPELINE_ROOT/judge-calibration-v5_4-reference-v2-capacity-v23"
REFERENCE_ROOT="$PIPELINE_ROOT/fixture-reference-adjudication-luna-v6-capacity-v21"
POLICY="$CONTROL_ROOT/capacity-policy-v23.json"
RUNTIME_LOCK="$REPO_ROOT/work/app-server-development-v2/unattended-runtime-lock-v23.json"
LAUNCH_RECEIPT="$CONTROL_ROOT/launch-receipt-v23.json"

cd "$REPO_ROOT"
unset OPENAI_API_KEY

if [[ -e "$PIPELINE_ROOT/STOP" || -e "$OUTPUT_ROOT/STOP" || -e "$CONTROL_ROOT/STOP" ]]; then
  print -u2 -- "A pipeline-v5 v23 STOP sentinel is present; refusing to start."
  exit 2
fi

/usr/bin/python3 -m research_factory.app_server_runtime_lock_v23 \
  --repo-root "$REPO_ROOT" \
  --manifest "$RUNTIME_LOCK" \
  --verify-only

/usr/bin/python3 -m research_factory.app_server_capacity_policy_v23 launch-receipt \
  --policy "$POLICY" \
  --runtime-lock "$RUNTIME_LOCK" \
  --output "$LAUNCH_RECEIPT"

exec /usr/bin/python3 -m research_factory.app_server_judge_v5_fresh_calibration_v23 \
  --policy "$POLICY" \
  --output-dir "$OUTPUT_ROOT" \
  --reference-root "$REFERENCE_ROOT"
