#!/bin/zsh
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CONTROL_ROOT="$REPO_ROOT/work/app-server-development-v2/unattended-control-v20"
PIPELINE_ROOT="$REPO_ROOT/work/app-server-development-v2/unattended-pipeline-v5"
REFERENCE_ROOT="$PIPELINE_ROOT/fixture-reference-adjudication-luna-v5-capacity-v20"
POLICY="$CONTROL_ROOT/capacity-policy-v20.json"
RUNTIME_LOCK="$REPO_ROOT/work/app-server-development-v2/unattended-runtime-lock-v20-r2.json"
LAUNCH_RECEIPT="$CONTROL_ROOT/launch-receipt-v20.json"

cd "$REPO_ROOT"
unset OPENAI_API_KEY

if [[ -e "$PIPELINE_ROOT/STOP" || -e "$REFERENCE_ROOT/STOP" || -e "$CONTROL_ROOT/STOP" ]]; then
  print -u2 -- "A pipeline-v5 v20 STOP sentinel is present; refusing to start."
  exit 2
fi

/usr/bin/python3 -m research_factory.app_server_runtime_lock_v20 \
  --repo-root "$REPO_ROOT" \
  --manifest "$RUNTIME_LOCK" \
  --verify-only

/usr/bin/python3 -m research_factory.app_server_capacity_policy_v20 launch-receipt \
  --policy "$POLICY" \
  --runtime-lock "$RUNTIME_LOCK" \
  --output "$LAUNCH_RECEIPT"

exec /usr/bin/python3 -m research_factory.app_server_judge_v5_reference_adjudication_v20 \
  --policy "$POLICY" \
  --output-dir "$REFERENCE_ROOT"
