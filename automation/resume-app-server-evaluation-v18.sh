#!/bin/zsh
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PIPELINE_ROOT="$REPO_ROOT/work/app-server-development-v2/unattended-pipeline-v5"
CALIBRATION_ROOT="$PIPELINE_ROOT/judge-calibration-v5_4-reference-v2-v1"
CALIBRATION_TERMINAL="$REPO_ROOT/work/app-server-development-v2/unattended-control-v17/terminal.json"
REUSE_CONTRACT="$PIPELINE_ROOT/reuse-contract-v4.json"
SELECTION_ROOT="$PIPELINE_ROOT/development-selection-v5-reference-v2"
CONTROL_ROOT="$REPO_ROOT/work/app-server-development-v2/unattended-control-v18"
RUNTIME_LOCK="$REPO_ROOT/work/app-server-development-v2/unattended-runtime-lock-v18.json"

cd "$REPO_ROOT"
unset OPENAI_API_KEY

if [[ -e "$PIPELINE_ROOT/STOP" || -e "$SELECTION_ROOT/STOP" || -e "$CONTROL_ROOT/STOP" ]]; then
  print -u2 -- "A pipeline-v5 selection STOP sentinel is present; refusing to start."
  exit 2
fi

exec /usr/bin/python3 -m research_factory.app_server_judge_v5_selection_continuation \
  --repo-root "$REPO_ROOT" \
  --calibration-root "$CALIBRATION_ROOT" \
  --calibration-continuation-terminal "$CALIBRATION_TERMINAL" \
  --reuse-contract "$REUSE_CONTRACT" \
  --selection-root "$SELECTION_ROOT" \
  --control-root "$CONTROL_ROOT" \
  --manifest "$RUNTIME_LOCK" \
  --poll-seconds 300
