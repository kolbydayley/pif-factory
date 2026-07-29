#!/bin/zsh
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PIPELINE_ROOT="$REPO_ROOT/work/app-server-development-v2/unattended-pipeline-v5"
CALIBRATION_ROOT="$PIPELINE_ROOT/judge-calibration-v5_4-v1"
RUNTIME_LOCK="$REPO_ROOT/work/app-server-development-v2/unattended-runtime-lock-v12.json"
JUDGE_FREEZE_RECEIPT="$PIPELINE_ROOT/judge-v5_4-freeze-receipt-v1.json"

cd "$REPO_ROOT"
unset OPENAI_API_KEY

if [[ -e "$PIPELINE_ROOT/STOP" || -e "$CALIBRATION_ROOT/STOP" ]]; then
  print -u2 -- "A pipeline-v5 full-calibration STOP sentinel is present; refusing to start."
  exit 2
fi

/usr/bin/python3 -m research_factory.app_server_runtime_lock \
  --repo-root "$REPO_ROOT" \
  --manifest "$RUNTIME_LOCK"

/usr/bin/python3 -c \
  'from pathlib import Path; from research_factory.app_server_judge_v5_calibration_runner import verify_judge_freeze_receipt; verify_judge_freeze_receipt(Path("work/app-server-development-v2/unattended-pipeline-v5/judge-v5_4-freeze-receipt-v1.json"))'

/usr/bin/python3 -m research_factory.app_server_runtime_lock \
  --repo-root "$REPO_ROOT" \
  --manifest "$RUNTIME_LOCK"

exec /usr/bin/python3 -m research_factory.app_server_judge_v5_calibration_runner \
  --output-dir "$CALIBRATION_ROOT" \
  --judge-freeze-receipt "$JUDGE_FREEZE_RECEIPT" \
  --model gpt-5.6-sol \
  --reasoning-effort high \
  --timeout-seconds 1200
