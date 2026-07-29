#!/bin/zsh
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RECOVERY_ROOT="$REPO_ROOT/work/app-server-development-v2/matrix-v1/recovery-batch-5-same-v1"
PIPELINE_ROOT="$REPO_ROOT/work/app-server-development-v2/unattended-pipeline-v1"
RUNTIME_LOCK="$REPO_ROOT/work/app-server-development-v2/unattended-runtime-lock-v2.json"

cd "$REPO_ROOT"
unset OPENAI_API_KEY

if [[ -e "$RECOVERY_ROOT/STOP" || -e "$PIPELINE_ROOT/STOP" ]]; then
  print -u2 -- "A STOP sentinel is present; refusing to start unattended evaluation."
  exit 2
fi

/usr/bin/python3 -m research_factory.app_server_runtime_lock \
  --repo-root "$REPO_ROOT" \
  --manifest "$RUNTIME_LOCK"

/usr/bin/python3 -m research_factory.app_server_interrupted_arm_recovery \
  --repo-root "$REPO_ROOT" \
  --run-spec "$REPO_ROOT/work/app-server-development-v2/run-spec-v2.json" \
  --matrix-root "$REPO_ROOT/work/app-server-development-v2/matrix-v1" \
  --not-before-epoch 1783840520 \
  --boundary-grace-seconds 30

/usr/bin/python3 -m research_factory.app_server_runtime_lock \
  --repo-root "$REPO_ROOT" \
  --manifest "$RUNTIME_LOCK"

exec /usr/bin/python3 -m research_factory.unattended_app_server_pipeline \
  --repo-root "$REPO_ROOT" \
  --pipeline-root "$PIPELINE_ROOT"
