#!/usr/bin/env bash
set -euo pipefail

ROOT="/tmp/Harvard-Algorithmic-Trading-with-AI"
RUN_SCRIPT="$ROOT/scripts/rbi-paper-btc-run.sh"
PID_FILE="$ROOT/outputs/runtime/paper-btc.pid"

cd "$ROOT"
mkdir -p outputs/runtime

if [[ -f "$PID_FILE" ]]; then
  pid="$(cat "$PID_FILE")"
  if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
    echo "already running: pid=$pid"
    exit 0
  fi
  rm -f "$PID_FILE"
fi

nohup "$RUN_SCRIPT" </dev/null >/dev/null 2>&1 &
pid=$!
echo "$pid" > "$PID_FILE"
echo "started: pid=$pid"
