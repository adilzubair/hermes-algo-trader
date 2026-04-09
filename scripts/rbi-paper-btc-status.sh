#!/usr/bin/env bash
set -euo pipefail

ROOT="/tmp/Harvard-Algorithmic-Trading-with-AI"
PID_FILE="$ROOT/outputs/runtime/paper-btc.pid"

if [[ ! -f "$PID_FILE" ]]; then
  echo "not running"
  exit 1
fi

pid="$(cat "$PID_FILE")"
if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
  ps -p "$pid" -o pid=,etime=,cmd=
  exit 0
fi

echo "stale pid file: $pid"
exit 1
