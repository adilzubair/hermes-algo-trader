#!/usr/bin/env bash
set -euo pipefail

ROOT="/tmp/Harvard-Algorithmic-Trading-with-AI"
PID_FILE="$ROOT/outputs/runtime/paper-btc.pid"

if [[ ! -f "$PID_FILE" ]]; then
  echo "not running"
  exit 0
fi

pid="$(cat "$PID_FILE")"
if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
  kill "$pid"
  for _ in {1..20}; do
    if ! kill -0 "$pid" 2>/dev/null; then
      rm -f "$PID_FILE"
      echo "stopped: pid=$pid"
      exit 0
    fi
    sleep 1
  done
  kill -9 "$pid" 2>/dev/null || true
  echo "force-stopped: pid=$pid"
else
  echo "stale pid file: $pid"
fi

rm -f "$PID_FILE"
