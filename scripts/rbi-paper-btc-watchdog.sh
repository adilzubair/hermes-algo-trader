#!/usr/bin/env bash
set -euo pipefail

ROOT="/tmp/Harvard-Algorithmic-Trading-with-AI"
LOCK_FILE="$ROOT/outputs/runtime/paper-btc-watchdog.lock"
START_SCRIPT="$ROOT/scripts/rbi-paper-btc-start.sh"
STATUS_SCRIPT="$ROOT/scripts/rbi-paper-btc-status.sh"

mkdir -p "$ROOT/outputs/runtime"

exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  exit 0
fi

if "$STATUS_SCRIPT" >/dev/null 2>&1; then
  exit 0
fi

"$START_SCRIPT" >> "$ROOT/outputs/runtime/paper-btc-watchdog.log" 2>&1
