#!/usr/bin/env bash
set -euo pipefail

ROOT="/tmp/Harvard-Algorithmic-Trading-with-AI"
cd "$ROOT"

mkdir -p outputs/runtime

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

export PYTHONPATH="$ROOT/src${PYTHONPATH:+:$PYTHONPATH}"
exec python3 -u -m rbi_trader.cli trade-loop --config config/paper-btc.yaml >> "$ROOT/outputs/runtime/paper-btc-loop.log" 2>&1
