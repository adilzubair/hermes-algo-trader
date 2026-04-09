from __future__ import annotations

import argparse

from dotenv import load_dotenv

from rbi_trader.config import load_config
from rbi_trader.pipeline import emit_json, run_research, run_trade_cycle, run_trade_loop


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="RBI Trader CLI")
    parser.add_argument("command", choices=["research", "trade-once", "trade-loop"], help="Command to run")
    parser.add_argument("--config", default="config/pipeline.example.yaml", help="Path to YAML config")
    return parser


def main() -> None:
    load_dotenv()
    args = build_parser().parse_args()
    config = load_config(args.config)
    if args.command == "research":
        emit_json(run_research(config))
        return
    if args.command == "trade-once":
        emit_json(run_trade_cycle(config))
        return
    run_trade_loop(config)


if __name__ == "__main__":
    main()
