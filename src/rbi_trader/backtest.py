from __future__ import annotations

import itertools
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from rbi_trader.config import ResearchConfig, StrategyConfig, strategy_with_overrides
from rbi_trader.strategy import apply_strategy, normalize_ohlcv


@dataclass(slots=True)
class Trade:
    entry_time: str
    exit_time: str
    side: str
    entry_price: float
    exit_price: float
    return_pct: float
    exit_reason: str


def _max_drawdown(equity_curve: list[float]) -> float:
    if not equity_curve:
        return 0.0
    series = pd.Series(equity_curve)
    running_max = series.cummax()
    drawdown = (series / running_max) - 1
    return float(drawdown.min() * 100)


def run_backtest(data: pd.DataFrame, config: StrategyConfig, initial_equity: float = 100_000.0) -> dict[str, Any]:
    frame = apply_strategy(data, config).dropna().copy()
    if len(frame) < 3:
        return {
            "trade_count": 0,
            "win_rate_pct": 0.0,
            "total_return_pct": 0.0,
            "profit_factor": 0.0,
            "max_drawdown_pct": 0.0,
            "trades": [],
        }

    equity = initial_equity
    equity_curve = [equity]
    trades: list[Trade] = []
    position: dict[str, Any] | None = None

    for idx in range(1, len(frame) - 1):
        current = frame.iloc[idx]
        current_time = frame.index[idx]
        next_bar = frame.iloc[idx + 1]
        next_time = frame.index[idx + 1]

        if position is not None:
            exit_price = None
            exit_reason = None
            if position["side"] == "long":
                stop_hit = current["low"] <= position["stop_price"]
                take_hit = current["high"] >= position["take_profit_price"]
                if stop_hit:
                    exit_price = position["stop_price"]
                    exit_reason = "stop_loss"
                elif take_hit:
                    exit_price = position["take_profit_price"]
                    exit_reason = "take_profit"
                elif current["signal"] == -1:
                    exit_price = float(next_bar["open"])
                    exit_reason = "reverse_signal"
            else:
                stop_hit = current["high"] >= position["stop_price"]
                take_hit = current["low"] <= position["take_profit_price"]
                if stop_hit:
                    exit_price = position["stop_price"]
                    exit_reason = "stop_loss"
                elif take_hit:
                    exit_price = position["take_profit_price"]
                    exit_reason = "take_profit"
                elif current["signal"] == 1:
                    exit_price = float(next_bar["open"])
                    exit_reason = "reverse_signal"

            if exit_price is not None:
                direction = 1 if position["side"] == "long" else -1
                trade_return = ((exit_price - position["entry_price"]) / position["entry_price"]) * direction
                equity *= 1 + trade_return
                equity_curve.append(equity)
                trades.append(
                    Trade(
                        entry_time=position["entry_time"].isoformat(),
                        exit_time=current_time.isoformat(),
                        side=position["side"],
                        entry_price=float(position["entry_price"]),
                        exit_price=float(exit_price),
                        return_pct=float(trade_return * 100),
                        exit_reason=str(exit_reason),
                    )
                )
                position = None

        if position is None and current["signal"] != 0:
            if current["signal"] == -1 and not config.allow_short:
                continue
            side = "long" if current["signal"] == 1 else "short"
            entry_price = float(next_bar["open"])
            if side == "long":
                stop_price = entry_price * (1 - config.stop_loss_pct)
                take_profit_price = entry_price * (1 + config.take_profit_pct)
            else:
                stop_price = entry_price * (1 + config.stop_loss_pct)
                take_profit_price = entry_price * (1 - config.take_profit_pct)
            position = {
                "side": side,
                "entry_price": entry_price,
                "entry_time": next_time,
                "stop_price": stop_price,
                "take_profit_price": take_profit_price,
            }

    if position is not None:
        final_close = float(frame.iloc[-1]["close"])
        direction = 1 if position["side"] == "long" else -1
        trade_return = ((final_close - position["entry_price"]) / position["entry_price"]) * direction
        equity *= 1 + trade_return
        equity_curve.append(equity)
        trades.append(
            Trade(
                entry_time=position["entry_time"].isoformat(),
                exit_time=frame.index[-1].isoformat(),
                side=position["side"],
                entry_price=float(position["entry_price"]),
                exit_price=final_close,
                return_pct=float(trade_return * 100),
                exit_reason="end_of_test",
            )
        )

    returns = [trade.return_pct for trade in trades]
    wins = [trade for trade in trades if trade.return_pct > 0]
    losses = [trade for trade in trades if trade.return_pct < 0]
    profit_factor = 0.0
    gross_profit = sum(t.return_pct for t in wins)
    gross_loss = abs(sum(t.return_pct for t in losses))
    if gross_loss > 0:
        profit_factor = gross_profit / gross_loss
    elif gross_profit > 0:
        profit_factor = float("inf")

    return {
        "trade_count": len(trades),
        "win_rate_pct": float((len(wins) / len(trades)) * 100) if trades else 0.0,
        "total_return_pct": float(((equity / initial_equity) - 1) * 100),
        "profit_factor": float(profit_factor if np.isfinite(profit_factor) else 999.0),
        "max_drawdown_pct": _max_drawdown(equity_curve),
        "final_equity": float(equity),
        "strategy": asdict(config),
        "trades": [asdict(trade) for trade in trades],
    }


def _candidate_overrides(research: ResearchConfig) -> list[dict[str, Any]]:
    keys = list(research.parameter_grid.keys())
    values = [research.parameter_grid[key] for key in keys]
    candidates = [dict(zip(keys, combo)) for combo in itertools.product(*values)]
    return candidates[: research.max_candidates]


def optimize_strategy(data: pd.DataFrame, strategy: StrategyConfig, research: ResearchConfig) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    frame = normalize_ohlcv(data)
    split_idx = max(int(len(frame) * research.train_fraction), 50)
    train = frame.iloc[:split_idx]
    test = frame.iloc[split_idx:]
    leaderboard: list[dict[str, Any]] = []

    for overrides in _candidate_overrides(research):
        candidate = strategy_with_overrides(strategy, overrides)
        train_result = run_backtest(train, candidate)
        test_result = run_backtest(test, candidate)
        row = {
            "params": overrides,
            "train_trade_count": train_result["trade_count"],
            "train_total_return_pct": train_result["total_return_pct"],
            "train_profit_factor": train_result["profit_factor"],
            "train_max_drawdown_pct": train_result["max_drawdown_pct"],
            "test_trade_count": test_result["trade_count"],
            "test_total_return_pct": test_result["total_return_pct"],
            "test_profit_factor": test_result["profit_factor"],
            "test_max_drawdown_pct": test_result["max_drawdown_pct"],
        }
        leaderboard.append(row)

    ranked = sorted(
        leaderboard,
        key=lambda row: (row[f"test_{research.metric}"], row["train_profit_factor"]),
        reverse=True,
    )
    best = ranked[0] if ranked else {"params": {}}
    return best, ranked


def save_research_report(data: pd.DataFrame, strategy: StrategyConfig, research: ResearchConfig) -> dict[str, Any]:
    best, leaderboard = optimize_strategy(data, strategy, research)
    output_dir = Path(research.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    best_path = output_dir / "latest_best_params.json"
    leaderboard_path = output_dir / "leaderboard.csv"
    summary_path = output_dir / "summary.json"

    best_path.write_text(json.dumps(best, indent=2))
    pd.DataFrame(leaderboard).to_csv(leaderboard_path, index=False)
    summary = {
        "best": best,
        "candidate_count": len(leaderboard),
        "leaderboard_path": str(leaderboard_path),
        "best_path": str(best_path),
    }
    summary_path.write_text(json.dumps(summary, indent=2))
    return summary
