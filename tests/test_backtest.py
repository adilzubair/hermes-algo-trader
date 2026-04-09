from pathlib import Path

import pandas as pd

from rbi_trader.backtest import optimize_strategy, run_backtest
from rbi_trader.config import ResearchConfig, StrategyConfig


def load_sample() -> pd.DataFrame:
    path = Path("backtest/data/BTC-6h-1000wks-data.csv")
    return pd.read_csv(path).head(1200)


def test_run_backtest_returns_metrics() -> None:
    result = run_backtest(load_sample(), StrategyConfig())
    assert result["trade_count"] >= 0
    assert "total_return_pct" in result
    assert "max_drawdown_pct" in result


def test_optimize_strategy_returns_leaderboard() -> None:
    research = ResearchConfig(
        lookback_bars=400,
        max_candidates=4,
        parameter_grid={
            "bb_window": [18, 20],
            "bb_std": [1.8],
            "keltner_window": [18],
            "keltner_atr_mult": [1.2],
            "adx_period": [12],
            "adx_threshold": [20, 25],
            "take_profit_pct": [0.04],
            "stop_loss_pct": [0.02],
        },
    )
    best, leaderboard = optimize_strategy(load_sample(), StrategyConfig(), research)
    assert leaderboard
    assert "params" in best
