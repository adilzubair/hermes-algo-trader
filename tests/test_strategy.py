from pathlib import Path

import pandas as pd

from rbi_trader.config import StrategyConfig
from rbi_trader.strategy import apply_strategy, latest_signal


def load_sample() -> pd.DataFrame:
    path = Path("backtest/data/BTC-6h-1000wks-data.csv")
    return pd.read_csv(path)


def test_apply_strategy_adds_expected_columns() -> None:
    data = load_sample().head(400)
    result = apply_strategy(data, StrategyConfig())
    for column in ["upper_bb", "lower_bb", "upper_kc", "lower_kc", "adx", "signal"]:
        assert column in result.columns
    assert result["signal"].isin([-1, 0, 1]).all()


def test_latest_signal_shape() -> None:
    data = load_sample().head(400)
    signal = latest_signal(data, StrategyConfig())
    assert signal["signal_name"] in {"long", "short", "flat"}
    assert "timestamp" in signal or signal["reason"] == "not-enough-data"
