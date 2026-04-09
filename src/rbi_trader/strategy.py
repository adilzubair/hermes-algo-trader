from __future__ import annotations

from dataclasses import asdict
from typing import Any

import numpy as np
import pandas as pd

from rbi_trader.config import StrategyConfig

REQUIRED_COLUMNS = ["open", "high", "low", "close", "volume"]


def normalize_ohlcv(data: pd.DataFrame) -> pd.DataFrame:
    frame = data.copy()
    frame.columns = [str(col).lower() for col in frame.columns]
    if "datetime" in frame.columns and "timestamp" not in frame.columns:
        frame = frame.rename(columns={"datetime": "timestamp"})
    if "timestamp" in frame.columns:
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
        frame = frame.set_index("timestamp")
    if not isinstance(frame.index, pd.DatetimeIndex):
        raise ValueError("OHLCV data must have a DatetimeIndex or a timestamp column.")
    frame.index = pd.to_datetime(frame.index, utc=True)
    missing = [col for col in REQUIRED_COLUMNS if col not in frame.columns]
    if missing:
        raise ValueError(f"OHLCV data missing columns: {missing}")
    frame = frame[REQUIRED_COLUMNS].astype(float)
    frame = frame.sort_index()
    frame = frame[~frame.index.duplicated(keep="last")]
    return frame


def _true_range(frame: pd.DataFrame) -> pd.Series:
    prev_close = frame["close"].shift(1)
    ranges = pd.concat(
        [
            frame["high"] - frame["low"],
            (frame["high"] - prev_close).abs(),
            (frame["low"] - prev_close).abs(),
        ],
        axis=1,
    )
    return ranges.max(axis=1)


def compute_atr(frame: pd.DataFrame, period: int) -> pd.Series:
    tr = _true_range(frame)
    return tr.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()


def compute_adx(frame: pd.DataFrame, period: int) -> pd.Series:
    up_move = frame["high"].diff()
    down_move = -frame["low"].diff()
    plus_dm = pd.Series(np.where((up_move > down_move) & (up_move > 0), up_move, 0.0), index=frame.index)
    minus_dm = pd.Series(np.where((down_move > up_move) & (down_move > 0), down_move, 0.0), index=frame.index)
    atr = compute_atr(frame, period)
    plus_di = 100 * plus_dm.ewm(alpha=1 / period, adjust=False, min_periods=period).mean() / atr
    minus_di = 100 * minus_dm.ewm(alpha=1 / period, adjust=False, min_periods=period).mean() / atr
    dx = ((plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)) * 100
    return dx.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()


def apply_strategy(data: pd.DataFrame, config: StrategyConfig) -> pd.DataFrame:
    frame = normalize_ohlcv(data)
    frame["middle_bb"] = frame["close"].rolling(config.bb_window).mean()
    std = frame["close"].rolling(config.bb_window).std(ddof=0)
    frame["upper_bb"] = frame["middle_bb"] + config.bb_std * std
    frame["lower_bb"] = frame["middle_bb"] - config.bb_std * std
    frame["atr"] = compute_atr(frame, config.keltner_window)
    frame["keltner_middle"] = frame["close"].rolling(config.keltner_window).mean()
    frame["upper_kc"] = frame["keltner_middle"] + config.keltner_atr_mult * frame["atr"]
    frame["lower_kc"] = frame["keltner_middle"] - config.keltner_atr_mult * frame["atr"]
    frame["adx"] = compute_adx(frame, config.adx_period)
    frame["squeeze"] = (frame["upper_bb"] < frame["upper_kc"]) & (frame["lower_bb"] > frame["lower_kc"])
    previous_squeeze = frame["squeeze"].shift(1)
    previous_squeeze = previous_squeeze.where(previous_squeeze.notna(), False).astype(bool)
    frame["squeeze_released"] = previous_squeeze & ~frame["squeeze"]
    frame["long_signal"] = frame["squeeze_released"] & (frame["adx"] > config.adx_threshold) & (frame["close"] > frame["upper_bb"])
    frame["short_signal"] = frame["squeeze_released"] & (frame["adx"] > config.adx_threshold) & (frame["close"] < frame["lower_bb"])
    frame["signal"] = np.select([frame["long_signal"], frame["short_signal"]], [1, -1], default=0)
    frame["stop_price_long"] = frame["close"] * (1 - config.stop_loss_pct)
    frame["take_profit_long"] = frame["close"] * (1 + config.take_profit_pct)
    frame["stop_price_short"] = frame["close"] * (1 + config.stop_loss_pct)
    frame["take_profit_short"] = frame["close"] * (1 - config.take_profit_pct)
    return frame


def latest_signal(data: pd.DataFrame, config: StrategyConfig) -> dict[str, Any]:
    frame = apply_strategy(data, config).dropna().copy()
    if frame.empty:
        return {
            "signal": 0,
            "signal_name": "flat",
            "reason": "not-enough-data",
            "strategy": asdict(config),
        }
    row = frame.iloc[-1]
    signal = int(row["signal"])
    signal_name = {1: "long", -1: "short", 0: "flat"}[signal]
    return {
        "timestamp": frame.index[-1].isoformat(),
        "signal": signal,
        "signal_name": signal_name,
        "close": float(row["close"]),
        "adx": float(row["adx"]),
        "squeeze": bool(row["squeeze"]),
        "squeeze_released": bool(row["squeeze_released"]),
        "upper_bb": float(row["upper_bb"]),
        "lower_bb": float(row["lower_bb"]),
        "stop_price_long": float(row["stop_price_long"]),
        "take_profit_long": float(row["take_profit_long"]),
        "stop_price_short": float(row["stop_price_short"]),
        "take_profit_short": float(row["take_profit_short"]),
        "strategy": asdict(config),
    }
