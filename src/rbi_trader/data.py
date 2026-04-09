from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from alpaca.data.enums import CryptoFeed, DataFeed
from alpaca.data.historical import CryptoHistoricalDataClient, StockHistoricalDataClient
from alpaca.data.requests import CryptoBarsRequest, StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit

from rbi_trader.config import AlpacaConfig, ExecutionConfig, ResearchConfig
from rbi_trader.strategy import normalize_ohlcv

_TIMEFRAME_UNITS = {
    "min": TimeFrameUnit.Minute,
    "minute": TimeFrameUnit.Minute,
    "hour": TimeFrameUnit.Hour,
    "day": TimeFrameUnit.Day,
    "week": TimeFrameUnit.Week,
    "month": TimeFrameUnit.Month,
}


def parse_timeframe(value: str) -> TimeFrame:
    match = re.fullmatch(r"(\d+)([A-Za-z]+)", value.strip())
    if not match:
        raise ValueError(f"Unsupported timeframe format: {value}")
    amount = int(match.group(1))
    unit_key = match.group(2).lower()
    if unit_key.endswith("s"):
        unit_key = unit_key[:-1]
    if unit_key not in _TIMEFRAME_UNITS:
        raise ValueError(f"Unsupported timeframe unit: {value}")
    return TimeFrame(amount, _TIMEFRAME_UNITS[unit_key])


class AlpacaDataProvider:
    def __init__(self, alpaca_config: AlpacaConfig, execution_config: ExecutionConfig) -> None:
        self.alpaca_config = alpaca_config
        self.execution_config = execution_config
        self.asset_class = execution_config.asset_class.lower()
        self.stock_client: StockHistoricalDataClient | None = None
        self.crypto_client: CryptoHistoricalDataClient | None = None

    def _stock_feed(self) -> DataFeed:
        return DataFeed(self.execution_config.data_feed)

    def _get_stock_client(self) -> StockHistoricalDataClient:
        if self.stock_client is None:
            self.stock_client = StockHistoricalDataClient()
        return self.stock_client

    def _get_crypto_client(self) -> CryptoHistoricalDataClient:
        if self.crypto_client is None:
            self.crypto_client = CryptoHistoricalDataClient()
        return self.crypto_client

    def fetch_bars(
        self,
        symbol: str,
        timeframe: str,
        lookback_bars: int,
        end: datetime | None = None,
    ) -> pd.DataFrame:
        end = end or datetime.now(timezone.utc)
        tf = parse_timeframe(timeframe)
        start = end - _lookback_delta(tf, lookback_bars)
        if self.asset_class == "crypto":
            request = CryptoBarsRequest(
                symbol_or_symbols=symbol,
                timeframe=tf,
                start=start,
                end=end,
                limit=lookback_bars,
            )
            bars = self._get_crypto_client().get_crypto_bars(request, feed=CryptoFeed.US)
        else:
            request = StockBarsRequest(
                symbol_or_symbols=symbol,
                timeframe=tf,
                start=start,
                end=end,
                limit=lookback_bars,
                feed=self._stock_feed(),
            )
            bars = self._get_stock_client().get_stock_bars(request)

        frame = bars.df.reset_index()
        if "symbol" in frame.columns:
            frame = frame[frame["symbol"] == symbol]
            frame = frame.drop(columns=["symbol"])
        frame = frame.rename(columns={"timestamp": "timestamp", "trade_count": "trade_count"})
        columns = [col for col in ["timestamp", "open", "high", "low", "close", "volume"] if col in frame.columns]
        return normalize_ohlcv(frame[columns])

    def fetch_research_dataset(self, symbol: str, timeframe: str, research: ResearchConfig) -> pd.DataFrame:
        if research.csv_path:
            csv_path = Path(research.csv_path)
            if csv_path.exists():
                return normalize_ohlcv(pd.read_csv(csv_path))
        return self.fetch_bars(symbol=symbol, timeframe=timeframe, lookback_bars=research.lookback_bars)


def _lookback_delta(timeframe: TimeFrame, bars: int) -> timedelta:
    minutes_per_unit = {
        TimeFrameUnit.Minute: 1,
        TimeFrameUnit.Hour: 60,
        TimeFrameUnit.Day: 60 * 24,
        TimeFrameUnit.Week: 60 * 24 * 7,
        TimeFrameUnit.Month: 60 * 24 * 30,
    }
    minutes = timeframe.amount * minutes_per_unit[timeframe.unit_value] * bars
    return timedelta(minutes=minutes + 60)
