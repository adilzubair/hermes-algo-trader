from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from rbi_trader.config import AppConfig, ExecutionConfig, NotificationConfig, ResearchConfig, RiskConfig
from rbi_trader.pipeline import emit_json, run_trade_cycle


class FakeBroker:
    def __init__(self, *_args, **_kwargs) -> None:
        self.closed = False

    def get_position(self, _symbol: str):
        return None

    def list_open_orders(self, _symbol: str):
        return [{"id": "open-1"}]

    def preflight(self, symbol: str):
        return {
            "account_id": "acct-1",
            "paper": True,
            "status": "ACTIVE",
            "buying_power": 1000.0,
            "equity": 1000.0,
            "last_equity": 1000.0,
            "cash": 1000.0,
            "multiplier": "1",
            "asset": symbol,
        }

    def close_position(self, _symbol: str):
        self.closed = True
        return {"closed": True}

    def submit_entry(self, _symbol: str, _signal: int, _last_price: float):
        return {"submitted": True}


class LossBroker(FakeBroker):
    def list_open_orders(self, _symbol: str):
        return []

    def preflight(self, symbol: str):
        payload = super().preflight(symbol)
        payload["equity"] = 970.0
        return payload


def sample_data() -> pd.DataFrame:
    return pd.read_csv(Path("backtest/data/BTC-6h-1000wks-data.csv")).tail(600)


def make_config(tmp_path: Path) -> AppConfig:
    return AppConfig(
        research=ResearchConfig(lookback_bars=300),
        risk=RiskConfig(
            trade_notional_usd=25,
            max_position_notional_usd=25,
            max_daily_loss_pct=2.0,
            min_buying_power_usd=25,
            stale_data_minutes=10_000_000,
            require_flat_before_entry=True,
        ),
        execution=ExecutionConfig(
            symbol="BTC/USD",
            asset_class="crypto",
            timeframe="6Hour",
            dry_run=False,
            state_path=str(tmp_path / "state.json"),
            journal_path=str(tmp_path / "journal.jsonl"),
        ),
        notifications=NotificationConfig(enabled=False, log_path=str(tmp_path / "alerts.jsonl")),
    )


def test_trade_cycle_blocks_on_open_orders(monkeypatch, tmp_path: Path) -> None:
    config = make_config(tmp_path)
    monkeypatch.setattr("rbi_trader.pipeline.AlpacaBroker", FakeBroker)
    monkeypatch.setattr(
        "rbi_trader.pipeline.AlpacaDataProvider.fetch_bars",
        lambda self, symbol, timeframe, lookback_bars: sample_data(),
    )

    result = run_trade_cycle(config)

    assert result["action"] == "skip"
    assert result["reason"] == "risk_open_orders_present"
    assert result["open_order_count"] == 1


def test_trade_cycle_sets_kill_switch_on_daily_loss(monkeypatch, tmp_path: Path) -> None:
    config = make_config(tmp_path)
    state_path = Path(config.execution.state_path)
    state_path.write_text('{"trading_day": "%s", "day_start_equity": 1000.0}' % datetime.now(timezone.utc).date().isoformat())
    monkeypatch.setattr("rbi_trader.pipeline.AlpacaBroker", LossBroker)
    monkeypatch.setattr(
        "rbi_trader.pipeline.AlpacaDataProvider.fetch_bars",
        lambda self, symbol, timeframe, lookback_bars: sample_data(),
    )

    result = run_trade_cycle(config)
    updated_state = state_path.read_text()

    assert result["reason"] == "kill_daily_loss_limit"
    assert result["risk"]["kill_switch"] is True
    assert '"kill_switch": true' in updated_state.lower()


class BrokenPipeStream:
    def write(self, _text: str) -> int:
        raise BrokenPipeError(32, "Broken pipe")

    def flush(self) -> None:
        raise AssertionError("flush should not run after a broken write")


def test_emit_json_swallow_broken_pipe() -> None:
    emit_json({"ok": True}, output_stream=BrokenPipeStream())
