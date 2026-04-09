from __future__ import annotations

from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

import yaml


@dataclass(slots=True)
class StrategyConfig:
    bb_window: int = 20
    bb_std: float = 2.0
    keltner_window: int = 20
    keltner_atr_mult: float = 1.5
    adx_period: int = 14
    adx_threshold: float = 25.0
    take_profit_pct: float = 0.05
    stop_loss_pct: float = 0.03
    allow_short: bool = False


@dataclass(slots=True)
class ResearchConfig:
    lookback_bars: int = 1500
    train_fraction: float = 0.7
    csv_path: str | None = None
    parameter_grid: dict[str, list[Any]] = field(
        default_factory=lambda: {
            "bb_window": [18, 20, 22],
            "bb_std": [1.8, 2.0, 2.2],
            "keltner_window": [18, 20, 22],
            "keltner_atr_mult": [1.2, 1.5, 1.8],
            "adx_period": [12, 14, 16],
            "adx_threshold": [20, 25, 30],
            "take_profit_pct": [0.04, 0.05, 0.06],
            "stop_loss_pct": [0.02, 0.03, 0.04],
        }
    )
    max_candidates: int = 30
    metric: str = "total_return_pct"
    output_dir: str = "outputs/research"


@dataclass(slots=True)
class RiskConfig:
    trade_notional_usd: float = 1000.0
    max_position_notional_usd: float = 1000.0
    max_daily_loss_pct: float = 2.0
    min_buying_power_usd: float = 100.0
    max_spread_bps: float = 40.0
    stale_data_minutes: int = 720
    allow_live: bool = False
    paper_only: bool = True
    require_flat_before_entry: bool = True


@dataclass(slots=True)
class ExecutionConfig:
    symbol: str = "BTC/USD"
    asset_class: str = "crypto"
    timeframe: str = "6Hour"
    poll_seconds: int = 60
    dry_run: bool = True
    use_limit_orders: bool = False
    limit_price_buffer_bps: float = 10.0
    extended_hours: bool = False
    data_feed: str = "iex"
    state_path: str = "outputs/runtime/state.json"
    journal_path: str = "outputs/runtime/journal.jsonl"


@dataclass(slots=True)
class AlpacaConfig:
    api_key_env: str = "ALPACA_API_KEY"
    secret_key_env: str = "ALPACA_SECRET_KEY"
    paper: bool = True


@dataclass(slots=True)
class NotificationConfig:
    enabled: bool = True
    notify_all_decisions: bool = False
    webhook_url_env: str = "RBI_WEBHOOK_URL"
    telegram_bot_token_env: str = "RBI_TELEGRAM_BOT_TOKEN"
    telegram_chat_id_env: str = "RBI_TELEGRAM_CHAT_ID"
    log_path: str = "outputs/runtime/alerts.jsonl"


@dataclass(slots=True)
class AppConfig:
    strategy: StrategyConfig = field(default_factory=StrategyConfig)
    research: ResearchConfig = field(default_factory=ResearchConfig)
    risk: RiskConfig = field(default_factory=RiskConfig)
    execution: ExecutionConfig = field(default_factory=ExecutionConfig)
    alpaca: AlpacaConfig = field(default_factory=AlpacaConfig)
    notifications: NotificationConfig = field(default_factory=NotificationConfig)


def _section(data: dict[str, Any], key: str) -> dict[str, Any]:
    value = data.get(key, {})
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise TypeError(f"Config section '{key}' must be a mapping.")
    return value


def load_config(path: str | Path) -> AppConfig:
    config_path = Path(path)
    raw = yaml.safe_load(config_path.read_text()) or {}
    if not isinstance(raw, dict):
        raise TypeError("Top-level config must be a mapping.")
    return AppConfig(
        strategy=StrategyConfig(**_section(raw, "strategy")),
        research=ResearchConfig(**_section(raw, "research")),
        risk=RiskConfig(**_section(raw, "risk")),
        execution=ExecutionConfig(**_section(raw, "execution")),
        alpaca=AlpacaConfig(**_section(raw, "alpaca")),
        notifications=NotificationConfig(**_section(raw, "notifications")),
    )


def strategy_with_overrides(config: StrategyConfig, overrides: dict[str, Any] | None) -> StrategyConfig:
    if not overrides:
        return config
    return replace(config, **overrides)
