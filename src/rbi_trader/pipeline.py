from __future__ import annotations

import json
import os
import sys
import time
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from rbi_trader.backtest import run_backtest, save_research_report
from rbi_trader.broker import AlpacaBroker
from rbi_trader.config import AppConfig, StrategyConfig, strategy_with_overrides
from rbi_trader.data import AlpacaDataProvider
from rbi_trader.notifier import Notifier
from rbi_trader.strategy import apply_strategy, latest_signal


def _ensure_parent(path: str) -> Path:
    file_path = Path(path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    return file_path


def _load_json(path: str, default: dict[str, Any]) -> dict[str, Any]:
    file_path = Path(path)
    if not file_path.exists():
        return default
    return json.loads(file_path.read_text())


def _write_json(path: str, payload: dict[str, Any]) -> None:
    file_path = _ensure_parent(path)
    file_path.write_text(json.dumps(payload, indent=2))


def _append_journal(path: str, payload: dict[str, Any]) -> None:
    file_path = _ensure_parent(path)
    with file_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload) + "\n")


def _redirect_broken_stdout(stream: Any) -> None:
    if stream is not sys.stdout:
        return
    try:
        devnull = os.open(os.devnull, os.O_WRONLY)
        os.dup2(devnull, sys.stdout.fileno())
        os.close(devnull)
    except OSError:
        pass



def emit_json(payload: dict[str, Any], output_stream: Any = None) -> None:
    stream = sys.stdout if output_stream is None else output_stream
    try:
        stream.write(json.dumps(payload, indent=2) + "\n")
        stream.flush()
    except BrokenPipeError:
        _redirect_broken_stdout(stream)
        return


def load_best_strategy(config: AppConfig) -> StrategyConfig:
    best_path = Path(config.research.output_dir) / "latest_best_params.json"
    if not best_path.exists():
        return config.strategy
    payload = json.loads(best_path.read_text())
    return strategy_with_overrides(config.strategy, payload.get("params", {}))


def run_research(config: AppConfig) -> dict[str, Any]:
    provider = AlpacaDataProvider(config.alpaca, config.execution)
    data = provider.fetch_research_dataset(config.execution.symbol, config.execution.timeframe, config.research)
    summary = save_research_report(data, config.strategy, config.research)
    backtest_result = run_backtest(data, load_best_strategy(config))
    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "symbol": config.execution.symbol,
        "timeframe": config.execution.timeframe,
        "research": summary,
        "best_backtest": backtest_result,
    }
    summary_path = Path(config.research.output_dir) / "latest_run.json"
    summary_path.write_text(json.dumps(payload, indent=2))
    return payload


def _stale(last_bar: pd.Timestamp, max_age_minutes: int) -> bool:
    age = datetime.now(timezone.utc) - last_bar.to_pydatetime()
    return age.total_seconds() / 60 > max_age_minutes


def _today_utc() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _update_daily_state(state: dict[str, Any], preflight: dict[str, Any] | None) -> None:
    today = _today_utc()
    if state.get("trading_day") != today:
        state["trading_day"] = today
        if preflight is not None:
            state["day_start_equity"] = preflight["equity"]
        state["kill_switch"] = False
        state["kill_reason"] = None


def _daily_loss_pct(state: dict[str, Any], preflight: dict[str, Any] | None) -> float:
    if preflight is None:
        return 0.0
    start_equity = float(state.get("day_start_equity", preflight["equity"]))
    if start_equity <= 0:
        return 0.0
    return max(0.0, ((start_equity - float(preflight["equity"])) / start_equity) * 100)


def _should_notify(config: AppConfig, decision: dict[str, Any]) -> bool:
    if config.notifications.notify_all_decisions:
        return True
    if decision.get("action") in {"enter_position", "close_position", "error"}:
        return True
    return str(decision.get("reason", "")).startswith(("risk_", "kill_"))


def _severity_for_payload(payload: dict[str, Any]) -> str:
    action = payload.get("action")
    reason = str(payload.get("reason", ""))
    if action == "error" or reason.startswith("kill_"):
        return "critical"
    if action in {"enter_position", "close_position"} or reason.startswith("risk_"):
        return "warning"
    return "info"


def _notify(config: AppConfig, event_type: str, title: str, payload: dict[str, Any]) -> dict[str, Any] | None:
    if not _should_notify(config, payload):
        return None
    notifier = Notifier(config.notifications)
    return notifier.send(event_type, title, payload, severity=_severity_for_payload(payload))


def run_trade_cycle(config: AppConfig) -> dict[str, Any]:
    if not config.execution.dry_run and config.risk.paper_only and not config.alpaca.paper:
        raise RuntimeError("Paper-only mode is enabled. Refusing non-paper execution.")
    if not config.execution.dry_run and not config.alpaca.paper and not config.risk.allow_live:
        raise RuntimeError("Live execution is disabled by config.")

    provider = AlpacaDataProvider(config.alpaca, config.execution)
    strategy = load_best_strategy(config)
    offline_dry_run = bool(config.execution.dry_run and config.research.csv_path and Path(config.research.csv_path).exists())
    if offline_dry_run:
        data = provider.fetch_research_dataset(config.execution.symbol, config.execution.timeframe, config.research)
    else:
        data = provider.fetch_bars(config.execution.symbol, config.execution.timeframe, config.research.lookback_bars)
    enriched = apply_strategy(data, strategy).dropna().copy()
    if enriched.empty:
        raise RuntimeError("Not enough bars to evaluate strategy.")

    last_bar_time = enriched.index[-1]
    if not offline_dry_run and _stale(last_bar_time, config.risk.stale_data_minutes):
        raise RuntimeError(f"Latest bar is stale: {last_bar_time.isoformat()}")

    state = _load_json(config.execution.state_path, default={})
    last_processed = state.get("last_processed_bar")
    signal_payload = latest_signal(enriched, strategy)
    decision: dict[str, Any] = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "bar_timestamp": last_bar_time.isoformat(),
        "symbol": config.execution.symbol,
        "timeframe": config.execution.timeframe,
        "signal": signal_payload,
        "mode": "dry_run" if config.execution.dry_run else "execution",
        "action": "skip",
    }

    if last_processed == last_bar_time.isoformat():
        decision["reason"] = "bar-already-processed"
        _append_journal(config.execution.journal_path, decision)
        notification = _notify(config, "decision", "RBI Trader: skipped duplicate bar", decision)
        if notification is not None:
            decision["notification"] = notification
        return decision

    broker = None if config.execution.dry_run else AlpacaBroker(config.alpaca, config.execution, config.risk)
    position = None if broker is None else broker.get_position(config.execution.symbol)
    open_orders = [] if broker is None else broker.list_open_orders(config.execution.symbol)
    preflight = None if broker is None else broker.preflight(config.execution.symbol)

    decision["position"] = position
    decision["open_order_count"] = len(open_orders)
    if preflight is not None:
        decision["preflight"] = preflight
    _update_daily_state(state, preflight)
    daily_loss_pct = _daily_loss_pct(state, preflight)
    decision["risk"] = {
        "daily_loss_pct": daily_loss_pct,
        "kill_switch": bool(state.get("kill_switch", False)),
    }

    if state.get("kill_switch"):
        decision["reason"] = str(state.get("kill_reason") or "kill_switch_active")
    elif preflight is not None and preflight["buying_power"] < config.risk.min_buying_power_usd:
        decision["reason"] = "risk_insufficient_buying_power"
    elif daily_loss_pct >= config.risk.max_daily_loss_pct:
        state["kill_switch"] = True
        state["kill_reason"] = "kill_daily_loss_limit"
        decision["risk"]["kill_switch"] = True
        decision["reason"] = "kill_daily_loss_limit"
    elif position and abs(float(position["market_value"])) > config.risk.max_position_notional_usd * 1.05:
        decision["action"] = "close_position"
        decision["reason"] = "risk_position_limit_exceeded"
        if broker is not None:
            decision["broker_response"] = str(broker.close_position(config.execution.symbol))
    elif open_orders and not position and config.risk.require_flat_before_entry:
        decision["reason"] = "risk_open_orders_present"

    if decision.get("action") == "close_position":
        pass
    elif decision.get("reason") in {
        "risk_insufficient_buying_power",
        "kill_daily_loss_limit",
        "risk_open_orders_present",
    }:
        pass
    elif position:
        entry_price = float(position["avg_entry_price"])
        latest = enriched.iloc[-1]
        if position["side"] == "long":
            stop_hit = latest["low"] <= entry_price * (1 - strategy.stop_loss_pct)
            take_hit = latest["high"] >= entry_price * (1 + strategy.take_profit_pct)
            reverse = signal_payload["signal"] == -1
        else:
            stop_hit = latest["high"] >= entry_price * (1 + strategy.stop_loss_pct)
            take_hit = latest["low"] <= entry_price * (1 - strategy.take_profit_pct)
            reverse = signal_payload["signal"] == 1

        if stop_hit or take_hit or reverse:
            decision["action"] = "close_position"
            decision["reason"] = "risk_exit" if (stop_hit or take_hit) else "reverse_signal"
            if broker is not None:
                decision["broker_response"] = str(broker.close_position(config.execution.symbol))
        else:
            decision["reason"] = "position-open-hold"
    else:
        signal = signal_payload["signal"]
        short_allowed = strategy.allow_short and config.execution.asset_class.lower() == "us_equity"
        if signal == -1 and not short_allowed:
            decision["reason"] = "short-signal-ignored"
        elif signal in (1, -1):
            decision["action"] = "enter_position"
            decision["reason"] = signal_payload["signal_name"]
            if broker is not None:
                decision["broker_response"] = broker.submit_entry(
                    config.execution.symbol,
                    signal,
                    signal_payload["close"],
                )
        else:
            decision["reason"] = "no-entry-signal"

    state["last_processed_bar"] = last_bar_time.isoformat()
    state["last_signal"] = signal_payload
    state["last_decision"] = {
        "timestamp": decision["timestamp"],
        "action": decision["action"],
        "reason": decision.get("reason"),
    }
    _write_json(config.execution.state_path, state)
    _append_journal(config.execution.journal_path, decision)
    notification = _notify(config, "decision", f"RBI Trader: {decision['action']}", decision)
    if notification is not None:
        decision["notification"] = notification
    return decision


def run_trade_loop(config: AppConfig) -> None:
    while True:
        try:
            result = run_trade_cycle(config)
            emit_json(result)
        except Exception as exc:  # pragma: no cover - loop safety
            error_payload = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "action": "error",
                "message": str(exc),
            }
            _append_journal(config.execution.journal_path, error_payload)
            notification = _notify(config, "error", "RBI Trader error", error_payload)
            if notification is not None:
                error_payload["notification"] = notification
            emit_json(error_payload)
        time.sleep(config.execution.poll_seconds)
