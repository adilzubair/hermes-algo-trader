from __future__ import annotations

import math
import os
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any

from alpaca.common.exceptions import APIError
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import AssetClass, OrderSide, OrderType, QueryOrderStatus, TimeInForce
from alpaca.trading.requests import GetOrdersRequest, LimitOrderRequest, MarketOrderRequest

from rbi_trader.config import AlpacaConfig, ExecutionConfig, RiskConfig


class AlpacaBroker:
    def __init__(self, alpaca_config: AlpacaConfig, execution_config: ExecutionConfig, risk_config: RiskConfig) -> None:
        api_key = os.getenv(alpaca_config.api_key_env)
        secret_key = os.getenv(alpaca_config.secret_key_env)
        if not api_key or not secret_key:
            raise RuntimeError(
                f"Missing Alpaca credentials. Expected env vars {alpaca_config.api_key_env} and {alpaca_config.secret_key_env}."
            )
        self.client = TradingClient(api_key, secret_key, paper=alpaca_config.paper)
        self.paper = alpaca_config.paper
        self.execution_config = execution_config
        self.risk_config = risk_config
        self.asset_class = execution_config.asset_class.lower()

    def preflight(self, symbol: str) -> dict[str, Any]:
        account = self.client.get_account()
        if getattr(account, "trading_blocked", False) or getattr(account, "account_blocked", False):
            raise RuntimeError("Alpaca account is blocked for trading.")
        asset = self.client.get_asset(symbol.replace("/", "")) if self.asset_class != "crypto" else None
        return {
            "account_id": str(account.id),
            "paper": self.paper,
            "status": str(account.status),
            "buying_power": float(account.buying_power),
            "equity": float(account.equity),
            "last_equity": float(account.last_equity),
            "cash": float(account.cash),
            "multiplier": str(account.multiplier),
            "asset": asset.symbol if asset else symbol,
        }

    def get_position(self, symbol: str) -> dict[str, Any] | None:
        lookup = symbol if self.asset_class == "crypto" else symbol.replace("/", "")
        try:
            position = self.client.get_open_position(lookup)
        except APIError as exc:
            message = str(exc).lower()
            if "position does not exist" in message or "not found" in message:
                return None
            raise
        qty = float(position.qty)
        return {
            "symbol": position.symbol,
            "qty": qty,
            "side": "long" if qty > 0 else "short",
            "market_value": float(position.market_value),
            "avg_entry_price": float(position.avg_entry_price),
            "unrealized_plpc": float(position.unrealized_plpc),
        }

    def list_open_orders(self, symbol: str) -> list[Any]:
        lookup = symbol if self.asset_class == "crypto" else symbol.replace("/", "")
        request = GetOrdersRequest(status=QueryOrderStatus.OPEN, symbols=[lookup])
        return list(self.client.get_orders(filter=request))

    def close_position(self, symbol: str) -> Any:
        lookup = symbol if self.asset_class == "crypto" else symbol.replace("/", "")
        return self.client.close_position(lookup)

    def submit_entry(self, symbol: str, signal: int, last_price: float) -> dict[str, Any]:
        side = OrderSide.BUY if signal == 1 else OrderSide.SELL
        lookup_symbol = symbol if self.asset_class == "crypto" else symbol.replace("/", "")
        order_id = f"rbi-{lookup_symbol.lower().replace('/', '-')}-{int(datetime.now(timezone.utc).timestamp())}-{signal}"
        time_in_force = TimeInForce.GTC if self.asset_class == "crypto" else TimeInForce.DAY
        order = None
        if self.execution_config.use_limit_orders:
            buffer = self.execution_config.limit_price_buffer_bps / 10_000
            limit_price = last_price * (1 + buffer if signal == 1 else 1 - buffer)
            order = LimitOrderRequest(
                symbol=lookup_symbol,
                side=side,
                notional=self.risk_config.trade_notional_usd,
                limit_price=round(limit_price, 4),
                type=OrderType.LIMIT,
                time_in_force=time_in_force,
                extended_hours=self.execution_config.extended_hours,
                client_order_id=order_id,
            )
        else:
            order = MarketOrderRequest(
                symbol=lookup_symbol,
                side=side,
                notional=self.risk_config.trade_notional_usd,
                type=OrderType.MARKET,
                time_in_force=time_in_force,
                extended_hours=self.execution_config.extended_hours,
                client_order_id=order_id,
            )
        response = self.client.submit_order(order)
        return {
            "id": str(response.id),
            "client_order_id": response.client_order_id,
            "symbol": response.symbol,
            "side": response.side.value,
            "type": response.order_type.value,
            "notional": float(response.notional or self.risk_config.trade_notional_usd),
        }
