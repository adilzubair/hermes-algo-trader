from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

from rbi_trader.config import NotificationConfig


class Notifier:
    def __init__(self, config: NotificationConfig) -> None:
        self.config = config

    def _severity_emoji(self, severity: str) -> str:
        return {
            "info": "ℹ️",
            "warning": "⚠️",
            "critical": "🚨",
        }.get(severity, "ℹ️")

    def _format_text(self, severity: str, title: str, payload: dict[str, Any]) -> str:
        emoji = self._severity_emoji(severity)
        lines = [f"{emoji} {title}"]

        for key in ("symbol", "timeframe", "action", "reason"):
            value = payload.get(key)
            if value is not None:
                lines.append(f"{key}: {value}")

        signal = payload.get("signal")
        if isinstance(signal, dict):
            lines.append(
                "signal: "
                f"{signal.get('signal_name', 'unknown')} | "
                f"close={signal.get('close', 'n/a')} | "
                f"adx={round(float(signal.get('adx', 0.0)), 2) if signal.get('adx') is not None else 'n/a'}"
            )

        risk = payload.get("risk")
        if isinstance(risk, dict):
            lines.append(
                "risk: "
                f"daily_loss_pct={round(float(risk.get('daily_loss_pct', 0.0)), 3)} | "
                f"kill_switch={risk.get('kill_switch', False)}"
            )

        broker_response = payload.get("broker_response")
        if broker_response is not None:
            lines.append(f"broker: {broker_response}")

        message = payload.get("message")
        if message:
            lines.append(f"message: {message}")

        return "\n".join(lines)

    def _append_log(self, payload: dict[str, Any]) -> None:
        path = Path(self.config.log_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload) + "\n")

    def _send_webhook(self, text: str, payload: dict[str, Any]) -> str | None:
        webhook_url = os.getenv(self.config.webhook_url_env)
        if not webhook_url:
            return None
        response = requests.post(webhook_url, json={"text": text, "payload": payload}, timeout=10)
        response.raise_for_status()
        return "webhook"

    def _send_telegram(self, text: str) -> str | None:
        bot_token = os.getenv(self.config.telegram_bot_token_env)
        chat_id = os.getenv(self.config.telegram_chat_id_env)
        if not bot_token or not chat_id:
            return None
        response = requests.post(
            f"https://api.telegram.org/bot{bot_token}/sendMessage",
            json={"chat_id": chat_id, "text": text},
            timeout=10,
        )
        response.raise_for_status()
        return "telegram"

    def send(self, event_type: str, title: str, payload: dict[str, Any], severity: str = "info") -> dict[str, Any]:
        event = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "event_type": event_type,
            "severity": severity,
            "title": title,
            "payload": payload,
        }
        if not self.config.enabled:
            event["delivered"] = []
            event["logged"] = True
            self._append_log(event)
            return {"delivered": [], "logged": True}

        text = self._format_text(severity, title, payload)
        delivered: list[str] = []
        errors: list[str] = []

        for sender in (self._send_webhook,):
            try:
                channel = sender(text, payload)
                if channel:
                    delivered.append(channel)
            except Exception as exc:  # pragma: no cover
                errors.append(str(exc))

        try:
            channel = self._send_telegram(text)
            if channel:
                delivered.append(channel)
        except Exception as exc:  # pragma: no cover
            errors.append(str(exc))

        result = {"delivered": delivered, "logged": True}
        if errors:
            result["errors"] = errors
        event["delivered"] = delivered
        event["logged"] = True
        if errors:
            event["errors"] = errors
        self._append_log(event)
        return result
