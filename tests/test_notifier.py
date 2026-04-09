from __future__ import annotations

from rbi_trader.config import NotificationConfig
from rbi_trader.notifier import Notifier


def test_notifier_formats_human_message(tmp_path) -> None:
    notifier = Notifier(NotificationConfig(enabled=False, log_path=str(tmp_path / 'alerts.jsonl')))
    text = notifier._format_text(
        'warning',
        'RBI Trader: enter_position',
        {
            'symbol': 'BTC/USD',
            'timeframe': '6Hour',
            'action': 'enter_position',
            'reason': 'long',
            'signal': {'signal_name': 'long', 'close': 70000.0, 'adx': 31.234},
            'risk': {'daily_loss_pct': 0.0, 'kill_switch': False},
        },
    )
    assert '⚠️ RBI Trader: enter_position' in text
    assert 'symbol: BTC/USD' in text
    assert 'signal: long | close=70000.0 | adx=31.23' in text
    assert 'risk: daily_loss_pct=0.0 | kill_switch=False' in text


def test_notifier_logs_delivery_result(tmp_path, monkeypatch) -> None:
    log_path = tmp_path / 'alerts.jsonl'
    notifier = Notifier(NotificationConfig(enabled=True, log_path=str(log_path)))
    monkeypatch.setattr(notifier, '_send_webhook', lambda _text, _payload: None)
    monkeypatch.setattr(notifier, '_send_telegram', lambda _text: 'telegram')

    result = notifier.send('test', 'delivery ok', {'symbol': 'BTC/USD'})

    assert result == {'delivered': ['telegram'], 'logged': True}
    logged = log_path.read_text()
    assert '"delivered": ["telegram"]' in logged
    assert '"title": "delivery ok"' in logged


def test_notifier_logs_delivery_errors(tmp_path, monkeypatch) -> None:
    log_path = tmp_path / 'alerts.jsonl'
    notifier = Notifier(NotificationConfig(enabled=True, log_path=str(log_path)))
    monkeypatch.setattr(notifier, '_send_webhook', lambda _text, _payload: None)

    def boom(_text: str) -> str:
        raise RuntimeError('telegram blew up')

    monkeypatch.setattr(notifier, '_send_telegram', boom)

    result = notifier.send('test', 'delivery fail', {'symbol': 'BTC/USD'})

    assert result['delivered'] == []
    assert result['logged'] is True
    assert result['errors'] == ['telegram blew up']
    logged = log_path.read_text()
    assert '"errors": ["telegram blew up"]' in logged
    assert '"title": "delivery fail"' in logged
