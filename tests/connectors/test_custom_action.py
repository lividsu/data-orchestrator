from __future__ import annotations

import pytest

from orchestrator.connectors import registry
from orchestrator.connectors.base import BaseConnector
from orchestrator.connectors.registry import register_connector


@pytest.fixture(autouse=True)
def clear_registry():
    registry._REGISTRY.clear()
    yield
    registry._REGISTRY.clear()


def test_fetch_only_connector():
    @register_connector("fetch_only")
    class FetchOnly(BaseConnector):
        def fetch(self, **kwargs):
            return [1, 2, 3]

    conn = FetchOnly(config={})
    assert conn.fetch() == [1, 2, 3]

    with pytest.raises(NotImplementedError):
        conn.push()
    with pytest.raises(NotImplementedError):
        conn.ping()


def test_push_only_connector():
    @register_connector("push_only")
    class PushOnly(BaseConnector):
        def push(self, data=None, **kwargs):
            return {"sent": True}

    conn = PushOnly(config={})
    assert conn.push(data={"x": 1}) == {"sent": True}

    with pytest.raises(NotImplementedError):
        conn.fetch()


def test_fully_custom_action_connector():
    @register_connector("feishu_notifier")
    class FeishuNotifier(BaseConnector):
        def send_report(self, content: str = "", channel: str = "", **kwargs):
            return {"ok": True, "channel": channel, "content": content}

        def send_alert(self, message: str = "", **kwargs):
            return {"ok": True, "message": message}

    conn = FeishuNotifier(config={"webhook_url": "https://example.com"})
    result = conn.send_report(content="日报", channel="#daily")
    assert result == {"ok": True, "channel": "#daily", "content": "日报"}

    with pytest.raises(NotImplementedError):
        conn.fetch()
    with pytest.raises(NotImplementedError):
        conn.push()
    with pytest.raises(NotImplementedError):
        conn.ping()
