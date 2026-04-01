from __future__ import annotations

from orchestrator import BaseConnector
from orchestrator import register_connector


@register_connector("demo_connector")
class DemoConnector(BaseConnector):
    def fetch(self, **kwargs):
        return [{"message": "hello orchestrator", "source": "demo"}]

    def push(self, data, **kwargs):
        return {"rows": len(data) if isinstance(data, list) else 1}

    def ping(self) -> bool:
        return True
