from __future__ import annotations

import time

from orchestrator import BaseConnector
from orchestrator import register_connector


@register_connector("parallel_demo")
class ParallelDemoConnector(BaseConnector):
    def fetch(self, delay_seconds: float = 1.0, value: str = "ok", **kwargs):
        time.sleep(delay_seconds)
        return {"value": value, "delay_seconds": delay_seconds}

    def push(self, data, **kwargs):
        return data

    def ping(self) -> bool:
        return True
