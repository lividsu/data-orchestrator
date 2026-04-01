from __future__ import annotations

import pytest

from orchestrator.connectors.base import BaseConnector


class MockConnector(BaseConnector):
    def fetch(self, **kwargs):
        return {"ok": True, "kwargs": kwargs}

    def push(self, data, **kwargs):
        return {"ok": True, "data": data, "kwargs": kwargs}

    def ping(self) -> bool:
        return True


@pytest.fixture()
def mock_connector() -> MockConnector:
    return MockConnector(config={"name": "mock"})
