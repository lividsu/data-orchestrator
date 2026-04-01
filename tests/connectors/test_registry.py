from __future__ import annotations

from pathlib import Path

import pytest

from orchestrator.connectors.base import BaseConnector
from orchestrator.connectors.loader import load_plugins
from orchestrator.connectors.registry import get_connector, register_connector
from orchestrator.connectors import registry
from orchestrator.exceptions import ConnectorAlreadyRegisteredError, ConnectorNotFoundError


@pytest.fixture(autouse=True)
def clear_registry():
    registry._REGISTRY.clear()
    yield
    registry._REGISTRY.clear()


def test_register_and_get():
    @register_connector("test_mock")
    class MockConnector(BaseConnector):
        def fetch(self, **kwargs):
            return {"data": 1}

        def push(self, data=None, **kwargs):
            return None

        def ping(self):
            return True

    connector = get_connector("test_mock", {})
    assert connector.ping() is True
    assert connector.fetch() == {"data": 1}


def test_duplicate_registration_raises():
    @register_connector("test_mock")
    class MockConnector(BaseConnector):
        def fetch(self, **kwargs):
            return {"data": 1}

        def push(self, data=None, **kwargs):
            return None

        def ping(self):
            return True

    with pytest.raises(ConnectorAlreadyRegisteredError) as exc:
        @register_connector("test_mock")
        class AnotherMock(BaseConnector):
            def fetch(self, **kwargs):
                return {"data": 2}

            def push(self, data=None, **kwargs):
                return None

            def ping(self):
                return True

    message = str(exc.value)
    assert "Existing: MockConnector" in message
    assert "New: AnotherMock" in message


def test_not_found_raises_with_helpful_message():
    with pytest.raises(ConnectorNotFoundError) as exc:
        get_connector("nonexistent", {})
    assert "nonexistent" in str(exc.value)
    assert "Available connectors" in str(exc.value)


def test_cannot_instantiate_abstract_class():
    with pytest.raises(TypeError):
        BaseConnector({})


def test_load_plugins_from_examples_directory():
    plugin_dir = Path(__file__).resolve().parents[2] / "examples" / "shopify_daily" / "connectors"
    load_plugins(plugin_dir)
    connector = get_connector("shopify", {})
    assert connector.__class__.__name__ == "ShopifyConnector"
