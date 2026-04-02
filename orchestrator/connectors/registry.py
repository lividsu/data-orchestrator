from __future__ import annotations

from typing import Type

from orchestrator.connectors.base import BaseConnector
from orchestrator.exceptions import ConnectorAlreadyRegisteredError, ConnectorNotFoundError

_REGISTRY: dict[str, Type[BaseConnector]] = {}


def register_connector(name: str):
    def decorator(cls: Type[BaseConnector]) -> Type[BaseConnector]:
        if name in _REGISTRY:
            raise ConnectorAlreadyRegisteredError(
                f"Connector '{name}' is already registered. "
                f"Existing: {_REGISTRY[name].__name__}, "
                f"New: {cls.__name__}"
            )
        _REGISTRY[name] = cls
        return cls

    return decorator


def get_connector_class(name: str) -> Type[BaseConnector]:
    if name not in _REGISTRY:
        available = ", ".join(sorted(_REGISTRY.keys()))
        raise ConnectorNotFoundError(
            f"Connector '{name}' not found. "
            f"Available connectors: [{available}]"
        )
    return _REGISTRY[name]


def get_connector(name: str, config: dict) -> BaseConnector:
    cls = get_connector_class(name)
    instance = cls(config)
    return instance


def list_connectors() -> list[str]:
    return sorted(_REGISTRY.keys())


def reset_registry() -> None:
    _REGISTRY.clear()
