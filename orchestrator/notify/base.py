from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Callable

from orchestrator.exceptions import NotifierAlreadyRegisteredError, NotifierNotFoundError

NotifierFactory = type["BaseNotifier"]
NOTIFIER_REGISTRY: dict[str, NotifierFactory] = {}


class BaseNotifier(ABC):
    def __init__(self, config: dict[str, Any] | None = None) -> None:
        self.config = config or {}

    @abstractmethod
    def send(self, title: str, body: str, level: str, context: dict[str, Any]) -> None:
        raise NotImplementedError


def register_notifier(name: str) -> Callable[[NotifierFactory], NotifierFactory]:
    def decorator(notifier_cls: NotifierFactory) -> NotifierFactory:
        if name in NOTIFIER_REGISTRY:
            raise NotifierAlreadyRegisteredError(f"Notifier already registered: {name}")
        NOTIFIER_REGISTRY[name] = notifier_cls
        return notifier_cls

    return decorator


def get_notifier(name: str) -> NotifierFactory:
    notifier_cls = NOTIFIER_REGISTRY.get(name)
    if notifier_cls is None:
        raise NotifierNotFoundError(f"Notifier not found: {name}")
    return notifier_cls


def reset_notifier_registry() -> None:
    NOTIFIER_REGISTRY.clear()
