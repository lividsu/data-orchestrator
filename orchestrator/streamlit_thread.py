from __future__ import annotations

from threading import Lock
from typing import Any

_LOCK = Lock()
_SHARED: dict[str, Any] = {}


def set_orchestrator(orchestrator: Any) -> None:
    with _LOCK:
        _SHARED["orchestrator"] = orchestrator


def get_orchestrator() -> Any | None:
    with _LOCK:
        return _SHARED.get("orchestrator")
