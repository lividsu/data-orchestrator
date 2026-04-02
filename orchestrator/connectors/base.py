from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from typing import Any


class BaseConnector(ABC):
    def __init__(self, config: dict):
        self.config = config
        self._initialized = False

    def initialize(self):
        self._initialized = True

    @abstractmethod
    def fetch(self, **kwargs: Any) -> Any:
        ...

    @abstractmethod
    def push(self, data: Any = None, **kwargs: Any) -> None:
        ...

    @abstractmethod
    def ping(self) -> bool:
        ...

    def close(self):
        pass

    async def async_fetch(self, **kwargs: Any) -> Any:
        return await asyncio.to_thread(self.fetch, **kwargs)

    async def async_push(self, data: Any = None, **kwargs: Any) -> Any:
        return await asyncio.to_thread(self.push, data, **kwargs)

    def __repr__(self):
        return f"{self.__class__.__name__}(config_keys={list(self.config.keys())})"
