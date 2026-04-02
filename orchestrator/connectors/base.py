from __future__ import annotations

import asyncio
from typing import Any


class BaseConnector:
    """所有 Connector 的基类。

    子类只需实现自己用到的方法。Runner 通过 getattr 按名称调用 action，
    因此 connector 上的任何公开方法都可以作为 YAML 里的 action 使用。

    内置的 fetch / push / ping 是常见约定，但不是强制要求。
    """

    def __init__(self, config: dict):
        self.config = config
        self._initialized = False

    def initialize(self):
        self._initialized = True

    def fetch(self, **kwargs: Any) -> Any:
        raise NotImplementedError(f"{self.__class__.__name__} does not implement 'fetch'.")

    def push(self, data: Any = None, **kwargs: Any) -> None:
        raise NotImplementedError(f"{self.__class__.__name__} does not implement 'push'.")

    def ping(self) -> bool:
        raise NotImplementedError(f"{self.__class__.__name__} does not implement 'ping'.")

    def close(self):
        pass

    async def async_fetch(self, **kwargs: Any) -> Any:
        return await asyncio.to_thread(self.fetch, **kwargs)

    async def async_push(self, data: Any = None, **kwargs: Any) -> Any:
        return await asyncio.to_thread(self.push, data, **kwargs)

    def __repr__(self):
        return f"{self.__class__.__name__}(config_keys={list(self.config.keys())})"
