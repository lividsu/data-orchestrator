from __future__ import annotations

from typing import Any

from sqlalchemy import create_engine
from sqlalchemy import text

from orchestrator.connectors.base import BaseConnector
from orchestrator.connectors.registry import register_connector


@register_connector("postgres")
class PostgresConnector(BaseConnector):
    def __init__(self, config: dict):
        super().__init__(config)
        self._engine = None

    def initialize(self):
        super().initialize()
        dsn = self.config.get("dsn") or self.config.get("url")
        if not dsn:
            raise ValueError("postgres connector requires dsn or url")
        self._engine = create_engine(dsn)

    def fetch(self, **kwargs: Any) -> Any:
        query = kwargs.get("query")
        params = kwargs.get("params") or {}
        if not query:
            raise ValueError("query is required for fetch")
        with self._engine.connect() as conn:
            rows = conn.execute(text(query), params).mappings().all()
        return [dict(row) for row in rows]

    def push(self, data: Any = None, **kwargs: Any) -> None:
        query = kwargs.get("query")
        if not query:
            raise ValueError("query is required for push")
        payload = kwargs.get("params", data)
        with self._engine.connect() as conn:
            if isinstance(payload, list):
                conn.execute(text(query), payload)
            else:
                conn.execute(text(query), payload or {})
            conn.commit()
        return None

    def ping(self) -> bool:
        try:
            if self._engine is None:
                self.initialize()
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception:
            return False

    def close(self):
        if self._engine is not None:
            self._engine.dispose()
