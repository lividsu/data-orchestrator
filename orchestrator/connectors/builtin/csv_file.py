from __future__ import annotations

import csv
import os
from pathlib import Path
from typing import Any

from orchestrator.connectors.base import BaseConnector
from orchestrator.connectors.registry import register_connector


@register_connector("csv_file")
class CsvFileConnector(BaseConnector):
    def __init__(self, config: dict):
        super().__init__(config)
        self.base_dir = Path(config.get("base_dir", ".")).expanduser()

    def _resolve_path(self, path: str) -> Path:
        candidate = Path(path).expanduser()
        if candidate.is_absolute():
            return candidate
        return self.base_dir / candidate

    def fetch(self, path: str, encoding: str = "utf-8", delimiter: str = ",", **kwargs: Any) -> Any:
        target_path = self._resolve_path(path)
        with target_path.open(mode="r", encoding=encoding, newline="") as f:
            reader = csv.DictReader(f, delimiter=delimiter)
            return [dict(row) for row in reader]

    def push(
        self,
        data: Any = None,
        path: str = "",
        mode: str = "overwrite",
        encoding: str = "utf-8",
        **kwargs: Any,
    ) -> None:
        if mode not in {"overwrite", "append"}:
            raise ValueError("mode must be 'overwrite' or 'append'")
        if not path:
            raise ValueError("path is required")

        if data is None:
            rows: list[dict[str, Any]] = []
        elif isinstance(data, dict):
            rows = [data]
        elif isinstance(data, list):
            if any(not isinstance(item, dict) for item in data):
                raise ValueError("all items in data list must be dict")
            rows = data
        else:
            raise ValueError("data must be dict, list[dict], or None")

        target_path = self._resolve_path(path)
        target_path.parent.mkdir(parents=True, exist_ok=True)

        file_exists = target_path.exists() and target_path.stat().st_size > 0
        write_mode = "w" if mode == "overwrite" else "a"
        if not rows:
            target_path.open(mode=write_mode, encoding=encoding).close()
            return

        fieldnames = list(rows[0].keys())
        with target_path.open(mode=write_mode, encoding=encoding, newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            write_header = mode == "overwrite" or not file_exists
            if write_header:
                writer.writeheader()
            writer.writerows(rows)

    def ping(self) -> bool:
        return self.base_dir.exists() and os.access(self.base_dir, os.W_OK)
