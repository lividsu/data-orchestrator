from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel
from pydantic import Field

from orchestrator.config.settings import Settings
from orchestrator.core.pipeline import Pipeline
from orchestrator.core.schedule import ScheduleConfig
from orchestrator.exceptions import ConfigNotFoundError, ConfigValidationError
from orchestrator.notify import NotifyPolicy

ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(:-([^}]*))?\}")


class RegisteredPipeline(BaseModel):
    pipeline: Pipeline
    schedule: ScheduleConfig
    notify: NotifyPolicy = Field(default_factory=NotifyPolicy)


def load_yaml(path: str | Path) -> dict[str, Any]:
    file_path = Path(path)
    if not file_path.exists():
        raise ConfigNotFoundError(f"Config file not found: {file_path}")
    raw_text = file_path.read_text(encoding="utf-8")
    raw_text = _interpolate_env_vars(raw_text, file_path)
    try:
        data = yaml.safe_load(raw_text)
    except yaml.YAMLError as exc:
        raise ConfigValidationError(f"{file_path}: {exc}") from exc
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ConfigValidationError(f"{file_path}: Top-level YAML object must be a mapping.")
    return data


class ConfigLoader:
    @staticmethod
    def load(config_path: str, settings: Settings | None = None) -> list[RegisteredPipeline]:
        effective_settings = settings or Settings()
        path = Path(config_path)
        if not path.exists():
            raise ConfigNotFoundError(f"Config not found: {path}")
        files: list[Path] = []
        if path.is_dir():
            files = sorted(path.glob("*.y*ml"))
        else:
            files = [path]
        registered: list[RegisteredPipeline] = []
        for file in files:
            merged = ConfigLoader._load_with_includes(file)
            pipeline_items = merged.get("pipelines") or []
            if not isinstance(pipeline_items, list):
                raise ConfigValidationError(f"{file}: pipelines must be a list.")
            for item in pipeline_items:
                normalized_item = ConfigLoader._apply_pipeline_defaults(item, effective_settings)
                try:
                    pipeline = Pipeline(**normalized_item)
                    schedule = ScheduleConfig(**normalized_item.get("schedule", {"type": "manual"}))
                    notify = NotifyPolicy(**normalized_item.get("notify", {}))
                except Exception as exc:
                    raise ConfigValidationError(f"{file}: pipelines.{item.get('id', '<unknown>')}: {exc}") from exc
                registered.append(RegisteredPipeline(pipeline=pipeline, schedule=schedule, notify=notify))
        return registered

    @staticmethod
    def _load_with_includes(file_path: Path, visited: set[Path] | None = None) -> dict[str, Any]:
        visited = visited or set()
        if file_path in visited:
            raise ConfigValidationError(f"{file_path}: include cycle detected.")
        visited.add(file_path)
        data = load_yaml(file_path)
        include_paths = data.get("include", [])
        merged: dict[str, Any] = {"pipelines": []}
        if isinstance(include_paths, str):
            include_paths = [include_paths]
        for include_path in include_paths:
            resolved_path = (file_path.parent / include_path).resolve()
            included_data = ConfigLoader._load_with_includes(resolved_path, visited)
            merged["pipelines"].extend(included_data.get("pipelines", []))
        merged["pipelines"].extend(data.get("pipelines", []))
        for key, value in data.items():
            if key not in {"pipelines", "include"}:
                merged[key] = value
        return merged

    @staticmethod
    def _apply_pipeline_defaults(item: dict[str, Any], settings: Settings) -> dict[str, Any]:
        normalized_item = dict(item)
        normalized_tasks: list[dict[str, Any]] = []
        for raw_task in normalized_item.get("tasks", []):
            if not isinstance(raw_task, dict):
                raise ConfigValidationError("Task item must be a mapping.")
            task_data = dict(raw_task)
            if "timeout_seconds" not in task_data:
                task_data["timeout_seconds"] = settings.default_timeout_seconds
            retry_config = task_data.get("retry")
            if retry_config is None:
                task_data["retry"] = {
                    "times": settings.default_retry_times,
                    "delay_seconds": settings.default_retry_delay,
                    "backoff": settings.default_retry_backoff,
                }
            elif isinstance(retry_config, dict):
                normalized_retry = dict(retry_config)
                if "times" not in normalized_retry:
                    normalized_retry["times"] = settings.default_retry_times
                if "delay_seconds" not in normalized_retry:
                    normalized_retry["delay_seconds"] = settings.default_retry_delay
                if "backoff" not in normalized_retry:
                    normalized_retry["backoff"] = settings.default_retry_backoff
                task_data["retry"] = normalized_retry
            normalized_tasks.append(task_data)
        normalized_item["tasks"] = normalized_tasks
        return normalized_item


def _interpolate_env_vars(raw_text: str, file_path: Path) -> str:
    def replace(match: re.Match[str]) -> str:
        var_name = match.group(1)
        default_value = match.group(3)
        env_value = os.environ.get(var_name)
        if env_value is not None:
            return env_value
        if default_value is not None:
            return default_value
        raise ConfigValidationError(f"{file_path}: missing env var '{var_name}'.")

    return ENV_PATTERN.sub(replace, raw_text)
