from __future__ import annotations

import os
import re
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Any

import yaml
from jinja2 import Environment
from pydantic import BaseModel
from pydantic import Field

from orchestrator.core.pipeline import Pipeline
from orchestrator.core.runner import generate_run_id
from orchestrator.core.schedule import ScheduleConfig
from orchestrator.exceptions import ConfigNotFoundError, ConfigValidationError
from orchestrator.notify import NotifyPolicy

ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(:-([^}]*))?\}")


class RegisteredPipeline(BaseModel):
    pipeline: Pipeline
    schedule: ScheduleConfig
    notify: NotifyPolicy = Field(default_factory=NotifyPolicy)


def load_yaml(path: str | Path, variables: dict[str, Any] | None = None) -> dict[str, Any]:
    file_path = Path(path)
    if not file_path.exists():
        raise ConfigNotFoundError(f"Config file not found: {file_path}")
    raw_text = file_path.read_text(encoding="utf-8")
    raw_text = _interpolate_env_vars(raw_text, file_path)
    rendered_text = Environment(autoescape=False).from_string(raw_text).render(variables or {})
    try:
        data = yaml.safe_load(rendered_text)
    except yaml.YAMLError as exc:
        raise ConfigValidationError(f"{file_path}: {exc}") from exc
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ConfigValidationError(f"{file_path}: Top-level YAML object must be a mapping.")
    return data


class ConfigLoader:
    @staticmethod
    def load(config_path: str) -> list[RegisteredPipeline]:
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
                try:
                    pipeline = Pipeline(**item)
                    schedule = ScheduleConfig(**item.get("schedule", {"type": "manual"}))
                    notify = NotifyPolicy(**item.get("notify", {}))
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
        data = load_yaml(file_path, variables=get_template_context())
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


def get_template_context() -> dict[str, Any]:
    now = datetime.now()
    return {
        "now": now.isoformat(),
        "today": now.strftime("%Y-%m-%d"),
        "yesterday": (now - timedelta(days=1)).strftime("%Y-%m-%d"),
        "yesterday_iso": (now - timedelta(days=1)).isoformat(),
        "week_start": (now - timedelta(days=now.weekday())).strftime("%Y-%m-%d"),
        "month_start": now.replace(day=1).strftime("%Y-%m-%d"),
        "run_id": generate_run_id(),
        "pipeline_id": "",
    }
