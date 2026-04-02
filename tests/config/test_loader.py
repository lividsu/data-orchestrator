from __future__ import annotations

from pathlib import Path

import pytest

from orchestrator.config.loader import ConfigLoader
from orchestrator.config.settings import Settings
from orchestrator.exceptions import ConfigValidationError


def test_env_var_interpolation(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("DB_URL", "postgresql://localhost/test")
    yaml_file = tmp_path / "pipe.yaml"
    yaml_file.write_text(
        """
pipelines:
  - id: p1
    schedule: {type: manual}
    tasks:
      - id: t1
        connector: demo
        action: fetch
        connector_config:
          dsn: "${DB_URL}"
""".strip(),
        encoding="utf-8",
    )
    pipeline = ConfigLoader.load(str(yaml_file))[0].pipeline
    assert pipeline.tasks[0].connector_config["dsn"] == "postgresql://localhost/test"


def test_missing_env_var_raises(tmp_path: Path):
    yaml_file = tmp_path / "pipe.yaml"
    yaml_file.write_text(
        """
pipelines:
  - id: p1
    schedule: {type: manual}
    tasks:
      - id: t1
        connector: demo
        action: fetch
        connector_config:
          dsn: "${MISSING_VAR}"
""".strip(),
        encoding="utf-8",
    )
    with pytest.raises(ConfigValidationError) as exc:
        ConfigLoader.load(str(yaml_file))
    assert "MISSING_VAR" in str(exc.value)


def test_default_value_fallback(tmp_path: Path):
    yaml_file = tmp_path / "pipe.yaml"
    yaml_file.write_text(
        """
pipelines:
  - id: p1
    schedule: {type: manual}
    tasks:
      - id: t1
        connector: demo
        action: fetch
        connector_config:
          dsn: "${VAR:-fallback}"
""".strip(),
        encoding="utf-8",
    )
    pipeline = ConfigLoader.load(str(yaml_file))[0].pipeline
    assert pipeline.tasks[0].connector_config["dsn"] == "fallback"


def test_template_variables_not_rendered_during_load(tmp_path: Path):
    yaml_file = tmp_path / "pipe.yaml"
    yaml_file.write_text(
        """
pipelines:
  - id: p1
    schedule: {type: manual}
    tasks:
      - id: t1
        connector: demo
        action: fetch
        action_kwargs:
          dt: "{{ yesterday }}"
""".strip(),
        encoding="utf-8",
    )
    pipeline = ConfigLoader.load(str(yaml_file))[0].pipeline
    assert pipeline.tasks[0].action_kwargs["dt"] == "{{ yesterday }}"


def test_cyclic_dependency_in_yaml(tmp_path: Path):
    yaml_file = tmp_path / "pipe.yaml"
    yaml_file.write_text(
        """
pipelines:
  - id: p1
    schedule: {type: manual}
    tasks:
      - id: task_a
        connector: demo
        action: fetch
        depends_on: [task_b]
      - id: task_b
        connector: demo
        action: fetch
        depends_on: [task_a]
""".strip(),
        encoding="utf-8",
    )
    with pytest.raises(ConfigValidationError):
        ConfigLoader.load(str(yaml_file))


def test_task_defaults_from_settings(tmp_path: Path):
    yaml_file = tmp_path / "pipe.yaml"
    yaml_file.write_text(
        """
pipelines:
  - id: p1
    schedule: {type: manual}
    tasks:
      - id: t1
        connector: demo
        action: fetch
""".strip(),
        encoding="utf-8",
    )
    settings = Settings(
        default_retry_times=7,
        default_retry_delay=1.5,
        default_retry_backoff=3.0,
        default_timeout_seconds=99.0,
    )
    pipeline = ConfigLoader.load(str(yaml_file), settings=settings)[0].pipeline
    task = pipeline.tasks[0]
    assert task.retry.times == 7
    assert task.retry.delay_seconds == 1.5
    assert task.retry.backoff == 3.0
    assert task.timeout_seconds == 99.0
