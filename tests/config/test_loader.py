from __future__ import annotations

from datetime import datetime
from datetime import timedelta
from pathlib import Path

import pytest

from orchestrator.config.loader import ConfigLoader
from orchestrator.config.loader import get_template_context
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


def test_template_variables_rendered(monkeypatch, tmp_path: Path):
    frozen_now = datetime(2024, 3, 15, 8, 0, 0)

    def fake_context():
        return {
            "now": frozen_now.isoformat(),
            "today": frozen_now.strftime("%Y-%m-%d"),
            "yesterday": (frozen_now - timedelta(days=1)).strftime("%Y-%m-%d"),
            "yesterday_iso": (frozen_now - timedelta(days=1)).isoformat(),
            "week_start": "2024-03-11",
            "month_start": "2024-03-01",
            "run_id": "r",
            "pipeline_id": "",
        }

    monkeypatch.setattr("orchestrator.config.loader.get_template_context", fake_context)
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
    assert pipeline.tasks[0].action_kwargs["dt"] == "2024-03-14"


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
