from __future__ import annotations

import csv
import importlib
import threading
import time
from pathlib import Path

import pytest
import responses
from sqlalchemy import create_engine
from sqlalchemy import select

import orchestrator.connectors.builtin.csv_file
import orchestrator.connectors.builtin.http_api
from orchestrator.config.loader import ConfigLoader
from orchestrator.connectors import registry
from orchestrator.connectors.registry import get_connector_class
from orchestrator.core.scheduler import Orchestrator
from orchestrator.log.models import pipeline_runs
from orchestrator.log.models import task_runs


@pytest.fixture(autouse=True)
def ensure_builtin_connectors_registered():
    registry._REGISTRY.clear()
    importlib.reload(orchestrator.connectors.builtin.http_api)
    importlib.reload(orchestrator.connectors.builtin.csv_file)


def test_full_pipeline_from_yaml(tmp_path: Path):
    base_url = "https://mock.example.com"
    plugin_dir = tmp_path / "connectors"
    config_dir = tmp_path / "pipelines"
    output_dir = tmp_path / "output"
    plugin_dir.mkdir()
    config_dir.mkdir()
    db_url = f"sqlite:///{tmp_path / 'e2e.db'}"
    yaml_path = config_dir / "demo.yaml"
    yaml_path.write_text(
        f"""
pipelines:
  - id: full_sync
    name: Full Sync
    schedule:
      type: manual
    tasks:
      - id: fetch_orders
        connector: http_api
        action: fetch
        connector_config:
          base_url: "{base_url}"
        action_kwargs:
          endpoint: /orders
      - id: save_orders
        connector: csv_file
        action: push
        connector_config:
          base_dir: "{output_dir.as_posix()}"
        depends_on: [fetch_orders]
        pass_output_from: fetch_orders
        action_kwargs:
          path: orders.csv
          mode: overwrite
""".strip(),
        encoding="utf-8",
    )
    with responses.RequestsMock(assert_all_requests_are_fired=False) as mock_server:
        mock_server.add(responses.GET, f"{base_url}/orders", json=[{"id": "1", "amount": "100"}], status=200)
        app = Orchestrator(config_dir=str(config_dir), plugin_dir=str(plugin_dir), db_url=db_url)
        app.ensure_loaded()
        app.trigger("full_sync", triggered_by="manual")
    engine = create_engine(db_url)
    with engine.connect() as conn:
        run_rows = conn.execute(select(pipeline_runs)).mappings().all()
        task_rows = conn.execute(select(task_runs)).mappings().all()
    assert len(run_rows) == 1
    assert len(task_rows) == 2
    assert run_rows[0]["status"] == "success"
    output_file = output_dir / "orders.csv"
    assert output_file.exists()
    with output_file.open("r", encoding="utf-8", newline="") as fp:
        rows = list(csv.DictReader(fp))
    assert rows == [{"id": "1", "amount": "100"}]


def test_retry_with_real_http(tmp_path: Path):
    base_url = "https://retry.example.com"
    db_url = f"sqlite:///{tmp_path / 'retry.db'}"
    config_dir = tmp_path / "pipelines"
    config_dir.mkdir()
    yaml_path = config_dir / "retry.yaml"
    yaml_path.write_text(
        f"""
pipelines:
  - id: retry_pipeline
    schedule:
      type: manual
    tasks:
      - id: fetch_retry
        connector: http_api
        action: fetch
        connector_config:
          base_url: "{base_url}"
        retry:
          times: 3
          delay_seconds: 0.1
          backoff: 1
        action_kwargs:
          endpoint: /unstable
""".strip(),
        encoding="utf-8",
    )
    with responses.RequestsMock(assert_all_requests_are_fired=False) as mock_server:
        mock_server.add(responses.GET, f"{base_url}/unstable", status=503)
        mock_server.add(responses.GET, f"{base_url}/unstable", status=503)
        mock_server.add(responses.GET, f"{base_url}/unstable", json={"ok": True}, status=200)
        app = Orchestrator(config_dir=str(config_dir), db_url=db_url)
        app.ensure_loaded()
        result = app.trigger("retry_pipeline", triggered_by="manual")
    assert result.status == "success"
    task_result = result.task_results["fetch_retry"]
    assert task_result.retry_count == 2
    assert task_result.status.value == "success"


def test_scheduler_triggers_and_logs(tmp_path: Path):
    db_url = f"sqlite:///{tmp_path / 'scheduler.db'}"
    config_dir = tmp_path / "pipelines"
    config_dir.mkdir()
    yaml_path = config_dir / "scheduler.yaml"
    yaml_path.write_text(
        """
pipelines:
  - id: schedule_demo
    schedule:
      type: interval
      interval_seconds: 1
    tasks:
      - id: fetch_data
        connector: http_api
        action: fetch
        connector_config:
          base_url: "https://schedule.example.com"
        action_kwargs:
          endpoint: /ok
""".strip(),
        encoding="utf-8",
    )
    with responses.RequestsMock(assert_all_requests_are_fired=False) as mock_server:
        mock_server.add(responses.GET, "https://schedule.example.com/ok", json={"ok": True}, status=200)
        mock_server.add(responses.GET, "https://schedule.example.com/ok", json={"ok": True}, status=200)
        mock_server.add(responses.GET, "https://schedule.example.com/ok", json={"ok": True}, status=200)
        mock_server.add(responses.GET, "https://schedule.example.com/ok", json={"ok": True}, status=200)
        app = Orchestrator(config_dir=str(config_dir), db_url=db_url)
        thread = threading.Thread(target=app.start, daemon=True)
        thread.start()
        time.sleep(3.5)
        app.stop()
        thread.join(timeout=1)
    engine = create_engine(db_url)
    with engine.connect() as conn:
        run_count = conn.execute(select(pipeline_runs.c.id)).all()
        task_count = conn.execute(select(task_runs.c.id)).all()
    assert len(run_count) >= 3
    assert len(task_count) >= 3


def test_external_connector_loaded_from_plugin_dir(tmp_path: Path):
    plugin_dir = tmp_path / "connectors"
    config_dir = tmp_path / "pipelines"
    plugin_dir.mkdir()
    config_dir.mkdir()
    db_url = f"sqlite:///{tmp_path / 'plugin.db'}"
    plugin_file = plugin_dir / "demo_connector.py"
    plugin_file.write_text(
        """
from orchestrator.connectors.base import BaseConnector
from orchestrator.connectors.registry import register_connector

@register_connector("plugin_demo")
class PluginDemoConnector(BaseConnector):
    def fetch(self, **kwargs):
        return {"ok": True}

    def push(self, data=None, **kwargs):
        return {"ok": True}

    def ping(self):
        return True
""".strip(),
        encoding="utf-8",
    )
    yaml_path = config_dir / "plugin.yaml"
    yaml_path.write_text(
        """
pipelines:
  - id: plugin_pipeline
    schedule:
      type: manual
    tasks:
      - id: plugin_task
        connector: plugin_demo
        action: fetch
""".strip(),
        encoding="utf-8",
    )
    loaded = ConfigLoader.load(str(config_dir))
    assert loaded[0].pipeline.id == "plugin_pipeline"
    app = Orchestrator(config_dir=str(config_dir), plugin_dir=str(plugin_dir), db_url=db_url)
    app.ensure_loaded()
    result = app.trigger("plugin_pipeline", triggered_by="manual")
    assert get_connector_class("plugin_demo") is not None
    assert result.status == "success"
