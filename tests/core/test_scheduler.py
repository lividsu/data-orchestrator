from __future__ import annotations

import threading
import time
from pathlib import Path

import pytest

from orchestrator.connectors import registry
from orchestrator.connectors.base import BaseConnector
from orchestrator.connectors.registry import register_connector
from orchestrator.core.pipeline import Pipeline
from orchestrator.core.schedule import ScheduleConfig
from orchestrator.core.scheduler import Orchestrator
from orchestrator.core.task import Task


@pytest.fixture(autouse=True)
def clear_registry():
    registry._REGISTRY.clear()
    yield
    registry._REGISTRY.clear()


def test_interval_trigger_count(tmp_path: Path):
    calls = []

    @register_connector("sched_interval")
    class C(BaseConnector):
        def fetch(self, **kwargs):
            calls.append(time.time())
            return {"ok": True}

        def push(self, data=None, **kwargs):
            return {"ok": True}

        def ping(self):
            return True

    db_url = f"sqlite:///{tmp_path / 'sched.db'}"
    app = Orchestrator(db_url=db_url)
    pipeline = Pipeline(id="p", tasks=[Task(id="a", connector="sched_interval", action="fetch")])
    app.register(pipeline, ScheduleConfig(type="interval", interval_seconds=1))
    thread = threading.Thread(target=app.start, daemon=True)
    thread.start()
    time.sleep(3.5)
    app.stop()
    thread.join(timeout=1)
    assert 3 <= len(calls) <= 4


def test_manual_trigger(tmp_path: Path):
    calls = []

    @register_connector("sched_manual")
    class C(BaseConnector):
        def fetch(self, **kwargs):
            calls.append(1)
            return {"ok": True}

        def push(self, data=None, **kwargs):
            return {"ok": True}

        def ping(self):
            return True

    app = Orchestrator(db_url=f"sqlite:///{tmp_path / 'manual.db'}")
    pipeline = Pipeline(id="manual_p", tasks=[Task(id="a", connector="sched_manual", action="fetch")])
    app.register(pipeline, ScheduleConfig(type="manual"))
    result = app.trigger("manual_p", triggered_by="manual")
    assert result.pipeline_id == "manual_p"
    assert len(calls) == 1


def test_max_instances_1(tmp_path: Path):
    running_count = 0
    max_seen = 0
    lock = threading.Lock()

    @register_connector("sched_max")
    class C(BaseConnector):
        def fetch(self, **kwargs):
            nonlocal running_count, max_seen
            with lock:
                running_count += 1
                max_seen = max(max_seen, running_count)
            time.sleep(2)
            with lock:
                running_count -= 1
            return {"ok": True}

        def push(self, data=None, **kwargs):
            return {"ok": True}

        def ping(self):
            return True

    app = Orchestrator(db_url=f"sqlite:///{tmp_path / 'max.db'}")
    pipeline = Pipeline(id="max_p", tasks=[Task(id="a", connector="sched_max", action="fetch")])
    app.register(pipeline, ScheduleConfig(type="interval", interval_seconds=1, max_instances=1))
    thread = threading.Thread(target=app.start, daemon=True)
    thread.start()
    time.sleep(3.2)
    app.stop()
    thread.join(timeout=1)
    assert max_seen == 1


def test_pause_and_resume(tmp_path: Path):
    calls = []

    @register_connector("sched_pause")
    class C(BaseConnector):
        def fetch(self, **kwargs):
            calls.append(time.time())
            return {"ok": True}

        def push(self, data=None, **kwargs):
            return {"ok": True}

        def ping(self):
            return True

    app = Orchestrator(db_url=f"sqlite:///{tmp_path / 'pause.db'}")
    pipeline = Pipeline(id="pause_p", tasks=[Task(id="a", connector="sched_pause", action="fetch")])
    app.register(pipeline, ScheduleConfig(type="interval", interval_seconds=1))
    thread = threading.Thread(target=app.start, daemon=True)
    thread.start()
    time.sleep(2)
    before_pause = len(calls)
    app.pause("pause_p")
    time.sleep(2)
    during_pause = len(calls) - before_pause
    app.resume("pause_p")
    time.sleep(2)
    app.stop()
    thread.join(timeout=1)
    assert during_pause == 0
    assert len(calls) > before_pause


def test_survive_restart(tmp_path: Path):
    pipelines_dir = tmp_path / "pipelines"
    pipelines_dir.mkdir()
    yaml_file = pipelines_dir / "pipes.yaml"
    yaml_file.write_text(
        """
pipelines:
  - id: p1
    schedule:
      type: interval
      interval_seconds: 1
    tasks:
      - id: a
        connector: sched_restart
        action: fetch
  - id: p2
    schedule:
      type: interval
      interval_seconds: 1
    tasks:
      - id: b
        connector: sched_restart
        action: fetch
""".strip(),
        encoding="utf-8",
    )
    calls = []

    @register_connector("sched_restart")
    class C(BaseConnector):
        def fetch(self, **kwargs):
            calls.append(time.time())
            return {"ok": True}

        def push(self, data=None, **kwargs):
            return {"ok": True}

        def ping(self):
            return True

    db_url = f"sqlite:///{tmp_path / 'restart.db'}"
    app1 = Orchestrator(config_dir=str(pipelines_dir), db_url=db_url)
    t1 = threading.Thread(target=app1.start, daemon=True)
    t1.start()
    time.sleep(1.2)
    app1.stop()
    t1.join(timeout=1)
    first_count = len(calls)

    app2 = Orchestrator(config_dir=str(pipelines_dir), db_url=db_url)
    t2 = threading.Thread(target=app2.start, daemon=True)
    t2.start()
    time.sleep(1.2)
    app2.stop()
    t2.join(timeout=1)
    assert len(calls) > first_count
