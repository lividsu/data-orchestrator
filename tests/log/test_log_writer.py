from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from datetime import timezone
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy import insert
from sqlalchemy import select

from orchestrator.connectors import registry
from orchestrator.connectors.base import BaseConnector
from orchestrator.connectors.registry import register_connector
from orchestrator.core.pipeline import Pipeline
from orchestrator.core.runner import PipelineRunner
from orchestrator.core.task import Task
from orchestrator.log.models import pipeline_runs
from orchestrator.log.models import task_runs
from orchestrator.log.reader import LogReader
from orchestrator.log.writer import LogWriter


@pytest.fixture(autouse=True)
def clear_registry():
    registry._REGISTRY.clear()
    yield
    registry._REGISTRY.clear()


@pytest.fixture
def tmp_db(tmp_path: Path):
    db_path = tmp_path / "orchestrator.db"
    db_url = f"sqlite:///{db_path}"
    return db_url, db_path


def _register_log_connector(name: str, fail: bool = False):
    @register_connector(name)
    class LogConnector(BaseConnector):
        def fetch(self, **kwargs):
            if fail:
                raise ValueError("test error")
            return {"ok": True}

        def push(self, data=None, **kwargs):
            if fail:
                raise ValueError("test error")
            return {"ok": True}

        def ping(self):
            return True


def test_pipeline_run_recorded(tmp_db):
    db_url, _ = tmp_db
    _register_log_connector("log_ok")
    pipeline = Pipeline(
        id="p_log",
        tasks=[
            Task(id="a", connector="log_ok", action="fetch"),
            Task(id="b", connector="log_ok", action="fetch"),
            Task(id="c", connector="log_ok", action="fetch"),
        ],
    )
    writer = LogWriter(db_url=db_url)
    result = PipelineRunner().run(pipeline, log_writer=writer)
    engine = create_engine(db_url)
    with engine.connect() as conn:
        pipeline_count = conn.execute(select(pipeline_runs.c.id)).all()
        task_count = conn.execute(select(task_runs.c.id)).all()
    assert len(pipeline_count) == 1
    assert len(task_count) == 3
    assert result.duration_seconds >= 0


def test_failed_task_traceback_saved(tmp_db):
    db_url, _ = tmp_db
    _register_log_connector("log_fail", fail=True)
    pipeline = Pipeline(id="p_fail", tasks=[Task(id="a", connector="log_fail", action="fetch")])
    writer = LogWriter(db_url=db_url)
    PipelineRunner().run(pipeline, log_writer=writer)
    engine = create_engine(db_url)
    with engine.connect() as conn:
        row = conn.execute(select(task_runs)).mappings().first()
    assert row["error_type"] == "ValueError"
    assert row["error_message"] == "test error"
    assert "Traceback" in row["error_traceback"]


def test_concurrent_pipelines_no_data_race(tmp_db):
    db_url, _ = tmp_db
    _register_log_connector("log_concurrent")
    writer = LogWriter(db_url=db_url)
    runner = PipelineRunner()
    pipeline = Pipeline(id="p_concurrent", tasks=[Task(id="a", connector="log_concurrent", action="fetch")])

    def execute():
        return runner.run(pipeline, log_writer=writer)

    with ThreadPoolExecutor(max_workers=5) as executor:
        list(executor.map(lambda _: execute(), range(5)))
    engine = create_engine(db_url)
    with engine.connect() as conn:
        rows = conn.execute(select(pipeline_runs.c.id)).all()
    assert len(rows) == 5
    assert len({row[0] for row in rows}) == 5


def test_log_write_failure_doesnt_break_pipeline(tmp_db, monkeypatch):
    db_url, _ = tmp_db
    _register_log_connector("log_safe")
    writer = LogWriter(db_url=db_url)

    def raise_error(*args, **kwargs):
        raise RuntimeError("write failed")

    monkeypatch.setattr(writer.engine, "connect", raise_error)
    pipeline = Pipeline(id="p_safe", tasks=[Task(id="a", connector="log_safe", action="fetch")])
    result = PipelineRunner().run(pipeline, log_writer=writer)
    assert result.status == "success"


def test_log_reader_pagination(tmp_db):
    db_url, _ = tmp_db
    writer = LogWriter(db_url=db_url)
    engine = create_engine(db_url)
    with engine.connect() as conn:
        for index in range(100):
            conn.execute(
                insert(pipeline_runs).values(
                    id=f"r{index}",
                    pipeline_id="p",
                    pipeline_name="p",
                    status="success",
                    started_at=f"2024-03-01T00:{index:02d}:00",
                )
            )
        conn.commit()
    reader = LogReader(db_url=db_url)
    page1 = reader.get_pipeline_runs(limit=10, offset=0)
    page10 = reader.get_pipeline_runs(limit=10, offset=90)
    assert len(page1) == 10
    assert len(page10) == 10


def test_log_reader_run_detail_and_count(tmp_db):
    db_url, _ = tmp_db
    LogWriter(db_url=db_url)
    engine = create_engine(db_url)
    with engine.connect() as conn:
        conn.execute(
            insert(pipeline_runs).values(
                id="r1",
                pipeline_id="p1",
                pipeline_name="Pipeline1",
                status="failed",
                started_at="2026-03-30T10:00:00+00:00",
                finished_at="2026-03-30T10:00:10+00:00",
                duration_seconds=10.0,
            )
        )
        conn.execute(
            insert(task_runs).values(
                id="t1",
                pipeline_run_id="r1",
                task_id="task1",
                task_name="task1",
                connector_name="demo",
                status="failed",
                error_type="ValueError",
                error_message="network timeout",
            )
        )
        conn.commit()
    reader = LogReader(db_url=db_url)
    row = reader.get_pipeline_run("r1")
    assert row is not None
    assert row["pipeline_name"] == "Pipeline1"
    total = reader.count_pipeline_runs(keyword="network")
    assert total == 1
    failed_map = reader.count_failed_tasks(["r1"])
    assert failed_map["r1"] == 1


def test_log_reader_dashboard_stats_hourly(tmp_db):
    db_url, _ = tmp_db
    now = datetime.now(timezone.utc)
    LogWriter(db_url=db_url)
    engine = create_engine(db_url)
    with engine.connect() as conn:
        conn.execute(
            insert(pipeline_runs).values(
                id="dashboard1",
                pipeline_id="p-dashboard",
                pipeline_name="dashboard",
                status="success",
                started_at=now.isoformat(),
                duration_seconds=2.5,
            )
        )
        conn.execute(
            insert(pipeline_runs).values(
                id="dashboard2",
                pipeline_id="p-dashboard",
                pipeline_name="dashboard",
                status="failed",
                started_at=now.isoformat(),
                duration_seconds=4.5,
            )
        )
        conn.commit()
    reader = LogReader(db_url=db_url)
    stats = reader.get_dashboard_stats(hours=24)
    assert stats["total_runs"] >= 2
    assert stats["success_runs"] >= 1
    assert stats["failed_runs"] >= 1
    assert len(stats["runs_by_hour"]) == 25
