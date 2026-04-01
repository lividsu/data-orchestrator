from __future__ import annotations

from sqlalchemy import Column
from sqlalchemy import Float
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import Text
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

metadata = MetaData()

pipeline_runs = Table(
    "pipeline_runs",
    metadata,
    Column("id", String, primary_key=True),
    Column("pipeline_id", String, nullable=False),
    Column("pipeline_name", String, nullable=False),
    Column("status", String, nullable=False),
    Column("started_at", String, nullable=False),
    Column("finished_at", String),
    Column("duration_seconds", Float),
    Column("total_tasks", Integer, default=0),
    Column("success_count", Integer, default=0),
    Column("failure_count", Integer, default=0),
    Column("skipped_count", Integer, default=0),
    Column("triggered_by", String, default="scheduler"),
)

task_runs = Table(
    "task_runs",
    metadata,
    Column("id", String, primary_key=True),
    Column("pipeline_run_id", String, ForeignKey("pipeline_runs.id"), nullable=False),
    Column("task_id", String, nullable=False),
    Column("task_name", String, nullable=False),
    Column("connector_name", String, nullable=False),
    Column("status", String, nullable=False),
    Column("started_at", String),
    Column("finished_at", String),
    Column("duration_seconds", Float),
    Column("retry_count", Integer, default=0),
    Column("error_type", String),
    Column("error_message", Text),
    Column("error_traceback", Text),
    Column("attempts_json", Text),
)


def create_tables(db_url: str | None = None, engine: Engine | None = None) -> None:
    target_engine = engine or create_engine(db_url or "sqlite:///orchestrator.db")
    metadata.create_all(target_engine)
