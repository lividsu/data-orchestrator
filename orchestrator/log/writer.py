from __future__ import annotations

import json
import logging
import threading
from uuid import uuid4

from sqlalchemy import create_engine
from sqlalchemy import insert
from sqlalchemy import update
from sqlalchemy.engine import Engine

from orchestrator.log.models import create_tables
from orchestrator.log.models import pipeline_runs
from orchestrator.log.models import task_runs

logger = logging.getLogger(__name__)


class LogWriter:
    def __init__(self, db_url: str, engine: Engine | None = None) -> None:
        self.engine = engine or create_engine(db_url)
        create_tables(engine=self.engine)
        self._lock = threading.Lock()

    def pipeline_run_start(self, pipeline_id: str, pipeline_name: str, run_id: str, triggered_by: str) -> None:
        payload = {
            "id": run_id,
            "pipeline_id": pipeline_id,
            "pipeline_name": pipeline_name,
            "status": "running",
            "started_at": self._utc_now_iso(),
            "triggered_by": triggered_by,
        }
        self._safe_write(insert(pipeline_runs).values(**payload))

    def task_run_complete(self, pipeline_run_id: str, task_result) -> None:
        payload = {
            "id": f"task-run-{uuid4().hex}",
            "pipeline_run_id": pipeline_run_id,
            "task_id": task_result.task_id,
            "task_name": task_result.task_name,
            "connector_name": getattr(task_result, "connector_name", "unknown"),
            "status": task_result.status.value if hasattr(task_result.status, "value") else str(task_result.status),
            "started_at": task_result.started_at.isoformat() if task_result.started_at else None,
            "finished_at": task_result.finished_at.isoformat() if task_result.finished_at else None,
            "duration_seconds": task_result.duration_seconds,
            "retry_count": task_result.retry_count,
            "error_type": task_result.error_type,
            "error_message": task_result.error_message,
            "error_traceback": task_result.error_traceback,
            "attempts_json": json.dumps(task_result.attempts, ensure_ascii=False),
        }
        self._safe_write(insert(task_runs).values(**payload))

    def pipeline_run_complete(self, run_id: str, pipeline_result) -> None:
        payload = {
            "status": pipeline_result.status,
            "finished_at": pipeline_result.finished_at.isoformat(),
            "duration_seconds": pipeline_result.duration_seconds,
            "total_tasks": pipeline_result.total_tasks,
            "success_count": pipeline_result.success_count,
            "failure_count": pipeline_result.failure_count,
            "skipped_count": pipeline_result.skipped_count,
        }
        statement = update(pipeline_runs).where(pipeline_runs.c.id == run_id).values(**payload)
        self._safe_write(statement)

    def _safe_write(self, statement) -> None:
        try:
            with self._lock:
                with self.engine.connect() as conn:
                    conn.execute(statement)
                    conn.commit()
        except Exception:
            logger.exception("Failed to write execution log.")

    @staticmethod
    def _utc_now_iso() -> str:
        from datetime import datetime
        from datetime import timezone

        return datetime.now(timezone.utc).isoformat()
