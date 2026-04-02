from __future__ import annotations

from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Any

from sqlalchemy import create_engine
from sqlalchemy import case
from sqlalchemy import cast
from sqlalchemy import func
from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy.engine import Engine

from orchestrator.log.models import create_tables
from orchestrator.log.models import pipeline_runs
from orchestrator.log.models import task_runs


class LogReader:
    def __init__(self, db_url: str, engine: Engine | None = None) -> None:
        self.engine = engine or create_engine(db_url)
        create_tables(engine=self.engine)

    def get_pipeline_runs(
        self,
        pipeline_id: str | None = None,
        pipeline_ids: list[str] | None = None,
        status: str | None = None,
        statuses: list[str] | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        keyword: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[dict]:
        statement = select(pipeline_runs).order_by(pipeline_runs.c.started_at.desc()).limit(limit).offset(offset)
        if pipeline_id:
            statement = statement.where(pipeline_runs.c.pipeline_id == pipeline_id)
        if pipeline_ids:
            statement = statement.where(pipeline_runs.c.pipeline_id.in_(pipeline_ids))
        if status:
            statement = statement.where(pipeline_runs.c.status == status)
        if statuses:
            statement = statement.where(pipeline_runs.c.status.in_(statuses))
        if start_time:
            statement = statement.where(pipeline_runs.c.started_at >= start_time.isoformat())
        if end_time:
            statement = statement.where(pipeline_runs.c.started_at <= end_time.isoformat())
        if keyword:
            keyword_clause = f"%{keyword.strip()}%"
            failed_run_ids = (
                select(task_runs.c.pipeline_run_id)
                .where(
                    or_(
                        task_runs.c.error_message.like(keyword_clause),
                        task_runs.c.error_type.like(keyword_clause),
                        task_runs.c.error_traceback.like(keyword_clause),
                        task_runs.c.terminal_output.like(keyword_clause),
                    )
                )
                .distinct()
            )
            statement = statement.where(pipeline_runs.c.id.in_(failed_run_ids))
        with self.engine.connect() as conn:
            rows = conn.execute(statement).mappings().all()
        return [dict(row) for row in rows]

    def get_task_runs(self, pipeline_run_id: str) -> list[dict]:
        statement = (
            select(task_runs)
            .where(task_runs.c.pipeline_run_id == pipeline_run_id)
            .order_by(task_runs.c.started_at.asc())
        )
        with self.engine.connect() as conn:
            rows = conn.execute(statement).mappings().all()
        return [dict(row) for row in rows]

    def get_pipeline_run(self, run_id: str) -> dict[str, Any] | None:
        statement = select(pipeline_runs).where(pipeline_runs.c.id == run_id).limit(1)
        with self.engine.connect() as conn:
            row = conn.execute(statement).mappings().first()
        if row is None:
            return None
        return dict(row)

    def count_pipeline_runs(
        self,
        pipeline_ids: list[str] | None = None,
        statuses: list[str] | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        keyword: str | None = None,
    ) -> int:
        statement = select(func.count()).select_from(pipeline_runs)
        if pipeline_ids:
            statement = statement.where(pipeline_runs.c.pipeline_id.in_(pipeline_ids))
        if statuses:
            statement = statement.where(pipeline_runs.c.status.in_(statuses))
        if start_time:
            statement = statement.where(pipeline_runs.c.started_at >= start_time.isoformat())
        if end_time:
            statement = statement.where(pipeline_runs.c.started_at <= end_time.isoformat())
        if keyword:
            keyword_clause = f"%{keyword.strip()}%"
            failed_run_ids = (
                select(task_runs.c.pipeline_run_id)
                .where(
                    or_(
                        task_runs.c.error_message.like(keyword_clause),
                        task_runs.c.error_type.like(keyword_clause),
                        task_runs.c.error_traceback.like(keyword_clause),
                        task_runs.c.terminal_output.like(keyword_clause),
                    )
                )
                .distinct()
            )
            statement = statement.where(pipeline_runs.c.id.in_(failed_run_ids))
        with self.engine.connect() as conn:
            total = conn.execute(statement).scalar_one()
        return int(total or 0)

    def get_dashboard_stats(self, hours: int = 24) -> dict:
        now = datetime.now(timezone.utc)
        start_time = now - timedelta(hours=hours)
        start_iso = start_time.isoformat()
        dialect_name = self.engine.dialect.name
        if dialect_name == "sqlite":
            hour_bucket = func.strftime("%Y-%m-%dT%H", pipeline_runs.c.started_at)
        else:
            hour_bucket = func.substr(cast(pipeline_runs.c.started_at, String), 1, 13)
        with self.engine.connect() as conn:
            total_runs = conn.execute(
                select(func.count()).select_from(pipeline_runs).where(pipeline_runs.c.started_at >= start_iso)
            ).scalar_one()
            success_runs = conn.execute(
                select(func.count())
                .select_from(pipeline_runs)
                .where(pipeline_runs.c.status == "success", pipeline_runs.c.started_at >= start_iso)
            ).scalar_one()
            failed_runs = conn.execute(
                select(func.count())
                .select_from(pipeline_runs)
                .where(pipeline_runs.c.status.in_(["failed", "partial"]), pipeline_runs.c.started_at >= start_iso)
            ).scalar_one()
            avg_duration = conn.execute(
                select(func.avg(pipeline_runs.c.duration_seconds)).where(pipeline_runs.c.started_at >= start_iso)
            ).scalar_one()
            runs_by_hour_rows = conn.execute(
                select(
                    hour_bucket.label("hour"),
                    func.count().label("count"),
                    func.sum(case((pipeline_runs.c.status == "success", 1), else_=0)).label("success_count"),
                )
                .where(pipeline_runs.c.started_at >= start_iso)
                .group_by("hour")
                .order_by("hour")
            ).mappings().all()
            recent_runs_rows = conn.execute(
                select(pipeline_runs).order_by(pipeline_runs.c.started_at.desc()).limit(10)
            ).mappings().all()
        hourly_map: dict[str, dict[str, float]] = {}
        for row in runs_by_hour_rows:
            total_count = int(row["count"] or 0)
            success_count = int(row["success_count"] or 0)
            hourly_map[row["hour"]] = {
                "count": total_count,
                "success_rate": (success_count / total_count) if total_count else 0.0,
            }
        runs_by_hour: list[dict[str, float | str]] = []
        cursor = start_time.replace(minute=0, second=0, microsecond=0)
        for _ in range(hours + 1):
            key = cursor.isoformat()[:13]
            value = hourly_map.get(key, {"count": 0, "success_rate": 0.0})
            runs_by_hour.append({"hour": key, "count": value["count"], "success_rate": value["success_rate"]})
            cursor += timedelta(hours=1)
        success_rate = (success_runs / total_runs) if total_runs else 0.0
        return {
            "total_runs": int(total_runs or 0),
            "success_runs": int(success_runs or 0),
            "failed_runs": int(failed_runs or 0),
            "success_rate": success_rate,
            "avg_duration": float(avg_duration or 0.0),
            "runs_by_hour": runs_by_hour,
            "recent_runs": [dict(row) for row in recent_runs_rows],
            "next_scheduled": [],
        }

    def count_failed_tasks(self, run_ids: list[str]) -> dict[str, int]:
        if not run_ids:
            return {}
        statement = (
            select(task_runs.c.pipeline_run_id, func.count().label("count"))
            .where(task_runs.c.pipeline_run_id.in_(run_ids), task_runs.c.status.in_(["failed", "timeout"]))
            .group_by(task_runs.c.pipeline_run_id)
        )
        with self.engine.connect() as conn:
            rows = conn.execute(statement).mappings().all()
        return {row["pipeline_run_id"]: int(row["count"]) for row in rows}
