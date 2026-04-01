from __future__ import annotations

import signal
import threading
from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from datetime import timezone
from pathlib import Path
from uuid import uuid4

from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.background import BackgroundScheduler

from orchestrator.config.loader import ConfigLoader
from orchestrator.config.settings import Settings
from orchestrator.connectors.loader import load_plugins as load_connector_plugins
from orchestrator.connectors.registry import get_connector
from orchestrator.connectors.registry import list_connectors
from orchestrator.core.pipeline import Pipeline
from orchestrator.core.runner import PipelineRunner
from orchestrator.core.schedule import ScheduleConfig
from orchestrator.log.writer import LogWriter
from orchestrator.notify import NotifyManager
from orchestrator.notify import NotifyPolicy

_ORCHESTRATOR_INSTANCES: dict[str, "Orchestrator"] = {}


def _execute_pipeline_job(instance_id: str, pipeline_id: str, triggered_by: str = "scheduler"):
    orchestrator = _ORCHESTRATOR_INSTANCES.get(instance_id)
    if orchestrator is None:
        return None
    return orchestrator.trigger(pipeline_id, triggered_by=triggered_by)


class Orchestrator:
    def __init__(
        self,
        plugin_dir: str | None = None,
        config_dir: str | None = None,
        db_url: str | None = None,
        settings: Settings | None = None,
    ) -> None:
        self.settings = settings or Settings()
        if db_url:
            self.settings.db_url = db_url
        self.config_dir = Path(config_dir) if config_dir else None
        self.plugin_dir = Path(plugin_dir) if plugin_dir else None
        self._scheduler = BackgroundScheduler(
            jobstores={"default": SQLAlchemyJobStore(url=self.settings.db_url)},
            timezone=self.settings.timezone,
        )
        self._log_writer = LogWriter(db_url=self.settings.db_url)
        self._notify_manager = NotifyManager()
        self._pipeline_runner = PipelineRunner()
        self._pipelines: dict[str, Pipeline] = {}
        self._schedules: dict[str, ScheduleConfig] = {}
        self._notify_policies: dict[str, NotifyPolicy] = {}
        self._running_futures: set[Future] = set()
        self._executor = ThreadPoolExecutor(max_workers=16)
        self._stop_event = threading.Event()
        self._loaded = False
        self._instance_id = f"orchestrator-{uuid4().hex}"
        _ORCHESTRATOR_INSTANCES[self._instance_id] = self

    def register(self, pipeline: Pipeline, schedule: ScheduleConfig, notify: NotifyPolicy | None = None):
        self._pipelines[pipeline.id] = pipeline
        self._schedules[pipeline.id] = schedule
        if notify is not None:
            self._notify_policies[pipeline.id] = notify
        if schedule.type == "manual":
            return
        if schedule.type == "interval":
            self._scheduler.add_job(
                _execute_pipeline_job,
                trigger="interval",
                seconds=schedule.interval_seconds,
                id=pipeline.id,
                kwargs={
                    "instance_id": self._instance_id,
                    "pipeline_id": pipeline.id,
                    "triggered_by": "scheduler",
                },
                max_instances=schedule.max_instances,
                replace_existing=True,
                start_date=schedule.start_date,
                end_date=schedule.end_date,
            )
            return
        self._scheduler.add_job(
            _execute_pipeline_job,
            trigger="cron",
            id=pipeline.id,
            kwargs={
                "instance_id": self._instance_id,
                "pipeline_id": pipeline.id,
                "triggered_by": "scheduler",
            },
            max_instances=schedule.max_instances,
            replace_existing=True,
            start_date=schedule.start_date,
            end_date=schedule.end_date,
            timezone=schedule.timezone,
            **self._parse_cron_expr(schedule.cron_expr or ""),
        )

    def load_config(self, config_dir: str):
        for item in ConfigLoader.load(config_dir):
            self.register(item.pipeline, item.schedule, item.notify)

    def load_plugins(self, plugin_dir: str):
        load_connector_plugins(plugin_dir)

    def ensure_loaded(self) -> None:
        if self._loaded:
            return
        if self.plugin_dir:
            self.load_plugins(str(self.plugin_dir))
        if self.config_dir:
            self.load_config(str(self.config_dir))
        self._loaded = True

    def trigger(self, pipeline_id: str, triggered_by: str = "manual", run_id: str | None = None):
        pipeline = self._pipelines[pipeline_id]
        notify_policy = self._notify_policies.get(pipeline_id)
        return self._pipeline_runner.run(
            pipeline,
            log_writer=self._log_writer,
            triggered_by=triggered_by,
            notify_manager=self._notify_manager,
            notify_policy=notify_policy,
            run_id=run_id,
        )

    def trigger_async(self, pipeline_id: str) -> str:
        run_id = f"run-{uuid4().hex}"

        def runner():
            self.trigger(pipeline_id, triggered_by="manual", run_id=run_id)

        future = self._executor.submit(runner)
        self._running_futures.add(future)
        future.add_done_callback(lambda f: self._running_futures.discard(f))
        return run_id

    def pause(self, pipeline_id: str):
        self._scheduler.pause_job(pipeline_id)

    def resume(self, pipeline_id: str):
        self._scheduler.resume_job(pipeline_id)

    def ping_all(self) -> dict[str, bool]:
        result: dict[str, bool] = {}
        for connector_name in list_connectors():
            try:
                connector = get_connector(connector_name, {})
                connector.initialize()
                result[connector_name] = bool(connector.ping())
                connector.close()
            except Exception:
                result[connector_name] = False
        return result

    def list_pipelines(self) -> list[dict[str, str | None]]:
        items: list[dict[str, str | None]] = []
        for pipeline_id, pipeline in sorted(self._pipelines.items()):
            schedule = self._schedules.get(pipeline_id)
            schedule_text = "manual"
            if schedule is not None and schedule.type == "interval":
                schedule_text = f"interval({schedule.interval_seconds}s)"
            if schedule is not None and schedule.type == "cron":
                schedule_text = schedule.cron_expr or "cron"
            job = self._scheduler.get_job(pipeline_id)
            next_run = job.next_run_time.isoformat() if job and job.next_run_time else None
            status = "paused" if job and job.next_run_time is None else "active"
            if schedule is not None and schedule.type == "manual":
                status = "manual"
            items.append(
                {
                    "id": pipeline_id,
                    "name": pipeline.name or pipeline_id,
                    "schedule": schedule_text,
                    "next_run": next_run,
                    "status": status,
                }
            )
        return items

    def get_pipeline(self, pipeline_id: str) -> Pipeline | None:
        return self._pipelines.get(pipeline_id)

    def get_upcoming_runs(self, hours: int = 2) -> list[dict[str, str]]:
        now = datetime.now(timezone.utc)
        threshold = now.timestamp() + (hours * 3600)
        upcoming: list[dict[str, str]] = []
        for item in self.list_pipelines():
            next_run = item.get("next_run")
            if not next_run:
                continue
            try:
                trigger_at = datetime.fromisoformat(next_run)
            except ValueError:
                continue
            delta_seconds = int(trigger_at.timestamp() - now.timestamp())
            if trigger_at.timestamp() > threshold or delta_seconds < 0:
                continue
            upcoming.append(
                {
                    "pipeline_name": str(item["name"]),
                    "trigger_time": trigger_at.isoformat(),
                    "in_seconds": str(delta_seconds),
                }
            )
        return sorted(upcoming, key=lambda value: value["trigger_time"])

    def start(self, ui: bool = True, host: str = "0.0.0.0", port: int = 8501, headless: bool = False) -> None:
        self.ensure_loaded()
        try:
            from orchestrator import streamlit_thread

            streamlit_thread.set_orchestrator(self)
        except Exception:
            pass
        self._register_signal_handlers()
        
        if ui:
            def _run_scheduler():
                if not self._scheduler.running:
                    self._scheduler.start()
                self._stop_event.wait()

            import threading
            scheduler_thread = threading.Thread(target=_run_scheduler, daemon=True)
            scheduler_thread.start()
            
            from orchestrator.ui.app import run_ui
            print(f"UI configured at http://{host}:{port}")
            try:
                run_ui(db_url=self.settings.db_url, host=host, port=port, headless=headless)
            finally:
                self.stop()
                scheduler_thread.join(timeout=1)
        else:
            if not self._scheduler.running:
                self._scheduler.start()
            self._stop_event.wait()

    def stop(self):
        if self._scheduler.running:
            self._scheduler.shutdown(wait=False)
        timeout = self.settings.default_timeout_seconds
        started = datetime.now().timestamp()
        while self._running_futures and (datetime.now().timestamp() - started) < timeout:
            done_futures = {future for future in self._running_futures if future.done()}
            self._running_futures -= done_futures
        self._executor.shutdown(wait=False, cancel_futures=False)
        self._log_writer.engine.dispose()
        self._stop_event.set()
        _ORCHESTRATOR_INSTANCES.pop(self._instance_id, None)

    def _register_signal_handlers(self) -> None:
        def handler(signum, frame):
            self.stop()

        try:
            signal.signal(signal.SIGINT, handler)
            if hasattr(signal, "SIGTERM"):
                signal.signal(signal.SIGTERM, handler)
        except ValueError:
            return

    @staticmethod
    def _parse_cron_expr(cron_expr: str) -> dict[str, str]:
        fields = cron_expr.split()
        if len(fields) != 5:
            raise ValueError(f"Invalid cron expression: {cron_expr}")
        return {
            "minute": fields[0],
            "hour": fields[1],
            "day": fields[2],
            "month": fields[3],
            "day_of_week": fields[4],
        }
