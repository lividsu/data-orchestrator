from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any

from pydantic import BaseModel
from pydantic import Field

from orchestrator.notify.base import get_notifier

logger = logging.getLogger(__name__)


class NotifyChannelConfig(BaseModel):
    name: str
    config: dict[str, Any] = Field(default_factory=dict)


class NotifyPolicy(BaseModel):
    on_task_failure: bool = True
    on_pipeline_failure: bool = True
    on_pipeline_success: bool = False
    failure_threshold: int = 1
    channels: list[NotifyChannelConfig] = Field(default_factory=list)


class NotifyManager:
    def __init__(self) -> None:
        self._failure_counts: dict[str, int] = defaultdict(int)

    def on_pipeline_result(self, pipeline_result, policy: NotifyPolicy) -> None:
        pipeline_id = pipeline_result.pipeline_id
        if pipeline_result.status == "success":
            self._failure_counts[pipeline_id] = 0
            if policy.on_pipeline_success:
                self._send_pipeline_notification(pipeline_result, policy, "info")
            return
        self._failure_counts[pipeline_id] += 1
        if self._failure_counts[pipeline_id] < policy.failure_threshold:
            return
        if policy.on_pipeline_failure:
            self._send_pipeline_notification(pipeline_result, policy, "error")
        if policy.on_task_failure:
            self._send_task_failure_notifications(pipeline_result, policy)

    def _send_pipeline_notification(self, pipeline_result, policy: NotifyPolicy, level: str) -> None:
        title = f"Pipeline {pipeline_result.pipeline_id} {pipeline_result.status}"
        body = (
            f"run_id={pipeline_result.run_id}\n"
            f"status={pipeline_result.status}\n"
            f"success={pipeline_result.success_count}\n"
            f"failure={pipeline_result.failure_count}\n"
            f"skipped={pipeline_result.skipped_count}"
        )
        context = {
            "pipeline_id": pipeline_result.pipeline_id,
            "pipeline_name": pipeline_result.pipeline_name,
            "run_id": pipeline_result.run_id,
            "started_at": pipeline_result.started_at.isoformat(),
            "duration_seconds": pipeline_result.duration_seconds,
        }
        self._dispatch_to_channels(policy.channels, title, body, level, context)

    def _send_task_failure_notifications(self, pipeline_result, policy: NotifyPolicy) -> None:
        for task_result in pipeline_result.task_results.values():
            status_value = task_result.status.value if hasattr(task_result.status, "value") else str(task_result.status)
            if status_value not in {"failed", "timeout"}:
                continue
            title = f"Task {task_result.task_id} failed"
            body = task_result.error_message or "Task execution failed"
            context = {
                "pipeline_id": pipeline_result.pipeline_id,
                "pipeline_name": pipeline_result.pipeline_name,
                "run_id": pipeline_result.run_id,
                "task_id": task_result.task_id,
                "error_message": task_result.error_message,
                "error_type": task_result.error_type,
                "started_at": task_result.started_at.isoformat() if task_result.started_at else "",
                "duration_seconds": task_result.duration_seconds,
            }
            self._dispatch_to_channels(policy.channels, title, body, "error", context)

    def _dispatch_to_channels(
        self,
        channels: list[NotifyChannelConfig],
        title: str,
        body: str,
        level: str,
        context: dict[str, Any],
    ) -> None:
        for channel in channels:
            try:
                notifier_cls = get_notifier(channel.name)
                notifier = notifier_cls(channel.config)
                notifier.send(title=title, body=body, level=level, context=context)
            except Exception:
                logger.exception("Notifier send failed for channel=%s", channel.name)
