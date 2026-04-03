from __future__ import annotations

import logging
import time

import pytest

from orchestrator.connectors import registry
from orchestrator.connectors.base import BaseConnector
from orchestrator.connectors.registry import register_connector
from orchestrator.core.runner import TaskRunner
from orchestrator.core.task import RetryConfig
from orchestrator.core.task import Task
from orchestrator.core.task import TaskResult
from orchestrator.core.task import TaskStatus

logger = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def clear_registry():
    registry._REGISTRY.clear()
    yield
    registry._REGISTRY.clear()


def test_success_first_try():
    @register_connector("success_connector")
    class SuccessConnector(BaseConnector):
        close_calls = 0

        def fetch(self, **kwargs):
            return {"ok": True, "kwargs": kwargs}

        def close(self):
            type(self).close_calls += 1

    task = Task(id="t1", connector="success_connector", action="fetch")
    result = TaskRunner().run(task)

    assert result.status == TaskStatus.SUCCESS
    assert result.output == {"ok": True, "kwargs": {}}
    assert result.retry_count == 0
    assert SuccessConnector.close_calls == 1


def test_retry_then_success():
    @register_connector("retry_success_connector")
    class RetrySuccessConnector(BaseConnector):
        fetch_calls = 0

        def fetch(self, **kwargs):
            type(self).fetch_calls += 1
            if type(self).fetch_calls < 3:
                raise RuntimeError("temporary failure")
            return {"ok": True}

    task = Task(
        id="t2",
        connector="retry_success_connector",
        action="fetch",
        retry=RetryConfig(times=3, delay_seconds=0, backoff=2.0),
    )
    result = TaskRunner().run(task)

    assert result.status == TaskStatus.SUCCESS
    assert result.retry_count == 2
    assert len(result.attempts) == 2
    assert all("at" in attempt and "error" in attempt for attempt in result.attempts)


def test_retry_exhausted():
    @register_connector("retry_exhausted_connector")
    class RetryExhaustedConnector(BaseConnector):
        close_calls = 0

        def fetch(self, **kwargs):
            raise RuntimeError("always failing")

        def close(self):
            type(self).close_calls += 1

    task = Task(
        id="t3",
        connector="retry_exhausted_connector",
        action="fetch",
        retry=RetryConfig(times=2, delay_seconds=0, backoff=2.0),
    )
    result = TaskRunner().run(task)

    assert result.status == TaskStatus.FAILED
    assert result.retry_count == 2
    assert result.error_type == "RuntimeError"
    assert RetryExhaustedConnector.close_calls == 1


def test_timeout():
    @register_connector("timeout_connector")
    class TimeoutConnector(BaseConnector):
        close_calls = 0

        def fetch(self, **kwargs):
            time.sleep(2)
            return {"ok": True}

        def close(self):
            type(self).close_calls += 1

    task = Task(
        id="t4",
        connector="timeout_connector",
        action="fetch",
        retry=RetryConfig(times=0, delay_seconds=0, backoff=2.0),
        timeout_seconds=0.1,
    )
    started = time.time()
    result = TaskRunner().run(task)
    duration = time.time() - started

    assert result.status == TaskStatus.TIMEOUT
    assert result.error_type == "TimeoutError"
    assert TimeoutConnector.close_calls == 1
    assert duration < 1.0


def test_upstream_output_passed_as_data():
    @register_connector("push_connector")
    class PushConnector(BaseConnector):
        last_data = None

        def push(self, data=None, **kwargs):
            type(self).last_data = data
            return {"received": data}

    upstream = TaskResult(task_id="fetch_orders", task_name="fetch_orders", status=TaskStatus.SUCCESS, output={"orders": [1, 2, 3]})
    task = Task(
        id="t5",
        connector="push_connector",
        action="push",
        pass_output_from="fetch_orders",
    )
    result = TaskRunner().run(task, upstream_results={"fetch_orders": upstream})

    assert result.status == TaskStatus.SUCCESS
    assert PushConnector.last_data == {"orders": [1, 2, 3]}


def test_pass_output_from_failed_upstream_marks_skipped():
    @register_connector("skip_push_connector")
    class PushConnector(BaseConnector):
        def push(self, data=None, **kwargs):
            return {"received": data}

    upstream = TaskResult(task_id="fetch_orders", task_name="fetch_orders", status=TaskStatus.FAILED, output=None)
    task = Task(
        id="t5_skip",
        connector="skip_push_connector",
        action="push",
        pass_output_from="fetch_orders",
    )
    result = TaskRunner().run(task, upstream_results={"fetch_orders": upstream})

    assert result.status == TaskStatus.SKIPPED
    assert "skip current task" in (result.error_message or "")


def test_on_failure_hook_called(monkeypatch):
    @register_connector("hook_failure_connector")
    class HookFailureConnector(BaseConnector):
        def fetch(self, **kwargs):
            raise RuntimeError("boom")

    calls = []

    def capture_failure(task, result):
        calls.append((task.id, result.status))

    import orchestrator.notify as notify_module

    monkeypatch.setattr(notify_module, "failure_hook", capture_failure, raising=False)
    task = Task(
        id="t6",
        connector="hook_failure_connector",
        action="fetch",
        on_failure="failure_hook",
        retry=RetryConfig(times=0, delay_seconds=0, backoff=2.0),
    )
    result = TaskRunner().run(task)

    assert result.status == TaskStatus.FAILED
    assert calls == [("t6", TaskStatus.FAILED)]


def test_retry_only_on_specified_exceptions():
    @register_connector("filtered_retry_connector")
    class FilteredRetryConnector(BaseConnector):
        mode = "value_error"
        calls = 0

        def fetch(self, **kwargs):
            type(self).calls += 1
            if type(self).mode == "value_error":
                raise ValueError("do not retry")
            raise ConnectionError("retry me")

    task = Task(
        id="t7",
        connector="filtered_retry_connector",
        action="fetch",
        retry=RetryConfig(times=2, delay_seconds=0, backoff=2.0, on_exceptions=["ConnectionError"]),
    )

    FilteredRetryConnector.mode = "value_error"
    FilteredRetryConnector.calls = 0
    value_error_result = TaskRunner().run(task)
    assert value_error_result.status == TaskStatus.FAILED
    assert value_error_result.retry_count == 0
    assert FilteredRetryConnector.calls == 1

    FilteredRetryConnector.mode = "connection_error"
    FilteredRetryConnector.calls = 0
    connection_error_result = TaskRunner().run(task)
    assert connection_error_result.status == TaskStatus.FAILED
    assert connection_error_result.retry_count == 2
    assert FilteredRetryConnector.calls == 3


def test_timeout_seconds_zero_raises_validation_error():
    with pytest.raises(ValueError, match="timeout_seconds must be greater than 0"):
        Task(id="t8", connector="success_connector", action="fetch", timeout_seconds=0)


def test_custom_action():
    @register_connector("custom_action_connector")
    class CustomActionConnector(BaseConnector):
        def send_report(self, content="", **kwargs):
            return {"sent": True, "content": content}

    task = Task(
        id="t_custom",
        connector="custom_action_connector",
        action="send_report",
        action_kwargs={"content": "日报内容"},
    )
    result = TaskRunner().run(task)

    assert result.status == TaskStatus.SUCCESS
    assert result.output == {"sent": True, "content": "日报内容"}


def test_missing_action_gives_clear_error():
    @register_connector("no_such_action_connector")
    class NoSuchActionConnector(BaseConnector):
        def fetch(self, **kwargs):
            return {}

    task = Task(
        id="t_missing",
        connector="no_such_action_connector",
        action="nonexistent_action",
    )
    result = TaskRunner().run(task)

    assert result.status == TaskStatus.FAILED
    assert "nonexistent_action" in (result.error_message or "")
    assert "fetch" in (result.error_message or "")


def test_unimplemented_default_action_gives_not_implemented():
    @register_connector("fetch_only_connector")
    class FetchOnlyConnector(BaseConnector):
        def fetch(self, **kwargs):
            return {"ok": True}

    task = Task(
        id="t_no_push",
        connector="fetch_only_connector",
        action="push",
    )
    result = TaskRunner().run(task)

    assert result.status == TaskStatus.FAILED
    assert result.error_type == "NotImplementedError"


def test_terminal_output_captures_action_thread_logs():
    @register_connector("log_capture_connector")
    class LogCaptureConnector(BaseConnector):
        def fetch(self, **kwargs):
            logger.warning("connector-thread-log")
            return {"ok": True}

    task = Task(id="t_log", connector="log_capture_connector", action="fetch")
    result = TaskRunner().run(task)

    assert result.status == TaskStatus.SUCCESS
    assert "connector-thread-log" in (result.terminal_output or "")


def test_terminal_output_captures_print_output():
    @register_connector("print_capture_connector")
    class PrintCaptureConnector(BaseConnector):
        def fetch(self, **kwargs):
            print("connector-print-line")
            return {"ok": True}

    task = Task(id="t_print", connector="print_capture_connector", action="fetch")
    result = TaskRunner().run(task)

    assert result.status == TaskStatus.SUCCESS
    assert "connector-print-line" in (result.terminal_output or "")
