from __future__ import annotations

import importlib
import io
import json
import logging
import sys
import threading
import traceback
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeoutError
from concurrent.futures import as_completed
from datetime import datetime
from datetime import timezone
from typing import Callable
from typing import Any
from uuid import uuid4

from jinja2 import Environment
from tenacity import RetryCallState
from tenacity import retry
from tenacity import retry_if_exception
from tenacity import stop_after_attempt
from tenacity import wait_exponential

from orchestrator.core.pipeline import Pipeline
from orchestrator.core.pipeline import PipelineResult
from orchestrator.core.pipeline import build_execution_layers
from orchestrator.core.task import Task
from orchestrator.core.task import TaskResult
from orchestrator.core.task import TaskStatus
from orchestrator.connectors.registry import get_connector
from orchestrator.config.template import get_template_context

logger = logging.getLogger(__name__)
_STDIO_PROXY_LOCK = threading.Lock()


class TaskRunner:
    def run(
        self,
        task: Task,
        upstream_results: dict[str, TaskResult] | None = None,
        runtime_context: dict[str, Any] | None = None,
        connector_instance: Any | None = None,
        output_callback: Callable[[str], None] | None = None,
    ) -> TaskResult:
        if task.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be greater than 0.")

        upstream_results = upstream_results or {}
        result = TaskResult(
            task_id=task.id,
            task_name=task.name or task.id,
            connector_name=task.connector_name,
            status=TaskStatus.RUNNING,
            started_at=datetime.now(timezone.utc),
        )
        if task.pass_output_from is not None:
            dependency_result = upstream_results.get(task.pass_output_from)
            if dependency_result is None:
                result.status = TaskStatus.SKIPPED
                result.error_message = f"Upstream task '{task.pass_output_from}' result not found."
                return result
            if dependency_result.status != TaskStatus.SUCCESS:
                result.status = TaskStatus.SKIPPED
                result.error_message = (
                    f"Upstream task '{task.pass_output_from}' is {dependency_result.status.value}, skip current task."
                )
                return result
        connector = connector_instance
        should_close_connector = connector_instance is None
        try:
            logger.info("Task started: %s", task.id)
            if connector is None:
                connector = get_connector(task.connector_name, task.connector_config)
                connector.initialize()
            upstream_output = None
            if task.pass_output_from is not None:
                upstream_output = upstream_results[task.pass_output_from].output

            kwargs = self._render_template_value(task.action_kwargs, runtime_context or {})
            if not isinstance(kwargs, dict):
                raise ValueError("action_kwargs must be a mapping.")
            if upstream_output is not None:
                kwargs["data"] = upstream_output

            exception_names = set(task.retry.on_exceptions)

            def should_retry(exc: BaseException) -> bool:
                if not exception_names:
                    return True
                qualified_name = f"{exc.__class__.__module__}.{exc.__class__.__name__}"
                return exc.__class__.__name__ in exception_names or qualified_name in exception_names

            def record_before_sleep(state: RetryCallState) -> None:
                error = state.outcome.exception()
                if error is None:
                    return
                result.attempts.append(
                    {
                        "at": datetime.now(timezone.utc).isoformat(),
                        "error": f"{error.__class__.__name__}: {error}",
                    }
                )

            @retry(
                stop=stop_after_attempt(task.retry.times + 1),
                wait=wait_exponential(
                    multiplier=task.retry.delay_seconds,
                    exp_base=task.retry.backoff,
                ),
                retry=retry_if_exception(should_retry),
                before_sleep=record_before_sleep,
                reraise=True,
            )
            def execute() -> Any:
                action = getattr(connector, task.action, None)
                if action is None or not callable(action):
                    available_actions = [
                        method
                        for method in dir(connector)
                        if not method.startswith("_") and callable(getattr(connector, method))
                    ]
                    raise AttributeError(
                        f"Connector '{task.connector_name}' does not have action '{task.action}'. "
                        f"Available actions: {available_actions}"
                    )
                return self._execute_with_terminal_capture(
                    action,
                    kwargs,
                    task.timeout_seconds,
                    result,
                    output_callback=output_callback,
                )

            result.output = execute()
            result.status = TaskStatus.SUCCESS
            logger.info("Task succeeded: %s", task.id)
            self._run_hook(task.on_success, task, result)
            return result
        except FuturesTimeoutError as error:
            logger.warning(
                "Task execution exceeded timeout; running thread cannot be forcefully terminated in current model.",
                extra={"task_id": task.id, "connector": task.connector_name, "timeout_seconds": task.timeout_seconds},
            )
            result.status = TaskStatus.TIMEOUT
            result.error_type = error.__class__.__name__
            result.error_message = str(error) or "Task execution timed out."
            result.error_traceback = traceback.format_exc()
            self._run_hook(task.on_failure, task, result)
            return result
        except Exception as error:
            result.status = TaskStatus.FAILED
            logger.error("Task failed: %s", task.id)
            result.error_type = error.__class__.__name__
            result.error_message = str(error)
            result.error_traceback = traceback.format_exc()
            self._run_hook(task.on_failure, task, result)
            return result
        finally:
            result.finished_at = datetime.now(timezone.utc)
            if result.started_at is not None:
                result.duration_seconds = (result.finished_at - result.started_at).total_seconds()
            result.retry_count = len(result.attempts)
            if should_close_connector and connector is not None:
                connector.close()

    def _execute_with_terminal_capture(
        self,
        action: Callable[..., Any],
        kwargs: dict[str, Any],
        timeout_seconds: float,
        result: TaskResult,
        output_callback: Callable[[str], None] | None = None,
    ) -> Any:
        output_buffer = _LiveStringBuffer(output_callback=output_callback)
        handler = _ThreadLogCaptureHandler(output_buffer)
        root_logger = logging.getLogger()
        root_logger.addHandler(handler)
        try:
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(self._execute_action, action, kwargs, handler)
                return future.result(timeout=timeout_seconds)
        finally:
            captured = output_buffer.getvalue().strip()
            if captured:
                result.terminal_output = captured
            root_logger.removeHandler(handler)

    @staticmethod
    def _execute_action(action: Callable[..., Any], kwargs: dict[str, Any], handler: "_ThreadLogCaptureHandler") -> Any:
        thread_id = threading.get_ident()
        handler.bind_thread(thread_id)
        _ensure_stdio_proxy_installed()
        stdout_proxy = sys.stdout
        stderr_proxy = sys.stderr
        if isinstance(stdout_proxy, _ThreadOutputProxy):
            stdout_proxy.register_thread(thread_id, handler.output_buffer)
        if isinstance(stderr_proxy, _ThreadOutputProxy):
            stderr_proxy.register_thread(thread_id, handler.output_buffer)
        try:
            return action(**kwargs)
        finally:
            if isinstance(stdout_proxy, _ThreadOutputProxy):
                stdout_proxy.unregister_thread(thread_id, handler.output_buffer)
            if isinstance(stderr_proxy, _ThreadOutputProxy):
                stderr_proxy.unregister_thread(thread_id, handler.output_buffer)

    def _run_hook(self, hook_name: str | None, task: Task, result: TaskResult) -> None:
        if not hook_name:
            return
        hook = self._resolve_hook(hook_name)
        if hook is None:
            return
        hook(task, result)

    def _resolve_hook(self, hook_name: str):
        if ":" in hook_name:
            module_name, func_name = hook_name.split(":", 1)
            module = importlib.import_module(module_name)
            hook = getattr(module, func_name, None)
            if callable(hook):
                return hook
            return None
        notify_module = importlib.import_module("orchestrator.notify")
        hook = getattr(notify_module, hook_name, None)
        if callable(hook):
            return hook
        try:
            hooks_module = importlib.import_module("orchestrator.notify.hooks")
        except ModuleNotFoundError:
            return None
        hook = getattr(hooks_module, hook_name, None)
        if callable(hook):
            return hook
        return None

    def _render_template_value(self, value: Any, context: dict[str, Any]) -> Any:
        if isinstance(value, str):
            return Environment(autoescape=False).from_string(value).render(context)
        if isinstance(value, list):
            return [self._render_template_value(item, context) for item in value]
        if isinstance(value, tuple):
            return tuple(self._render_template_value(item, context) for item in value)
        if isinstance(value, dict):
            return {
                key: self._render_template_value(item_value, context) for key, item_value in value.items()
            }
        return value


class PipelineRunner:
    def __init__(self, task_runner: TaskRunner | None = None) -> None:
        self.task_runner = task_runner or TaskRunner()

    def run(
        self,
        pipeline: Pipeline,
        log_writer: Any | None = None,
        triggered_by: str = "scheduler",
        notify_manager: Any | None = None,
        notify_policy: Any | None = None,
        run_id: str | None = None,
        pipeline_hook_handler: Callable[[str, PipelineResult], None] | None = None,
        runtime_kwargs: dict[str, Any] | None = None,
        task_output_callback: Callable[[str, str], None] | None = None,
    ) -> PipelineResult:
        run_id = run_id or generate_run_id()
        started_at = datetime.now(timezone.utc)
        runtime_context = get_template_context(run_id=run_id, pipeline_id=pipeline.id, now=started_at.replace(tzinfo=None))
        if runtime_kwargs:
            runtime_context.update(runtime_kwargs)
        if log_writer is not None:
            log_writer.pipeline_run_start(
                pipeline_id=pipeline.id,
                pipeline_name=pipeline.name or pipeline.id,
                run_id=run_id,
                triggered_by=triggered_by,
            )

        all_results: dict[str, TaskResult] = {}
        connector_pool: dict[str, Any] = {}
        connector_pool_lock = threading.Lock()
        layers = build_execution_layers(pipeline.tasks)
        stop_later_layers = False
        try:
            for layer in layers:
                if stop_later_layers:
                    break
                if pipeline.max_concurrency == 1 or len(layer) == 1:
                    layer_results = [
                        self._run_task_with_upstream(
                            task,
                            all_results,
                            runtime_context,
                            connector_pool,
                            connector_pool_lock,
                            task_output_callback,
                        )
                        for task in layer
                    ]
                else:
                    layer_results = self._run_layer_concurrently(
                        layer,
                        all_results,
                        pipeline.max_concurrency,
                        runtime_context,
                        connector_pool,
                        connector_pool_lock,
                        task_output_callback,
                    )
                for result in layer_results:
                    all_results[result.task_id] = result
                    if log_writer is not None:
                        log_writer.task_run_complete(run_id, result)
                if pipeline.stop_on_failure and any(
                    result.status in {TaskStatus.FAILED, TaskStatus.TIMEOUT} for result in layer_results
                ):
                    stop_later_layers = True

            if stop_later_layers:
                for task in pipeline.tasks:
                    if task.id in all_results:
                        continue
                    skipped_result = TaskResult(
                        task_id=task.id,
                        task_name=task.name or task.id,
                        connector_name=task.connector_name,
                        status=TaskStatus.SKIPPED,
                        started_at=started_at,
                        finished_at=datetime.now(timezone.utc),
                        duration_seconds=0.0,
                    )
                    all_results[task.id] = skipped_result
                    if log_writer is not None:
                        log_writer.task_run_complete(run_id, skipped_result)
        finally:
            for connector in connector_pool.values():
                try:
                    connector.close()
                except Exception:
                    continue

        finished_at = datetime.now(timezone.utc)
        success_count = sum(1 for result in all_results.values() if result.status == TaskStatus.SUCCESS)
        failure_count = sum(
            1 for result in all_results.values() if result.status in {TaskStatus.FAILED, TaskStatus.TIMEOUT}
        )
        skipped_count = sum(1 for result in all_results.values() if result.status == TaskStatus.SKIPPED)
        if failure_count == 0 and skipped_count == 0:
            status = "success"
        elif failure_count > 0 and success_count > 0 and skipped_count == 0:
            status = "partial"
        else:
            status = "failed"
        result = PipelineResult(
            pipeline_id=pipeline.id,
            pipeline_name=pipeline.name or pipeline.id,
            run_id=run_id,
            status=status,
            task_results=all_results,
            started_at=started_at,
            finished_at=finished_at,
            duration_seconds=(finished_at - started_at).total_seconds(),
            total_tasks=len(pipeline.tasks),
            success_count=success_count,
            failure_count=failure_count,
            skipped_count=skipped_count,
        )
        if log_writer is not None:
            log_writer.pipeline_run_complete(run_id, result)
        if notify_manager is not None and notify_policy is not None:
            notify_manager.on_pipeline_result(result, notify_policy)
        if pipeline_hook_handler is not None:
            if result.status == "success" and pipeline.on_success:
                pipeline_hook_handler(pipeline.on_success, result)
            elif result.status != "success" and pipeline.on_failure:
                pipeline_hook_handler(pipeline.on_failure, result)
        return result

    def _run_task_with_upstream(
        self,
        task: Task,
        all_results: dict[str, TaskResult],
        runtime_context: dict[str, Any],
        connector_pool: dict[str, Any],
        connector_pool_lock: threading.Lock,
        task_output_callback: Callable[[str, str], None] | None = None,
    ) -> TaskResult:
        upstream_results = {task_id: all_results[task_id] for task_id in task.depends_on if task_id in all_results}
        connector = self._get_or_create_connector(task, connector_pool, connector_pool_lock)
        output_callback: Callable[[str], None] | None = None
        if task_output_callback is not None:
            output_callback = lambda text: task_output_callback(task.id, text)
        return self.task_runner.run(
            task,
            upstream_results=upstream_results,
            runtime_context=runtime_context,
            connector_instance=connector,
            output_callback=output_callback,
        )

    def _run_layer_concurrently(
        self,
        layer: list[Task],
        all_results: dict[str, TaskResult],
        max_concurrency: int,
        runtime_context: dict[str, Any],
        connector_pool: dict[str, Any],
        connector_pool_lock: threading.Lock,
        task_output_callback: Callable[[str, str], None] | None = None,
    ) -> list[TaskResult]:
        ordered_results: dict[str, TaskResult] = {}
        with ThreadPoolExecutor(max_workers=min(max_concurrency, len(layer))) as executor:
            future_to_task_id = {
                executor.submit(
                    self._run_task_with_upstream,
                    task,
                    all_results,
                    runtime_context,
                    connector_pool,
                    connector_pool_lock,
                    task_output_callback,
                ): task.id
                for task in layer
            }
            for future in as_completed(future_to_task_id):
                task_id = future_to_task_id[future]
                ordered_results[task_id] = future.result()
        return [ordered_results[task.id] for task in layer]

    def _get_or_create_connector(
        self,
        task: Task,
        connector_pool: dict[str, Any],
        connector_pool_lock: threading.Lock,
    ) -> Any:
        config_key = json.dumps(task.connector_config, sort_keys=True, default=str)
        pool_key = f"{task.connector_name}:{config_key}"
        existing = connector_pool.get(pool_key)
        if existing is not None:
            return existing
        with connector_pool_lock:
            existing = connector_pool.get(pool_key)
            if existing is not None:
                return existing
            connector = get_connector(task.connector_name, task.connector_config)
            connector.initialize()
            connector_pool[pool_key] = connector
            return connector


def generate_run_id() -> str:
    return f"run-{uuid4().hex}"


class _ThreadLogCaptureHandler(logging.Handler):
    def __init__(self, output_buffer: io.StringIO) -> None:
        super().__init__()
        self.output_buffer = output_buffer
        self.thread_id: int | None = None
        self.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s"))

    def bind_thread(self, thread_id: int) -> None:
        self.thread_id = thread_id

    def emit(self, record: logging.LogRecord) -> None:
        if self.thread_id is None or record.thread != self.thread_id:
            return
        self.output_buffer.write(f"{self.format(record)}\n")


class _LiveStringBuffer(io.StringIO):
    def __init__(self, output_callback: Callable[[str], None] | None = None) -> None:
        super().__init__()
        self._output_callback = output_callback

    def write(self, text: str) -> int:
        if not isinstance(text, str):
            text = str(text)
        if self._output_callback is not None and text:
            self._output_callback(text)
        return super().write(text)


class _ThreadOutputProxy:
    def __init__(self, original_stream) -> None:
        self._original_stream = original_stream
        self._lock = threading.RLock()
        self._buffers: dict[int, list[io.StringIO]] = {}

    def register_thread(self, thread_id: int, output_buffer: io.StringIO) -> None:
        with self._lock:
            bucket = self._buffers.setdefault(thread_id, [])
            bucket.append(output_buffer)

    def unregister_thread(self, thread_id: int, output_buffer: io.StringIO) -> None:
        with self._lock:
            bucket = self._buffers.get(thread_id)
            if not bucket:
                return
            self._buffers[thread_id] = [buffer for buffer in bucket if buffer is not output_buffer]
            if not self._buffers[thread_id]:
                self._buffers.pop(thread_id, None)

    def write(self, text: str) -> int:
        if not isinstance(text, str):
            text = str(text)
        thread_id = threading.get_ident()
        with self._lock:
            buffers = list(self._buffers.get(thread_id, []))
        for output_buffer in buffers:
            output_buffer.write(text)
        return self._original_stream.write(text)

    def flush(self) -> None:
        self._original_stream.flush()

    def writelines(self, lines) -> None:
        for line in lines:
            self.write(line)

    def __getattr__(self, name: str):
        return getattr(self._original_stream, name)


def _ensure_stdio_proxy_installed() -> None:
    with _STDIO_PROXY_LOCK:
        if not isinstance(sys.stdout, _ThreadOutputProxy):
            sys.stdout = _ThreadOutputProxy(sys.stdout)
        if not isinstance(sys.stderr, _ThreadOutputProxy):
            sys.stderr = _ThreadOutputProxy(sys.stderr)
