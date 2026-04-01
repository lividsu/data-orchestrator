from __future__ import annotations

import time

import pytest

from orchestrator.connectors import registry
from orchestrator.connectors.base import BaseConnector
from orchestrator.connectors.registry import register_connector
from orchestrator.core.pipeline import Pipeline
from orchestrator.core.runner import PipelineRunner
from orchestrator.core.task import Task
from orchestrator.core.task import TaskStatus
from orchestrator.exceptions import ConfigValidationError


@pytest.fixture(autouse=True)
def clear_registry():
    registry._REGISTRY.clear()
    yield
    registry._REGISTRY.clear()


def _register_recorder_connector(name: str, events: list[tuple[str, float]], fail_ids: set[str] | None = None):
    fail_ids = fail_ids or set()

    @register_connector(name)
    class RecorderConnector(BaseConnector):
        def fetch(self, **kwargs):
            task_id = kwargs["task_id"]
            sleep_seconds = kwargs.get("sleep", 0)
            time.sleep(sleep_seconds)
            events.append((task_id, time.time()))
            if task_id in fail_ids:
                raise ValueError(f"{task_id} failed")
            return kwargs.get("output", task_id)

        def push(self, data=None, **kwargs):
            task_id = kwargs["task_id"]
            sleep_seconds = kwargs.get("sleep", 0)
            time.sleep(sleep_seconds)
            events.append((task_id, time.time()))
            if task_id in fail_ids:
                raise ValueError(f"{task_id} failed")
            return {"data": data}

        def ping(self):
            return True

    return RecorderConnector


def test_sequential_execution():
    events: list[tuple[str, float]] = []
    _register_recorder_connector("seq", events)
    pipeline = Pipeline(
        id="p1",
        max_concurrency=4,
        tasks=[
            Task(id="A", connector="seq", action="fetch", action_kwargs={"task_id": "A"}),
            Task(id="B", connector="seq", action="fetch", depends_on=["A"], action_kwargs={"task_id": "B"}),
            Task(id="C", connector="seq", action="fetch", depends_on=["B"], action_kwargs={"task_id": "C"}),
        ],
    )
    PipelineRunner().run(pipeline)
    order = [task_id for task_id, _ in events]
    assert order == ["A", "B", "C"]
    times = {task_id: ts for task_id, ts in events}
    assert times["A"] <= times["B"] <= times["C"]


def test_parallel_execution():
    events: list[tuple[str, float]] = []
    _register_recorder_connector("parallel", events)
    pipeline = Pipeline(
        id="p2",
        max_concurrency=4,
        tasks=[
            Task(id="A", connector="parallel", action="fetch", action_kwargs={"task_id": "A", "sleep": 0.5}),
            Task(id="B", connector="parallel", action="fetch", action_kwargs={"task_id": "B", "sleep": 0.5}),
        ],
    )
    started = time.time()
    PipelineRunner().run(pipeline)
    duration = time.time() - started
    assert duration < 0.8


def test_fan_in():
    events: list[tuple[str, float]] = []
    _register_recorder_connector("fanin", events)
    pipeline = Pipeline(
        id="p3",
        tasks=[
            Task(id="A", connector="fanin", action="fetch", action_kwargs={"task_id": "A", "sleep": 0.2}),
            Task(id="B", connector="fanin", action="fetch", action_kwargs={"task_id": "B", "sleep": 0.2}),
            Task(
                id="C",
                connector="fanin",
                action="push",
                depends_on=["A", "B"],
                pass_output_from="A",
                action_kwargs={"task_id": "C"},
            ),
        ],
    )
    PipelineRunner().run(pipeline)
    times = {task_id: ts for task_id, ts in events}
    assert times["C"] >= times["A"]
    assert times["C"] >= times["B"]


def test_stop_on_failure_true():
    events: list[tuple[str, float]] = []
    _register_recorder_connector("stop_true", events, fail_ids={"A"})
    pipeline = Pipeline(
        id="p4",
        stop_on_failure=True,
        tasks=[
            Task(id="A", connector="stop_true", action="fetch", action_kwargs={"task_id": "A"}),
            Task(id="B", connector="stop_true", action="fetch", action_kwargs={"task_id": "B"}),
            Task(id="C", connector="stop_true", action="fetch", depends_on=["A"], action_kwargs={"task_id": "C"}),
        ],
    )
    result = PipelineRunner().run(pipeline)
    assert result.task_results["A"].status == TaskStatus.FAILED
    assert result.task_results["B"].status == TaskStatus.SUCCESS
    assert result.task_results["C"].status == TaskStatus.SKIPPED


def test_stop_on_failure_false():
    events: list[tuple[str, float]] = []
    _register_recorder_connector("stop_false", events, fail_ids={"A"})
    pipeline = Pipeline(
        id="p5",
        stop_on_failure=False,
        tasks=[
            Task(id="A", connector="stop_false", action="fetch", action_kwargs={"task_id": "A"}),
            Task(id="B", connector="stop_false", action="fetch", action_kwargs={"task_id": "B"}),
            Task(id="C", connector="stop_false", action="fetch", depends_on=["A"], action_kwargs={"task_id": "C"}),
        ],
    )
    result = PipelineRunner().run(pipeline)
    assert result.task_results["A"].status == TaskStatus.FAILED
    assert result.task_results["B"].status == TaskStatus.SUCCESS
    assert result.task_results["C"].status in {TaskStatus.SUCCESS, TaskStatus.FAILED}


def test_cyclic_dependency_detected_at_init():
    with pytest.raises(ConfigValidationError) as exc:
        Pipeline(
            id="cycle",
            tasks=[
                Task(id="A", connector="x", action="fetch", depends_on=["B"]),
                Task(id="B", connector="x", action="fetch", depends_on=["A"]),
            ],
        )
    assert "A" in str(exc.value) and "B" in str(exc.value)


def test_data_flow_end_to_end():
    received_payload = {}

    @register_connector("flow")
    class FlowConnector(BaseConnector):
        def fetch(self, **kwargs):
            return [1, 2, 3]

        def push(self, data=None, **kwargs):
            received_payload["data"] = data
            return {"ok": True}

        def ping(self):
            return True

    pipeline = Pipeline(
        id="flow",
        tasks=[
            Task(id="fetch", connector="flow", action="fetch"),
            Task(id="push", connector="flow", action="push", depends_on=["fetch"], pass_output_from="fetch"),
        ],
    )
    result = PipelineRunner().run(pipeline)
    assert result.task_results["fetch"].status == TaskStatus.SUCCESS
    assert result.task_results["push"].status == TaskStatus.SUCCESS
    assert received_payload["data"] == [1, 2, 3]


def test_pipeline_result_aggregation():
    events: list[tuple[str, float]] = []
    _register_recorder_connector("agg", events, fail_ids={"A"})
    pipeline = Pipeline(
        id="agg",
        stop_on_failure=True,
        tasks=[
            Task(id="A", connector="agg", action="fetch", action_kwargs={"task_id": "A"}),
            Task(id="B", connector="agg", action="fetch", action_kwargs={"task_id": "B"}),
            Task(id="C", connector="agg", action="fetch", depends_on=["A"], action_kwargs={"task_id": "C"}),
        ],
    )
    result = PipelineRunner().run(pipeline)
    assert result.success_count == 1
    assert result.failure_count == 1
    assert result.skipped_count == 1
    assert result.status == "failed"


def test_max_concurrency_one_degrades_to_serial():
    events: list[tuple[str, float]] = []
    _register_recorder_connector("serial", events)
    pipeline = Pipeline(
        id="serial",
        max_concurrency=1,
        tasks=[
            Task(id="A", connector="serial", action="fetch", action_kwargs={"task_id": "A", "sleep": 0.3}),
            Task(id="B", connector="serial", action="fetch", action_kwargs={"task_id": "B", "sleep": 0.3}),
        ],
    )
    started = time.time()
    PipelineRunner().run(pipeline)
    duration = time.time() - started
    assert duration >= 0.55
