from __future__ import annotations

from collections import defaultdict
from collections import deque
from datetime import datetime
from typing import Any
from typing import Literal

from pydantic import BaseModel
from pydantic import Field
from pydantic import field_validator
from pydantic import model_validator

from orchestrator.core.task import Task
from orchestrator.core.task import TaskResult
from orchestrator.exceptions import ConfigValidationError
from orchestrator.exceptions import CyclicDependencyError


class PipelineResult(BaseModel):
    pipeline_id: str
    pipeline_name: str
    run_id: str
    status: Literal["success", "failed", "partial"]
    task_results: dict[str, TaskResult]
    started_at: datetime
    finished_at: datetime
    duration_seconds: float
    total_tasks: int
    success_count: int
    failure_count: int
    skipped_count: int


class Pipeline(BaseModel):
    id: str
    name: str = ""
    description: str = ""
    tasks: list[Task] = Field(default_factory=list)
    max_concurrency: int = 4
    stop_on_failure: bool = True
    on_success: str | None = None
    on_failure: str | None = None

    @field_validator("max_concurrency")
    @classmethod
    def validate_max_concurrency(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("max_concurrency must be greater than 0.")
        return value

    @model_validator(mode="after")
    def validate_dag(self):
        task_ids = {task.id for task in self.tasks}
        for task in self.tasks:
            missing_dependencies = [task_id for task_id in task.depends_on if task_id not in task_ids]
            if missing_dependencies:
                missing_text = ", ".join(missing_dependencies)
                raise ConfigValidationError(
                    f"Task '{task.id}' depends_on missing tasks: {missing_text}"
                )
        try:
            build_execution_layers(self.tasks)
        except CyclicDependencyError as error:
            raise ConfigValidationError(str(error)) from error
        return self


def parse_pipeline(raw: dict[str, Any]) -> Pipeline:
    return Pipeline(
        id=raw["id"],
        name=raw.get("name", ""),
        description=raw.get("description", ""),
        tasks=[Task(**task_data) for task_data in raw.get("tasks", [])],
        max_concurrency=raw.get("max_concurrency", 4),
        stop_on_failure=raw.get("stop_on_failure", True),
        on_success=raw.get("on_success"),
        on_failure=raw.get("on_failure"),
    )


def build_execution_layers(tasks: list[Task]) -> list[list[Task]]:
    task_map = {task.id: task for task in tasks}
    in_degree = {task.id: len(task.depends_on) for task in tasks}
    downstream_map: dict[str, list[str]] = defaultdict(list)
    for task in tasks:
        for dependency in task.depends_on:
            downstream_map[dependency].append(task.id)
    queue = deque(sorted(task.id for task in tasks if in_degree[task.id] == 0))
    layers: list[list[Task]] = []
    visited_count = 0
    while queue:
        layer_task_ids = list(queue)
        queue.clear()
        current_layer = [task_map[task_id] for task_id in layer_task_ids]
        layers.append(current_layer)
        visited_count += len(layer_task_ids)
        next_ready: list[str] = []
        for task_id in layer_task_ids:
            for child_task_id in sorted(downstream_map.get(task_id, [])):
                in_degree[child_task_id] -= 1
                if in_degree[child_task_id] == 0:
                    next_ready.append(child_task_id)
        for task_id in sorted(next_ready):
            queue.append(task_id)
    if visited_count != len(tasks):
        cyclic_task_ids = sorted(task_id for task_id, degree in in_degree.items() if degree > 0)
        cycle_text = ", ".join(cyclic_task_ids)
        raise CyclicDependencyError(f"Cyclic dependency detected among tasks: {cycle_text}")
    return layers
