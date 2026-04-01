from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any
from typing import Literal

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_validator


class RetryConfig(BaseModel):
    times: int = 3
    delay_seconds: float = 5.0
    backoff: float = 2.0
    on_exceptions: list[str] = Field(default_factory=list)

    @field_validator("times")
    @classmethod
    def validate_times(cls, value: int) -> int:
        if value < 0:
            raise ValueError("retry.times must be greater than or equal to 0.")
        return value

    @field_validator("delay_seconds")
    @classmethod
    def validate_delay_seconds(cls, value: float) -> float:
        if value < 0:
            raise ValueError("retry.delay_seconds must be greater than or equal to 0.")
        return value

    @field_validator("backoff")
    @classmethod
    def validate_backoff(cls, value: float) -> float:
        if value <= 0:
            raise ValueError("retry.backoff must be greater than 0.")
        return value


class Task(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    id: str
    name: str = ""
    connector_name: str = Field(alias="connector")
    connector_config: dict[str, Any] = Field(default_factory=dict)
    action: Literal["fetch", "push"]
    action_kwargs: dict[str, Any] = Field(default_factory=dict)
    retry: RetryConfig = Field(default_factory=RetryConfig)
    timeout_seconds: float = 60.0
    depends_on: list[str] = Field(default_factory=list)
    pass_output_from: str | None = None
    on_success: str | None = None
    on_failure: str | None = None

    @field_validator("timeout_seconds")
    @classmethod
    def validate_timeout_seconds(cls, value: float) -> float:
        if value <= 0:
            raise ValueError("timeout_seconds must be greater than 0.")
        return value


class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    TIMEOUT = "timeout"


class TaskResult(BaseModel):
    task_id: str
    task_name: str
    connector_name: str = ""
    status: TaskStatus
    output: Any = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    duration_seconds: float = 0.0
    retry_count: int = 0
    error_type: str | None = None
    error_message: str | None = None
    error_traceback: str | None = None
    attempts: list[dict[str, str]] = Field(default_factory=list)
