from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel
from pydantic import model_validator


class ScheduleConfig(BaseModel):
    type: Literal["cron", "interval", "manual"]
    cron_expr: str | None = None
    interval_seconds: int | None = None
    timezone: str = "Asia/Shanghai"
    max_instances: int = 1
    start_date: datetime | None = None
    end_date: datetime | None = None

    @model_validator(mode="after")
    def validate_config(self):
        if self.type == "cron" and not self.cron_expr:
            raise ValueError("cron_expr required for cron schedule")
        if self.type == "interval" and not self.interval_seconds:
            raise ValueError("interval_seconds required for interval schedule")
        return self
