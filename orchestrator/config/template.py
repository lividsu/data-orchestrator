from __future__ import annotations

from datetime import datetime
from datetime import timedelta
from typing import Any


def get_template_context(
    run_id: str = "",
    pipeline_id: str = "",
    now: datetime | None = None,
) -> dict[str, Any]:
    context_now = now or datetime.now()
    return {
        "now": context_now.isoformat(),
        "today": context_now.strftime("%Y-%m-%d"),
        "yesterday": (context_now - timedelta(days=1)).strftime("%Y-%m-%d"),
        "yesterday_iso": (context_now - timedelta(days=1)).isoformat(),
        "week_start": (context_now - timedelta(days=context_now.weekday())).strftime("%Y-%m-%d"),
        "month_start": context_now.replace(day=1).strftime("%Y-%m-%d"),
        "run_id": run_id,
        "pipeline_id": pipeline_id,
    }
