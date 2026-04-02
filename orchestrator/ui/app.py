from __future__ import annotations

import csv
import io
import json
import os
import time
from datetime import datetime
from datetime import timezone
from datetime import timedelta
from pathlib import Path
from typing import Any
from urllib.request import Request
from urllib.request import urlopen

from orchestrator.log.reader import LogReader
from orchestrator.streamlit_thread import get_orchestrator


def _format_time(iso_str: str | None, tz_name: str) -> str:
    if not iso_str:
        return "-"
    try:
        dt = datetime.fromisoformat(iso_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
            
        if tz_name == "pst":
            dt = dt.astimezone(timezone(timedelta(hours=-8)))
        elif tz_name == "cst":
            dt = dt.astimezone(timezone(timedelta(hours=8)))
        else:
            dt = dt.astimezone(timezone.utc)
            
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(iso_str)


def run_ui(db_url: str = "sqlite:///orchestrator.db", host: str = "0.0.0.0", port: int = 8501, headless: bool = False) -> None:
    try:
        from streamlit.web import bootstrap
    except ImportError as exc:
        raise RuntimeError("streamlit is required, please install: pip install 'data-orchestrator[ui]'") from exc
    os.environ["STREAMLIT_SERVER_HEADLESS"] = "true" if headless else "false"
    os.environ["STREAMLIT_BROWSER_GATHER_USAGE_STATS"] = "false"
    os.environ["STREAMLIT_SERVER_ADDRESS"] = host
    os.environ["STREAMLIT_SERVER_PORT"] = str(port)
    os.environ["ORCHESTRATOR_DB_URL"] = db_url
    bootstrap.run(str(Path(__file__).resolve()), False, [], {})


class OrchestratorApiClient:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")

    def list_pipelines(self) -> list[dict[str, Any]]:
        return self._get("/pipelines")

    def get_pipeline(self, pipeline_id: str) -> Any | None:
        try:
            payload = self._get(f"/pipeline/{pipeline_id}")
        except Exception:
            return None
        return payload if payload else None

    def get_upcoming_runs(self, hours: int = 2) -> list[dict[str, str]]:
        return self._get(f"/upcoming?hours={hours}")

    def trigger_async(self, pipeline_id: str) -> str:
        payload = self._post(f"/trigger/{pipeline_id}")
        return payload.get("run_id", "")

    def pause(self, pipeline_id: str) -> None:
        self._post(f"/pause/{pipeline_id}")

    def resume(self, pipeline_id: str) -> None:
        self._post(f"/resume/{pipeline_id}")

    def _get(self, path: str) -> Any:
        with urlopen(f"{self.base_url}{path}", timeout=5) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def _post(self, path: str) -> Any:
        request = Request(url=f"{self.base_url}{path}", method="POST")
        with urlopen(request, timeout=5) as resp:
            return json.loads(resp.read().decode("utf-8"))


def render_app(default_db_url: str = "sqlite:///orchestrator.db") -> None:
    import streamlit as st

    st.set_page_config(page_title="Data Orchestrator", layout="wide")
    orchestrator = get_orchestrator()
    if orchestrator is None:
        api_url = os.environ.get("ORCHESTRATOR_API_URL", "").strip()
        if api_url:
            orchestrator = OrchestratorApiClient(api_url)
    db_url = os.environ.get("ORCHESTRATOR_DB_URL", default_db_url)
    if orchestrator is not None and getattr(orchestrator, "settings", None) is not None:
        db_url = orchestrator.settings.db_url
    reader = LogReader(db_url=db_url)
    run_id = st.query_params.get("run_id", "")
    default_page = "Dashboard"
    if run_id:
        default_page = "Run Detail"
        
    tz_option = st.sidebar.selectbox("时区", ["pst", "cst", "+00"], index=1)
    
    page = st.sidebar.radio("页面", ["Dashboard", "Pipelines", "Run Detail", "Log Search"], index=["Dashboard", "Pipelines", "Run Detail", "Log Search"].index(default_page))
    if page == "Dashboard":
        page_dashboard(st, reader, orchestrator, tz_option)
        return
    if page == "Pipelines":
        page_pipelines(st, orchestrator, tz_option)
        return
    if page == "Run Detail":
        page_run_detail(st, reader, orchestrator, run_id, tz_option)
        return
    page_log_search(st, reader, tz_option)


def page_dashboard(st, reader: LogReader, orchestrator: Any | None, tz_option: str) -> None:
    col1, col2 = st.columns([0.9, 0.1])
    with col1:
        st.title("今日总览")
    with col2:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("刷新", key="btn_refresh_dashboard"):
            st.rerun()
    stats = reader.get_dashboard_stats(hours=24)
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("总执行次数", stats["total_runs"])
    c2.metric("成功次数", stats["success_runs"])
    c3.metric("失败次数", stats["failed_runs"])
    c4.metric("平均耗时", f"{stats['avg_duration']:.2f}s")
    trend_data = [{"hour": _format_time(item["hour"] + ":00:00", tz_option)[:13], "执行次数": item["count"], "成功率": item["success_rate"]} for item in stats["runs_by_hour"]]
    try:
        import pandas as pd

        trend_df = pd.DataFrame(trend_data).set_index("hour")
        st.line_chart(trend_df)
    except Exception:
        st.table(trend_data)
    st.subheader("最近 10 次 Pipeline 执行")
    recent_runs = stats.get("recent_runs", [])
    for run in recent_runs:
        cols = st.columns([2, 2, 1, 1, 1])
        cols[0].write(run["pipeline_name"])
        cols[1].write(_format_time(run["started_at"], tz_option))
        cols[2].write(f"{(run.get('duration_seconds') or 0):.2f}s")
        cols[3].markdown(_status_tag(run["status"]), unsafe_allow_html=True)
        if cols[4].button("查看", key=f"recent-{run['id']}"):
            st.query_params["run_id"] = run["id"]
            st.rerun()
    st.subheader("即将执行（未来 2 小时）")
    upcoming = orchestrator.get_upcoming_runs(hours=2) if orchestrator is not None else []
    if not upcoming:
        st.write("暂无即将执行任务")
        return
    now = datetime.now(timezone.utc)
    for item in upcoming:
        trigger_time = datetime.fromisoformat(item["trigger_time"])
        delta = int((trigger_time - now).total_seconds())
        st.write(f"{item['pipeline_name']} | {_format_time(item['trigger_time'], tz_option)} | {max(delta, 0)} 秒后")


def page_pipelines(st, orchestrator: Any | None, tz_option: str) -> None:
    col1, col2 = st.columns([0.9, 0.1])
    with col1:
        st.title("Pipeline 管理")
    with col2:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("刷新", key="btn_refresh_pipelines"):
            st.rerun()
    if orchestrator is None:
        st.warning("当前未连接调度器实例，请使用 orchestrator run 或 orchestrator ui 启动。")
        return
    pipelines = orchestrator.list_pipelines()
    for item in pipelines:
        cols = st.columns([1.2, 2, 2, 2, 1, 2])
        cols[0].write(item["id"])
        cols[1].write(item["name"])
        cols[2].write(item["schedule"])
        cols[3].write(_format_time(item["next_run"], tz_option) if item["next_run"] else "-")
        cols[4].markdown(_status_tag(item["status"]), unsafe_allow_html=True)
        action_col = cols[5]
        if action_col.button("触发", key=f"trigger-{item['id']}"):
            run_id = orchestrator.trigger_async(item["id"])
            st.success(f"已触发 {item['id']}，run_id={run_id}")
        if item["status"] == "active" and action_col.button("暂停", key=f"pause-{item['id']}"):
            orchestrator.pause(item["id"])
            st.rerun()
        if item["status"] == "paused" and action_col.button("恢复", key=f"resume-{item['id']}"):
            orchestrator.resume(item["id"])
            st.rerun()


def page_run_detail(st, reader: LogReader, orchestrator: Any | None, run_id: str, tz_option: str) -> None:
    st.title("执行详情")
    if not run_id:
        st.info("请从 Dashboard 或 Log Search 进入，或在 URL 传入 run_id。")
        return
    run = reader.get_pipeline_run(run_id)
    if run is None:
        st.error(f"未找到 run_id={run_id}")
        return
    cols = st.columns(3)
    cols[0].metric("Pipeline", run["pipeline_name"])
    cols[1].metric("run_id", run["id"])
    cols[2].metric("状态", run["status"])
    cols2 = st.columns(3)
    cols2[0].write(f"开始：{_format_time(run['started_at'], tz_option)}")
    cols2[1].write(f"结束：{_format_time(run.get('finished_at'), tz_option) if run.get('finished_at') else '-'}")
    cols2[2].write(f"耗时：{(run.get('duration_seconds') or 0):.2f}s")
    tasks = reader.get_task_runs(run_id)
    st.subheader("Task 执行时间线")
    gantt_rows = _build_gantt_rows(tasks, tz_option)
    try:
        import pandas as pd

        st.dataframe(pd.DataFrame(gantt_rows), width="stretch")
    except Exception:
        st.table(gantt_rows)
    st.subheader("Task 列表")
    for task in tasks:
        title = f"{task['task_name']} | {task['status']} | {(task.get('duration_seconds') or 0):.2f}s | 重试 {task.get('retry_count') or 0}"
        with st.expander(title):
            st.write(f"connector: {task['connector_name']}")
            if task["status"] in {"failed", "timeout"}:
                st.write(f"error_type: {task.get('error_type') or ''}")
                st.write(f"error_message: {task.get('error_message') or ''}")
                st.code(task.get("error_traceback") or "", language="python")
    st.subheader("依赖关系图")
    if orchestrator is None:
        st.info("未连接调度器，无法展示 DAG。")
        return
    pipeline = orchestrator.get_pipeline(run["pipeline_id"])
    if pipeline is None:
        st.info("当前调度器中未注册该 Pipeline。")
        return
    status_by_task = {item["task_id"]: item["status"] for item in tasks}
    st.markdown(_build_dag_svg(pipeline.tasks, status_by_task), unsafe_allow_html=True)


def page_log_search(st, reader: LogReader, tz_option: str) -> None:
    st.title("日志查询")
    all_runs = reader.get_pipeline_runs(limit=1000)
    pipeline_candidates = sorted({row["pipeline_id"] for row in all_runs})
    with st.sidebar:
        selected_pipelines = st.multiselect("Pipeline", pipeline_candidates, default=pipeline_candidates)
        selected_statuses = st.multiselect("状态", ["success", "failed", "partial"], default=["success", "failed", "partial"])
        start_date = st.date_input("开始日期")
        end_date = st.date_input("结束日期")
        keyword = st.text_input("关键词(error_message)")
        
    # User's selected dates are in their local timezone. To query DB correctly, we should ideally convert them to UTC.
    # We will assume start_date is midnight of selected date in selected tz, and end_date is 23:59:59 in selected tz.
    
    if tz_option == "pst":
        tz_info = timezone(timedelta(hours=-8))
    elif tz_option == "cst":
        tz_info = timezone(timedelta(hours=8))
    else:
        tz_info = timezone.utc
        
    start_time = datetime.combine(start_date, datetime.min.time()).replace(tzinfo=tz_info).astimezone(timezone.utc)
    end_time = datetime.combine(end_date, datetime.max.time()).replace(tzinfo=tz_info).astimezone(timezone.utc)
    
    page_size = 20
    page_index = st.number_input("页码", min_value=1, value=1, step=1)
    offset = (int(page_index) - 1) * page_size
    rows = reader.get_pipeline_runs(
        pipeline_ids=selected_pipelines,
        statuses=selected_statuses,
        start_time=start_time,
        end_time=end_time,
        keyword=keyword or None,
        limit=page_size,
        offset=offset,
    )
    total_count = reader.count_pipeline_runs(
        pipeline_ids=selected_pipelines,
        statuses=selected_statuses,
        start_time=start_time,
        end_time=end_time,
        keyword=keyword or None,
    )
    failed_counts = reader.count_failed_tasks([row["id"] for row in rows])
    display_rows = []
    for row in rows:
        display_rows.append(
            {
                "Pipeline": row["pipeline_name"],
                "开始时间": _format_time(row["started_at"], tz_option),
                "耗时": f"{(row.get('duration_seconds') or 0):.2f}s",
                "状态": row["status"],
                "失败任务数": failed_counts.get(row["id"], 0),
                "run_id": row["id"],
            }
        )
    st.write(f"共 {total_count} 条记录")
    st.dataframe(display_rows, width="stretch")
    for item in display_rows:
        if st.button(f"查看 {item['run_id']}", key=f"log-detail-{item['run_id']}"):
            st.query_params["run_id"] = item["run_id"]
            st.rerun()
    csv_data = _to_csv(display_rows)
    st.download_button(
        label="导出当前过滤结果 CSV",
        data=csv_data,
        file_name="orchestrator_logs.csv",
        mime="text/csv",
    )


def _build_gantt_rows(task_rows: list[dict[str, Any]], tz_option: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in task_rows:
        rows.append(
            {
                "task": item["task_name"],
                "status": item["status"],
                "started_at": _format_time(item["started_at"], tz_option) if item["started_at"] else None,
                "finished_at": _format_time(item["finished_at"], tz_option) if item["finished_at"] else None,
                "duration_seconds": item.get("duration_seconds") or 0,
                "retry_count": item.get("retry_count") or 0,
            }
        )
    return rows


def _status_tag(status: str) -> str:
    color_map = {
        "success": "#1b8f4d",
        "failed": "#c62828",
        "partial": "#ef6c00",
        "running": "#1565c0",
        "paused": "#6d4c41",
        "active": "#1b8f4d",
        "manual": "#455a64",
        "timeout": "#ad1457",
        "skipped": "#757575",
    }
    color = color_map.get(status, "#37474f")
    return f"<span style='display:inline-block;padding:2px 8px;border-radius:8px;background:{color};color:white'>{status}</span>"


def _build_dag_svg(tasks: list[Any], status_by_task: dict[str, str]) -> str:
    node_height = 50
    col_width = 260
    layout: dict[str, tuple[int, int]] = {}
    queue = [task for task in tasks if not task.depends_on]
    level_map: dict[str, int] = {task.id: 0 for task in queue}
    unresolved = {task.id: set(task.depends_on) for task in tasks}
    while queue:
        current = queue.pop(0)
        current_level = level_map[current.id]
        for child in [task for task in tasks if current.id in task.depends_on]:
            unresolved[child.id].discard(current.id)
            level_map[child.id] = max(level_map.get(child.id, 0), current_level + 1)
            if not unresolved[child.id]:
                queue.append(child)
    grouped: dict[int, list[Any]] = {}
    for task in tasks:
        grouped.setdefault(level_map.get(task.id, 0), []).append(task)
    for level, nodes in grouped.items():
        for row, task in enumerate(nodes):
            x = 20 + level * col_width
            y = 20 + row * node_height
            layout[task.id] = (x, y)
    width = (max(grouped.keys()) + 1) * col_width + 100 if grouped else 300
    height = max((len(nodes) for nodes in grouped.values()), default=1) * node_height + 80
    parts = [f"<svg width='{width}' height='{height}' xmlns='http://www.w3.org/2000/svg'>"]
    for task in tasks:
        x1, y1 = layout[task.id]
        for dependency in task.depends_on:
            if dependency not in layout:
                continue
            x0, y0 = layout[dependency]
            parts.append(f"<line x1='{x0+180}' y1='{y0+15}' x2='{x1}' y2='{y1+15}' stroke='#9e9e9e' stroke-width='1.5' />")
    for task in tasks:
        status = status_by_task.get(task.id, "pending")
        fill = {
            "success": "#2e7d32",
            "failed": "#c62828",
            "skipped": "#757575",
            "running": "#1565c0",
        }.get(status, "#455a64")
        x, y = layout[task.id]
        parts.append(f"<rect x='{x}' y='{y}' width='180' height='30' rx='8' fill='{fill}' />")
        parts.append(f"<text x='{x+8}' y='{y+20}' fill='white' font-size='12'>{task.name or task.id}</text>")
    parts.append("</svg>")
    return "".join(parts)


def _to_csv(rows: list[dict[str, Any]]) -> str:
    stream = io.StringIO()
    if not rows:
        return ""
    writer = csv.DictWriter(stream, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)
    return stream.getvalue()


if __name__ == "__main__":
    render_app()
