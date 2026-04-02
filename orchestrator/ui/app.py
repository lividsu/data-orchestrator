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

    def trigger_async(self, pipeline_id: str, runtime_kwargs: dict[str, Any] | None = None) -> str:
        payload = {}
        if runtime_kwargs:
            payload["runtime_kwargs"] = runtime_kwargs
        response = self._post(f"/trigger/{pipeline_id}", payload=payload)
        return response.get("run_id", "")

    def pause(self, pipeline_id: str) -> None:
        self._post(f"/pause/{pipeline_id}")

    def resume(self, pipeline_id: str) -> None:
        self._post(f"/resume/{pipeline_id}")

    def _get(self, path: str) -> Any:
        with urlopen(f"{self.base_url}{path}", timeout=5) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def _post(self, path: str, payload: dict | None = None) -> Any:
        data = None
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
        request = Request(url=f"{self.base_url}{path}", data=data, method="POST")
        if data:
            request.add_header("Content-Type", "application/json")
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
    
    page = st.sidebar.radio(
        "导航", 
        ["📊 Dashboard", "⚡ Pipelines", "📝 Run Detail", "🔍 Log Search"], 
        index=["Dashboard", "Pipelines", "Run Detail", "Log Search"].index(default_page)
    )
    if page == "📊 Dashboard":
        page_dashboard(st, reader, orchestrator, tz_option)
        return
    if page == "⚡ Pipelines":
        page_pipelines(st, orchestrator, tz_option)
        return
    if page == "📝 Run Detail":
        page_run_detail(st, reader, orchestrator, run_id, tz_option)
        return
    page_log_search(st, reader, tz_option)


def page_dashboard(st, reader: LogReader, orchestrator: Any | None, tz_option: str) -> None:
    col1, col2 = st.columns([0.9, 0.1])
    with col1:
        st.title("📊 今日总览")
    with col2:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("🔄 刷新", key="btn_refresh_dashboard", use_container_width=True):
            st.rerun()
    
    stats = reader.get_dashboard_stats(hours=24)
    
    with st.container(border=True):
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("总执行次数", stats["total_runs"])
        c2.metric("成功次数", stats["success_runs"])
        c3.metric("失败次数", stats["failed_runs"])
        c4.metric("平均耗时", f"{stats['avg_duration']:.2f}s")
        
    st.subheader("📈 执行趋势 (24h)")
    trend_data = [{"hour": _format_time(item["hour"] + ":00:00", tz_option)[:13], "执行次数": item["count"], "成功率": item["success_rate"]} for item in stats["runs_by_hour"]]
    try:
        import pandas as pd

        trend_df = pd.DataFrame(trend_data).set_index("hour")
        st.line_chart(trend_df, use_container_width=True)
    except Exception:
        st.table(trend_data)
        
    col_left, col_right = st.columns(2)
    
    with col_left:
        st.subheader("⏱️ 最近 10 次执行")
        recent_runs = stats.get("recent_runs", [])
        if not recent_runs:
            st.info("暂无执行记录")
        for run in recent_runs:
            with st.container(border=True):
                cols = st.columns([2.5, 2.5, 1.5, 1.5])
                cols[0].markdown(f"**{run['pipeline_name']}**")
                cols[1].caption(_format_time(run["started_at"], tz_option))
                cols[2].markdown(_status_tag(run["status"]), unsafe_allow_html=True)
                if cols[3].button("🔍 查看", key=f"recent-{run['id']}", use_container_width=True):
                    st.query_params["run_id"] = run["id"]
                    st.rerun()
                
    with col_right:
        st.subheader("⏰ 即将执行 (未来 2 小时)")
        upcoming = orchestrator.get_upcoming_runs(hours=2) if orchestrator is not None else []
        if not upcoming:
            st.info("暂无即将执行任务")
        else:
            now = datetime.now(timezone.utc)
            for item in upcoming:
                with st.container(border=True):
                    trigger_time = datetime.fromisoformat(item["trigger_time"])
                    delta = int((trigger_time - now).total_seconds())
                    c1, c2 = st.columns([3, 1])
                    c1.markdown(f"**{item['pipeline_name']}**")
                    c1.caption(f"{_format_time(item['trigger_time'], tz_option)}")
                    c2.markdown(f"<div style='text-align: right; color: #666; font-size: 0.9em; padding-top: 0.5rem;'>{max(delta, 0)}s 后</div>", unsafe_allow_html=True)


def page_pipelines(st, orchestrator: Any | None, tz_option: str) -> None:
    col1, col2 = st.columns([0.9, 0.1])
    with col1:
        st.title("⚡ Pipeline 管理")
    with col2:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("🔄 刷新", key="btn_refresh_pipelines", use_container_width=True):
            st.rerun()
    if orchestrator is None:
        st.warning("⚠️ 当前未连接调度器实例，请使用 orchestrator run 或 orchestrator ui 启动。")
        return
    pipelines = orchestrator.list_pipelines()
    if not pipelines:
        st.info("暂无 Pipeline 注册。")
        return
        
    for item in pipelines:
        with st.container(border=True):
            cols = st.columns([1.5, 2, 2, 2, 1.5, 2.5])
            cols[0].markdown(f"**ID:** `{item['id']}`")
            cols[1].markdown(f"**名称:** {item['name']}")
            cols[2].markdown(f"**Schedule:** `{item['schedule']}`")
            cols[3].caption(f"下次: {_format_time(item['next_run'], tz_option) if item['next_run'] else '-'}")
            cols[4].markdown(_status_tag(item["status"]), unsafe_allow_html=True)
            
            action_col = cols[5]
            btn_cols = action_col.columns(2)
            
            def _render_trigger_form(pipeline_id: str):
                trigger_kwargs_str = st.text_area(
                    "单次触发变量 (JSON)",
                    value="{\n}",
                    key=f"kwargs-{pipeline_id}",
                    height=100,
                    help="配置单次触发的环境变量（如 {\"biz_date\": \"2024-01-01\"}），将合并到 Jinja2 上下文中。"
                )
                if st.button("确认触发", key=f"trigger-btn-{pipeline_id}", use_container_width=True):
                    try:
                        kwargs = json.loads(trigger_kwargs_str)
                        if not isinstance(kwargs, dict):
                            st.error("必须是 JSON 对象 (dict)")
                        else:
                            run_id = orchestrator.trigger_async(pipeline_id, runtime_kwargs=kwargs)
                            st.success(f"已触发，run_id={run_id}")
                            time.sleep(1)
                            st.rerun()
                    except json.JSONDecodeError:
                        st.error("JSON 格式错误")

            if hasattr(st, "popover"):
                with btn_cols[0].popover("🚀 触发", use_container_width=True):
                    _render_trigger_form(item["id"])
            else:
                with st.expander(f"🚀 触发 {item['id']}"):
                    _render_trigger_form(item["id"])
                    
            if item["status"] == "active" and btn_cols[1].button("⏸️ 暂停", key=f"pause-{item['id']}", use_container_width=True):
                orchestrator.pause(item["id"])
                st.rerun()
            if item["status"] == "paused" and btn_cols[1].button("▶️ 恢复", key=f"resume-{item['id']}", use_container_width=True):
                orchestrator.resume(item["id"])
                st.rerun()


def page_run_detail(st, reader: LogReader, orchestrator: Any | None, run_id: str, tz_option: str) -> None:
    st.title("📝 执行详情")
    if not run_id:
        st.info("ℹ️ 请从 Dashboard 或 Log Search 进入，或在 URL 传入 run_id。")
        return
    run = reader.get_pipeline_run(run_id)
    if run is None:
        st.error(f"❌ 未找到 run_id={run_id}")
        return
        
    with st.container(border=True):
        cols = st.columns(3)
        cols[0].metric("Pipeline", run["pipeline_name"])
        cols[1].metric("run_id", run["id"])
        cols[2].metric("状态", run["status"])
        st.markdown("---")
        cols2 = st.columns(3)
        cols2[0].markdown(f"**开始:** {_format_time(run['started_at'], tz_option)}")
        cols2[1].markdown(f"**结束:** {_format_time(run.get('finished_at'), tz_option) if run.get('finished_at') else '-'}")
        cols2[2].markdown(f"**耗时:** {(run.get('duration_seconds') or 0):.2f}s")
        
    tasks = reader.get_task_runs(run_id)
    
    st.subheader("📊 Task 执行时间线")
    gantt_rows = _build_gantt_rows(tasks, tz_option)
    try:
        import pandas as pd
        df = pd.DataFrame(gantt_rows)
        st.dataframe(df, use_container_width=True, hide_index=True)
    except Exception:
        st.table(gantt_rows)
        
    st.subheader("📋 Task 列表")
    for task in tasks:
        status_icon = "✅" if task["status"] == "success" else ("❌" if task["status"] in ["failed", "timeout"] else "⏳")
        title = f"{status_icon} **{task['task_name']}** | {task['status']} | {(task.get('duration_seconds') or 0):.2f}s | 重试: {task.get('retry_count') or 0}"
        with st.expander(title):
            st.markdown(f"**Connector:** `{task['connector_name']}`")
            if task["status"] in {"failed", "timeout"}:
                st.error(f"**Error Type:** {task.get('error_type') or ''}\n\n**Message:** {task.get('error_message') or ''}")
                if task.get("error_traceback"):
                    st.code(task.get("error_traceback"), language="python")
            terminal_output = task.get("terminal_output") or ""
            if terminal_output:
                st.markdown("**Terminal Output:**")
                st.code(terminal_output, language="text")
                
    st.subheader("🕸️ 依赖关系图")
    if orchestrator is None:
        st.info("ℹ️ 未连接调度器，无法展示 DAG。")
        return
    pipeline = orchestrator.get_pipeline(run["pipeline_id"])
    if pipeline is None:
        st.info("ℹ️ 当前调度器中未注册该 Pipeline。")
        return
    status_by_task = {item["task_id"]: item["status"] for item in tasks}
    with st.container(border=True):
        st.markdown(_build_dag_svg(pipeline.tasks, status_by_task), unsafe_allow_html=True)


def page_log_search(st, reader: LogReader, tz_option: str) -> None:
    st.title("🔍 日志查询")
    all_runs = reader.get_pipeline_runs(limit=1000)
    pipeline_candidates = sorted({row["pipeline_id"] for row in all_runs})
    with st.sidebar:
        st.markdown("---")
        st.subheader("过滤条件")
        selected_pipelines = st.multiselect("Pipeline", pipeline_candidates, default=pipeline_candidates)
        selected_statuses = st.multiselect("状态", ["success", "failed", "partial", "running", "timeout"], default=["success", "failed", "partial"])
        start_date = st.date_input("开始日期")
        end_date = st.date_input("结束日期")
        keyword = st.text_input("关键词 (Error/Traceback/Output)")
        
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
                "run_id": row["id"],
                "Pipeline": row["pipeline_name"],
                "开始时间": _format_time(row["started_at"], tz_option),
                "耗时": f"{(row.get('duration_seconds') or 0):.2f}s",
                "状态": row["status"],
                "失败任务数": failed_counts.get(row["id"], 0),
            }
        )
        
    st.markdown(f"**共找到 {total_count} 条记录** (当前显示第 {offset + 1} - {min(offset + page_size, total_count)} 条)")
    
    if display_rows:
        import pandas as pd
        df = pd.DataFrame(display_rows)
        st.dataframe(df, use_container_width=True, hide_index=True)
        
        st.markdown("---")
        col1, col2 = st.columns([3, 1])
        with col1:
            selected_run_id = st.selectbox("选择要查看详情的 run_id:", [r["run_id"] for r in display_rows])
        with col2:
            st.markdown("<br>", unsafe_allow_html=True)
            if st.button("👁️ 查看详情", use_container_width=True):
                st.query_params["run_id"] = selected_run_id
                st.rerun()
    else:
        st.info("未找到匹配的日志记录。")

    csv_data = _to_csv(display_rows)
    st.download_button(
        label="📥 导出当前页 CSV",
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
    
    parts = [f"<svg width='100%' height='{height}' viewBox='0 0 {width} {height}' xmlns='http://www.w3.org/2000/svg'>"]
    
    parts.append("""
        <defs>
            <filter id='shadow' x='-20%' y='-20%' width='140%' height='140%'>
                <feDropShadow dx='1' dy='2' stdDeviation='2' flood-opacity='0.2'/>
            </filter>
        </defs>
    """)
    
    # Draw edges
    for task in tasks:
        x1, y1 = layout[task.id]
        for dependency in task.depends_on:
            if dependency not in layout:
                continue
            x0, y0 = layout[dependency]
            # Curved path instead of straight line
            parts.append(f"<path d='M {x0+180} {y0+15} C {x0+220} {y0+15}, {x1-40} {y1+15}, {x1} {y1+15}' fill='transparent' stroke='#b0bec5' stroke-width='2' />")
            
    # Draw nodes
    for task in tasks:
        status = status_by_task.get(task.id, "pending")
        fill = {
            "success": "#1b8f4d",
            "failed": "#c62828",
            "skipped": "#757575",
            "running": "#1565c0",
            "timeout": "#ad1457",
        }.get(status, "#546e7a")
        x, y = layout[task.id]
        parts.append(f"<rect x='{x}' y='{y}' width='180' height='32' rx='6' fill='{fill}' filter='url(#shadow)' />")
        # Truncate text if too long
        display_name = (task.name or task.id)
        if len(display_name) > 22:
            display_name = display_name[:19] + "..."
        parts.append(f"<text x='{x+10}' y='{y+21}' fill='white' font-family='sans-serif' font-size='13' font-weight='500'>{display_name}</text>")
        
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
