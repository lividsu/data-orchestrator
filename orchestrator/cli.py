from __future__ import annotations

import threading

import click
from rich.console import Console
from rich.table import Table

from orchestrator.config.loader import ConfigLoader
from orchestrator.core.scheduler import Orchestrator
from orchestrator.log.reader import LogReader
from orchestrator.ui.app import run_ui

console = Console()


@click.group()
def main() -> None:
    return None


@main.command("run")
@click.option("--config", "config_path", default="./pipelines/")
@click.option("--plugins", "plugin_dir", default="./connectors/")
@click.option("--host", default="0.0.0.0")
@click.option("--port", default=8501, type=int)
@click.option("--no-ui", is_flag=True, default=False)
@click.option("--headless", is_flag=True, default=True)
def run(config_path: str, plugin_dir: str, host: str, port: int, no_ui: bool, headless: bool):
    app = Orchestrator(config_dir=config_path, plugin_dir=plugin_dir)
    if no_ui:
        console.print("Running without UI.")
        app.start(ui=False)
        return
    app.start(ui=True, host=host, port=port, headless=headless)


@main.command("ui")
@click.option("--config", "config_path", default="./pipelines/")
@click.option("--plugins", "plugin_dir", default="./connectors/")
@click.option("--host", default="0.0.0.0")
@click.option("--port", default=8501, type=int)
@click.option("--headless", is_flag=True, default=False)
def ui(config_path: str, plugin_dir: str, host: str, port: int, headless: bool):
    app = Orchestrator(config_dir=config_path, plugin_dir=plugin_dir)
    app.start(ui=True, host=host, port=port, headless=headless)


@main.command("trigger")
@click.argument("pipeline_id")
@click.option("--config", "config_path", default="./pipelines/")
@click.option("--plugins", "plugin_dir", default="./connectors/")
@click.option("--wait/--async", "wait_for_finish", default=True)
def trigger(pipeline_id: str, config_path: str, plugin_dir: str, wait_for_finish: bool):
    app = Orchestrator(config_dir=config_path, plugin_dir=plugin_dir)
    app.load_plugins(plugin_dir)
    app.load_config(config_path)
    if wait_for_finish:
        result = app.trigger(pipeline_id, triggered_by="manual")
        console.print(result.model_dump_json(indent=2))
        return
    run_id = app.trigger_async(pipeline_id)
    console.print(run_id)


@main.command("validate")
@click.option("--config", "config_path", default="./pipelines/")
def validate(config_path: str):
    try:
        ConfigLoader.load(config_path)
        console.print("✅ config is valid")
    except Exception as exc:
        console.print(f"❌ error: {exc}")
        raise click.ClickException(str(exc))


@main.command("list")
@click.option("--config", "config_path", default="./pipelines/")
def list_pipelines(config_path: str):
    registered = ConfigLoader.load(config_path)
    table = Table(title="Pipelines")
    table.add_column("Pipeline ID")
    table.add_column("Schedule")
    table.add_column("Next Run")
    table.add_column("Status")
    for item in registered:
        schedule = item.schedule.type
        if item.schedule.type == "cron":
            schedule = item.schedule.cron_expr or "cron"
        if item.schedule.type == "interval":
            schedule = f"every {item.schedule.interval_seconds}s"
        table.add_row(item.pipeline.id, schedule, "-", "✅ active")
    console.print(table)


@main.command("status")
@click.argument("pipeline_id")
@click.option("--db-url", default="sqlite:///orchestrator.db")
def status(pipeline_id: str, db_url: str):
    reader = LogReader(db_url=db_url)
    runs = reader.get_pipeline_runs(pipeline_id=pipeline_id, limit=10)
    console.print(runs)


@main.command("pause")
@click.argument("pipeline_id")
@click.option("--config", "config_path", default="./pipelines/")
@click.option("--plugins", "plugin_dir", default="./connectors/")
def pause(pipeline_id: str, config_path: str, plugin_dir: str):
    app = Orchestrator(config_dir=config_path, plugin_dir=plugin_dir)
    app.load_plugins(plugin_dir)
    app.load_config(config_path)
    if not app._scheduler.running:
        app._scheduler.start()
    app.pause(pipeline_id)
    console.print(f"paused {pipeline_id}")
    app.stop()


@main.command("resume")
@click.argument("pipeline_id")
@click.option("--config", "config_path", default="./pipelines/")
@click.option("--plugins", "plugin_dir", default="./connectors/")
def resume(pipeline_id: str, config_path: str, plugin_dir: str):
    app = Orchestrator(config_dir=config_path, plugin_dir=plugin_dir)
    app.load_plugins(plugin_dir)
    app.load_config(config_path)
    if not app._scheduler.running:
        app._scheduler.start()
    app.resume(pipeline_id)
    console.print(f"resumed {pipeline_id}")
    app.stop()


@main.command("ping")
@click.option("--plugins", "plugin_dir", default="./connectors/")
def ping(plugin_dir: str):
    app = Orchestrator(plugin_dir=plugin_dir)
    app.load_plugins(plugin_dir)
    result = app.ping_all()
    table = Table(title="Connector Health")
    table.add_column("Connector")
    table.add_column("Status")
    for name, ok in result.items():
        table.add_row(name, "✅" if ok else "❌")
    console.print(table)





if __name__ == "__main__":
    main()

