from __future__ import annotations

from pathlib import Path

from click.testing import CliRunner

import orchestrator.cli as cli
from orchestrator.cli import main


def test_cli_validate_command(tmp_path: Path):
    config = tmp_path / "valid_pipeline.yaml"
    config.write_text(
        """
pipelines:
  - id: demo
    schedule: {type: manual}
    tasks:
      - id: t1
        connector: demo
        action: fetch
""".strip(),
        encoding="utf-8",
    )
    runner = CliRunner()
    result = runner.invoke(main, ["validate", "--config", str(config)])
    assert result.exit_code == 0
    assert "✅" in result.output


def test_cli_validate_invalid_yaml(tmp_path: Path):
    config = tmp_path / "invalid_pipeline.yaml"
    config.write_text(
        """
pipelines:
  - id demo
""".strip(),
        encoding="utf-8",
    )
    runner = CliRunner()
    result = runner.invoke(main, ["validate", "--config", str(config)])
    assert result.exit_code != 0
    assert "error" in result.output.lower()


def test_cli_help_commands():
    runner = CliRunner()
    result = runner.invoke(main, ["--help"])
    assert result.exit_code == 0
    assert "run" in result.output
    assert "validate" in result.output


def test_cli_run_no_ui(monkeypatch):
    started = {"called": False, "ui": None}

    class FakeOrchestrator:
        def __init__(self, *args, **kwargs):
            pass

        def start(self, **kwargs):
            started["called"] = True
            started["ui"] = kwargs.get("ui")

    monkeypatch.setattr(cli, "Orchestrator", FakeOrchestrator)
    runner = CliRunner()
    result = runner.invoke(main, ["run", "--no-ui"])
    assert result.exit_code == 0
    assert started["called"] is True
    assert started["ui"] is False


def test_cli_run_with_ui(monkeypatch):
    captured = {}

    class FakeOrchestrator:
        def __init__(self, *args, **kwargs):
            pass

        def start(self, **kwargs):
            captured["kwargs"] = kwargs

    monkeypatch.setattr(cli, "Orchestrator", FakeOrchestrator)
    runner = CliRunner()
    result = runner.invoke(main, ["run", "--host", "127.0.0.1", "--port", "9000"])
    assert result.exit_code == 0
    assert captured["kwargs"]["ui"] is True
    assert captured["kwargs"]["host"] == "127.0.0.1"
    assert captured["kwargs"]["port"] == 9000


def test_cli_ui_command(monkeypatch):
    captured = {"called": False}

    class FakeOrchestrator:
        def __init__(self, *args, **kwargs):
            pass

        def start(self, **kwargs):
            captured["called"] = True
            captured["host"] = kwargs.get("host")
            captured["port"] = kwargs.get("port")

    monkeypatch.setattr(cli, "Orchestrator", FakeOrchestrator)
    runner = CliRunner()
    result = runner.invoke(main, ["ui"])
    assert result.exit_code == 0
    assert captured["called"] is True
    assert captured["port"] == 8501


def test_cli_trigger_async(monkeypatch):
    class FakeOrchestrator:
        def __init__(self, *args, **kwargs):
            pass

        def ensure_loaded(self):
            return None

        def trigger_async(self, pipeline_id):
            return f"run-{pipeline_id}"

    monkeypatch.setattr(cli, "Orchestrator", FakeOrchestrator)
    runner = CliRunner()
    result = runner.invoke(main, ["trigger", "daily", "--async"])
    assert result.exit_code == 0
    assert "run-daily" in result.output


def test_cli_pause_resume(monkeypatch):
    calls = []

    class FakeScheduler:
        pass

    class FakeOrchestrator:
        def __init__(self, *args, **kwargs):
            self._scheduler = FakeScheduler()

        def ensure_loaded(self):
            return None

        def pause(self, pipeline_id):
            calls.append(("pause", pipeline_id))

        def resume(self, pipeline_id):
            calls.append(("resume", pipeline_id))

        def stop(self):
            calls.append(("stop", "x"))

    monkeypatch.setattr(cli, "Orchestrator", FakeOrchestrator)
    runner = CliRunner()
    pause_result = runner.invoke(main, ["pause", "p1"])
    resume_result = runner.invoke(main, ["resume", "p1"])
    assert pause_result.exit_code == 0
    assert resume_result.exit_code == 0
    assert ("pause", "p1") in calls
    assert ("resume", "p1") in calls


def test_cli_ping(monkeypatch):
    class FakeOrchestrator:
        def __init__(self, *args, **kwargs):
            pass

        def ensure_loaded(self):
            return None

        def ping_all(self):
            return {"a": True, "b": False}

    monkeypatch.setattr(cli, "Orchestrator", FakeOrchestrator)
    runner = CliRunner()
    result = runner.invoke(main, ["ping"])
    assert result.exit_code == 0
    assert "a" in result.output
    assert "b" in result.output


def test_cli_init_project(tmp_path: Path):
    runner = CliRunner()
    target = tmp_path / "demo_project"
    result = runner.invoke(main, ["init", str(target)])
    assert result.exit_code == 0
    assert (target / "main.py").exists()
    assert (target / "connectors" / "demo.py").exists()
    assert (target / "pipelines" / "demo.yaml").exists()
