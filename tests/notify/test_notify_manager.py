from __future__ import annotations

from types import SimpleNamespace

import pytest

from orchestrator.notify import NotifyChannelConfig
from orchestrator.notify import NotifyManager
from orchestrator.notify import NotifyPolicy
from orchestrator.notify.base import NOTIFIER_REGISTRY
from orchestrator.notify.base import BaseNotifier
from orchestrator.notify.base import register_notifier
from orchestrator.notify.builtin.feishu_notifier import FeishuNotifier


@pytest.fixture(autouse=True)
def reset_notifier_registry():
    default_notifiers = {
        "log": NOTIFIER_REGISTRY.get("log"),
        "feishu": NOTIFIER_REGISTRY.get("feishu"),
    }
    yield
    NOTIFIER_REGISTRY.clear()
    for name, notifier in default_notifiers.items():
        if notifier is not None:
            NOTIFIER_REGISTRY[name] = notifier


class DummyTaskResult:
    def __init__(self, task_id: str, status: str, error_message: str = "boom", error_type: str = "ValueError"):
        from datetime import datetime
        from datetime import timezone

        self.task_id = task_id
        self.status = SimpleNamespace(value=status)
        self.error_message = error_message
        self.error_type = error_type
        self.started_at = datetime.now(timezone.utc)
        self.duration_seconds = 0.1


class DummyPipelineResult:
    def __init__(self, pipeline_id: str, status: str):
        from datetime import datetime
        from datetime import timezone

        self.pipeline_id = pipeline_id
        self.pipeline_name = pipeline_id
        self.run_id = "run-test"
        self.status = status
        self.success_count = 0 if status != "success" else 1
        self.failure_count = 1 if status != "success" else 0
        self.skipped_count = 0
        self.started_at = datetime.now(timezone.utc)
        self.duration_seconds = 0.1
        self.task_results = {"t1": DummyTaskResult("t1", "failed")}


def test_failure_threshold_debounce(monkeypatch):
    sent = []

    @register_notifier("mem_threshold")
    class MemoryNotifier(BaseNotifier):
        def send(self, title: str, body: str, level: str, context: dict) -> None:
            sent.append((title, level))

    manager = NotifyManager()
    policy = NotifyPolicy(
        failure_threshold=3,
        channels=[NotifyChannelConfig(name="mem_threshold", config={})],
    )
    manager.on_pipeline_result(DummyPipelineResult("p", "failed"), policy)
    manager.on_pipeline_result(DummyPipelineResult("p", "failed"), policy)
    assert sent == []
    manager.on_pipeline_result(DummyPipelineResult("p", "failed"), policy)
    assert len(sent) >= 1
    manager.on_pipeline_result(DummyPipelineResult("p", "success"), policy)
    sent.clear()
    manager.on_pipeline_result(DummyPipelineResult("p", "failed"), policy)
    manager.on_pipeline_result(DummyPipelineResult("p", "failed"), policy)
    assert sent == []


def test_success_after_failure_resets_count():
    sent = []

    @register_notifier("mem_reset")
    class MemoryNotifier(BaseNotifier):
        def send(self, title: str, body: str, level: str, context: dict) -> None:
            sent.append((title, level))

    manager = NotifyManager()
    policy = NotifyPolicy(failure_threshold=3, channels=[NotifyChannelConfig(name="mem_reset", config={})])
    manager.on_pipeline_result(DummyPipelineResult("p", "failed"), policy)
    manager.on_pipeline_result(DummyPipelineResult("p", "failed"), policy)
    manager.on_pipeline_result(DummyPipelineResult("p", "success"), policy)
    manager.on_pipeline_result(DummyPipelineResult("p", "failed"), policy)
    assert sent == []


def test_multiple_channels_all_called():
    calls = []

    @register_notifier("mem_c1")
    class N1(BaseNotifier):
        def send(self, title: str, body: str, level: str, context: dict) -> None:
            calls.append("c1")

    @register_notifier("mem_c2")
    class N2(BaseNotifier):
        def send(self, title: str, body: str, level: str, context: dict) -> None:
            calls.append("c2")

    manager = NotifyManager()
    policy = NotifyPolicy(channels=[NotifyChannelConfig(name="mem_c1"), NotifyChannelConfig(name="mem_c2")])
    manager.on_pipeline_result(DummyPipelineResult("p", "failed"), policy)
    assert "c1" in calls and "c2" in calls


def test_notifier_failure_doesnt_break_pipeline():
    @register_notifier("mem_fail")
    class FailNotifier(BaseNotifier):
        def send(self, title: str, body: str, level: str, context: dict) -> None:
            raise ConnectionError("down")

    manager = NotifyManager()
    policy = NotifyPolicy(channels=[NotifyChannelConfig(name="mem_fail")])
    manager.on_pipeline_result(DummyPipelineResult("p", "failed"), policy)


def test_feishu_notifier_sends_correct_payload(monkeypatch):
    captured = {}

    class DummyResponse:
        def raise_for_status(self):
            return None

    def fake_post(url, json, timeout):
        captured["url"] = url
        captured["payload"] = json
        return DummyResponse()

    monkeypatch.setattr("orchestrator.notify.builtin.feishu_notifier.requests.post", fake_post)
    notifier = FeishuNotifier({"webhook_url": "https://example.com/hook"})
    notifier.send(
        title="test",
        body="failed",
        level="error",
        context={"pipeline_id": "p1", "error_message": "boom"},
    )
    assert captured["url"] == "https://example.com/hook"
    payload_text = str(captured["payload"])
    assert "pipeline_id" in payload_text
    assert "error_message" in payload_text
