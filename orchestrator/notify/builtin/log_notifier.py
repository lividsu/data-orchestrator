from __future__ import annotations

from rich.console import Console

from orchestrator.notify.base import BaseNotifier, register_notifier

console = Console()


@register_notifier("log")
class LogNotifier(BaseNotifier):
    def send(self, title: str, body: str, level: str, context: dict) -> None:
        style = "green"
        if level == "warning":
            style = "yellow"
        if level == "error":
            style = "red"
        console.print(f"[{style}]{level.upper()}[/{style}] {title}")
        console.print(body)
        if context:
            console.print(context)
