from __future__ import annotations

import requests

from orchestrator.notify.base import BaseNotifier, register_notifier


@register_notifier("feishu")
class FeishuNotifier(BaseNotifier):
    def send(self, title: str, body: str, level: str, context: dict) -> None:
        webhook_url = self.config.get("webhook_url", "")
        if not webhook_url:
            raise ValueError("webhook_url is required for FeishuNotifier")
        icon = "✅"
        color = "green"
        if level == "warning":
            icon = "⚠️"
            color = "yellow"
        if level == "error":
            icon = "❌"
            color = "red"
        fields = [{"is_short": False, "text": {"tag": "lark_md", "content": body}}]
        for key, value in context.items():
            fields.append(
                {
                    "is_short": True,
                    "text": {"tag": "lark_md", "content": f"**{key}**: {value}"},
                }
            )
        payload = {
            "msg_type": "interactive",
            "card": {
                "header": {
                    "title": {"tag": "plain_text", "content": f"{icon} {title}"},
                    "template": color,
                },
                "elements": [{"tag": "div", "fields": fields}],
            },
        }
        response = requests.post(webhook_url, json=payload, timeout=10)
        response.raise_for_status()
