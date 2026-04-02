from __future__ import annotations

import requests

from orchestrator import BaseConnector
from orchestrator import register_connector


@register_connector("feishu_daily")
class FeishuDailyConnector(BaseConnector):
    def __init__(self, config: dict):
        super().__init__(config)
        self.webhook_url = config["webhook_url"]

    def send_report(self, title: str = "日报", content: str = "", **kwargs):
        payload = {
            "msg_type": "interactive",
            "card": {
                "header": {"title": {"tag": "plain_text", "content": title}},
                "elements": [{"tag": "markdown", "content": content}],
            },
        }
        response = requests.post(self.webhook_url, json=payload, timeout=10)
        response.raise_for_status()
        return response.json()
