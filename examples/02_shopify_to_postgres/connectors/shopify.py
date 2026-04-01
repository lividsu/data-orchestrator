from __future__ import annotations

import requests

from orchestrator import BaseConnector
from orchestrator import register_connector


@register_connector("shopify")
class ShopifyConnector(BaseConnector):
    def __init__(self, config: dict):
        super().__init__(config)
        self.base_url = str(config.get("base_url", "https://{shop}.myshopify.com/admin/api/2024-10"))
        token = config.get("access_token", "")
        self.session = requests.Session()
        self.session.headers.update(
            {
                "X-Shopify-Access-Token": token,
                "Content-Type": "application/json",
            }
        )

    def fetch(self, endpoint: str = "orders.json", params: dict | None = None, **kwargs):
        url = f"{self.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        response = self.session.get(url, params=params or {})
        response.raise_for_status()
        payload = response.json()
        return payload.get("orders", payload)

    def push(self, data, **kwargs):
        return data

    def ping(self) -> bool:
        try:
            response = self.session.get(f"{self.base_url.rstrip('/')}/shop.json", timeout=5)
            return response.status_code < 500
        except Exception:
            return False

    def close(self):
        self.session.close()
