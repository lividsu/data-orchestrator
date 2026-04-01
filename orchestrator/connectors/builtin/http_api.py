from __future__ import annotations

import time
from typing import Any

import requests

from orchestrator.connectors.base import BaseConnector
from orchestrator.connectors.registry import register_connector


class NonRetryableHttpError(Exception):
    pass


@register_connector("http_api")
class HttpApiConnector(BaseConnector):
    def __init__(self, config: dict):
        super().__init__(config)
        self.base_url = str(config.get("base_url", "")).rstrip("/")
        self.default_headers = dict(config.get("headers", {}))
        self.rate_limit_rps = config.get("rate_limit_rps")
        self._last_request_ts: float | None = None
        self._session = requests.Session()
        self._configure_auth()

    def _configure_auth(self) -> None:
        auth_config = self.config.get("auth") or {}
        auth_type = auth_config.get("type")
        if auth_type == "bearer":
            token = auth_config.get("token")
            if token:
                self.default_headers["Authorization"] = f"Bearer {token}"
        elif auth_type == "basic":
            user = auth_config.get("user")
            password = auth_config.get("password")
            if user is not None and password is not None:
                self._session.auth = (user, password)

    def _build_url(self, endpoint: str) -> str:
        if endpoint.startswith(("http://", "https://")):
            return endpoint
        normalized = endpoint.lstrip("/")
        if not normalized:
            return self.base_url
        return f"{self.base_url}/{normalized}"

    def _sleep_for_rate_limit(self) -> None:
        if not self.rate_limit_rps:
            return
        min_interval = 1 / float(self.rate_limit_rps)
        now = time.monotonic()
        if self._last_request_ts is not None:
            elapsed = now - self._last_request_ts
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)
        self._last_request_ts = time.monotonic()

    def _request(
        self,
        endpoint: str,
        method: str = "GET",
        params: dict | None = None,
        body: Any = None,
        headers: dict | None = None,
    ) -> requests.Response:
        self._sleep_for_rate_limit()
        url = self._build_url(endpoint)
        merged_headers = {**self.default_headers, **(headers or {})}
        response = self._session.request(
            method=method.upper(),
            url=url,
            params=params,
            json=body,
            headers=merged_headers,
        )
        if 400 <= response.status_code < 500:
            raise NonRetryableHttpError(
                f"Non-retryable client error: {response.status_code} {method.upper()} {url}"
            )
        response.raise_for_status()
        return response

    def fetch(
        self,
        endpoint: str,
        method: str = "GET",
        params: dict | None = None,
        body: Any = None,
        headers: dict | None = None,
    ) -> Any:
        response = self._request(
            endpoint=endpoint,
            method=method,
            params=params,
            body=body,
            headers=headers,
        )
        return response.json()

    def push(
        self,
        data: Any = None,
        endpoint: str = "",
        method: str = "POST",
        headers: dict | None = None,
        **kwargs: Any,
    ) -> None:
        self._request(endpoint=endpoint, method=method, body=data, headers=headers)

    def ping(self) -> bool:
        try:
            self._sleep_for_rate_limit()
            response = self._session.get(self.base_url)
            return response.status_code < 500
        except requests.RequestException:
            return False

    def close(self):
        self._session.close()
