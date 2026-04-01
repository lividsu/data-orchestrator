from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests

from orchestrator.connectors.builtin.csv_file import CsvFileConnector
from orchestrator.connectors.builtin.http_api import HttpApiConnector, NonRetryableHttpError


class FakeResponse:
    def __init__(self, status_code: int, payload=None):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.content = b"content"

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(response=self)


def test_http_fetch_merges_headers_and_returns_json():
    connector = HttpApiConnector(
        {
            "base_url": "https://api.example.com",
            "headers": {"X-Global": "1"},
            "auth": {"type": "bearer", "token": "token-123"},
        }
    )
    connector._session.request = MagicMock(return_value=FakeResponse(200, {"ok": True}))

    result = connector.fetch(endpoint="/v1/items", params={"page": 1}, headers={"X-Req": "2"})

    assert result == {"ok": True}
    call = connector._session.request.call_args.kwargs
    assert call["url"] == "https://api.example.com/v1/items"
    assert call["params"] == {"page": 1}
    assert call["headers"]["X-Global"] == "1"
    assert call["headers"]["X-Req"] == "2"
    assert call["headers"]["Authorization"] == "Bearer token-123"


def test_http_push_sends_json_body():
    connector = HttpApiConnector({"base_url": "https://api.example.com"})
    connector._session.request = MagicMock(return_value=FakeResponse(200, {"ok": True}))

    connector.push(data={"name": "alice"}, endpoint="users")

    call = connector._session.request.call_args.kwargs
    assert call["method"] == "POST"
    assert call["url"] == "https://api.example.com/users"
    assert call["json"] == {"name": "alice"}


def test_http_fetch_4xx_raises_non_retryable_error():
    connector = HttpApiConnector({"base_url": "https://api.example.com"})
    connector._session.request = MagicMock(return_value=FakeResponse(404))

    with pytest.raises(NonRetryableHttpError):
        connector.fetch(endpoint="missing")


def test_http_fetch_5xx_raises_http_error():
    connector = HttpApiConnector({"base_url": "https://api.example.com"})
    connector._session.request = MagicMock(return_value=FakeResponse(502))

    with pytest.raises(requests.HTTPError):
        connector.fetch(endpoint="unstable")


def test_http_rate_limit_sleeps_between_requests():
    connector = HttpApiConnector({"base_url": "https://api.example.com", "rate_limit_rps": 2})
    with patch("orchestrator.connectors.builtin.http_api.time.monotonic", side_effect=[0.0, 0.0, 0.1, 0.1]):
        with patch("orchestrator.connectors.builtin.http_api.time.sleep") as sleep_mock:
            connector._sleep_for_rate_limit()
            connector._sleep_for_rate_limit()
    sleep_mock.assert_called_once_with(0.4)


def test_http_ping_status_behavior():
    connector = HttpApiConnector({"base_url": "https://api.example.com"})
    connector._session.get = MagicMock(return_value=FakeResponse(499))
    assert connector.ping() is True
    connector._session.get = MagicMock(return_value=FakeResponse(500))
    assert connector.ping() is False


def test_csv_fetch_supports_relative_and_absolute_path(tmp_path: Path):
    base_dir = tmp_path / "data"
    base_dir.mkdir()
    relative_file = base_dir / "input.csv"
    relative_file.write_text("id,name\n1,Alice\n2,Bob\n", encoding="utf-8")
    connector = CsvFileConnector({"base_dir": str(base_dir)})

    rows_from_relative = connector.fetch(path="input.csv")
    rows_from_absolute = connector.fetch(path=str(relative_file))

    assert rows_from_relative == [{"id": "1", "name": "Alice"}, {"id": "2", "name": "Bob"}]
    assert rows_from_absolute == rows_from_relative


def test_csv_push_overwrite_creates_parent_directory(tmp_path: Path):
    connector = CsvFileConnector({"base_dir": str(tmp_path)})
    connector.push(
        data=[{"id": "1", "name": "Alice"}, {"id": "2", "name": "Bob"}],
        path="nested/output.csv",
        mode="overwrite",
    )

    output_file = tmp_path / "nested" / "output.csv"
    assert output_file.exists()
    assert output_file.read_text(encoding="utf-8").splitlines() == [
        "id,name",
        "1,Alice",
        "2,Bob",
    ]


def test_csv_push_append_writes_header_once(tmp_path: Path):
    connector = CsvFileConnector({"base_dir": str(tmp_path)})
    connector.push(data=[{"id": "1", "name": "Alice"}], path="append.csv", mode="append")
    connector.push(data=[{"id": "2", "name": "Bob"}], path="append.csv", mode="append")

    output_file = tmp_path / "append.csv"
    assert output_file.read_text(encoding="utf-8").splitlines() == [
        "id,name",
        "1,Alice",
        "2,Bob",
    ]


def test_csv_ping_checks_base_dir_exists_and_writable(tmp_path: Path):
    connector = CsvFileConnector({"base_dir": str(tmp_path)})
    assert connector.ping() is True

    missing = tmp_path / "missing_dir"
    connector_missing = CsvFileConnector({"base_dir": str(missing)})
    assert connector_missing.ping() is False
