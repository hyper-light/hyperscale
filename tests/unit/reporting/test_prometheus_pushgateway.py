"""The Prometheus reporter pushes to a real pushgateway endpoint.

A local asyncio HTTP server stands in for the pushgateway and records each
request, so the test sees what ``push_to_gateway`` actually sends: the push
reaches the configured host and port, and with credentials it carries Basic
Auth on the push's own method.
"""

import asyncio
import base64

import pytest

prometheus_client = pytest.importorskip("prometheus_client")

from prometheus_client.core import REGISTRY

from hyperscale.reporting.prometheus.prometheus import Prometheus
from hyperscale.reporting.prometheus.prometheus_config import PrometheusConfig


class RecordingPushgateway:
    """An HTTP endpoint that answers every request 200 and records its
    request line and headers."""

    def __init__(self) -> None:
        self.requests: list[tuple[str, dict[str, str]]] = []
        self._server: asyncio.Server | None = None

    async def start(self) -> int:
        self._server = await asyncio.start_server(self._answer, "127.0.0.1", 0)
        return self._server.sockets[0].getsockname()[1]

    async def stop(self) -> None:
        self._server.close()
        await self._server.wait_closed()

    async def _answer(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        request_line = (await reader.readline()).decode().strip()
        headers: dict[str, str] = {}
        while (header_line := (await reader.readline()).decode().strip()):
            name, _, value = header_line.partition(":")
            headers[name.strip().lower()] = value.strip()
        await reader.readexactly(int(headers.get("content-length", "0")))
        self.requests.append((request_line, headers))
        writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
        await writer.drain()
        writer.close()
        await writer.wait_closed()


@pytest.mark.asyncio
@pytest.mark.parametrize("credentials", [None, ("reporter", "secret")], ids=["anonymous", "basic-auth"])
async def test_a_push_reaches_the_gateway_with_its_own_method(credentials: tuple[str, str] | None) -> None:
    gateway = RecordingPushgateway()
    port = await gateway.start()
    username, password = credentials if credentials is not None else (None, None)
    reporter = Prometheus(
        PrometheusConfig(
            pushgateway_host="127.0.0.1",
            pushgateway_port=port,
            username=username,
            password=password,
            job_name="push-test",
        )
    )
    await reporter.connect()
    try:
        await reporter._submit_to_pushgateway()
    finally:
        REGISTRY.unregister(reporter.registry)
        await gateway.stop()

    ((request_line, headers),) = gateway.requests
    assert request_line.startswith("PUT /metrics/job/push-test "), request_line
    if credentials is None:
        assert "authorization" not in headers
    else:
        expected = base64.b64encode(f"{username}:{password}".encode()).decode()
        assert headers["authorization"] == f"Basic {expected}"
