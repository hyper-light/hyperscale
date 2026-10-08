"""
`hyperscale ping <protocol> --filepath PATH` writes the request's result to
PATH as one JSON object.

Every `ping` subcommand documented `--filepath` and handed it to its request
helper, and no helper used it: the option was dead. The result is now
written atomically once the request completes, whether the server answered,
the engine answered for a failed request, or the request raised; a write
that fails is reported on stderr and the command exits non-zero.

Real `hyperscale` processes against servers started in the test:
* a plain HTTP server answering a known JSON body and headers;
* a self-signed HTTPS server, reached with `--insecure`;
* an HTTP server answering a body that is not UTF-8 (written as base64);
* a port nothing listens on (the engine's error answer is the result);
* a TCP echo server for `ping tcp`;
* a `--filepath` naming a directory: non-zero exit, a stderr message, and
  no file — neither a partial result nor the write's temp file — left.
"""

import asyncio
import base64
import json
import pathlib
import socket

import pytest

from tests.integration.cli.node_processes import HYPERSCALE, command_environment
from tests.unit.core.test_engine_tls_verification import HOST, free_port, start_https_target

COMMAND_TIMEOUT_SECONDS = 60
JSON_BODY = b'{"hello": "world"}'
BINARY_BODY = bytes(range(256))
ECHO_PAYLOAD = "hello-tcp"
CONNECTION_TIMINGS = {
    "request_start",
    "connect_start",
    "connect_end",
    "write_start",
    "write_end",
    "read_start",
    "read_end",
    "request_end",
}


class PingRun:
    """The exit code and output of one `hyperscale ping` process."""

    def __init__(self, return_code: int, standard_error: str) -> None:
        self.return_code = return_code
        self.standard_error = standard_error


async def run_ping(*arguments: str) -> PingRun:
    process = await asyncio.create_subprocess_exec(
        HYPERSCALE,
        "ping",
        *arguments,
        "--quiet",
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
        env=command_environment(),
    )
    _, standard_error = await asyncio.wait_for(process.communicate(), COMMAND_TIMEOUT_SECONDS)
    return PingRun(process.returncode, standard_error.decode())


def answer_with(head: bytes, body: bytes):
    """An asyncio stream handler answering every request with one fixed
    HTTP/1.1 response, then closing."""

    async def answer(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            await reader.readuntil(b"\r\n\r\n")
            writer.write(head + b"Content-Length: %d\r\n\r\n" % len(body) + body)
            await writer.drain()
        finally:
            writer.close()

    return answer


async def echo(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        writer.write(await reader.read(65536))
        await writer.drain()
    finally:
        writer.close()


async def start_server(handler) -> tuple[asyncio.Server, int]:
    port = free_port(socket.SOCK_STREAM)
    server = await asyncio.start_server(handler, HOST, port)
    return server, port


async def stop_server(server: asyncio.Server) -> None:
    server.close()
    await server.wait_closed()


def read_result(path: pathlib.Path) -> dict:
    return json.loads(path.read_text())


def assert_connection_timings(result: dict) -> None:
    """Timings carry the connection's eight instants, in order, and
    ``elapsed`` is request_end - request_start."""
    timings = result["timings"]
    assert set(timings) == CONNECTION_TIMINGS
    ordered = [timings[name] for name in ("request_start", "connect_start", "connect_end", "request_end")]
    assert all(isinstance(instant, float) for instant in ordered)
    assert ordered == sorted(ordered)
    assert result["elapsed"] == timings["request_end"] - timings["request_start"]


def http_result(url: str, **fields) -> dict:
    """The full result schema for a `ping http` request, with ``fields``
    over the defaults."""
    result = {
        "protocol": "http",
        "url": url,
        "method": "GET",
        "status": None,
        "status_message": None,
        "error": None,
        "headers": None,
        "trailers": None,
        "redirects": 0,
        "timings": None,
        "elapsed": None,
        "content": None,
        "content_encoding": None,
        "files": None,
    }
    result.update(fields)
    return result


@pytest.mark.asyncio
async def test_ping_http_writes_the_response(tmp_path: pathlib.Path) -> None:
    server, port = await start_server(
        answer_with(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nX-Probe: one\r\n", JSON_BODY)
    )
    url = f"http://{HOST}:{port}/probe"
    output_path = tmp_path / "result.json"
    try:
        run = await run_ping("http", url, "--filepath", str(output_path))
    finally:
        await stop_server(server)

    assert run.return_code == 0, run.standard_error
    result = read_result(output_path)
    assert_connection_timings(result)
    assert result == http_result(
        url,
        status=200,
        headers={"content-type": "application/json", "x-probe": "one", "content-length": str(len(JSON_BODY))},
        timings=result["timings"],
        elapsed=result["elapsed"],
        content=JSON_BODY.decode(),
        content_encoding="utf-8",
    )


@pytest.mark.asyncio
async def test_ping_http_writes_a_self_signed_https_response_with_insecure(tmp_path: pathlib.Path) -> None:
    server, target, port = await start_https_target(tmp_path)
    url = f"https://{HOST}:{port}/"
    output_path = tmp_path / "result.json"
    try:
        run = await run_ping("http", url, "--insecure", "--filepath", str(output_path))
    finally:
        await stop_server(server)

    assert run.return_code == 0, run.standard_error
    assert target.requests_answered == 1
    result = read_result(output_path)
    assert_connection_timings(result)
    assert result == http_result(
        url,
        status=200,
        headers={"content-length": "2", "connection": "keep-alive"},
        timings=result["timings"],
        elapsed=result["elapsed"],
        content="ok",
        content_encoding="utf-8",
    )


@pytest.mark.asyncio
async def test_ping_http_writes_a_binary_body_as_base64(tmp_path: pathlib.Path) -> None:
    server, port = await start_server(
        answer_with(b"HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\n", BINARY_BODY)
    )
    url = f"http://{HOST}:{port}/blob"
    output_path = tmp_path / "result.json"
    try:
        run = await run_ping("http", url, "--filepath", str(output_path))
    finally:
        await stop_server(server)

    assert run.return_code == 0, run.standard_error
    result = read_result(output_path)
    assert_connection_timings(result)
    assert base64.b64decode(result["content"]) == BINARY_BODY
    assert result == http_result(
        url,
        status=200,
        headers={"content-type": "application/octet-stream", "content-length": str(len(BINARY_BODY))},
        timings=result["timings"],
        elapsed=result["elapsed"],
        content=base64.b64encode(BINARY_BODY).decode(),
        content_encoding="base64",
    )


@pytest.mark.asyncio
async def test_ping_http_writes_the_engine_error_for_a_refused_connection(tmp_path: pathlib.Path) -> None:
    url = f"http://{HOST}:{free_port(socket.SOCK_STREAM)}/"
    output_path = tmp_path / "result.json"

    run = await run_ping("http", url, "--filepath", str(output_path))

    assert run.return_code == 0, run.standard_error
    result = read_result(output_path)
    timings = result["timings"]
    assert set(timings) == CONNECTION_TIMINGS
    assert [timings[name] for name in ("write_start", "write_end", "read_start", "read_end")] == [None] * 4
    assert result["elapsed"] == timings["request_end"] - timings["request_start"]
    assert result == http_result(
        url,
        status=400,
        status_message="Connection failed.",
        timings=timings,
        elapsed=result["elapsed"],
        content="",
        content_encoding="utf-8",
    )


@pytest.mark.asyncio
async def test_ping_tcp_writes_the_echoed_bytes(tmp_path: pathlib.Path) -> None:
    server, port = await start_server(echo)
    url = f"{HOST}:{port}"
    output_path = tmp_path / "result.json"
    try:
        run = await run_ping(
            "tcp",
            url,
            "--method",
            "bidirectional",
            "--data",
            ECHO_PAYLOAD,
            "--options",
            json.dumps({"size": len(ECHO_PAYLOAD)}),
            "--filepath",
            str(output_path),
        )
    finally:
        await stop_server(server)

    assert run.return_code == 0, run.standard_error
    result = read_result(output_path)
    assert_connection_timings(result)
    assert result == {
        "protocol": "tcp",
        "url": url,
        "method": "BIDIRECTIONAL",
        "status": None,
        "status_message": None,
        "error": None,
        "headers": None,
        "trailers": None,
        "redirects": None,
        "timings": result["timings"],
        "elapsed": result["elapsed"],
        "content": ECHO_PAYLOAD,
        "content_encoding": "utf-8",
        "files": None,
    }


@pytest.mark.asyncio
async def test_ping_reports_an_unwritable_filepath_and_exits_non_zero(tmp_path: pathlib.Path) -> None:
    server, port = await start_server(answer_with(b"HTTP/1.1 200 OK\r\n", JSON_BODY))
    occupied_path = tmp_path / "occupied"
    occupied_path.mkdir()
    try:
        run = await run_ping("http", f"http://{HOST}:{port}/", "--filepath", str(occupied_path))
    finally:
        await stop_server(server)

    assert run.return_code != 0
    assert f"hyperscale ping: could not write the result to {occupied_path}: " in run.standard_error
    assert list(tmp_path.iterdir()) == [occupied_path]
    assert list(occupied_path.iterdir()) == []
