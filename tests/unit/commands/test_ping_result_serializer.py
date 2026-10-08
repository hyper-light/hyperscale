"""
`PingResultSerializer` builds one result schema from every engine response
type `hyperscale ping` receives, and `PingResultOutput` writes it to
`--filepath` atomically or holds the failure for the command to report.

Each response type's expected values are stated in full: the fields it
carries, decoded (header octets as ISO-8859-1, bodies as UTF-8 text or else
base64), and every field it does not carry null.
"""

import base64
import json
import pathlib

import msgspec
import pytest

from hyperscale.commands.requests.models import PingResult, PingResultFile
from hyperscale.commands.requests.ping_result_output import PingResultOutput
from hyperscale.commands.requests.ping_result_serializer import PingResultSerializer
from hyperscale.core.engines.client.ftp.models.ftp import FTPResponse
from hyperscale.core.engines.client.graphql.models.graphql import GraphQLResponse
from hyperscale.core.engines.client.http.models.http import HTTPResponse
from hyperscale.core.engines.client.http2.models.http2 import HTTP2Response
from hyperscale.core.engines.client.http3.models.http3 import HTTP3Response
from hyperscale.core.engines.client.sftp.models import SFTPResponse, TransferResult
from hyperscale.core.engines.client.shared.models import URLMetadata
from hyperscale.core.engines.client.smtp.models.smtp import SMTPResponse
from hyperscale.core.engines.client.tcp.models.tcp import TCPResponse
from hyperscale.core.engines.client.udp.models.udp import UDPResponse
from hyperscale.core.engines.client.websocket.models.websocket import WebsocketResponse

URL = "http://127.0.0.1:8080/probe"
URL_METADATA = URLMetadata(host="127.0.0.1", path="/probe")
TIMINGS = {"request_start": 10.0, "connect_start": 10.25, "request_end": 12.5}
NOT_UTF8 = b"\xff\xfe\x00binary"


def full_result(protocol: str, method: str | None, **fields) -> dict:
    """The whole result schema, null except for ``fields``."""
    result = {
        "protocol": protocol,
        "url": URL,
        "method": method,
        "status": None,
        "status_message": None,
        "error": None,
        "headers": None,
        "trailers": None,
        "redirects": None,
        "timings": None,
        "elapsed": None,
        "content": None,
        "content_encoding": None,
        "files": None,
    }
    result.update(fields)
    return result


def as_json(result: PingResult) -> dict:
    """The result as the output file holds it."""
    return json.loads(msgspec.json.encode(result))


def test_an_http_response_writes_status_headers_and_utf8_body() -> None:
    response = HTTPResponse(
        url=URL_METADATA,
        method="GET",
        status=200,
        headers={b"content-type": b"application/json", b"x-latin": b"caf\xe9"},
        content=b'{"hello": "world"}',
        timings=TIMINGS,
        redirects=1,
    )

    result = PingResultSerializer("http", URL, "GET").from_http_response(response)

    assert as_json(result) == full_result(
        "http",
        "GET",
        status=200,
        headers={"content-type": "application/json", "x-latin": "café"},
        redirects=1,
        timings=TIMINGS,
        elapsed=2.5,
        content='{"hello": "world"}',
        content_encoding="utf-8",
    )


def test_an_http_engine_error_answer_is_written_as_the_result() -> None:
    response = HTTPResponse(
        url=URL_METADATA,
        method="GET",
        status=400,
        status_message="Connection failed.",
        timings={"request_start": 1.0, "request_end": None},
    )

    result = PingResultSerializer("http", URL, "GET").from_http_response(response)

    assert as_json(result) == full_result(
        "http",
        "GET",
        status=400,
        status_message="Connection failed.",
        redirects=0,
        timings={"request_start": 1.0, "request_end": None},
        content="",
        content_encoding="utf-8",
    )


def test_a_bytearray_body_that_is_not_utf8_is_written_as_base64() -> None:
    response = GraphQLResponse(url=URL_METADATA, status=200, content=bytearray(NOT_UTF8))

    result = PingResultSerializer("graphql", URL, "QUERY").from_http_response(response)

    assert as_json(result) == full_result(
        "graphql",
        "QUERY",
        status=200,
        redirects=0,
        content=base64.b64encode(NOT_UTF8).decode(),
        content_encoding="base64",
    )


def test_a_websocket_response_writes_its_upgrade() -> None:
    response = WebsocketResponse(
        url=URL_METADATA,
        status=101,
        headers={b"upgrade": b"websocket"},
        content=b"frame",
        timings=TIMINGS,
    )

    result = PingResultSerializer("websocket", URL, "SEND").from_http_response(response)

    assert as_json(result) == full_result(
        "websocket",
        "SEND",
        status=101,
        headers={"upgrade": "websocket"},
        redirects=0,
        timings=TIMINGS,
        elapsed=2.5,
        content="frame",
        content_encoding="utf-8",
    )


def test_an_http2_response_writes_text_headers_and_trailers() -> None:
    response = HTTP2Response(
        url=URL_METADATA,
        method="POST",
        status=200,
        headers={"content-type": "text/plain"},
        trailers={"grpc-status": "0"},
        content=b"done",
        timings=TIMINGS,
    )

    result = PingResultSerializer("http2", URL, "POST").from_multiplexed_http_response(response)

    assert as_json(result) == full_result(
        "http2",
        "POST",
        status=200,
        headers={"content-type": "text/plain"},
        trailers={"grpc-status": "0"},
        redirects=0,
        timings=TIMINGS,
        elapsed=2.5,
        content="done",
        content_encoding="utf-8",
    )


def test_an_http3_response_writes_byte_headers_and_trailers() -> None:
    response = HTTP3Response(
        url=URL_METADATA,
        method="GET",
        status=204,
        headers={b"server": b"probe"},
        trailers={b"x-checksum": b"abc"},
        timings=TIMINGS,
    )

    result = PingResultSerializer("http3", URL, "GET").from_multiplexed_http_response(response)

    assert as_json(result) == full_result(
        "http3",
        "GET",
        status=204,
        headers={"server": "probe"},
        trailers={"x-checksum": "abc"},
        redirects=0,
        timings=TIMINGS,
        elapsed=2.5,
        content="",
        content_encoding="utf-8",
    )


@pytest.mark.parametrize("response_type", [TCPResponse, UDPResponse])
def test_a_socket_response_writes_its_bytes_and_error(response_type: type[TCPResponse] | type[UDPResponse]) -> None:
    response = response_type(url=URL_METADATA, error="Connection reset.", content=NOT_UTF8, timings=TIMINGS)

    result = PingResultSerializer("tcp", URL, "SEND").from_socket_response(response)

    assert as_json(result) == full_result(
        "tcp",
        "SEND",
        error="Connection reset.",
        timings=TIMINGS,
        elapsed=2.5,
        content=base64.b64encode(NOT_UTF8).decode(),
        content_encoding="base64",
    )


def test_an_ftp_response_writes_its_reply_data() -> None:
    response = FTPResponse(action="PWD", data=b"/home/probe", timings=TIMINGS)

    result = PingResultSerializer("ftp", URL, "PWD").from_ftp_response(response)

    assert as_json(result) == full_result(
        "ftp",
        "PWD",
        timings=TIMINGS,
        elapsed=2.5,
        content="/home/probe",
        content_encoding="utf-8",
    )


def test_an_ftp_size_reply_is_written_as_decimal_text_and_its_error_as_text() -> None:
    response = FTPResponse(action="SIZE", data=4096, error=TimeoutError())

    result = PingResultSerializer("ftp", URL, "SIZE").from_ftp_response(response)

    assert as_json(result) == full_result(
        "ftp",
        "SIZE",
        error="TimeoutError",
        content="4096",
        content_encoding="utf-8",
    )


def test_an_sftp_response_writes_one_entry_per_transferred_path() -> None:
    response = SFTPResponse(
        url=URL_METADATA,
        operation="getcwd",
        transferred={
            b"/home/probe": TransferResult(file_path=b"/home/probe", file_type="DIRECTORY"),
            b"/data.bin": TransferResult(file_path=b"/data\xff.bin", file_data=NOT_UTF8),
        },
        timings=TIMINGS,
    )

    result = PingResultSerializer("sftp", URL, "GETCWD").from_sftp_response(response)

    assert result.files == [
        PingResultFile(path="/home/probe", file_type="DIRECTORY", content=None, content_encoding=None),
        PingResultFile(
            path="/data\\xff.bin",
            file_type="FILE",
            content=base64.b64encode(NOT_UTF8).decode(),
            content_encoding="base64",
        ),
    ]
    assert as_json(result) == full_result(
        "sftp",
        "GETCWD",
        timings=TIMINGS,
        elapsed=2.5,
        files=[
            {"path": "/home/probe", "file_type": "DIRECTORY", "content": None, "content_encoding": None},
            {
                "path": "/data\\xff.bin",
                "file_type": "FILE",
                "content": base64.b64encode(NOT_UTF8).decode(),
                "content_encoding": "base64",
            },
        ],
    )


def test_a_failed_sftp_response_writes_its_error_and_no_files() -> None:
    response = SFTPResponse(url=URL_METADATA, error=ConnectionRefusedError("refused"))

    result = PingResultSerializer("sftp", URL, "GETCWD").from_sftp_response(response)

    assert as_json(result) == full_result("sftp", "GETCWD", error="refused")


def test_an_smtp_response_writes_the_last_reply() -> None:
    response = SMTPResponse(
        recipients=["probe@example.test"],
        sender="ping@example.test",
        server="smtp.example.test",
        email="body",
        last_smtp_code=250,
        last_smtp_message=b"2.0.0 Ok: queued",
        encoding="ascii",
        timings=TIMINGS,
    )

    result = PingResultSerializer("smtp", URL, "SEND").from_smtp_response(response)

    assert as_json(result) == full_result(
        "smtp",
        "SEND",
        status=250,
        status_message="2.0.0 Ok: queued",
        timings=TIMINGS,
        elapsed=2.5,
    )


def test_an_smtp_response_without_a_reply_writes_a_null_status() -> None:
    """An SMTP response that got no reply serializes a null status: its
    ``last_smtp_code`` defaults to None (a stray trailing comma once made
    the default the tuple ``(None,)``, written as ``[null]``)."""
    response = SMTPResponse(
        recipients=["probe@example.test"],
        sender="ping@example.test",
        server="smtp.example.test",
        email="body",
        error=ConnectionRefusedError("refused"),
    )

    result = PingResultSerializer("smtp", URL, "SEND").from_smtp_response(response)

    assert as_json(result) == full_result("smtp", "SEND", error="refused")


def test_a_request_that_raised_writes_the_error_alone() -> None:
    result = PingResultSerializer("graphql", URL, "QUERY").from_error(ValueError("bad query"))

    assert as_json(result) == full_result("graphql", "QUERY", error="bad query")


def test_an_error_without_a_message_is_named_by_its_type() -> None:
    result = PingResultSerializer("http", URL, "GET").from_error(TimeoutError())

    assert as_json(result) == full_result("http", "GET", error="TimeoutError")


def tcp_response(content: bytes) -> TCPResponse:
    return TCPResponse(url=URL_METADATA, content=content, timings=TIMINGS)


@pytest.mark.asyncio
async def test_the_output_writes_the_result_as_one_json_object(tmp_path: pathlib.Path) -> None:
    output_path = tmp_path / "result.json"
    serializer = PingResultSerializer("tcp", URL, "SEND")
    output = PingResultOutput(str(output_path), serializer)

    await output.record(serializer.from_socket_response, tcp_response(b"PONG"))
    output.raise_on_write_failure()

    assert json.loads(output_path.read_text()) == full_result(
        "tcp", "SEND", timings=TIMINGS, elapsed=2.5, content="PONG", content_encoding="utf-8"
    )
    assert list(tmp_path.iterdir()) == [output_path]


@pytest.mark.asyncio
async def test_a_failure_after_the_response_keeps_the_response_result(tmp_path: pathlib.Path) -> None:
    output_path = tmp_path / "result.json"
    serializer = PingResultSerializer("tcp", URL, "SEND")
    output = PingResultOutput(str(output_path), serializer)

    await output.record(serializer.from_socket_response, tcp_response(b"PONG"))
    await output.record_failure(RuntimeError("terminal update failed"))

    assert json.loads(output_path.read_text())["content"] == "PONG"
    assert json.loads(output_path.read_text())["error"] is None


@pytest.mark.asyncio
async def test_a_request_that_raised_is_recorded_as_its_error(tmp_path: pathlib.Path) -> None:
    output_path = tmp_path / "result.json"
    output = PingResultOutput(str(output_path), PingResultSerializer("http", URL, "GET"))

    await output.record_failure(ConnectionResetError("reset by peer"))
    output.raise_on_write_failure()

    assert json.loads(output_path.read_text()) == full_result("http", "GET", error="reset by peer")


@pytest.mark.asyncio
async def test_without_a_filepath_nothing_is_built_or_written(tmp_path: pathlib.Path) -> None:
    def refuse_to_build(response: TCPResponse) -> PingResult:
        raise AssertionError("built a result with no --filepath")

    output = PingResultOutput(None, PingResultSerializer("tcp", URL, "SEND"))

    await output.record(refuse_to_build, tcp_response(b"PONG"))
    await output.record_failure(RuntimeError("raised"))
    output.raise_on_write_failure()

    assert list(tmp_path.iterdir()) == []


@pytest.mark.asyncio
async def test_a_failed_write_is_reported_and_exits_non_zero(
    tmp_path: pathlib.Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    occupied_path = tmp_path / "occupied"
    occupied_path.mkdir()
    serializer = PingResultSerializer("tcp", URL, "SEND")
    output = PingResultOutput(str(occupied_path), serializer)

    await output.record(serializer.from_socket_response, tcp_response(b"PONG"))

    with pytest.raises(SystemExit) as exit_info:
        output.raise_on_write_failure()

    assert exit_info.value.code == 1
    assert isinstance(exit_info.value.__cause__, OSError)
    assert capsys.readouterr().err.startswith(f"hyperscale ping: could not write the result to {occupied_path}: ")
    assert list(tmp_path.iterdir()) == [occupied_path]
    assert list(occupied_path.iterdir()) == []
