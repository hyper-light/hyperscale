import base64
from collections.abc import Mapping
from typing import Literal

from hyperscale.core.engines.client.ftp.models.ftp import FTPResponse
from hyperscale.core.engines.client.http.models.http import HTTPResponse
from hyperscale.core.engines.client.http2.models.http2 import HTTP2Response
from hyperscale.core.engines.client.http3.models.http3 import HTTP3Response
from hyperscale.core.engines.client.sftp.models import SFTPResponse, TransferResult
from hyperscale.core.engines.client.smtp.models.smtp import SMTPResponse
from hyperscale.core.engines.client.tcp.models.tcp import TCPResponse
from hyperscale.core.engines.client.udp.models.udp import UDPResponse

from .models import PingResult, PingResultFile

ContentBytes = bytes | bytearray | memoryview
ContentEncoding = Literal["utf-8", "base64"]
FieldText = bytes | bytearray | str | int | float


class PingResultSerializer:
    """Builds the ``PingResult`` one ``hyperscale ping`` request wrote, from
    the engine response it received or the error it raised.

    Configured with what the command requested: the protocol (the ``ping``
    subcommand's name), the URL and the method or operation. Each builder
    reads only the fields its response type carries and leaves the rest of
    the schema null.
    """

    __slots__ = ("_protocol", "_url", "_method", "_path_encoding")

    def __init__(
        self,
        protocol: str,
        url: str,
        method: str | None,
        path_encoding: str = "utf-8",
    ) -> None:
        self._protocol = protocol
        self._url = url
        self._method = method
        self._path_encoding = path_encoding

    def from_http_response(self, response: HTTPResponse) -> PingResult:
        """The result of an HTTP/1.1-carried request: HTTP, GraphQL or
        websocket. HTTP/1.1 responses carry no trailers field."""
        return self._http_result(response, None)

    def from_multiplexed_http_response(self, response: HTTP2Response | HTTP3Response) -> PingResult:
        """The result of an HTTP/2 or HTTP/3 request (GraphQL over HTTP/2
        included), trailers and all."""
        return self._http_result(response, response.trailers)

    def from_socket_response(self, response: TCPResponse | UDPResponse) -> PingResult:
        """The result of a TCP or UDP request."""
        content, content_encoding = self.encode_content(response.content)
        return PingResult(
            protocol=self._protocol,
            url=self._url,
            method=self._method,
            error=self.describe_error(response.error),
            timings=self.copy_timings(response.timings),
            elapsed=self.measure_elapsed(response.timings),
            content=content,
            content_encoding=content_encoding,
        )

    def from_ftp_response(self, response: FTPResponse) -> PingResult:
        """The result of an FTP request; a numeric reply (``SIZE``) is
        written as its decimal text."""
        response_data = str(response.data).encode() if isinstance(response.data, int) else response.data
        content, content_encoding = self.encode_content(response_data)
        return PingResult(
            protocol=self._protocol,
            url=self._url,
            method=self._method,
            error=self.describe_error(response.error),
            timings=self.copy_timings(response.timings),
            elapsed=self.measure_elapsed(response.timings),
            content=content,
            content_encoding=content_encoding,
        )

    def from_sftp_response(self, response: SFTPResponse) -> PingResult:
        """The result of an SFTP request, one ``files`` entry per path it
        transferred."""
        return PingResult(
            protocol=self._protocol,
            url=self._url,
            method=self._method,
            error=self.describe_error(response.error),
            timings=self.copy_timings(response.timings),
            elapsed=self.measure_elapsed(response.timings),
            files=self.describe_files(response.transferred),
        )

    def from_smtp_response(self, response: SMTPResponse) -> PingResult:
        """The result of an SMTP send: the last reply's code and text."""
        return PingResult(
            protocol=self._protocol,
            url=self._url,
            method=self._method,
            status=response.last_smtp_code,
            status_message=self.smtp_reply_text(response.last_smtp_message, response.encoding),
            error=self.describe_error(response.error),
            timings=self.copy_timings(response.timings),
            elapsed=self.measure_elapsed(response.timings),
        )

    def from_error(self, error: Exception) -> PingResult:
        """The result of a request that raised instead of returning a
        response: the error alone."""
        return PingResult(
            protocol=self._protocol,
            url=self._url,
            method=self._method,
            error=self.describe_error(error),
        )

    def describe_files(self, transferred: Mapping[bytes, TransferResult] | None) -> list[PingResultFile] | None:
        """One ``PingResultFile`` per transferred path, null when the
        response transferred nothing."""
        if transferred is None:
            return None
        return [self.describe_file(transfer_result) for transfer_result in transferred.values()]

    def describe_file(self, transfer_result: TransferResult) -> PingResultFile:
        """A transferred path, decoded with the command's path encoding;
        undecodable bytes are written as backslash escapes."""
        content, content_encoding = self.encode_content(transfer_result.file_data)
        return PingResultFile(
            path=transfer_result.file_path.decode(self._path_encoding, errors="backslashreplace"),
            file_type=transfer_result.file_type,
            content=content,
            content_encoding=content_encoding,
        )

    def _http_result(
        self,
        response: HTTPResponse | HTTP2Response | HTTP3Response,
        trailers: Mapping[FieldText, FieldText] | None,
    ) -> PingResult:
        content, content_encoding = self.encode_content(response.content)
        return PingResult(
            protocol=self._protocol,
            url=self._url,
            method=self._method,
            status=response.status,
            status_message=response.status_message,
            headers=self.decode_fields(response.headers),
            trailers=self.decode_fields(trailers),
            redirects=response.redirects,
            timings=self.copy_timings(response.timings),
            elapsed=self.measure_elapsed(response.timings),
            content=content,
            content_encoding=content_encoding,
        )

    @staticmethod
    def encode_content(content: ContentBytes | None) -> tuple[str | None, ContentEncoding | None]:
        """Body bytes as JSON text: the text itself when they decode as
        UTF-8, else base64."""
        if content is None:
            return None, None
        try:
            return bytes(content).decode("utf-8"), "utf-8"
        except UnicodeDecodeError:
            return base64.b64encode(content).decode("ascii"), "base64"

    @classmethod
    def decode_fields(cls, fields: Mapping[FieldText, FieldText] | None) -> dict[str, str] | None:
        """Header or trailer fields as text, null when the response has
        none."""
        if fields is None:
            return None
        return {cls.field_text(name): cls.field_text(value) for name, value in fields.items()}

    @staticmethod
    def field_text(field: FieldText) -> str:
        """A field name or value as text. Raw octets decode as ISO-8859-1,
        which maps every byte to one character and so loses nothing: RFC
        9110 section 5.5 has recipients treat octets outside US-ASCII as
        opaque data."""
        return field.decode("latin-1") if isinstance(field, (bytes, bytearray)) else str(field)

    @staticmethod
    def smtp_reply_text(message: str | bytes | None, encoding: str | None) -> str | None:
        """The SMTP reply text, decoded with the response's encoding (UTF-8
        when it names none); undecodable bytes are written as backslash
        escapes."""
        if isinstance(message, bytes):
            return message.decode(encoding or "utf-8", errors="backslashreplace")
        return message

    @staticmethod
    def describe_error(error: Exception | str | None) -> str | None:
        """An error as text; one with no message is named by its type."""
        if error is None:
            return None
        return str(error) or type(error).__name__

    @staticmethod
    def copy_timings(timings: Mapping[str, float | None] | None) -> dict[str, float | None] | None:
        """The response's timings, copied so the result owns them."""
        return None if timings is None else dict(timings)

    @staticmethod
    def measure_elapsed(timings: Mapping[str, float | None] | None) -> float | None:
        """``request_end - request_start``, null unless both are recorded."""
        recorded_timings = timings or {}
        request_start = recorded_timings.get("request_start")
        request_end = recorded_timings.get("request_end")
        if None in (request_start, request_end):
            return None
        return request_end - request_start
