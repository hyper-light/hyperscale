
from __future__ import annotations
import time
from ipaddress import ip_address, IPv4Address
from hyperscale.metadata import __name__, __version__
from typing import Optional, Dict, Union
from .tracing_types import (
    RequestHook,
    ResponseHook,
    TraceSignal,
    UrlFilter
)
from .url_filters import default_params_strip_filter
from .utils import (
    SUPPRESS_INSTRUMENTATION_KEY_PLAIN,
    http_status_to_status_code,
    remove_url_credentials,
)

try:
    from opentelemetry import context as context_api
    from opentelemetry import trace
    from opentelemetry.propagate import inject
    from opentelemetry.semconv.trace import SpanAttributes
    from opentelemetry.trace import SpanKind,  get_tracer, Span
    from opentelemetry.trace.status import Status, StatusCode

    opentelemetry_enabled = True

except Exception:

    opentelemetry_enabled = False

    class context_api:
        pass

    class trace:
        pass

    def inject(*args, **kwargs):
        pass

    class SpanAttributes:
        pass

    class SpanKind:
        pass

    class Span:
        pass

    def get_tracer(*args, **kwargs):
        pass

    class Span:
        pass

    class Status:
        pass

    class StatusCode:
        pass


OpenTelemetryTracingConfig = UrlFilter | RequestHook | ResponseHook | TraceSignal

class HTTPTrace:
    enabled = opentelemetry_enabled
    """First-class used to trace requests launched via ClientSession objects."""

    def __init__(
        self, 
        protocol: str,
        max_connections: int,
        ssl_version: str | None = None,
        url_filter: Optional[UrlFilter]=None,
        request_hook: Optional[RequestHook]=None,
        response_hook: Optional[ResponseHook]=None,
    ) -> None:
        self.protocol = protocol
        self.max_connections = max_connections
        self.ssl_version = ssl_version

        if url_filter is None:
            url_filter = default_params_strip_filter

        self.url_filter = url_filter

        self.request_hook: Union[RequestHook, None] = request_hook
        self.response_hook: Union[RequestHook, None] = response_hook

        self.tracer = get_tracer(__name__, __version__, None)
        self.token: object = None
        self._context_active = False

    async def on_request_queued_start(
        self,
        span: Span
    ):
        attributes = {
            'http.max_connections': self.max_connections,
        }

        span.add_event(
            'http.request_queued_start',
            attributes=attributes,
            timestamp=int(time.monotonic())
        )

        return span

    async def on_request_queued_end(
        self,
        span: Span
    ):
        attributes = {
            'http.max_connections': self.max_connections,
        }

        span.add_event(
            'http.request_queued_end',
            attributes=attributes,
            timestamp=int(time.monotonic())
        )

        return span

    async def on_connection_create_start(
        self,
        span: Span,
        connection_url: str,
        ssl_upgrade_url: str | None = None,
    ):
        span.add_event(
            'http.connection_create_start',
            attributes={
                'http.connection_url': connection_url,
                'http.ssl_upgrade_url': ssl_upgrade_url,
                'http.ssl_version': self.ssl_version,
            },
            timestamp=int(time.monotonic())
        )

        return span

    async def on_connection_create_end(
        self,
        span: Span,
        address: str,
        port: int,
    ):
        ip_version = 'ipv4'
        if not isinstance(ip_address(address), IPv4Address):
            ip_version = 'ipv6'
        
        attributes = {
            'http.protocol': self.protocol,
            'http.ip_address': address,
            'http.ip_version': ip_version,
            'http.port': port,
            'http.ssl_version': self.ssl_version
            
        }

        span.add_event(
            'http.connection_create_end',
            attributes=attributes,
            timestamp=int(time.monotonic())
        )

        return span

    async def on_dns_cache_hit(
        self,
        span: Span,
        address: str,
        port: int,
    ):  
        ip_version = 'ipv4'
        if not isinstance(ip_address(address), IPv4Address):
            ip_version = 'ipv6'
        
        attributes = {
            'http.protocol': self.protocol,
            'http.ip_address': address,
            'http.ip_version': ip_version,
            'http.port': port,
            'http.ssl_version': self.ssl_version
            
        }

        span.add_event(
            'http.dns_cache_hit',
            attributes=attributes,
            timestamp=int(time.monotonic())
        )

        return span

    async def on_dns_cache_miss(
        self,
        span: Span
    ):
        attributes = {
            'http.ssl_version': self.ssl_version,
        }

        span.add_event(
            'http.dns_cache_miss',
            attributes=attributes,
            timestamp=int(time.monotonic())
        )

        return span

    async def on_dns_resolve_host_start(
        self,
        span: Span,
    ):
        attributes = {
            'http.ssl_version': self.ssl_version,
        }

        span.add_event(
            'http.dns_resolve_host_start',
            attributes=attributes,
            timestamp=int(time.monotonic())
        )

        return span

    async def on_dns_resolve_host_end(
        self,
        span: Span,
        addresses: list[str],
        port: int,
    ):  
        attributes = {
            'http.protocol': self.protocol,
            'http.ip_address': addresses,
            'http.port': port,
            'http.ssl_version': self.ssl_version,
            
        }

        span.add_event(
            'http.dns_resolve_host_end',
            attributes=attributes,
            timestamp=int(time.monotonic())
        )

        return span

    async def on_connection_reuse(
        self,
        span: Span,
        address: str,
        port: int,
    ):
        ip_version = 'ipv4'
        if not isinstance(ip_address(address), IPv4Address):
            ip_version = 'ipv6'
        
        attributes = {
            'http.protocol': self.protocol,
            'http.ip_address': address,
            'http.ip_version': ip_version,
            'http.port': port,
            'http.ssl_version': self.ssl_version,
            
        }

        span.add_event(
            'http.connection_reuse',
            attributes=attributes,
            timestamp=int(time.monotonic())
        )

        return span

    async def on_request_headers_sent(
        self,
        span: Span,
        encoded_headers: bytes,
    ):
        span.add_event(
            'http.request_headers_sent',
            attributes={
                'http.bytes_sent': len(encoded_headers)
            },
            timestamp=int(time.monotonic())
        )

        return span

    async def on_request_data_sent(
        self,
        span: Span,
        encoded_data: bytes,
    ):
        attributes = {
            'http.bytes_sent': len(encoded_data),
        }

        span.add_event(
            'http.request_data_sent',
            attributes=attributes,
            timestamp=int(time.monotonic())
        )

        return span

    async def on_request_chunk_sent(
        self,
        span: Span,
        chunk_data: bytes,
    ):
        attributes = {
            'http.bytes_sent': len(chunk_data),
        }

        span.add_event(
            'http.request_chunk_sent',
            attributes=attributes,
            timestamp=int(time.monotonic())
        )

        return span

    async def on_response_header_line_received(
        self,
        span: Span,
        header_line: bytes,
    ):
        span.add_event(
            'http.response_header_line_received',
            attributes={
                'http.header_line': str(header_line)
            },
            timestamp=int(time.monotonic())
        )

        return span

    async def on_response_headers_received(
        self,
        span: Span,
        response_headers: dict[bytes, bytes],
    ):
        span.add_event(
            'http.response_headers_received',
            attributes={
                f'http.{key.decode()}': value.decode() for key, value in response_headers.items()
            },
            timestamp=int(time.monotonic())
        )

        return span

    async def on_response_data_received(
        self,
        span: Span,
        data: bytes | int,
    ):
        span.add_event(
            'http.response_headers_received',
            attributes={
                'http.bytes_received': len(bytes(data))
            },
            timestamp=int(time.monotonic())
        )

        return span

    async def on_response_chunk_received(
        self,
        span: Span,
        data: bytes,
    ):
        span.add_event(
            'http.response_chunk_received',
            attributes={
                'http.bytes_received': len(data)
            },
            timestamp=int(time.monotonic())
        )

        return span

    async def on_request_redirect(
        self,
        span: Span,
        redirect_url: str,
        redirects_taken: int,
        max_redirects: int,
        is_ssl_upgrade: bool = False
    ):
        span.add_event(
            'http.request_redirect',
            attributes={
                'http.redirect_url': redirect_url,
                'http.redirects_taken': redirects_taken,
                'http.max_redirects': max_redirects,
                'http.is_ssl_upgrade': is_ssl_upgrade,
            },
            timestamp=int(time.monotonic())
        )

        return span

    async def on_request_start(
        self,
        url: str,
        method: str,
        headers: Dict[str, str] | None = None,
    ):
        span: Span | None = None
        if context_api.get_value(SUPPRESS_INSTRUMENTATION_KEY_PLAIN):
            return span

        span_name = f"{self.protocol} {method}"

        request_url = (
            remove_url_credentials(self.url_filter(url))
            if self.url_filter
            else remove_url_credentials(url)
        )

        span_attributes = {
            SpanAttributes.HTTP_METHOD: method,
            SpanAttributes.HTTP_URL: request_url,
        }

        span: Span = self.tracer.start_span(
            span_name, 
            kind=SpanKind.CLIENT, 
            attributes=span_attributes,
        )

        if self.request_hook:
            self.request_hook(
                span,
                url,
                method=method,
                headers=headers,
            )

        self.token = context_api.attach(
            trace.set_span_in_context(span)
        )

        self._context_active = True

        inject(headers)

        return span

    async def on_request_end(
        self,
        span: Span,
        url: str,
        method: str,
        status: int,
        status_message: str | None = None,
        headers: Dict[str, str] | None = None,
    ):
        if self.response_hook:
            self.request_hook(
                span,
                url,
                method=method,
                headers=headers,
                status=status,
            )

        if status:
            span.set_status(
                Status(
                    http_status_to_status_code(status),
                    description=status_message,
                )
            )

            span.set_attribute(
                SpanAttributes.HTTP_STATUS_CODE, status
            )

        return self._end_trace(span)

    async def on_request_exception(
        self,
        span: Span,
        url: str,
        method: str,
        error: Exception,
        status: int | None = None,
        headers: Dict[str, str] | None = None,
    ):
        exception_message = str(error)

        if status:
            span.set_status(
                Status(
                    StatusCode.ERROR,
                    description=exception_message
                )
            )

        span.record_exception(exception_message)

        if self.response_hook:
            self.request_hook(
                span,
                url,
                method=method,
                headers=headers,
                status=status,
                error=error,
            )

        return self._end_trace(span)
    
    def _end_trace(
        self,
        span: Span,
    ):
        # context = context_api.get_current()

        if self._context_active:
            context_api.detach(self.token)
            self._context_active = False

            span.end()

        return span
