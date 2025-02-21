
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


try:
    from opentelemetry import context as context_api
    from opentelemetry import trace
    from opentelemetry.trace import Tracer
    from opentelemetry.propagate import inject
    from opentelemetry.semconv.trace import SpanAttributes
    from opentelemetry.trace import SpanKind,  get_tracer, Span
    from opentelemetry.trace.status import Status, StatusCode
    from .utils import (
        SUPPRESS_INSTRUMENTATION_KEY_PLAIN,
        http_status_to_status_code,
        remove_url_credentials,
    )

    opentelemetry_enabled = True

except Exception:

    opentelemetry_enabled = False

    class context_api:
        pass

    class trace:
        pass

    class Tracer:
        pass

    def http_status_to_status_code(*args, **kwargs):
        pass

    def inject(*args, **kwargs):
        pass

    class SpanAttribuates:
        pass

    class SpanKind:
        pass

    def get_tracer(*args, **kwargs):
        return Tracer()

    class Span:
        pass

    class Status:
        pass

    class StatusCode:
        pass

    def remove_url_credentials(url: str):
        return url


OpenTelemetryTracingConfig = UrlFilter | RequestHook | ResponseHook | TraceSignal


class HTTP2Trace:
    enabled = opentelemetry_enabled
    """First-class used to trace requests launched via ClientSession objects."""

    __slots__ = (
        'protocol',
        'max_connections',
        'ssl_version',
        'tracer',
        'span',
        'token',
        'allowed_traces',
        'url_filter',
        'request_hook',
        'response_hook',
        '_context_active'
    )

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
            'http2.max_connections': self.max_connections,
        }


        span.add_event(
            'http2.request_queued_start',
            attributes=attributes,
            timestamp=int(time.monotonic())
        )

        return span

    async def on_request_queued_end(
        self,
        span: Span
    ):
        attributes = {
            'http2.max_connections': self.max_connections,
        }


        span.add_event(
            'http2.request_queued_end',
            attributes=attributes,
            timestamp=int(time.monotonic())
        )

        return span
    
    async def on_preamble_sent(
        self,
        span: Span
    ):
        span.add_event(
            'http2.preamble_sent',
            attributes={
                'http2.protocol': self.protocol,
            }
        )

    async def on_connection_create_start(
        self,
        span: Span,
        connection_url: str,
        ssl_upgrade_url: str | None = None,
    ):
        span.add_event(
            'http2.connection_create_start',
            attributes={
                'http2.connection_url': connection_url,
                'http2.ssl_upgrade_url': ssl_upgrade_url,
                'http2.ssl_version': self.ssl_version,
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
            'http2.protocol': self.protocol,
            'http2.ip_address': address,
            'http2.ip_version': ip_version,
            'http2.port': port,
            'http2.ssl_version': self.ssl_version
            
        }

        span.add_event(
            'http2.connection_create_end',
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
            'http2.protocol': self.protocol,
            'http2.ip_address': address,
            'http2.ip_version': ip_version,
            'http2.port': port,
            'http2.ssl_version': self.ssl_version
            
        }

        span.add_event(
            'http2.dns_cache_hit',
            attributes=attributes,
            timestamp=int(time.monotonic())
        )

        return span

    async def on_dns_cache_miss(
        self,
        span: Span
    ):
        attributes = {
            'http2.ssl_version': self.ssl_version,
        }

        span.add_event(
            'http2.dns_cache_miss',
            attributes=attributes,
            timestamp=int(time.monotonic())
        )

        return span

    async def on_dns_resolve_host_start(
        self,
        span: Span,
    ):
        attributes = {
            'http2.ssl_version': self.ssl_version,
        }

        span.add_event(
            'http2.dns_resolve_host_start',
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
            'http2.protocol': self.protocol,
            'http2.ip_address': addresses,
            'http2.port': port,
            'http2.ssl_version': self.ssl_version,
            
        }

        span.add_event(
            'http2.dns_resolve_host_end',
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
            'http2.protocol': self.protocol,
            'http2.ip_address': address,
            'http2.ip_version': ip_version,
            'http2.port': port,
            'http2.ssl_version': self.ssl_version,
            
        }

        span.add_event(
            'http2.connection_reuse',
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
            'http2.request_headers_sent',
            attributes={
                'http2.bytes_sent': len(encoded_headers)
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
            'http2.bytes_sent': len(encoded_data),
        }

        span.add_event(
            'http2.request_data_sent',
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
            'http2.bytes_sent': len(chunk_data),
        }

        span.add_event(
            'http2.request_chunk_sent',
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
            'http2.response_header_line_received',
            attributes={
                'http2.header_line': str(header_line)
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
            'http2.response_headers_received',
            attributes={
                f'http2.{key.decode()}': value.decode() for key, value in response_headers.items()
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
            'http2.response_headers_received',
            attributes={
                'http2.bytes_received': len(bytes(data))
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
            'http2.response_chunk_received',
            attributes={
                'http2.bytes_received': len(data)
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
            'http2.request_redirect',
            attributes={
                'http2.redirect_url': redirect_url,
                'http2.redirects_taken': redirects_taken,
                'http2.max_redirects': max_redirects,
                'http2.is_ssl_upgrade': is_ssl_upgrade,
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
            'http2.method': method,
            'http2.url': request_url,
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
                'http2.status_code', status
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
