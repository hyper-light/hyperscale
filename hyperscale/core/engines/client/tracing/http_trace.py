
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


class HTTPTrace:
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
        self.span: Span = None
        self.token: object = None
        self._context_active = False
        self._span_name: str | None = None

    async def on_request_queued_start(self):
        if self.span is None or not self.span.is_recording():
            return
        
        attributes = {
            'max_connections': self.max_connections,
        }


        self.span.add_event(
            f'request_queued_start',
            attributes=attributes,
            timestamp=int(time.monotonic())
        )

    async def on_request_queued_end(self):
        if self.span is None or not self.span.is_recording():
            return
        
        attributes = {
            'max_connections': self.max_connections,
        }


        self.span.add_event(
            f'request_queued_end',
            attributes=attributes,
            timestamp=int(time.monotonic())
        )


    async def on_connection_create_start(
        self,
        connection_url: str,
        ssl_upgrade_url: str | None = None,
    ):
        if self.span is None or not self.span.is_recording():
            return

        self.span.add_event(
            f'connection_create_start',
            attributes={
                'connection_url': connection_url,
                'ssl_upgrade_url': ssl_upgrade_url,
                'ssl_version': self.ssl_version,
            },
            timestamp=int(time.monotonic())
        )

    async def on_connection_create_end(
        self,
        address: str,
        port: int,
    ):
        if self.span is None or not self.span.is_recording():
            return
        
        ip_version = 'ipv4'
        if not isinstance(ip_address(address), IPv4Address):
            ip_version = 'ipv6'
        
        attributes = {
            'protocol': self.protocol,
            'ip_address': address,
            'ip_version': ip_version,
            'port': port,
            'ssl_version': self.ssl_version
            
        }

        self.span.add_event(
            f'connection_create_end',
            attributes=attributes,
            timestamp=int(time.monotonic())
        )

    async def on_dns_cache_hit(
        self,
        address: str,
        port: int,
    ):
        if self.span is None or not self.span.is_recording():
            return
        
        ip_version = 'ipv4'
        if not isinstance(ip_address(address), IPv4Address):
            ip_version = 'ipv6'
        
        attributes = {
            'protocol': self.protocol,
            'ip_address': address,
            'ip_version': ip_version,
            'port': port,
            'ssl_version': self.ssl_version
            
        }

        self.span.add_event(
            f'dns_cache_hit',
            attributes=attributes,
            timestamp=int(time.monotonic())
        )

    async def on_dns_cache_miss(self):
        if self.span is None or not self.span.is_recording():
            return
        
        attributes = {
            'ssl_version': self.ssl_version,
        }

        self.span.add_event(
            f'dns_cache_miss',
            attributes=attributes,
            timestamp=int(time.monotonic())
        )

    async def on_dns_resolve_host_start(self):
        if self.span is None or not self.span.is_recording():
            return
        
        attributes = {
            'ssl_version': self.ssl_version,
        }

        self.span.add_event(
            f'dns_resolve_host_start',
            attributes=attributes,
            timestamp=int(time.monotonic())
        )

    async def on_dns_resolve_host_end(
        self,
        addresses: list[str],
        port: int,
    ):
        if self.span is None or not self.span.is_recording():
            return
        
        attributes = {
            'protocol': self.protocol,
            'ip_address': addresses,
            'port': port,
            'ssl_version': self.ssl_version,
            
        }

        self.span.add_event(
            f'dns_resolve_host_end',
            attributes=attributes,
            timestamp=int(time.monotonic())
        )

    async def on_connection_reuse(
        self,
        address: str,
        port: int,
    ):
        if self.span is None or not self.span.is_recording():
            return
        
        ip_version = 'ipv4'
        if not isinstance(ip_address(address), IPv4Address):
            ip_version = 'ipv6'
        
        attributes = {
            'protocol': self.protocol,
            'ip_address': address,
            'ip_version': ip_version,
            'port': port,
            'ssl_version': self.ssl_version,
            
        }

        self.span.add_event(
            f'connection_reuse',
            attributes=attributes,
            timestamp=int(time.monotonic())
        )

    async def on_request_headers_sent(
        self,
        encoded_headers: bytes,
    ):
        if self.span is None or not self.span.is_recording():
            return

        self.span.add_event(
            f'request_headers_sent',
            attributes={
                'bytes_sent': len(encoded_headers)
            },
            timestamp=int(time.monotonic())
        )

    async def on_request_data_sent(
        self,
        encoded_data: bytes,
    ):
        if self.span is None or not self.span.is_recording():
            return
        
        attributes = {
            'bytes_sent': len(encoded_data),
        }

        self.span.add_event(
            f'request_data_sent',
            attributes=attributes,
            timestamp=int(time.monotonic())
        )

    async def on_request_chunk_sent(
        self,
        chunk_data: bytes,
    ):
        if self.span is None or not self.span.is_recording():
            return
        
        attributes = {
            'bytes_sent': len(chunk_data),
        }

        self.span.add_event(
            f'request_chunk_sent',
            attributes=attributes,
            timestamp=int(time.monotonic())
        )

    async def on_response_header_line_received(
        self,
        header_line: bytes,
    ):
        if self.span is None or not self.span.is_recording():
            return

        self.span.add_event(
            f'response_headers_received',
            attributes={
                'header_line': str(header_line)
            },
            timestamp=int(time.monotonic())
        )

    async def on_response_headers_received(
        self,
        response_headers: dict[bytes, bytes],
    ):
        if self.span is None or not self.span.is_recording():
            return

        self.span.add_event(
            f'response_headers_received',
            attributes={
                key.decode(): value.decode() for key, value in response_headers.items()
            },
            timestamp=int(time.monotonic())
        )

    async def on_response_data_received(
        self,
        data: bytes | int,
    ):
        if self.span is None or not self.span.is_recording():
            return
        
        self.span.add_event(
            f'response_headers_received',
            attributes={
                'bytes_received': len(bytes(data))
            },
            timestamp=int(time.monotonic())
        )

    async def on_response_chunk_received(
        self,
        data: bytes,
    ):
        if self.span is None or not self.span.is_recording():
            return
        
        self.span.add_event(
            f'response_chunk_received',
            attributes={
                'bytes_received': len(data)
            },
            timestamp=int(time.monotonic())
        )

    async def on_request_redirect(
        self,
        redirect_url: str,
        redirects_taken: int,
        max_redirects: int,
        is_ssl_upgrade: bool = False
    ):
        if self.span is None or not self.span.is_recording():
            return
        
        self.span.add_event(
            f'request_redirect',
            attributes={
                'redirect_url': redirect_url,
                'redirects_taken': redirects_taken,
                'max_redirects': max_redirects,
                'is_ssl_upgrade': is_ssl_upgrade,
            },
            timestamp=int(time.monotonic())
        )
        

    async def on_request_start(
        self,
        url: str,
        method: str,
        headers: Dict[str, str] | None = None,
    ):
        
        if opentelemetry_enabled is False:
            return

        if context_api.get_value(SUPPRESS_INSTRUMENTATION_KEY_PLAIN):
            self.span = None
            return

        self._span_name = f"{self.protocol} {method}"

        request_url = (
            remove_url_credentials(self.url_filter(url))
            if self.url_filter
            else remove_url_credentials(url)
        )

        span_attributes = {
            SpanAttributes.HTTP_METHOD: method,
            SpanAttributes.HTTP_URL: request_url,
        }

        self.span: Span = self.tracer.start_span(
            self._span_name, 
            kind=SpanKind.CLIENT, 
            attributes=span_attributes,
        )

        if self.request_hook:
            self.request_hook(
                self.span,
                url,
                method=method,
                headers=headers,
            )

        self.token = context_api.attach(
            trace.set_span_in_context(self.span)
        )

        self._context_active = True

        inject(headers)

    async def on_request_end(
        self,
        url: str,
        method: str,
        status: int,
        status_message: str | None = None,
        headers: Dict[str, str] | None = None,
    ):
        if self.span is None or not self.span.is_recording():
            return

        if self.response_hook:
            self.request_hook(
                self.span,
                url,
                method=method,
                headers=headers,
                status=status,
            )

        if self.span.is_recording() and status:
            self.span.set_status(
                Status(
                    http_status_to_status_code(status),
                    description=status_message,
                )
            )

            self.span.set_attribute(
                SpanAttributes.HTTP_STATUS_CODE, status
            )

        self._end_trace()

    async def on_request_exception(
        self,
        url: str,
        method: str,
        error: Exception,
        status: int | None = None,
        headers: Dict[str, str] | None = None,
    ) -> None:
        if self.span is None or not self.span.is_recording():
            return

        exception_message = str(error)

        if status:
            self.span.set_status(
                Status(
                    StatusCode.ERROR,
                    description=exception_message
                )
            )

        self.span.record_exception(exception_message)

        if self.response_hook:
            self.request_hook(
                self.span,
                url,
                method=method,
                headers=headers,
                status=status,
                error=error,
            )

        self._end_trace()
    
    def _end_trace(self) -> None:
        # context = context_api.get_current()

        if self._context_active:
            context_api.detach(self.token)
            self._context_active = False

            self.span.end()
