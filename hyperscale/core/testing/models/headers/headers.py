from typing import (
    Any,
    Dict,
    Generator,
    Generic,
    Iterable,
    List,
    Optional,
    Tuple,
    TypeVar,
)

from hyperscale.core.engines.client.shared.models import RequestType
from hyperscale.core.engines.client.shared.protocols import NEW_LINE
from hyperscale.core.testing.models.base import FrozenDict, OptimizedArg
from hyperscale.core.testing.models.base.base_types import (
    HTTPEncodableValue,
)

from .constants import WEBSOCKETS_VERSION
from .headers_validator import HeaderValidator
from .utils import create_sec_websocket_key

T = TypeVar("T")


class Headers(Generic[T], OptimizedArg):
    def __init__(
        self,
        headers: Dict[str, HTTPEncodableValue],
    ) -> None:
        super(
            OptimizedArg,
            self,
        ).__init__()

        validated_headers = HeaderValidator(value=headers)

        self.call_name: Optional[str] = None
        self.data: Dict[str, HTTPEncodableValue] = FrozenDict(validated_headers.value)
        self.optimized: Optional[str | List[str] | List[Tuple[bytes, bytes]]] = None

    async def optimize(self, request_type: RequestType):
        if self.optimized is not None:
            return

        match request_type:
            case RequestType.HTTP | RequestType.GRAPHQL:
                header_items = {
                    "Keep-Alive": "timeout=60, max=100000",
                    "User-Agent": "hyperscale/client",
                    **self.data,
                }

                optimized: str = ""
                for key, value in header_items.items():
                    optimized += f"{key}: {value}{NEW_LINE}"

                self.optimized = optimized

            case RequestType.GRAPHQL_HTTP2 | RequestType.HTTP2 | RequestType.HTTP3:
                encoded_headers = [
                    (k.lower().encode(), v.encode())
                    for k, v in self.data.items()
                    if k.lower()
                    not in (
                        "host",
                        "transfer-encoding",
                    )
                ]

                self.optimized = encoded_headers

            case RequestType.WEBSOCKET:
                encoded_headers: List[str] = [
                    "Upgrade: websocket",
                    "Keep-Alive: timeout=60, max=100000",
                    "User-Agent: hyperscale/client",
                ]

                headers: Dict[str, HTTPEncodableValue] = {}

                for header_name, header_value in self.data.items():
                    header_name_lowered = header_name.lower()
                    headers[header_name_lowered] = header_value

                host = headers.get("host")
                if host:
                    encoded_headers.append(f"Host: {host}")

                origin = headers.get("origin")
                if not headers.get("suppress_origin") and origin:
                    encoded_headers.append(f"Origin: {origin}")

                key = create_sec_websocket_key()
                header = headers.get("header")
                if not header or "Sec-WebSocket-Key" not in header:
                    encoded_headers.append(f"Sec-WebSocket-Key: {key}")
                else:
                    key = headers.get("header", {}).get("Sec-WebSocket-Key")

                if not header or "Sec-WebSocket-Version" not in header:
                    encoded_headers.append(
                        f"Sec-WebSocket-Version: {WEBSOCKETS_VERSION}"
                    )

                connection = headers.get("connection")
                if not connection:
                    encoded_headers.append("Connection: Upgrade")
                else:
                    encoded_headers.append(connection)

                subprotocols = headers.get("subprotocols")
                if subprotocols:
                    encoded_headers.append(
                        "Sec-WebSocket-Protocol: %s" % ",".join(subprotocols)
                    )

                additional_headers: List[str] = []

                if len(self.data) > 0:
                    for key, value in self.data.items():
                        additional_headers.append(
                            f"{key}: {value}",
                        )

                encoded_headers.extend(
                    [
                        header
                        for header in additional_headers
                        if header not in encoded_headers
                    ]
                )

                self.optimized = encoded_headers

            case _:
                pass

    def __getitem__(self, key: str) -> HTTPEncodableValue:
        return self.data[key]

    def __iter__(self) -> Generator[str, Any, None]:
        for key in self.data:
            yield key

    def items(self) -> Iterable[Tuple[str, HTTPEncodableValue]]:
        return self.data.items()

    def keys(self) -> Iterable[str]:
        return self.data.keys()

    def values(self) -> Iterable[HTTPEncodableValue]:
        return self.data.values()

    def get(
        self,
        key: str,
        default: Optional[Any] = None,
    ) -> Optional[HTTPEncodableValue]:
        return self.data.get(key, default)
