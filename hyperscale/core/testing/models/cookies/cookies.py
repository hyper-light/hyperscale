from typing import (
    Generic,
    List,
    Optional,
    Tuple,
    TypeVar,
)

from hyperscale.core.engines.client.shared.models import RequestType
from hyperscale.core.engines.client.shared.protocols import NEW_LINE
from hyperscale.core.testing.models.base import OptimizedArg

from .cookies_types import HTTPCookie
from .cookies_validator import CookiesValidator

T = TypeVar("T")


class Cookies(OptimizedArg, Generic[T]):
    def __init__(self, cookies: List[HTTPCookie]) -> None:
        super(
            Cookies,
            self,
        ).__init__()

        validated_cookies = CookiesValidator(value=cookies)

        self.call_name: Optional[str] = None
        self.data = validated_cookies.value
        self.optimized: Optional[str | Tuple[str, str]] = None

    async def optimize(self, request_type: RequestType):
        if self.optimized is not None:
            return

        if request_type == RequestType.UDP:
            return

        cookies = []

        for cookie_data in self.data:
            if len(cookie_data) == 1:
                cookies.append(cookie_data[0])

            elif len(cookie_data) == 2:
                cookie_name, cookie_value = cookie_data
                cookies.append(f"{cookie_name}={cookie_value}")

        encoded = "; ".join(cookies)
        match request_type:
            case RequestType.GRAPHQL | RequestType.HTTP | RequestType.WEBSOCKET:
                self.optimized = f"cookie: {encoded}{NEW_LINE}"

            case RequestType.GRAPHQL_HTTP2 | RequestType.HTTP2 | RequestType.HTTP3:
                self.optimized = ("cookie", encoded)

            case _:
                pass

    def __iter__(self):
        for cookie in self.data:
            yield cookie
