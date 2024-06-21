from typing import (
    Generic,
    List,
    Optional,
    TypeVar,
)

from hyperscale.core_rewrite.hooks.optimized.models.base import OptimizedArg

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

        self.data = validated_cookies.value
        self.optimized: Optional[str] = None

    def __iter__(self):
        for cookie in self.data:
            yield cookie
