from typing import Generic, Optional, TypeVar
from urllib.parse import urlparse

from hyperscale.core_rewrite.engines.client.shared.models import URL as OptimizedUrl
from hyperscale.core_rewrite.hooks.optimized.models.base import OptimizedArg

from .url_validator import URLValidator

T = TypeVar("T")


class URL(OptimizedArg, Generic[T]):
    def __init__(self, url: str) -> None:
        super(
            URL,
            self,
        ).__init__()

        URLValidator(value=url)

        self.data = url
        self.parsed = urlparse(url)
        self.optimized: Optional[OptimizedUrl] = None

    def __str__(self) -> str:
        return self.data

    def __repr__(self) -> str:
        return self.data
