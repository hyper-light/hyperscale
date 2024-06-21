from typing import Generic, Optional, TypeVar

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
        self.optimized: Optional[OptimizedUrl] = None

    async def optimize(self):
        self.optimized = OptimizedUrl(self.data)
        await self.optimized.lookup()
