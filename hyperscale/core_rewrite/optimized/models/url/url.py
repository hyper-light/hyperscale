from typing import Generic, Optional, TypeVar

from hyperscale.core_rewrite.engines.client.shared.models import URL as OptimizedUrl
from hyperscale.core_rewrite.engines.client.shared.models import RequestType
from hyperscale.core_rewrite.engines.client.shared.protocols import ProtocolMap
from hyperscale.core_rewrite.optimized.models.base import OptimizedArg

from .url_validator import URLValidator

T = TypeVar("T")


class URL(OptimizedArg, Generic[T]):
    def __init__(
        self,
        url: str,
    ) -> None:
        super(
            URL,
            self,
        ).__init__()

        URLValidator(value=url)

        self.data = url
        self.optimized_params: Optional[str] = None
        self.optimized: Optional[OptimizedUrl] = None

    def __iter__(self):
        return self.data

    async def optimize(self, request_type: RequestType):
        protocols = ProtocolMap()

        address_family, protocol = protocols[request_type]

        self.optimized = OptimizedUrl(
            self.data,
            family=address_family,
            protocol=protocol,
        )
        await self.optimized.lookup()
