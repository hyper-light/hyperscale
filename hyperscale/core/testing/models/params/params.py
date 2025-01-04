from typing import (
    Any,
    Dict,
    Generator,
    Generic,
    Iterable,
    Optional,
    Tuple,
    TypeVar,
)
from urllib.parse import urlencode

from hyperscale.core.engines.client.shared.models import RequestType
from hyperscale.core.testing.models.base import FrozenDict, OptimizedArg
from hyperscale.core.testing.models.base.base_types import (
    HTTPEncodableValue,
)

from .params_validator import ParamsValidator

T = TypeVar("T")


class Params(OptimizedArg, Generic[T]):
    def __init__(
        self,
        params: Dict[str, HTTPEncodableValue],
    ) -> None:
        super(
            OptimizedArg,
            self,
        ).__init__()

        validated_params = ParamsValidator(value=params)

        self.call_name: Optional[str] = None
        self.data: FrozenDict = FrozenDict(validated_params.value)
        self.optimized: Optional[str] = None
        self._params_types = [
            RequestType.HTTP,
            RequestType.HTTP2,
            RequestType.HTTP3,
            RequestType.WEBSOCKET,
        ]

    async def optimize(self, request_type: RequestType):
        if self.optimized is not None:
            return

        if request_type in self._params_types:
            url_params = urlencode(self.data)
            self.optimized = f"?{url_params}"

    def __getitem__(self, key: str) -> HTTPEncodableValue:
        return self.data[key]

    def __iter__(self) -> Generator[str, Any, None]:
        for key in self.data:
            yield key

    def items(self) -> Iterable[Tuple[str, HTTPEncodableValue],]:
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
