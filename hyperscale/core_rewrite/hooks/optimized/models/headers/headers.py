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

from hyperscale.core_rewrite.hooks.optimized.models.base import FrozenDict, OptimizedArg
from hyperscale.core_rewrite.hooks.optimized.models.base.base_types import (
    HTTPEncodableValue,
)

from .headers_validator import HeaderValidator

T = TypeVar("T")


class Headers(OptimizedArg, Generic[T]):
    def __init__(
        self,
        headers: Dict[str, HTTPEncodableValue],
    ) -> None:
        super(
            OptimizedArg,
            self,
        ).__init__()

        validated_headers = HeaderValidator(value=headers)
        self.data: FrozenDict = FrozenDict(validated_headers.value)
        self.optimized: Optional[str] = None

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
