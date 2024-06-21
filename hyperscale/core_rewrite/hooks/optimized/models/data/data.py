from typing import Generic, Optional, TypeVar

from hyperscale.core_rewrite.hooks.optimized.models.base import OptimizedArg

from .data_types import DataValue, OptimizedData
from .data_validator import DataValidator

T = TypeVar("T")


class Data(OptimizedArg, Generic[T]):
    def __init__(self, data: DataValue) -> None:
        super(
            Data,
            self,
        ).__init__()

        validated_data = DataValidator(data)
        self.data = validated_data.value
        self.optimized: Optional[OptimizedData] = None
