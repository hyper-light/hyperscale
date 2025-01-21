from typing import Generic, Optional, TypeVar

from hyperscale.core.testing.models.base import OptimizedArg

from .query_validator import QueryValidator

T = TypeVar("T")


class Query(OptimizedArg, Generic[T]):
    def __init__(self, query: str) -> None:
        super(
            OptimizedArg,
            self,
        ).__init__()

        validated_query = QueryValidator(value=query)

        self.call_name: Optional[str] = None
        self.data = validated_query.value

        self.optimized: Optional[str] = None

    async def optimize(self):
        if self.optimized is not None:
            return

        query_string = "".join(
            self.data.replace(
                "query",
                "",
            ).split()
        )
        self.optimized = f"?query={{{query_string}}}"

    def __str__(self) -> str:
        return self.data

    def __repr__(self) -> str:
        return self.data
