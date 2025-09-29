from typing import Generic, Optional, TypeVar

from hyperscale.core.engines.client.shared.models import RequestType
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

    async def optimize(
        self,
        request_type: RequestType,
    ):
        if self.optimized is not None:
            return
        
        match request_type:
            case RequestType.GRAPHQL | RequestType.GRAPHQL_HTTP2:
                query_string = "".join(
                    self.data.replace(
                        "query",
                        "",
                    ).split()
                )
                self.optimized = f"?query={{{query_string}}}"

            case _:
                pass
            
    def __str__(self) -> str:
        return self.data

    def __repr__(self) -> str:
        return self.data
