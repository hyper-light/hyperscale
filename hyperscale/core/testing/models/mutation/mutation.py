from typing import (
    Any,
    Dict,
    Generator,
    Generic,
    Iterable,
    Literal,
    Optional,
    Tuple,
    TypeVar,
)

import orjson

from hyperscale.core.testing.models.base import OptimizedArg
from hyperscale.core.testing.models.base.base_types import (
    HTTPEncodableValue,
)

from .mutation_validator import MutationValidator

T = TypeVar("T")


class Mutation(OptimizedArg, Generic[T]):
    def __init__(
        self,
        mutation: Dict[
            Literal[
                "query",
                "operation_name",
                "variables",
            ],
            str | Dict[str, HTTPEncodableValue],
        ],
    ) -> None:
        super(
            Mutation,
            self,
        ).__init__()

        validated_mutation = MutationValidator(**mutation)

        self.call_name: Optional[str] = None
        self.data: Dict[
            Literal[
                "query",
                "operation_name",
                "variables",
            ],
            str | Dict[str, HTTPEncodableValue],
        ] = validated_mutation.model_dump()

        self.optimized: Optional[bytes] = None
        self.content_length: Optional[int] = None
        self.content_type = "application/graphql-response+json"

    async def optimize(self):
        if self.optimized is not None:
            return

        self.optimized = orjson.dumps(self.data)
        self.content_length = len(self.optimized)

    def __getitem__(
        self,
        key: Literal[
            "query",
            "operation_name",
            "variables",
        ],
    ) -> str | Dict[str, HTTPEncodableValue]:
        return self.data[key]

    def __iter__(
        self,
    ) -> Generator[
        Literal[
            "query",
            "operation_name",
            "variables",
        ],
        Any,
        None,
    ]:
        for key in self.data:
            yield key

    def items(
        self,
    ) -> Iterable[
        Tuple[
            Literal[
                "query",
                "operation_name",
                "variables",
            ],
            str | Dict[str, HTTPEncodableValue],
        ]
    ]:
        return self.data.items()

    def keys(
        self,
    ) -> Iterable[
        Literal[
            "query",
            "operation_name",
            "variables",
        ],
    ]:
        return self.data.keys()

    def values(self) -> Iterable[str | Dict[str, HTTPEncodableValue]]:
        return self.data.values()

    def get(
        self,
        key: Literal[
            "query",
            "operation_name",
            "variables",
        ],
        default: Optional[Any] = None,
    ) -> Optional[str | Dict[str, HTTPEncodableValue] | Any]:
        return self.data.get(key, default)
