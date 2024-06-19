from _collections_abc import (
    dict_items,
    dict_keys,
    dict_values,
)
from typing import (
    Any,
    Dict,
    Generator,
    Generic,
    Literal,
    Optional,
    TypeVar,
)

from hyperscale.core_rewrite.hooks.optimized.models.base import OptimizedArg

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
            str | Dict[str, Any],
        ],
    ) -> None:
        super(
            Mutation,
            self,
        ).__init__()

        validated_mutation = MutationValidator(**mutation)
        self.data: Dict[
            Literal[
                "query",
                "operation_name",
                "variables",
            ],
            str | Dict[str, Any],
        ] = validated_mutation.model_dump()

        self.optimized: Optional[bytes] = None

    def __getitem__(
        self,
        key: Literal[
            "query",
            "operation_name",
            "variables",
        ],
    ) -> str | Dict[str, Any]:
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
    ) -> dict_items[
        Literal[
            "query",
            "operation_name",
            "variables",
        ],
        str | Dict[str, Any],
    ]:
        return self.data.items()

    def keys(
        self,
    ) -> dict_keys[
        Literal[
            "query",
            "operation_name",
            "variables",
        ],
    ]:
        return self.data.keys()

    def values(self) -> dict_values[str | Dict[str, Any]]:
        return self.data.values()

    def get(
        self,
        key: Literal[
            "query",
            "operation_name",
            "variables",
        ],
        default: Optional[Any] = None,
    ) -> Optional[str | Dict[str, Any] | Any]:
        return self.data.get(key, default)
