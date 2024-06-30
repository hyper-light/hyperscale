from __future__ import annotations

from hyperscale.core.engines.types.common.types import RequestTypes
from hyperscale.core.engines.types.http.result import HTTPResult

from .action import GraphQLAction


class GraphQLResult(HTTPResult):
    def __init__(self, action: GraphQLAction, error: Exception = None) -> None:
        super().__init__(action, error)

        self.type = RequestTypes.GRAPHQL
