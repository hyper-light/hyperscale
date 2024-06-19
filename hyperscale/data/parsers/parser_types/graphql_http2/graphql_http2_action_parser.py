import json
import uuid
from typing import Any, Coroutine, Dict

from hyperscale.core.engines.client.config import Config
from hyperscale.core.engines.types.common.types import RequestTypes
from hyperscale.core.engines.types.graphql_http2 import (
    GraphQLHTTP2Action,
    MercuryGraphQLHTTP2Client,
)
from hyperscale.core.hooks.types.action.hook import ActionHook
from hyperscale.core.hooks.types.base.simple_context import SimpleContext
from hyperscale.data.parsers.parser_types.common.base_parser import BaseParser
from hyperscale.data.parsers.parser_types.common.parsing import (
    normalize_headers,
    parse_tags,
)

from .graphql_http2_action_validator import GraphQLHTTP2ActionValidator


class GraphQLHTTP2ActionParser(BaseParser):

    def __init__(
        self,
        config: Config,
        options: Dict[str, Any]={}
    ) -> None:
        super().__init__(
            GraphQLHTTP2ActionParser.__name__,
            config,
            RequestTypes.GRAPHQL_HTTP2,
            options

        )

    async def parse(
        self, 
        action_data: Dict[str, Any],
        stage: str
    ) -> Coroutine[Any, Any, Coroutine[Any, Any, ActionHook]]:
        
        normalized_headers = normalize_headers(action_data)
        tags_data = parse_tags(action_data)

        graphql_variables_data = action_data.get('variables')
        if isinstance(graphql_variables_data, (str, bytes, bytearray,)):
            graphql_variables_data = json.loads(graphql_variables_data)

        generator_action = GraphQLHTTP2ActionValidator(**{
            **action_data,
            'headers': normalized_headers,
            'variables': graphql_variables_data,
            'tags': tags_data
        })

        action = GraphQLHTTP2Action(
            generator_action.name,
            generator_action.url,
            method=generator_action.method,
            headers=generator_action.headers,
            data={
                'query': generator_action.query,
                'operation_name': generator_action.operation_name,
                'variables': generator_action.variables
            },
            user=generator_action.user,
            tags=[
                tag.dict() for tag in generator_action.tags
            ]
        )

        session = MercuryGraphQLHTTP2Client(
            concurrency=self.config.batch_size,
            timeouts=self.timeouts,
            reset_connections=self.config.reset_connections,
            tracing_session=self.config.tracing
        )

        await session.prepare(action)

        hook = ActionHook(
            f'{stage}.{generator_action.name}',
            generator_action.name,
            None,
            sourcefile=generator_action.sourcefile,
        )

        hook.session = session
        hook.action = action
        hook.stage = stage
        hook.context = SimpleContext()
        hook.hook_id = uuid.uuid4()
      
        hook.metadata.order = generator_action.order
        hook.metadata.weight = generator_action.weight
        hook.metadata.tags = generator_action.tags
        hook.metadata.user = generator_action.user

        return hook



