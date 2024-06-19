from hyperscale.core.graphs.stages import (
    Execute,
)
from hyperscale.core.hooks import action


class ExecuteGraphQLStage(Execute):

    @action()
    async def http_query(self):
        return await self.client.graphql.query(
            """
            query <query_name> {
                ...
            }
            """
        )