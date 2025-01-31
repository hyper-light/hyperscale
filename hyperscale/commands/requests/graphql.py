from pydantic import BaseModel, StrictStr
from typing import Dict, Any

class GraphQLQuery(BaseModel):
    query: StrictStr
    operation_name: StrictStr | None = None
    variables: Dict[StrictStr, Any] | None = None


from typing import Literal, Any
from hyperscale.core.engines.client.setup_clients import setup_client
from hyperscale.core.engines.client.graphql import MercurySyncGraphQLConnection
from hyperscale.core.engines.client.shared.timeouts import Timeouts
from hyperscale.core.engines.client.shared.models import HTTPCookie


async def make_graphql_request(
    url: str,
    cookies: list[HTTPCookie],
    params: dict[str, str],
    headers: dict[str, Any],
    method: Literal[
        "query",
        "mutation",
    ],
    data: (
        Dict[
            Literal["query"], 
            str,
        ]
        | Dict[
            Literal["query", "operation_name"],
            str,
        ]
        | Dict[
            Literal["query", "operation_name", "variables"], 
            str | dict[str, Any],
        ]
    ),
    redirects: int, 
    timeout: int | float,
    output_file: str | None = None,
    wait: bool = False,
    quiet:bool= False,
):
    
    graphql_data = GraphQLQuery(**data)
    
    timeouts = Timeouts(request_timeout=timeout)
    graphql = MercurySyncGraphQLConnection(
        timeouts=timeouts,
    )

    graphql = setup_client(graphql, 1)

    match method:
        case "query":
            return await graphql.query(
                url,
                headers=headers,
                cookies=cookies,
                query=graphql_data.query,
                timeout=timeout,
                redirects=redirects,
            )
        
        case "mutation":
            return await graphql.mutate(
                url,
                params=params,
                headers=headers,
                cookies=cookies,
                mutation=graphql_data.model_dump(),
                timeout=timeout,
                redirects=redirects,
            )
        
        case _:
            return await graphql.query(
                url,
                headers=headers,
                cookies=cookies,
                query=graphql_data.query,
                timeout=timeout,
                redirects=redirects,
            )

