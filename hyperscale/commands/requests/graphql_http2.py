from pydantic import BaseModel, StrictStr
from typing import Dict, Any

class GraphQLQuery(BaseModel):
    query: StrictStr
    operation_name: StrictStr | None = None
    variables: Dict[StrictStr, Any] | None = None


from typing import Literal, Any
from hyperscale.core.engines.client.setup_clients import setup_client
from hyperscale.core.engines.client.graphql_http2 import MercurySyncGraphQLHTTP2Connection
from hyperscale.core.engines.client.shared.timeouts import Timeouts
from hyperscale.core.engines.client.shared.models import HTTPCookie


async def make_graphqlh2_request(
    url: str,
    cookies: list[HTTPCookie],
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
    graphqlh2 = MercurySyncGraphQLHTTP2Connection(
        timeouts=timeouts,
    )

    graphqlh2 = setup_client(graphqlh2, 1)

    match method:
        case "query":
            return await graphqlh2.query(
                url,
                query=graphql_data.query,
                headers=headers,
                cookies=cookies,
                timeout=timeout,
                redirects=redirects,
            )
        
        case "mutation":
            return await graphqlh2.mutate(
                url,
                mutation=graphql_data.model_dump(),
                headers=headers,
                cookies=cookies,
                timeout=timeout,
                redirects=redirects,
            )
        
        case _:
            return await graphqlh2.query(
                url,
                query=graphql_data.query,
                headers=headers,
                cookies=cookies,
                timeout=timeout,
                redirects=redirects,
            )

