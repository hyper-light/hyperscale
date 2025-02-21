import asyncio
from pydantic import BaseModel, StrictStr
from typing import Dict, Any, Literal
from hyperscale.core.engines.client.setup_clients import setup_client
from hyperscale.core.engines.client.graphql import MercurySyncGraphQLConnection
from hyperscale.core.engines.client.shared.timeouts import Timeouts
from hyperscale.core.engines.client.shared.models import HTTPCookie
from .terminal_ui import (
    update_status,
    update_cookies,
    update_elapsed,
    update_headers,
    update_params,
    update_redirects,
    update_text,
    create_ping_ui,
    map_status_to_error,
)



class GraphQLQuery(BaseModel):
    query: StrictStr
    operation_name: StrictStr | None = None
    variables: Dict[StrictStr, Any] | None = None


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
    terminal = create_ping_ui(
        url,
        method,
    )

    try:

        if quiet is False:
            await terminal.render(
                horizontal_padding=4,
                vertical_padding=1
            )


        match method:
            case "query":
                response = await graphql.query(
                    url,
                    headers=headers,
                    cookies=cookies,
                    query=graphql_data.query,
                    timeout=timeout,
                    redirects=redirects,
                )
            
            case "mutation":
                response = await graphql.mutate(
                    url,
                    params=params,
                    headers=headers,
                    cookies=cookies,
                    mutation=graphql_data.model_dump(),
                    timeout=timeout,
                    redirects=redirects,
                )
            
            case _:
                response = await graphql.query(
                    url,
                    headers=headers,
                    cookies=cookies,
                    query=graphql_data.query,
                    timeout=timeout,
                    redirects=redirects,
                )

        if quiet is False:
            response_text = response.reason
            response_status = response.status

            if response_text is None and response.status_message:
                response_text = response.status_message

            elif response_text is None and response_status >= 200 and response_status < 300:
                response_text = "OK!"

            elif response_text is None and response_status:
                response_text = map_status_to_error(response_status)

            response_end = response.timings.get('request_end', 0)
            if response_end is None:
                response_end = 0

            response_start = response.timings.get('request_start', 0)
            if response_start is None:
                response_start = 0

            elapsed = response_end - response_start
            if elapsed < 0:
                elapsed = 0
                response_text = "Encountered unknown error."
                
            updates = [
                update_redirects(response.redirects),
                update_status(response.status),
                update_headers(response.headers),
                update_text(response_text),
                update_elapsed(elapsed),
                update_params([], {
                    "query": data.get("query"),
                    "operation_name": data.get("operation_name", "None")
                })
            ]

            if cookies := response.cookies:
                updates.append(
                    update_cookies(cookies)
                )
            
            await asyncio.sleep(0.5)
            await asyncio.gather(*updates)

            if wait:
                loop = asyncio.get_event_loop()

                await loop.create_future()

            await asyncio.sleep(0.5)
            await terminal.stop()

    except (
        KeyboardInterrupt,
        asyncio.CancelledError,
    ):
        if quiet is False:
            await update_text("Aborted")
            await terminal.stop()

    except Exception as err:
        error_message = str(err)
        if str(err) == "":
            error_message = "Encountered unknown error"

        if quiet is False:
            await update_text(error_message)
            await terminal.stop()
