from typing import Literal, Any
from hyperscale.core.engines.client.setup_clients import setup_client
from hyperscale.core.engines.client.websocket import MercurySyncWebsocketConnection
from hyperscale.core.engines.client.shared.timeouts import Timeouts
from hyperscale.core.engines.client.shared.models import HTTPCookie


async def make_websocket_request(
    url: str,
    cookies: list[HTTPCookie],
    params: dict[str, str],
    headers: dict[str, Any],
    method: Literal[
        "send",
        "receive",
    ],
    data: Any | None,
    redirects: int, 
    timeout: int | float,
    output_file: str | None = None,
    wait: bool = False,
    quiet:bool= False,
):
    
    timeouts = Timeouts(request_timeout=timeout)
    websocket = MercurySyncWebsocketConnection(
        timeouts=timeouts,
    )

    websocket = setup_client(websocket, 1)


    match method:
        case "send":
            return await websocket.send(
                url,
                cookies=cookies,
                headers=headers,
                params=params,
                data=data,
                redirects=redirects,
                timeout=timeout,
            )
        
        case "receive":
            return await websocket.receive(
                url,
                cookies=cookies,
                headers=headers,
                params=params,
                redirects=redirects,
                timeout=timeout,

            )
        
        case _:
            return await websocket.send(
                url,
                cookies=cookies,
                headers=headers,
                params=params,
                data=data,
                redirects=redirects,
                timeout=timeout,
            )