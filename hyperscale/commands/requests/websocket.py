import asyncio
from typing import Literal, Any
from hyperscale.core.engines.client.setup_clients import setup_client
from hyperscale.core.engines.client.websocket import MercurySyncWebsocketConnection
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
from .ping_result_output import PingResultOutput
from .ping_result_serializer import PingResultSerializer


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
    verify_tls: bool = True,
):
    
    if method is None or method not in ["send", "receive"]:
        method = "send"
    
    timeouts = Timeouts(request_timeout=timeout)
    websocket = MercurySyncWebsocketConnection(
        timeouts=timeouts,
    )

    websocket = setup_client(websocket, 1, verify_tls=verify_tls)
    terminal = create_ping_ui(
        url,
        method,
    )

    result_serializer = PingResultSerializer('websocket', url, method.upper())
    result_output = PingResultOutput(output_file, result_serializer)

    try:
        if quiet is False:
            await terminal.render(
                horizontal_padding=4,
                vertical_padding=1
            )

        match method:
            case "send":
                response = await websocket.send(
                    url,
                    cookies=cookies,
                    headers=headers,
                    params=params,
                    data=data,
                    redirects=redirects,
                    timeout=timeout,
                )
            
            case "receive":
                response = await websocket.receive(
                    url,
                    cookies=cookies,
                    headers=headers,
                    params=params,
                    redirects=redirects,
                    timeout=timeout,

                )
            
            case _:
                response = await websocket.send(
                    url,
                    cookies=cookies,
                    headers=headers,
                    params=params,
                    data=data,
                    redirects=redirects,
                    timeout=timeout,
                )

        await result_output.record(result_serializer.from_http_response, response)

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
                update_params(response.params, params),
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
        await result_output.record_failure(err)
        error_message = str(err)
        if str(err) == "":
            error_message = "Encountered unknown error"

        if quiet is False:
            await update_text(error_message)
            await terminal.stop()

    result_output.raise_on_write_failure()
