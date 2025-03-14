import asyncio
from typing import Literal, Any
from hyperscale.core.engines.client.setup_clients import setup_client
from hyperscale.core.engines.client.http import MercurySyncHTTPConnection
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



async def make_http_request(
    url: str,
    params: dict[str, str],
    cookies: list[HTTPCookie],
    headers: dict[str, Any],
    method: Literal[
        "get",
        "post",
        "put",
        "patch",
        "delete",
        "head",
        "options"
    ],
    data: Any | None,
    redirects: int, 
    timeout: int | float,
    output_file: str | None = None,
    wait: bool = False,
    quiet:bool= False,
):
    
    timeouts = Timeouts(request_timeout=timeout)
    http = MercurySyncHTTPConnection(
        timeouts=timeouts,
    )

    http = setup_client(http, 1)

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
            case "get":
                response = await http.get(
                    url,
                    headers=headers,
                    cookies=cookies,
                    params=params,
                    redirects=redirects,
                    timeout=timeout,
                )
            
            case "post":
                response = await http.post(
                    url,
                    params=params,
                    headers=headers,
                    cookies=cookies,
                    data=data,
                    redirects=redirects,
                    timeout=timeout,
                )
            
            case "put":
                response = await http.put(
                    url,
                    params=params,
                    headers=headers,
                    cookies=cookies,
                    data=data,
                    redirects=redirects,
                    timeout=timeout,
                )
            
            case "patch":
                response = await http.patch(
                    url,
                    params=params,
                    headers=headers,
                    cookies=cookies,
                    data=data,
                    redirects=redirects,
                    timeout=timeout,
                )
            
            case "delete":
                response = await http.delete(
                    url,
                    params=params,
                    headers=headers,
                    cookies=cookies,
                    redirects=redirects,
                    timeout=timeout,
                )
            
            case "head":
                response = await http.head(
                    url,
                    params=params,
                    headers=headers,
                    cookies=cookies,
                    redirects=redirects,
                    timeout=timeout,
                )
            
            case "options":
                response = await http.options(
                    url,
                    params=params,
                    headers=headers,
                    cookies=cookies,
                    redirects=redirects,
                    timeout=timeout,
                )
            
            case _:
                response = await http.get(
                    url,
                    params=params,
                    headers=headers,
                    cookies=cookies,
                    redirects=redirects,
                    timeout=timeout,
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

    except (
        KeyboardInterrupt,
        asyncio.CancelledError,
    ):
        if quiet is False:
            await update_text("Aborted")

    except Exception as err:
        error_message = str(err)
        if str(err) == "":
            error_message = "Encountered unknown error"

        if quiet is False:
            await update_text(error_message)
    
    if quiet is False:
        await terminal.stop()
