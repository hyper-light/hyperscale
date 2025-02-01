import asyncio
from typing import Literal, Any
from pydantic import BaseModel, StrictStr, StrictInt

from hyperscale.core.engines.client.setup_clients import setup_client
from hyperscale.core.engines.client.udp import MercurySyncUDPConnection
from hyperscale.core.engines.client.shared.timeouts import Timeouts
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


class UDPOptions(BaseModel):
    delimiter: StrictStr = "\n"
    size: StrictInt | None = None


async def make_udp_request(
    url: str,
    method: Literal[
        "send",
        "receive",
        "bidirectional"
    ],
    data: Any | None,
    options: dict[
        Literal[
            "delimiter",
            "size"
        ],
        str | int,
    ],
    timeout: int | float,
    output_file: str | None = None,
    wait: bool = False,
    quiet:bool= False,
):
    
    timeouts = Timeouts(request_timeout=timeout)
    udp = MercurySyncUDPConnection(
        timeouts=timeouts,
    )

    udp = setup_client(udp, 1)
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
            case "send":
                response = await udp.send(
                    url,
                    data=data,
                    timeout=timeout,
                )
            
            case "receive":
                udp_options = UDPOptions(**options)
                response = await udp.receive(
                    url,
                    delimiter=udp_options.delimiter.encode(),
                    response_size=udp_options.size,
                    timeout=timeout,
                )
            
            case "bidirectional":
                udp_options = UDPOptions(**options)
                response = await udp.bidirectional(
                    url,
                    data=data,
                    delimiter=udp_options.delimiter.encode(),
                    response_size=udp_options.size,
                    timeout=timeout,
                )
            
            case _:
                response = await udp.send(
                    url,
                    data=data,
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


            elapsed = response.timings.get('request_end', 0) - response.timings.get('request_start', 0)
                
            updates = [
                update_redirects(response.redirects),
                update_status(response.status),
                update_headers(response.headers),
                update_text(response_text),
                update_elapsed(elapsed),
                update_params([], {}),
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