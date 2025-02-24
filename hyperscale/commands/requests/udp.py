import asyncio
from typing import Literal, Any
from pydantic import BaseModel, StrictStr, StrictInt

from hyperscale.core.engines.client.setup_clients import setup_client
from hyperscale.core.engines.client.udp import MercurySyncUDPConnection
from hyperscale.core.engines.client.shared.timeouts import Timeouts
from .terminal_ui import (
    update_elapsed,
    update_text,
    create_ping_ui,
    colorize_udp_or_tcp,
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
   
    if method is None or method not in ["send", "receive", "bidirectional"]:
        method = "send"
        data = b"PING"

    timeouts = Timeouts(request_timeout=timeout)
    udp = MercurySyncUDPConnection(
        timeouts=timeouts,
    )

    udp = setup_client(udp, 1)
    terminal = create_ping_ui(
        url,
        method,
        override_status_colorizer=colorize_udp_or_tcp,
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
            response_text = "OK!"

            if response.error:
                response_text = str(response.error)

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
                update_text(response_text),
                update_elapsed(elapsed),
            ]

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
