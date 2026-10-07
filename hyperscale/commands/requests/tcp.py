import asyncio
from typing import Literal, Any
from pydantic import BaseModel, StrictStr, StrictInt

from hyperscale.core.engines.client.setup_clients import setup_client
from hyperscale.core.engines.client.tcp import MercurySyncTCPConnection
from hyperscale.core.engines.client.shared.timeouts import Timeouts
from .terminal_ui import (
    update_elapsed,
    update_text,
    update_status,
    create_ping_ui,
    colorize_udp_or_tcp,
)
from .ping_result_output import PingResultOutput
from .ping_result_serializer import PingResultSerializer


class TCPOptions(BaseModel):
    delimiter: StrictStr = "\n"
    size: StrictInt | None = None


async def make_tcp_request(
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
    verify_tls: bool = True,
):
    
    if method is None or method not in ["send", "receive", "bidirectional"]:
        method = "send"
        data = b"PING"
    
    timeouts = Timeouts(request_timeout=timeout)
    tcp = MercurySyncTCPConnection(
        timeouts=timeouts,
    )

    tcp = setup_client(tcp, 1, verify_tls=verify_tls)
    terminal = create_ping_ui(
        url,
        method,
        override_status_colorizer=colorize_udp_or_tcp,
    )

    result_serializer = PingResultSerializer('tcp', url, method.upper())
    result_output = PingResultOutput(output_file, result_serializer)

    try:

        if quiet is False:
            await terminal.render(
                horizontal_padding=4,
                vertical_padding=1
            )


        match method:
            case "send":
                response = await tcp.send(
                    url,
                    data=data,
                    timeout=timeout,
                )
            
            case "receive":
                tcp_options = TCPOptions(**options)
                response = await tcp.receive(
                    url,
                    delimiter=tcp_options.delimiter.encode(),
                    response_size=tcp_options.size,
                    timeout=timeout,
                )
            
            case "bidirectional":
                tcp_options = TCPOptions(**options)
                response = await tcp.bidirectional(
                    url,
                    data=data,
                    delimiter=tcp_options.delimiter.encode(),
                    response_size=tcp_options.size,
                    timeout=timeout,
                )
            
            case _:
                response = await tcp.send(
                    url,
                    data=data,
                    timeout=timeout,
                )

        await result_output.record(result_serializer.from_socket_response, response)

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
                update_status('Errored' if response.error else 'OK')
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
        await result_output.record_failure(err)
        error_message = str(err)
        if str(err) == "":
            error_message = "Encountered unknown error"

        if quiet is False:
            await update_text(error_message)
            await terminal.stop()

    result_output.raise_on_write_failure()
