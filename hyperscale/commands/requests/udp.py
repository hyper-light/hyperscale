from typing import Literal, Any
from pydantic import BaseModel, StrictStr, StrictInt

from hyperscale.core.engines.client.setup_clients import setup_client
from hyperscale.core.engines.client.udp import MercurySyncUDPConnection
from hyperscale.core.engines.client.shared.timeouts import Timeouts
from hyperscale.core.engines.client.shared.models import HTTPCookie


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


    match method:
        case "send":
            return await udp.send(
                url,
                data=data,
                timeout=timeout,
            )
        
        case "receive":
            udp_options = UDPOptions(**options)

            return await udp.receive(
                url,
                delimiter=udp_options.delimiter.encode(),
                response_size=udp_options.size,
                timeout=timeout,
            )
        
        case "bidirectional":
            udp_options = UDPOptions(**options)

            return await udp.bidirectional(
                url,
                data=data,
                delimiter=udp_options.delimiter.encode(),
                response_size=udp_options.size,
                timeout=timeout,
            )
        
        case _:
            return await udp.send(
                url,
                data=data,
                timeout=timeout,
            )