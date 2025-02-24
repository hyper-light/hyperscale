import asyncio
import time
from hyperscale.core.engines.client.shared.models import URL
from .lookup_ui import (
    update_elapsed,
    update_socket_info,
    update_socket_info_smtp,
    update_text,
    create_lookup_ui
)


async def lookup_url(
    url: str,
    as_smtp: bool = False,
    wait: bool = False,
    quiet:bool= False,
):
    
    url_data = URL(url)
    terminal = create_lookup_ui(
        url,
    )

    error: Exception | None = None


    start = time.monotonic()
    
    try:

        if quiet is False:
            await terminal.render(
                horizontal_padding=4,
                vertical_padding=1
            )

        if as_smtp:
            await url_data.lookup_smtp()

        else:
            await url_data.lookup()

        elapsed = time.monotonic() - start

    except (
        Exception,
        KeyboardInterrupt,
        asyncio.CancelledError,
    ) as err:

        elapsed = time.monotonic() - start
        error = err

    if quiet is False and isinstance(error, (asyncio.CancelledError, KeyboardInterrupt)):
        await update_text("Aborted")
        await terminal.stop()
    
    elif quiet is False:

        message = "OK" if error is None else str(error)
        
        await asyncio.sleep(0.5)

        if as_smtp:
            await asyncio.gather(*[
                update_elapsed(elapsed),
                update_socket_info_smtp(url_data.ip_addresses),
                update_text(message)
            ])

        else:
            await asyncio.gather(*[
                update_elapsed(elapsed),
                update_socket_info(url_data.ip_addresses),
                update_text(message)
            ])

        if wait:
            loop = asyncio.get_event_loop()

            await loop.create_future()

        await asyncio.sleep(0.5)
        await terminal.stop()


