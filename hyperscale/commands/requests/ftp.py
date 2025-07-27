import asyncio
from hyperscale.core.engines.client.setup_clients import setup_client
from hyperscale.core.engines.client.ftp import MercurySyncFTPConnection
from hyperscale.core.engines.client.shared.timeouts import Timeouts
from .terminal_ui import (
    update_status,
    update_elapsed,
    update_params,
    update_text,
    create_ping_ui,
    colorize_ftp,
)


async def make_ftp_request(
    url: str,
    timeout: int | float,
    auth: tuple[str, str, str] | None = None,
    secure_connection: bool = False,
    output_file: str | None = None,
    wait: bool = False,
    quiet:bool= False,
):
    
    timeouts = Timeouts(request_timeout=timeout)

    ftp = MercurySyncFTPConnection(timeouts=timeouts)
    ftp = setup_client(ftp, 1)

    terminal = create_ping_ui(
        url,
        'PWD',
        override_status_colorizer=colorize_ftp
    )

    try:
        if quiet is False:
            await terminal.render(
                horizontal_padding=4,
                vertical_padding=1,
            )

        response = await ftp.pwd(
            url,
            auth=auth,
            secure_connection=secure_connection,
        )

        if quiet is False:

            response_status = 'OK'
            response_text = response.data
            if response.error:
                response_status = 'FAILED'
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
            update_status(response_status),
            update_text(response_text),
            update_elapsed(elapsed),
            update_params({}, {}),
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
