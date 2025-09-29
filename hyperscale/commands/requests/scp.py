import asyncio
from hyperscale.core.engines.client.setup_clients import setup_client
from hyperscale.core.engines.client.scp import MercurySyncSCPConnection
from hyperscale.core.engines.client.shared.timeouts import Timeouts
from .terminal_ui import (
    update_status,
    update_elapsed,
    update_params,
    update_text,
    create_ping_ui,
    colorize_ftp_or_scp_or_sftp,
)


async def make_scp_request(
    url: str,
    path: str,
    timeout: int | float,
    auth: tuple[str, str] | None = None,
    insecure: bool = False,
    recurse: bool = False,
    wait: bool = False,
    quiet:bool= False,
):
    
    timeouts = Timeouts(request_timeout=timeout)

    scp = MercurySyncSCPConnection(timeouts=timeouts)
    scp = setup_client(scp, 1)

    terminal = create_ping_ui(
        url,
        'RECIEVE',
        override_status_colorizer=colorize_ftp_or_scp_or_sftp
    )

    username: str | None = None
    password: str | None = None
    if auth:
        username, password = auth

    try:
        if quiet is False:
            await terminal.render(
                horizontal_padding=4,
                vertical_padding=1,
            )

        response = await scp.receive(
            url,
            path,
            username=username,
            password=password,
            disable_host_check=insecure,
            preserve_file_attributes=True,
            recurse=recurse,
            timeout=timeout,
        )

        if quiet is False:

            response_status = 'OK'
            response_text = f'Got: {len(response.transferred)} files'
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
