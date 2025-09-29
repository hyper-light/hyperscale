import asyncio
from hyperscale.core.engines.client.setup_clients import setup_client
from hyperscale.core.engines.client.sftp import MercurySyncSFTPConnction
from hyperscale.core.engines.client.ssh.models.ssh import ConnectionOptions
from hyperscale.core.engines.client.shared.timeouts import Timeouts
from .terminal_ui import (
    update_status,
    update_elapsed,
    update_params,
    update_text,
    create_ping_ui,
    colorize_ftp_or_scp_or_sftp,
)


async def make_sftp_request(
    url: str,
    timeout: int | float,
    auth: tuple[str, str] | None = None,
    insecure: bool = False,
    path_encoding: str = 'utf-8',
    version: int = 3,
    output_file: str | None = None,
    wait: bool = False,
    quiet:bool= False,
):
    
    timeouts = Timeouts(request_timeout=timeout)

    sftp = MercurySyncSFTPConnction(timeouts=timeouts)
    sftp = setup_client(sftp, 1)

    sftp.sftp_version = version
    sftp.path_encoding = path_encoding

    terminal = create_ping_ui(
        url,
        'GETCWD',
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

        response = await sftp.getcwd(
            url,
            username=username,
            password=password,
            insecure=insecure,
            timeout=timeout,
        )

        assert len(response.transferred) == 1, "Err. - Too many results returned for GETCWD"

        result = list(response.transferred.values()).pop()
        result_path = result.file_path.decode(encoding=path_encoding)

        if quiet is False:

            response_status = 'OK'
            response_text = f'Current dir: {result_path}'
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
