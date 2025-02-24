import asyncio
from hyperscale.core.engines.client.setup_clients import setup_client
from hyperscale.core.engines.client.smtp import MercurySyncSMTPConnection
from hyperscale.core.engines.client.shared.timeouts import Timeouts
from .terminal_ui import (
    update_status,
    update_elapsed,
    update_params,
    update_text,
    create_ping_ui,
    colorize_smtp,
)

async def make_smtp_request(
    url: str,
    auth: str,
    email: str,
    sender: str,
    subject: str,
    recipients: str,
    timeout: int | float,
    output_file: str | None = None,
    wait: bool = False,
    quiet:bool= False,

):

    timeouts = Timeouts(request_timeout=timeout)

    smtp = MercurySyncSMTPConnection(timeouts=timeouts)
    smtp = setup_client(smtp, 1)

    terminal = create_ping_ui(
        url,
        'SEND',
        override_status_colorizer=colorize_smtp,
    )

    auth_data = tuple(auth.split(':'))
    if len(auth_data) < 2:
        auth_data = None

    try:
        if quiet is False:
            await terminal.render(
                horizontal_padding=4,
                vertical_padding=1
            )

        recipients_list = recipients.split(',')
        if len(recipients_list) < 1:
            raise Exception('--recipients/-r is required for SMTP ping requests - please pass at least one')
        
        response = await smtp.send(
            url,
            sender,
            recipients=recipients_list,
            subject=subject,
            email=email,
            auth=auth_data,
        )

        if quiet is False:
            response_text = response.last_smtp_message.decode(response.encoding).replace('\n', ' ')
            response_status = response.last_smtp_code

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

            params = {
                'sender': sender,
            }

            for idx, recipient in enumerate(recipients_list):
                if idx > 0:
                    params[f'recipient_{idx}'] = recipient

                else:
                    params['recipient'] = recipient
                
            updates = [
                update_status(response_status),
                update_text(response_text),
                update_elapsed(elapsed),
                update_params({},params),
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
