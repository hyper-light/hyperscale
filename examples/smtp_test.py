import asyncio
import ssl
import smtplib
from ssl import _DEFAULT_CIPHERS
from hyperscale.core.engines.client.smtp import MercurySyncSMTPConnection
from hyperscale.core.engines.client.smtp.protocols import SMTPConnection


def create_ssl():
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE


    return ctx

async def run():
    client = MercurySyncSMTPConnection()
    client._ssl_context = create_ssl()
    client._connections = [
        SMTPConnection() for _ in range(1000)
    ]

    server_name = 'opm-gov.mail.protection.outlook.com'
    server_name = 'smtp.gmail.com'

    (
        err,
        connection,
        url
    ) = await client._connect_to_url_location(server_name)

    if err:
        raise err

    (code, message) = await client._get_reply(connection)
    if code != 220:
        raise Exception(message)

    (
        code,
        message,
        options
    ) = await client._ehlo(connection)

    if options is None or code != 250:
        raise Exception(f'Err. - {code} - {message}')
    
    if 'starttls' in options and url.port == 587:

        (code, message) = await client._check_start_tls(connection)

        print(code, message)

        if code == 220:

            await connection.make_connection(
                server_name,
                url.address,
                ssl=client._ssl_context,
                connection_type='tls',
                ssl_upgrade=True
            )

        else:
            raise Exception(f'Err. - {code} {message}')
    
    elif  'starttls' in options and url.port == 465:

        await connection.make_connection(
            server_name,
            url.address,
            ssl=client._ssl_context,
            connection_type='ssl',
            ssl_upgrade=True,
        )


    print(await client._ehlo(connection))
    

asyncio.run(run())

ctx = create_ssl()

server = smtplib.SMTP('smtp.gmail.com:587')
print(server.ehlo())
print(server.starttls())
print(server.ehlo())