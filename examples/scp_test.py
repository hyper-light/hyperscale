import asyncio
from hyperscale.core.engines.client.scp.protocol.protocol import SCPProtocol


async def run():
    protocol = SCPProtocol()

    username = "02a345508e2b4862b0efbe70806b32a9"
    password = "w66SnswnxCRwokDeG8kZvxbpBdnqJrgr"

    await protocol.connect(
        ['./test.txt'],
        'sftpcloud.io:22:/dev/null',
        username=username,
        password=password,
    )


asyncio.run(run())