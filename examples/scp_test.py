import asyncio
from hyperscale.core.engines.client.scp.protocol.protocol import SCPProtocol


async def run():
    protocol = SCPProtocol()


    await protocol.connect(
        ['./test.txt'],
        'sftpcloud.io:22:/dev/null',
    )


asyncio.run(run())