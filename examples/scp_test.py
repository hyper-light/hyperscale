import asyncio
from hyperscale.core.engines.client.scp.protocol.protocol import SCPProtocol


async def run():
    protocol = SCPProtocol()

    await protocol.connect(
        ['./test.txt'],
        'localhost:example.txt',
        port=8022,
    )


asyncio.run(run())