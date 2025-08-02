import asyncio
from hyperscale.core.engines.client.scp.protocol.protocol import SCPProtocol


async def run():
    protocol = SCPProtocol()

    await protocol.connect(
        'localhost:simple_scp_server.py',
        'localhost:simple_scp_server.py',
        port=8022,
    )

    await protocol.connection.copy()


asyncio.run(run())