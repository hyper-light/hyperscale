import asyncio
import time
from hyperscale.core.engines.client.scp.scp_command import SCPCommand
from hyperscale.core.engines.client.scp.mercury_sync_scp_connection import MercurySyncSCPConnction
from hyperscale.core.engines.client.scp.protocols.scp_connection import SCPConnection


async def run():

    start_a = time.monotonic()
    conn = MercurySyncSCPConnction()


    src_conn = SCPConnection()
    dst_conn = SCPConnection()

    conn._source_connections.append(src_conn)
    conn._destination_connections.append(dst_conn)

    connections = await conn._create_connections(
        'eu-central-1.sftpcloud.io:22',
        'eu-central-1.sftpcloud.io:22',
        username="987f4d3a00de410c8f25aa5e4cc2d539",
        password="Tjtv5y81Jj8KfziBBaACG84Eh3gMaB3X",
        local_path="test.txt",
        destination_path="test.txt",
        disable_host_check=True,
    )

    (
        _,
        _,
        source,
        dest,
        _,
        _,
    ) = connections

    command = SCPCommand(
        source,
        dest,
    )

    await command.copy()

    stop_a = time.monotonic() - start_a

    conn._source_connections.append(src_conn)
    conn._destination_connections.append(dst_conn)

    print('TOOK', stop_a)

    start_b = time.monotonic()

    connections = await conn._create_connections(
        'eu-central-1.sftpcloud.io:22',
        'eu-central-1.sftpcloud.io:22',
        username="987f4d3a00de410c8f25aa5e4cc2d539",
        password="Tjtv5y81Jj8KfziBBaACG84Eh3gMaB3X",
        local_path="test.txt",
        destination_path="test.txt",
        disable_host_check=True,
    )

    (
        _,
        _,
        source,
        dest,
        _,
        _,
    ) = connections

    command = SCPCommand(
        source,
        dest,
    )

    await command.copy()

    stop_b = time.monotonic() - start_b

    print("TOOK", stop_b)




asyncio.run(run())