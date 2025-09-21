import asyncio
import os
import time
from hyperscale.core.engines.client.scp.scp_command import SCPCommand
from hyperscale.core.engines.client.scp.mercury_sync_scp_connection import MercurySyncSCPConnction
from hyperscale.core.engines.client.scp.protocols.scp_connection import SCPConnection

data = str(os.urandom(5000000))


async def run():
    
    conn = MercurySyncSCPConnction()


    conn._semaphore = asyncio.Semaphore(value=4000)


    for _ in range(1000):
        src_conn = SCPConnection("SOURCE")
        dst_conn = SCPConnection("DEST")

        conn._source_connections.append(src_conn)
        conn._destination_connections.append(dst_conn)


    start_a = time.monotonic()

    res = await asyncio.gather(*[
        conn.send(
            'eu-central-1.sftpcloud.io:22',
            "test.txt",
            "Hello!",
            username="201af83015af4081abb6e84645314607",
            password="NALy5B6KGrCX77ve5CuHQWVshiQydSXT",
            disable_host_check=True,
        ) for _ in range(100000)
    ])

    # res = await conn.copy(
    #     'eu-central-1.sftpcloud.io:22',
    #     'eu-central-1.sftpcloud.io:22',
    #     "test.txt",
    #     "test.txt",
    #     username="ca1b158d4b864cae8c44257a118f1c02",
    #     password="LO4hoT35ItUry3m5AFUVuua2iBqT2wyL",
    #     disable_host_check=True,
    # )
    stop_a = time.monotonic() - start_a

    print('TOOK', stop_a)




asyncio.run(run())