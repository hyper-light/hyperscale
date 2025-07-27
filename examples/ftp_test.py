import asyncio
from hyperscale.core.engines.client.ftp.mercury_sync_ftp_connection import MercurySyncFTPConnection, FTPConnection
from hyperscale.core.engines.client.ftp.ftp import FTP

async def run():
    ftp = MercurySyncFTPConnection()
    ftp._control_connections = [
        FTPConnection() for _ in range(1000)
    ]
    
    ftp._data_connections = [
        FTPConnection() for _ in range(1000)
    ]

    print(
        await ftp._execute(
            'ftp://test.rebex.net',
            'LIST',
        )

    )

asyncio.run(run())


ftp = FTP('194.108.117.16', user='anonymous')

for file in ftp.mlsd():
    print(file)