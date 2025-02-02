import asyncio
import os
from hyperscale.core.jobs.distributed import DistributedWorker
from hyperscale.logging import LogLevelName, LoggingConfig


async def run():
    logging_config = LoggingConfig()
    logging_config.update(
        log_directory=os.path.join(
            os.getcwd(),
            'logs'
        ),
        log_level='fatal',
        log_output='stderr',
    )


    try:
        worker = DistributedWorker(
            '127.0.0.1',
            17885,
            workers=4
        )

        await worker.run()
    
    except (
        Exception,
        asyncio.CancelledError
    ):
        pass

if __name__ == "__main__":
    asyncio.run(run())