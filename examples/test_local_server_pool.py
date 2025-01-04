import asyncio

from hyperscale.core.jobs.models import Env
from hyperscale.core.jobs.runner.local_server_pool import LocalServerPool


async def run():
    pool = LocalServerPool(pool_size=4)

    pool.setup()
    await pool.run_pool(
        [
            ("0.0.0.0", 11223),
            ("0.0.0.0", 11225),
            ("0.0.0.0", 11227),
            ("0.0.0.0", 11229),
        ],
        Env(MERCURY_SYNC_AUTH_SECRET="testthis"),
    )


if __name__ == "__main__":
    asyncio.run(run())
