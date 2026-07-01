"""
The real ``LocalServerPool`` exercised over the multi-process
coordinator — the picklable child entry a test drives.

One coordinator child plays the worker-node side: it starts the
production pool-leader ``RemoteGraphController`` (the same server
``RemoteGraphManager`` runs in a real worker process) and then drives
``LocalServerPool.run_pool`` with the ``process_spawner`` seam pointed
at its ``ChildContext``. The pool requests its executors as further
coordinator children — each one a REAL OS process running the unchanged
production executor lifecycle (``run_server``: start, connect back to
the leader, ``acknowledge_start``, serve) on a lockstep
``SimulationLoop``. The entry records the virtual time at which every
executor's start acknowledgement lands on the leader.

Lives in an importable module because ``spawn`` re-imports the child
entry by module + qualname.
"""

import asyncio
import os

from hyperscale.core.jobs.graphs.remote_graph_controller import (
    RemoteGraphController,
)
from hyperscale.core.jobs.models import Env
from hyperscale.core.jobs.runner.local_server_pool import LocalServerPool

_AUTH_SECRET = "sim-multiprocess-secret-00000000"


def _env() -> Env:
    os.environ.setdefault("MERCURY_SYNC_AUTH_SECRET", _AUTH_SECRET)
    return Env(MERCURY_SYNC_AUTH_SECRET=_AUTH_SECRET)


def pool_leader_entry(context, host, port, pool_size) -> None:
    """Worker-node child: pool leader + ``LocalServerPool`` under SIM.

    Executor addresses are ``(host, port + 1) .. (host, port + pool_size)``;
    the pool derives each executor's process id from its address
    (``executor-<host>-<port>``), which is what the driving test asserts
    against in the coordinator's results.
    """
    env = _env()
    leader = RemoteGraphController(
        None, host, port, env, loop=context.loop, transport_factory=context.transport
    )
    pool = LocalServerPool(pool_size, loop=context.loop, process_spawner=context)

    executor_addresses = [
        (host, port + executor_offset)
        for executor_offset in range(1, pool_size + 1)
    ]

    log: list = []
    context.set_result(log)

    async def run() -> None:
        await leader.start_server()
        await pool.setup()
        await pool.run_pool((host, port), executor_addresses, env)

        # ``wait_for_workers`` is the production wait path, but it drags
        # in the UI updates controller; this slice asserts the same
        # underlying state — every executor's ``acknowledge_start``
        # arriving — on virtual time directly.
        while len(leader.acknowledged_starts) < pool_size:
            await asyncio.sleep(0.05)

        log.append(
            (
                "workers-acknowledged",
                sorted(leader.acknowledged_starts),
                round(context.loop.time(), 6),
            )
        )

    context.loop.create_task(run())
