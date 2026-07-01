"""
The worker node's production pool bring-up exercised over the
multi-process coordinator — the picklable child entry a test drives.

One coordinator child runs the exact sequence ``WorkerServer.start()``
runs for its executor pool — ``WorkerLifecycleManager.setup_server_pool
-> initialize_remote_manager -> start_remote_manager -> run_worker_pool
-> connect_to_workers`` — with the Phase 6 SIM seams threaded the same
way ``WorkerServer`` threads them (``transport_factory`` to the
``RemoteGraphManager`` pool leader, ``process_spawner`` to the
``LocalServerPool``). The executors are admitted as coordinator child
processes, dial the leader over the encrypted UDP handshake,
acknowledge, and the production wait path (``wait_for_workers`` +
leader-side ``connect_client`` back to every executor) completes on
virtual time.

Lives in an importable module because ``spawn`` re-imports the child
entry by module + qualname.
"""

from hyperscale.distributed.env import Env
from hyperscale.distributed.nodes.worker.lifecycle import WorkerLifecycleManager
from hyperscale.ui import InterfaceUpdatesController

_AUTH_SECRET = "sim-multiprocess-secret-00000000"


def worker_lifecycle_entry(context, host, tcp_port, udp_port, total_cores) -> None:
    """Worker-node child: the production pool bring-up under SIM.

    The leader binds ``(host, udp_port + total_cores**2)`` and the
    executor addresses derive from it exactly as in production
    (``WorkerLifecycleManager.get_worker_ips``); the driving test
    asserts against the resulting ``executor-<host>-<port>`` process
    ids and ``host:port`` acknowledgement strings.
    """
    env = Env(MERCURY_SYNC_AUTH_SECRET=_AUTH_SECRET)
    lifecycle = WorkerLifecycleManager(
        host=host,
        tcp_port=tcp_port,
        udp_port=udp_port,
        total_cores=total_cores,
        env=env,
        logger=None,
        loop=context.loop,
        transport_factory=context.transport,
        process_spawner=context,
    )
    updates_controller = InterfaceUpdatesController()

    log: list = []
    context.set_result(log)

    async def run() -> None:
        # The exact pool sequence from WorkerServer.start().
        await lifecycle.setup_server_pool()
        remote_manager = await lifecycle.initialize_remote_manager(
            updates_controller,
            status_update_poll_interval=0.05,
        )
        await lifecycle.start_remote_manager()
        await lifecycle.run_worker_pool()
        await lifecycle.connect_to_workers()

        # Under SIM the executors are coordinator children; the
        # exit-code snapshot flows through the spawner seam with REAL
        # semantics (``None`` = running, fault-injected kills flip an
        # entry at their virtual instant).
        exitcodes = lifecycle.get_server_pool_process_exitcodes()

        leader_controller = remote_manager._controller
        log.append(
            (
                "pool-connected",
                sorted(leader_controller.acknowledged_starts),
                exitcodes,
                round(context.loop.time(), 6),
            )
        )

    context.loop.create_task(run())
