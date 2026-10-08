import asyncio
from collections.abc import Awaitable, Callable

from hyperscale.core.jobs.runner.shutdown_signals import ShutdownSignals
from hyperscale.distributed.swim.health_aware_server import HealthAwareServer
from hyperscale.ui.node_dashboard import NodeDashboard

from .shared import drain_cluster_membership

# What ends a running node's wait as an operator stop (ShutdownSignals
# cancels the command's task) or a node shutting itself down.
NODE_STOP_ERRORS = (
    KeyboardInterrupt,
    asyncio.CancelledError,
    asyncio.InvalidStateError,
    asyncio.TimeoutError,
)


async def boot_node_or_abort(
    node: HealthAwareServer,
    boot: Callable[[], Awaitable[object]],
    shutdown_timeout_seconds: float,
) -> None:
    """Boot ``node``; a failed or interrupted boot has already started some
    of the node (an executor pool, listeners), so abort it before the
    failure propagates and nothing outlives the command."""
    try:
        await boot()

    except BaseException:
        await node.abort_and_wait(timeout=shutdown_timeout_seconds)
        raise


async def wait_node_or_stop(
    node: HealthAwareServer,
    drain_before_stop: bool,
    shutdown_timeout_seconds: float,
) -> None:
    """Wait on the running ``node``; on an operator stop, have its cluster
    release its membership first when it holds one (managers and gates),
    then abort it within the shutdown timeout."""
    try:
        await node.wait()

    except NODE_STOP_ERRORS:
        if drain_before_stop:
            await drain_cluster_membership(node, shutdown_timeout_seconds)

        await node.abort_and_wait(timeout=shutdown_timeout_seconds)


async def run_node_until_stopped(
    node: HealthAwareServer,
    boot: Callable[[], Awaitable[object]],
    dashboard: NodeDashboard,
    drain_before_stop: bool,
    shutdown_timeout_seconds: float,
) -> None:
    """Run a ``hyperscale run worker|manager|gate`` node from boot to stop
    with its dashboard up the whole time.

    SIGINT/SIGTERM cancel the command (``ShutdownSignals``) from before the
    boot until the dashboard is stopped: claimed after the dashboard's
    terminal registers its own handlers, and again after the boot, whose
    components register theirs. A second signal never interrupts the
    shutdown midway. The dashboard is stopped -- its sampling cancelled
    and awaited, the terminal restored -- on every exit path, the node's
    own failures included.
    """
    with ShutdownSignals(asyncio.current_task()) as shutdown_signals:
        await dashboard.start()
        shutdown_signals.route()

        try:
            await boot_node_or_abort(node, boot, shutdown_timeout_seconds)
            shutdown_signals.route()
            await wait_node_or_stop(node, drain_before_stop, shutdown_timeout_seconds)

        finally:
            await dashboard.stop()
