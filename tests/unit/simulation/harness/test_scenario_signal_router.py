"""
``ScenarioSignalRouter``: a signal stops the scenario even after an
in-process node component registered its own abort handler for it, and
no handler outlives the scenario.

The component handler stands in for the ``hyperscale.core.jobs``
protocols' and local server pool's abort-on-signal handlers, which a
REAL-mode node registers on the pytest loop as it starts (the cause of
the churn and L3 gate scenarios ignoring SIGTERM).
"""

import asyncio
import os
import signal

import pytest

from tests.simulation.harness.scenario_signal_router import ScenarioSignalRouter


def _register_component_abort(loop: asyncio.AbstractEventLoop, aborted: list[signal.Signals]) -> None:
    for component_signal in ScenarioSignalRouter.COMPONENT_SIGNALS:
        loop.add_signal_handler(component_signal, aborted.append, component_signal)


async def _scenario_until_signalled(routed_signal: signal.Signals) -> tuple[bool, list[signal.Signals], ScenarioSignalRouter]:
    loop = asyncio.get_running_loop()
    aborted: list[signal.Signals] = []
    scenario_started = asyncio.Event()
    router_holder: list[ScenarioSignalRouter] = []

    async def scenario() -> None:
        router = ScenarioSignalRouter(asyncio.current_task())
        router_holder.append(router)
        router.claim()
        # A node starts and takes the signals over; the harness re-claims.
        _register_component_abort(loop, aborted)
        router.claim()
        scenario_started.set()
        await asyncio.Event().wait()

    scenario_task = asyncio.ensure_future(scenario())
    await scenario_started.wait()
    os.kill(os.getpid(), routed_signal)
    # The signal lands on whichever handler owns it: the scenario's
    # (cancelling the task) or the component's (recording an abort).
    while not scenario_task.done() and not aborted:
        await asyncio.sleep(0)
    scenario_task.cancel()
    try:
        await scenario_task
    except asyncio.CancelledError:
        pass
    return router_holder[0].received is routed_signal, aborted, router_holder[0]


@pytest.mark.parametrize("routed_signal", ScenarioSignalRouter.ROUTED_SIGNALS)
def test_a_signal_cancels_the_scenario_not_a_node_component(routed_signal: signal.Signals) -> None:
    loop = asyncio.new_event_loop()
    try:
        cancelled, aborted, router = loop.run_until_complete(_scenario_until_signalled(routed_signal))
        router.release()
    finally:
        loop.close()
    assert cancelled
    assert aborted == []
    assert router.received is routed_signal


def test_release_removes_every_handler_and_redelivery_takes_the_default_effect() -> None:
    loop = asyncio.new_event_loop()
    try:
        _cancelled, _aborted, router = loop.run_until_complete(_scenario_until_signalled(signal.SIGINT))
        router.release()
        for component_signal in ScenarioSignalRouter.COMPONENT_SIGNALS:
            assert loop.remove_signal_handler(component_signal) is False
    finally:
        loop.close()
    # SIGINT's default disposition raises KeyboardInterrupt: the session
    # stops, as it would have without in-process nodes.
    with pytest.raises(KeyboardInterrupt):
        router.redeliver()
