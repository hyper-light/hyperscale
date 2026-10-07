"""
ScenarioSignalRouter -- keeps SIGINT / SIGTERM stopping a REAL-mode
scenario while its in-process nodes run.

REAL-mode nodes run inside the pytest process, and their executor-side
components (``hyperscale.core.jobs`` protocols and the local server pool)
register their own abort-on-signal handlers on the running loop as each
node starts: SIGINT, SIGTERM, and SIGHUP (``signal.SIG_IGN`` -- the
value 1 -- names SIGHUP when passed to ``add_signal_handler``). A loop
keeps one handler per signal, so the last node to start owned SIGTERM
and SIGINT and aborted only its own component: the scenario kept
running, a ``timeout``/supervisor SIGTERM never stopped it, and the
handlers -- holding the aborted components -- outlived the scenario.

The router claims the signals for the scenario itself (the same pattern
as the CLI's ``ShutdownSignals``, re-claimed after every node start):
the first signal cancels the scenario task, so the harness tears the
cluster down -- reaping every node and child process -- and then
re-delivers the signal under its default disposition; a second signal
while teardown runs is re-delivered at once. On release every routed
and component-registered handler is removed, so none outlives the
scenario.
"""

import asyncio
import signal


class ScenarioSignalRouter:
    """Route SIGINT / SIGTERM to cancelling one scenario task."""

    ROUTED_SIGNALS: tuple[signal.Signals, ...] = (signal.SIGINT, signal.SIGTERM)
    # Signals node components register abort handlers for; all are
    # removed on release.
    COMPONENT_SIGNALS: tuple[signal.Signals, ...] = (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)

    def __init__(self, scenario_task: asyncio.Task) -> None:
        self._scenario_task = scenario_task
        self._loop = scenario_task.get_loop()
        self._received: signal.Signals | None = None

    @property
    def received(self) -> signal.Signals | None:
        """The first routed signal received, if any."""
        return self._received

    def claim(self) -> None:
        """Claim the routed signals for the scenario. Call again after any
        node starts: the loop keeps only the latest handler per signal."""
        for routed_signal in self.ROUTED_SIGNALS:
            self._loop.add_signal_handler(routed_signal, self._on_signal, routed_signal)

    def release(self) -> None:
        """Remove every routed and component-registered handler, restoring
        each signal's default disposition."""
        if self._loop.is_closed():
            return
        for component_signal in self.COMPONENT_SIGNALS:
            self._loop.remove_signal_handler(component_signal)

    def redeliver(self) -> None:
        """Re-raise the received signal (after ``release``) so it takes its
        default effect once the scenario has been torn down."""
        if self._received is not None:
            signal.raise_signal(self._received)

    def _on_signal(self, received_signal: signal.Signals) -> None:
        if self._received is None:
            self._received = received_signal
            self._scenario_task.cancel()
            return
        self.release()
        signal.raise_signal(received_signal)
