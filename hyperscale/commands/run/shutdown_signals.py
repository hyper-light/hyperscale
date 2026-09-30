import asyncio
import signal


class ShutdownSignals:
    """Route SIGINT/SIGTERM to cancelling a running node command's task.

    ``asyncio.run`` normally cancels the main task on the first SIGINT,
    but node components (the executor pool, the pool-leader protocols,
    the monitors) register their own abort-on-signal handlers while the
    node boots, and a loop keeps only one handler per signal. Once boot
    has finished, the last component to register owns the signal and
    aborts only itself: the node server keeps running and the command
    blocked in ``wait()`` never wakes (measured: the CLI hung with its
    executor pool respawning orphans).

    Installed after boot, this restores the command's own cancellation
    path, whose ``abort_and_wait`` aborts every component of the node.
    Only the first signal cancels; later ones are ignored so they cannot
    interrupt the abort midway (it is already bounded by the command's
    shutdown timeout). SIGTERM is included because process supervisors
    (Kubernetes, systemd, docker stop) deliver it to the parent alone.
    """

    ROUTED_SIGNALS: tuple[signal.Signals, ...] = (signal.SIGINT, signal.SIGTERM)

    def __init__(self, task: asyncio.Task) -> None:
        self._task = task
        self._loop = task.get_loop()
        self._received: signal.Signals | None = None

    @property
    def received(self) -> signal.Signals | None:
        return self._received

    def __enter__(self) -> "ShutdownSignals":
        for routed_signal in self.ROUTED_SIGNALS:
            self._loop.add_signal_handler(
                routed_signal,
                self._on_signal,
                routed_signal,
            )

        return self

    def __exit__(self, *exc_info) -> None:
        if self._loop.is_closed():
            return

        for routed_signal in self.ROUTED_SIGNALS:
            self._loop.remove_signal_handler(routed_signal)

    def _on_signal(self, received_signal: signal.Signals) -> None:
        if self._received is not None:
            return

        self._received = received_signal
        self._task.cancel()
