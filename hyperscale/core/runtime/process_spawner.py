"""
ProcessSpawner interface — the dependency-injection seam ``LocalServerPool``
uses to start its workflow-executor processes under SIM.

Why this exists
--------------

In REAL mode ``LocalServerPool`` fans its executors out through a
``ProcessPoolExecutor`` — real OS subprocesses driven by
``loop.run_in_executor``. Under multi-process SIM every process in the
simulation must be a *coordinator child*: an OS process running its own
``SimulationLoop`` whose virtual clock the ``SimulationCoordinator``
keeps in lockstep with every other process. A pool executor spawned
outside the coordinator would run on wall-clock time and break the
deterministic cross-process schedule.

So the pool gets the same style of seam ``UDPProtocol`` got for its
transport (``TransportFactory``): a keyword-only ``process_spawner``
that is ``None`` in REAL mode (the ``ProcessPoolExecutor`` path runs
unchanged) and, under SIM, an object that *requests* child processes
from the coordinator. The SIM implementation (the harness
``ChildContext`` under ``tests/simulation/``) buffers each request and
reports it to the coordinator at the next window barrier; the
coordinator spawns the process, hands it a ``SimulationLoop`` starting
at the current global virtual time, and admits it into the lockstep.

Multi-process is preserved: each executor is still a real OS process —
only the *spawn authority* moves from ``ProcessPoolExecutor`` to the
coordinator so the new process joins the deterministic schedule.
"""

from typing import Callable, Protocol


class ProcessSpawner(Protocol):
    """Request a new simulation child process from the coordinator.

    Only SIM implementations exist; REAL mode passes ``None`` and never
    calls this.
    """

    def spawn_process(
        self,
        process_id: str,
        entry: Callable[..., None],
        *entry_args,
    ) -> None:
        """Buffer a request to spawn ``entry(child_context, *entry_args)``
        as a coordinator child process named ``process_id``.

        ``entry`` must be a top-level (picklable) callable — ``spawn``
        re-imports it by module + qualname in the new process. It
        receives a ``SimulationChildContext`` (the new process's
        ``SimulationLoop`` + cross-process transport) followed by
        ``entry_args``, which must themselves be picklable.

        The spawn is asynchronous in virtual time: the coordinator
        admits the process at the *next window barrier*, with its
        virtual clock initialized to the global virtual time at that
        barrier. ``process_id`` must be unique across the whole
        simulation — the coordinator raises on collision rather than
        silently shadowing an existing process.
        """
        ...

    def get_process_exitcodes(self) -> dict:
        """Exit-code snapshot of the processes spawned through this seam.

        Keyed by the ``process_id`` passed to ``spawn_process``;
        ``None`` means still running, a non-``None`` code means the
        process died (fault-injected coordinator kills surface here at
        their exact virtual instant). Mirrors the contract of
        ``LocalServerPool.get_process_exitcodes`` in REAL mode so the
        worker's pool-health polling runs unchanged over it.
        """
        ...

    def set_process_exit_listener(
        self,
        listener: Callable[[str, int], None],
    ) -> None:
        """Register the callable told of each spawned process's exit.

        The spawner calls ``listener(process_id, exitcode)`` at the
        instant it reaps the exit of a process it spawned — the same
        instant the exit becomes visible in ``get_process_exitcodes`` —
        so the owner reacts to the reap itself instead of waiting for
        its next poll. One listener per spawner; registering again
        replaces it.
        """
        ...
