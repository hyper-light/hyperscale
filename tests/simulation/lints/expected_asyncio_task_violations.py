"""
Phase 6b ratcheted snapshot — the production files under ``hyperscale/``
(outside the lint's ``EXEMPT_PATH_PREFIXES``) that contain a direct call
to ``asyncio.create_task`` or ``asyncio.ensure_future``, each with the
one-line reason it is exempt. Every entry stores and owns the task
handle it creates (or is engine code, changed only as authorized).

Why this lint
-------------

Phase 6's deterministic ``SimulationLoop`` guarantees ordering only
for tasks that route through ``loop.create_task`` (which the SIM
harness *controls*) or through a ``TaskRunner.run`` (which uses the
same loop's ``create_task`` under the hood). Direct calls to
``asyncio.create_task`` resolve through ``asyncio.get_running_loop()``
which, while it returns the right loop in well-behaved code, makes
every callsite implicitly dependent on the running-loop context.
That coupling is fine for REAL — but for SIM, a stray
``asyncio.create_task`` from a code path the harness doesn't
explicitly drive can land on the *wrong* loop (e.g., a default
``ProactorEventLoop`` on Windows, a real ``SelectorEventLoop`` on
unix) and silently bypass the SimulationLoop's determinism
guarantees.

The Phase 6b migration routes every direct call through one of:

1. ``self._task_runner.run(coro)`` — when the construction site has
   a ``TaskRunner`` injected.
2. ``loop.create_task(coro)`` — when the caller has the loop
   reference (rare; only loop-internal helpers).

Each migration commit shrinks the allowlist by one file. Final
state (after Phase 6b): only the three taskex internals remain —
``taskex/run.py``, ``taskex/task_runner.py``, ``taskex/task.py``.
These *are* the TaskRunner; their direct ``asyncio.ensure_future``
calls are how the runner submits work to the loop, so they stay on
the allowlist permanently.

R-G66 (2026-10-07) widened the scan to all of ``hyperscale/``. The
logging layer keeps raw tasks: ``hyperscale.logging`` is the base layer
``hyperscale.distributed`` (and its TaskRunner) is built on, and a
TaskRunner needs an ``Env`` and its own lifecycle, which no Logger has.

Stored as Python (not a text snapshot) so the project's ``*.txt``
gitignore rule doesn't accidentally hide it from version control.
"""

EXPECTED_ASYNCIO_TASK_VIOLATIONS: dict[str, str] = {
    # TaskRunner internals — permanent entries: these files implement
    # the runner itself and submit work to the loop by design.
    "hyperscale/distributed/taskex/run.py": "TaskRunner internals: a Run holds the task it executes in _task",
    "hyperscale/distributed/taskex/task_runner.py": "TaskRunner internals: the runner holds its _cleanup_task",
    "hyperscale/distributed/taskex/task.py": "TaskRunner internals: a Task holds its scheduled runs in _schedules",
    # Logging layer: below the TaskRunner (see above); each handle is held.
    "hyperscale/logging/queue/log_consumer.py": "_pull_task held, cancelled and awaited by stop()/abort()",
    "hyperscale/logging/streams/logger.py": "_watch_tasks[name] held, cancelled by stop_watch()/close()",
    "hyperscale/logging/streams/logger_stream.py": (
        "scheduled log tasks held in _scheduled_tasks (discarded when done); _batch_flush_task held"
    ),
    # Terminal UI: each handle is held and ended by stop()/abort().
    "hyperscale/ui/hyperscale_interface.py": "_terminal_task/_spinner_task held, cancelled by stop()/abort()",
    "hyperscale/ui/components/terminal/terminal.py": (
        "_run_engine/_spin_thread held, awaited by stop() and cancelled by abort()"
    ),
    # Monitoring: one held task per workflow monitor.
    "hyperscale/core/monitoring/base/monitor.py": (
        "_background_monitors[run_id][workflow] held, stopped/aborted by the monitor"
    ),
    # Engines: changed only as authorized.
    "hyperscale/core/engines/client/sftp/protocols/sftp.py": "parallel-read tasks held in _pending until consumed",
    "hyperscale/core/engines/client/ssh/protocol/ssh/connection.py": (
        "SSHConnection.create_task holds each task in _tasks, reaped by a done callback"
    ),
    "hyperscale/core/engines/client/playwright/mercury_sync_playwright_connection.py": (
        "close() starts one close task per session; engine code, changed only as authorized"
    ),
}
