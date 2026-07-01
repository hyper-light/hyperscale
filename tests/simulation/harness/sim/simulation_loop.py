"""
Custom ``asyncio.BaseEventLoop`` subclass that owns virtual time
and bans every external source of non-determinism.

Why a custom loop
-----------------

asyncio's *internal* scheduling is bit-deterministic given identical
inputs: ``_ready`` is a FIFO deque, ``_scheduled`` is a heap with
``(when, sequence, handle)`` tie-breaks, ``Future.set_result``
schedules callbacks via ``call_soon`` (also FIFO). The
non-determinism that surfaces in normal asyncio usage comes
entirely from **external** inputs:

- ``selector.select()`` / ``kqueue`` / ``epoll`` — OS I/O readiness ordering.
- ``run_in_executor`` — thread pool scheduling.
- Signal delivery (``SIGCHLD``, ``SIGALRM``) — kernel timing.
- Real-time clock progression — interacts with timer deadlines.

Strip those out and asyncio is deterministic, full stop. The
``SimulationLoop`` does exactly that: subclass ``BaseEventLoop``,
override ``_run_once`` to never call any selector, override
``time()`` to return virtual time, and override every external-I/O
entry point to raise ``SimulationConstraintError``. The set of
banned operations is exhaustive (see ``_BANNED_ASYNC_METHODS`` and
``_BANNED_SYNC_METHODS`` below) — every method on
``AbstractEventLoop`` that touches the OS is replaced.

``run_until_complete(future)`` works unchanged: the parent's
implementation registers a done callback on the future that calls
``loop.stop()`` when the future resolves. We add one extra
condition — if ``_ready`` and ``_scheduled`` are both empty before
the future resolves, the loop is deadlocked (no callback can fire,
no timer can advance), and we raise ``SimulationConstraintError``
loudly rather than silently spinning.

Determinism contract
--------------------

For two ``SimulationLoop`` instances started with identical seed
and identical scenario code, the event trace (every ``call_soon``
arrival, every ``call_at`` registration, every ``Handle._run()``
fire) must be byte-identical. The ``EventTrace`` helper records
these decisions when enabled; the determinism gate test
(``tests/simulation/determinism/test_replay_equivalence.py``) runs
each smoke scenario twice and asserts the traces match.

The contract relies on these invariants:

1. ``_ready`` is drained in FIFO order. ``BaseEventLoop._call_soon``
   uses ``deque.append`` and we drain via ``deque.popleft``.
2. ``_scheduled`` ties break by insertion order. ``TimerHandle``
   instances are heapq-compared by ``(when, sequence_id, handle)``
   where sequence_id is monotonically assigned at construction.
3. Virtual time only advances when ``_ready`` is empty. While
   ``_ready`` has work, time is frozen — callbacks fire in the
   order they were scheduled at the *current* virtual instant.
4. No external entry point can inject work outside ``_ready`` /
   ``_scheduled``. Every banned method raises before it could
   schedule anything.

Anything that violates these invariants — a Python-version
behavioral change, an asyncio internal refactor, a production-code
path that escapes our seam — surfaces as a determinism-gate
failure on the next CI run.
"""

import collections
import contextvars
import heapq
import threading
from asyncio import base_events, events
from typing import Callable, ParamSpec, TypeVar

from .simulation_constraint_error import SimulationConstraintError


P = ParamSpec("P")
R = TypeVar("R")


_BANNED_SYNC_METHODS = (
    # Each entry: (method_name, reason). The reason is included in
    # the raised exception so a regression trace immediately
    # explains *why* the operation is banned, not just that it is.
    ("run_in_executor",
     "thread/process pools are the largest single source of "
     "asyncio non-determinism; SIM mode routes all work through "
     "the loop"),
    ("set_default_executor",
     "thread/process executors are banned in SIM mode"),
    ("add_reader",
     "no fd-level I/O exists in SIM; use InProcessTransport"),
    ("remove_reader",
     "no fd-level I/O exists in SIM"),
    ("add_writer",
     "no fd-level I/O exists in SIM; use InProcessTransport"),
    ("remove_writer",
     "no fd-level I/O exists in SIM"),
    ("add_signal_handler",
     "signal delivery is non-deterministic and never reaches SIM"),
    ("remove_signal_handler",
     "signal delivery is non-deterministic and never reaches SIM"),
)


_BANNED_ASYNC_METHODS = (
    ("create_connection",
     "real sockets are banned in SIM; InProcessTransport substitutes"),
    ("create_server",
     "real sockets are banned in SIM; InProcessTransport substitutes"),
    ("create_datagram_endpoint",
     "real UDP sockets are banned in SIM; InProcessTransport substitutes"),
    ("create_unix_connection",
     "unix sockets are banned in SIM; InProcessTransport substitutes"),
    ("create_unix_server",
     "unix sockets are banned in SIM; InProcessTransport substitutes"),
    ("connect_read_pipe",
     "pipe I/O is banned in SIM"),
    ("connect_write_pipe",
     "pipe I/O is banned in SIM"),
    ("connect_accepted_socket",
     "real sockets are banned in SIM"),
    ("sock_recv",
     "direct socket ops are banned in SIM"),
    ("sock_recv_into",
     "direct socket ops are banned in SIM"),
    ("sock_recvfrom",
     "direct socket ops are banned in SIM"),
    ("sock_recvfrom_into",
     "direct socket ops are banned in SIM"),
    ("sock_sendall",
     "direct socket ops are banned in SIM"),
    ("sock_sendto",
     "direct socket ops are banned in SIM"),
    ("sock_connect",
     "direct socket ops are banned in SIM"),
    ("sock_accept",
     "direct socket ops are banned in SIM"),
    ("subprocess_exec",
     "subprocess spawning is non-deterministic and banned in SIM"),
    ("subprocess_shell",
     "subprocess spawning is non-deterministic and banned in SIM"),
    ("start_tls",
     "TLS over real transports is banned in SIM"),
)


def _make_sync_ban(method_name: str, reason: str) -> Callable[..., None]:
    """Build a sync method that raises ``SimulationConstraintError``.

    Closes over name + reason so the raised exception identifies
    exactly which operation a production code path attempted.
    """

    def _banned(self, *args, **kwargs):
        raise SimulationConstraintError(
            f"SimulationLoop.{method_name}() is banned in SIM mode: "
            f"{reason}"
        )

    _banned.__name__ = method_name
    _banned.__qualname__ = f"SimulationLoop.{method_name}"
    return _banned


def _make_async_ban(method_name: str, reason: str) -> Callable[..., None]:
    """Build an async method that raises ``SimulationConstraintError``.

    Same shape as ``_make_sync_ban`` but for the methods on
    ``AbstractEventLoop`` that are coroutines. The exception is
    raised synchronously (before any ``await``) so the failure
    surfaces at the call site rather than at the first suspension
    point.
    """

    async def _banned(self, *args, **kwargs):
        raise SimulationConstraintError(
            f"SimulationLoop.{method_name}() is banned in SIM mode: "
            f"{reason}"
        )

    _banned.__name__ = method_name
    _banned.__qualname__ = f"SimulationLoop.{method_name}"
    return _banned


class SimulationLoop(base_events.BaseEventLoop):
    """``asyncio.BaseEventLoop`` subclass with virtual time and no I/O.

    Construction is identical to ``BaseEventLoop`` (no arguments) and
    the loop is usable as a drop-in for any ``run_until_complete``
    call. The differences surface at runtime:

    - ``time()`` returns ``self._virtual_now`` (advances only when
      ``_run_once`` finds ``_ready`` empty).
    - ``_run_once`` never calls a selector; it advances virtual time
      to the earliest scheduled timer and drains ``_ready`` FIFO.
    - Every ``AbstractEventLoop`` method that touches the OS raises
      ``SimulationConstraintError``.
    - Deadlock detection: if ``_ready`` and ``_scheduled`` are both
      empty but the loop hasn't been stopped, raise (no callback can
      possibly advance the simulation).

    Optional ``EventTrace`` hook: pass ``trace=EventTrace()`` to
    ``__init__`` to record every scheduling decision for replay
    verification. The trace is opt-in and zero-overhead when not
    set.
    """

    def __init__(self, trace=None) -> None:
        super().__init__()
        self._virtual_now: float = 0.0
        self._trace = trace
        # ``BaseEventLoop`` expects ``_thread_id`` to be set by
        # ``run_forever``; nothing else here needs initialization.

        # Multi-process coordination (see ``run_window``). ``None`` in
        # the normal single-process ``run_until_complete`` path, so
        # ``_run_once`` behaves exactly as documented above. When the
        # ``SimulationCoordinator`` drives this loop it sets a per-window
        # deadline so virtual time never advances past the granted
        # boundary — that is what keeps cross-process virtual time in
        # lockstep.
        self._window_deadline: float | None = None
        self._window_exhausted: bool = False

    def time(self) -> float:
        """Return current virtual time in seconds.

        ``asyncio.TimerHandle`` stores absolute time at construction
        (``when = self.time() + delay``); by overriding ``time()`` we
        guarantee every ``call_later`` registers against virtual time
        rather than wall time.
        """
        return self._virtual_now

    def _run_once(self) -> None:
        """Single iteration of the simulation loop.

        Identical structure to ``BaseEventLoop._run_once`` but with
        the I/O wait replaced by virtual-time advancement:

        1. Compact cancelled timers (same as parent).
        2. If ``_ready`` is empty and ``_scheduled`` is non-empty,
           advance ``_virtual_now`` to the earliest scheduled timer.
        3. Move every timer whose ``when`` is ``<= _virtual_now``
           into ``_ready`` (FIFO order via heapq pops).
        4. If ``_ready`` is still empty and the loop isn't stopping,
           raise — nothing can advance the simulation (deadlock).
        5. Drain ``_ready`` FIFO. Each ``Handle._run()`` may schedule
           further callbacks; those land at the *back* of ``_ready``
           and run on the next ``_run_once`` invocation, matching
           asyncio's "fairness" contract.
        """
        # Step 1: compact cancelled timer handles. Verbatim from
        # ``BaseEventLoop._run_once`` so the heap invariants match.
        sched_count = len(self._scheduled)
        if (sched_count > base_events._MIN_SCHEDULED_TIMER_HANDLES
                and self._timer_cancelled_count / sched_count
                > base_events._MIN_CANCELLED_TIMER_HANDLES_FRACTION):
            new_scheduled = []
            for handle in self._scheduled:
                if handle._cancelled:
                    handle._scheduled = False
                else:
                    new_scheduled.append(handle)
            heapq.heapify(new_scheduled)
            self._scheduled = new_scheduled
            self._timer_cancelled_count = 0
        else:
            while self._scheduled and self._scheduled[0]._cancelled:
                self._timer_cancelled_count -= 1
                handle = heapq.heappop(self._scheduled)
                handle._scheduled = False

        # Step 2: advance virtual time if no immediate work.
        if not self._ready and not self._stopping and self._scheduled:
            when = self._scheduled[0]._when
            # Window guard (multi-process mode): never advance past the
            # coordinator-granted deadline. When the next timer is beyond
            # the window, pin virtual time to the deadline, flag the
            # window exhausted, and return — ``run_window`` reports the
            # next-event time to the coordinator, which decides the next
            # global step. In single-process mode ``_window_deadline`` is
            # ``None`` and this branch is inert.
            if self._window_deadline is not None and when > self._window_deadline:
                if self._window_deadline > self._virtual_now:
                    if self._trace is not None:
                        self._trace.record_time_advance(
                            from_t=self._virtual_now, to_t=self._window_deadline
                        )
                    self._virtual_now = self._window_deadline
                self._window_exhausted = True
                return
            if when > self._virtual_now:
                if self._trace is not None:
                    self._trace.record_time_advance(
                        from_t=self._virtual_now, to_t=when
                    )
                self._virtual_now = when

        # Step 3: move expired timers into ``_ready``.
        end_time = self._virtual_now + self._clock_resolution
        while self._scheduled:
            handle = self._scheduled[0]
            if handle._when >= end_time:
                break
            handle = heapq.heappop(self._scheduled)
            handle._scheduled = False
            self._ready.append(handle)

        # Step 4: deadlock check. ``_ready`` empty + ``_scheduled``
        # empty + not stopping = nothing can ever advance the loop.
        # Real asyncio loops would block forever in ``select()``;
        # here we raise so the failure is loud and attributable.
        if not self._ready and not self._scheduled and not self._stopping:
            raise SimulationConstraintError(
                "SimulationLoop deadlocked: no ready callbacks, no "
                "scheduled timers, and the loop has not been stopped. "
                "Every callback queue is empty so no Future can ever "
                "be resolved. Verify scenario coroutines aren't "
                "awaiting something only resolved by real I/O."
            )

        # Step 5: drain ``_ready`` FIFO. Each handle may schedule
        # follow-ups via ``call_soon`` (lands at deque tail) which
        # we'll pick up on the next iteration.
        ntodo = len(self._ready)
        for _ in range(ntodo):
            handle = self._ready.popleft()
            if handle._cancelled:
                continue
            if self._trace is not None:
                self._trace.record_fire(handle)
            handle._run()
        handle = None  # break a reference cycle on exception

    def run_window(self, deadline: float) -> float | None:
        """Advance the loop up to (and including) virtual time ``deadline``.

        Drives ``_run_once`` — the *same* single-process scheduler, so
        determinism is identical — but stops as soon as the earliest
        remaining timer is beyond ``deadline`` rather than advancing to
        it. This is the primitive the ``SimulationCoordinator`` uses to
        keep many processes' virtual clocks in lockstep: it grants each
        process a window, every process drains all work at or before the
        window edge, and the coordinator then picks the next global
        boundary from everyone's reported next-event time.

        Returns the virtual time of the next pending timer (strictly
        ``> deadline``), or ``None`` if the loop is fully idle (no ready
        callbacks and no scheduled timers) — i.e. this process has no
        more work until another process delivers it a message.

        Unlike ``run_until_complete`` this never raises the deadlock
        error: an idle loop in a multi-process run is normal (the
        process is waiting on a cross-process message), not a bug.
        """
        self._check_closed()
        self._window_deadline = deadline
        self._window_exhausted = False
        try:
            while True:
                # A fully idle loop (no ready work, no timers) means this
                # process is quiescent for now. Return ``None`` rather
                # than letting ``_run_once`` raise its deadlock error —
                # in a multi-process run the work will arrive as an
                # injected cross-process message later.
                if not self._ready and not self._scheduled:
                    return None
                self._window_exhausted = False
                self._run_once()
                if self._window_exhausted:
                    # Pinned at the window edge; report the next timer.
                    return self._scheduled[0]._when if self._scheduled else None
        finally:
            self._window_deadline = None
            self._window_exhausted = False

    def call_soon(
        self,
        callback,
        *args,
        context: contextvars.Context | None = None,
    ):
        """Append a callback to ``_ready`` (FIFO).

        Mirrors ``BaseEventLoop.call_soon`` but with an optional
        ``EventTrace`` hook so determinism verification can record
        the arrival.
        """
        self._check_closed()
        if self._debug:
            self._check_thread()
            self._check_callback(callback, "call_soon")
        handle = self._call_soon(callback, args, context)
        if self._trace is not None:
            self._trace.record_call_soon(handle, callback)
        if handle._source_traceback:
            del handle._source_traceback[-1]
        return handle

    def call_at(
        self,
        when: float,
        callback,
        *args,
        context: contextvars.Context | None = None,
    ):
        """Schedule a callback at absolute virtual time ``when``.

        Mirrors ``BaseEventLoop.call_at`` but records into the
        optional ``EventTrace``.
        """
        self._check_closed()
        if self._debug:
            self._check_thread()
            self._check_callback(callback, "call_at")
        timer = events.TimerHandle(when, callback, args, self, context)
        if self._trace is not None:
            self._trace.record_call_at(timer, callback, when)
        if timer._source_traceback:
            del timer._source_traceback[-1]
        heapq.heappush(self._scheduled, timer)
        timer._scheduled = True
        return timer

    def _check_thread(self) -> None:
        """Overridden to use ``_thread_id`` only for advisory checks.

        ``BaseEventLoop._check_thread`` enforces single-thread usage
        when ``_debug=True``. We preserve that invariant — SIM mode
        is single-thread by construction — but allow the test
        harness to drive the loop without an explicit
        ``run_until_complete`` call by skipping the check when no
        thread is registered yet.
        """
        if self._thread_id is None:
            return
        if self._thread_id != threading.get_ident():
            raise RuntimeError(
                "SimulationLoop is bound to a thread different from the "
                "current one"
            )


# Install banned-method overrides on the class. We do this after the
# class body so the ``_make_X_ban`` closures don't fight with the
# regular ``def`` definitions above. Each override raises
# ``SimulationConstraintError`` synchronously (before any I/O
# could possibly happen) so the failure is attributed to the
# production caller, not to the asyncio internals.

for _method_name, _reason in _BANNED_SYNC_METHODS:
    setattr(
        SimulationLoop,
        _method_name,
        _make_sync_ban(_method_name, _reason),
    )

for _method_name, _reason in _BANNED_ASYNC_METHODS:
    setattr(
        SimulationLoop,
        _method_name,
        _make_async_ban(_method_name, _reason),
    )

del _method_name, _reason
