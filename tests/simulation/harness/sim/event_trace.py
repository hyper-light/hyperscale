"""
Deterministic event-trace recorder for replay-equivalence
verification.

Why a trace
-----------

The Phase 6 determinism contract requires that two
``SimulationLoop`` runs from the same seed and same scenario
produce byte-identical scheduling decisions. The trace records
each decision — ``call_soon`` arrivals, ``call_at`` registrations,
``Handle._run`` fires, virtual-time advances — as a structured
log. Two traces are equivalent iff their entry sequences are
identical.

Recording cost
--------------

The trace hook on ``SimulationLoop`` is opt-in. When
``self._trace`` is ``None`` the hot path has one ``is None`` check
per scheduling decision; no allocation, no formatting.

When enabled, each entry stores:

- ``virtual_time`` — the loop's ``_virtual_now`` at the moment of
  the decision (a float).
- ``op_kind`` — one of ``call_soon`` / ``call_at`` / ``fire`` /
  ``time_advance``, indicating which decision was made.
- ``descriptor`` — a stable, hashable identifier for the callback.
  We use ``callable.__qualname__`` rather than ``repr(callable)``
  because ``repr`` includes object ``id()`` which varies across
  runs. For ``functools.partial`` and lambdas the descriptor is a
  best-effort string; equivalence relies on the scenario using
  the same callable references at the same scheduling moments,
  which is true for deterministic production code.

Why we don't capture arguments
------------------------------

Argument repr can include object ``id()``, dict ordering effects,
or non-stable string representations. The descriptor-based
identity (``qualname`` of the callback) is sufficient because:

- The set of scheduled callbacks is fixed by the production code.
- The order they're scheduled in is what we care about — the
  arguments are a function of the prior scheduling decisions.
- If two runs schedule the same callable in the same order with
  different arguments, that's a non-determinism in the arguments
  themselves, which surfaces as a downstream behavioral difference
  the scenario will catch.

Anyone debugging a determinism-gate failure can re-enable a
verbose mode (separate hook) that adds ``repr(args)`` per entry;
the production gate uses the lean trace for fast equivalence
checks.
"""

from dataclasses import dataclass
from typing import Callable, Literal


TraceOpKind = Literal["call_soon", "call_at", "fire", "time_advance"]


@dataclass(frozen=True, slots=True)
class TraceEntry:
    """One scheduling decision in a ``SimulationLoop`` run.

    Frozen + slotted so entries are hashable and lightweight.
    Equality compares all fields, which is what ``EventTrace.equals``
    relies on for byte-equivalence checking.
    """

    virtual_time: float
    op_kind: TraceOpKind
    descriptor: str
    when: float | None = None


class EventTrace:
    """Append-only log of ``SimulationLoop`` scheduling decisions.

    The loop calls ``record_call_soon`` / ``record_call_at`` /
    ``record_fire`` / ``record_time_advance`` at each decision
    point. Entries are appended in order; the trace is read back
    via ``entries`` (a tuple, frozen for safe comparison).
    """

    __slots__ = ("_entries", "_loop")

    def __init__(self, loop) -> None:
        self._entries: list[TraceEntry] = []
        self._loop = loop

    @property
    def entries(self) -> tuple[TraceEntry, ...]:
        """Snapshot of recorded entries in arrival order."""
        return tuple(self._entries)

    def record_call_soon(self, handle, callback: Callable) -> None:
        """Record a ``call_soon`` arrival.

        ``virtual_time`` is the loop's time at the moment of
        registration (which equals the time the callback will fire
        at, since ``call_soon`` schedules for the current instant).
        """
        self._entries.append(TraceEntry(
            virtual_time=self._loop.time(),
            op_kind="call_soon",
            descriptor=_describe(callback),
        ))

    def record_call_at(
        self,
        handle,
        callback: Callable,
        when: float,
    ) -> None:
        """Record a ``call_at`` / ``call_later`` registration.

        ``virtual_time`` is the time at registration; ``when`` is
        the future fire time. Both contribute to equivalence —
        registrations at different times-of-registration are not
        equivalent even if they fire at the same ``when``.
        """
        self._entries.append(TraceEntry(
            virtual_time=self._loop.time(),
            op_kind="call_at",
            descriptor=_describe(callback),
            when=when,
        ))

    def record_fire(self, handle) -> None:
        """Record a ``Handle._run()`` fire.

        The descriptor is reconstructed from ``handle._callback`` —
        the same callable that was passed to ``call_soon`` /
        ``call_at`` at registration. ``virtual_time`` is the time at
        which the callback runs.
        """
        callback = getattr(handle, "_callback", None)
        self._entries.append(TraceEntry(
            virtual_time=self._loop.time(),
            op_kind="fire",
            descriptor=_describe(callback),
        ))

    def record_time_advance(self, from_t: float, to_t: float) -> None:
        """Record a virtual-time advance.

        Time advances are part of the trace because a determinism
        failure could manifest as the loop advancing to different
        times across runs — useful diagnostic.
        """
        self._entries.append(TraceEntry(
            virtual_time=to_t,
            op_kind="time_advance",
            descriptor=f"from={from_t}",
            when=to_t,
        ))

    def equals(self, other: "EventTrace") -> bool:
        """Return True iff ``self.entries == other.entries`` exactly.

        Used by the determinism gate test
        (``tests/simulation/determinism/test_replay_equivalence.py``).
        Failures dump both traces side-by-side via ``diff_against``.
        """
        return self.entries == other.entries

    def diff_against(self, other: "EventTrace") -> list[str]:
        """Return a per-entry diff between two traces.

        Used by the determinism gate test to surface *which*
        scheduling decision diverged when the equivalence check
        fails. Returns an empty list when the traces are equal.
        """
        diffs: list[str] = []
        own = self.entries
        their = other.entries
        for index, (a, b) in enumerate(zip(own, their)):
            if a != b:
                diffs.append(f"#{index}: A={a!r} B={b!r}")
        if len(own) != len(their):
            diffs.append(
                f"length differs: A={len(own)} B={len(their)}; "
                f"longer trace's tail: "
                f"{(own if len(own) > len(their) else their)[len(diffs):]}"
            )
        return diffs


def _describe(callback) -> str:
    """Stable identifier for a callable.

    Uses ``__qualname__`` when available (covers regular functions,
    bound methods, and class methods). Falls back to
    ``repr(callable)`` without object ``id()`` — for lambdas this
    means the source location string, which is deterministic across
    runs of the same code.
    """
    if callback is None:
        return "<no-callback>"
    qualname = getattr(callback, "__qualname__", None)
    if qualname:
        return qualname
    return type(callback).__name__
