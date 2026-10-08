"""
LogicalIdGenerator — deterministic-unique ids for logical entities
(jobs, workflows).

Logical ids need UNIQUENESS, not unpredictability — the AD-40
idempotency key carries the anti-replay/anti-guess burden, and nothing
authorizes by job id. That distinction picks the construction:

* NOT ``secrets`` / ``uuid4``: wall-entropy ids differ per run, which
  breaks SIM byte-identical replay (job ids appear in every message
  and WAL record downstream of submission).
* NOT the shared seeded ``Random`` seam: protocol decisions (probe
  order, jitter) draw from that stream, so consuming draws for ids
  reshuffles the entire downstream schedule (proved when seeding
  taskex ids broke a pinned scenario).

Identity + injected-``Clock`` monotonic nanoseconds + a per-instance
monotone counter gives collision-free ids that are deterministic under
SIM (virtual clock), unique across clients (the scope embeds host and
port), unique within a process (the counter breaks same-nanosecond
ties), and unique across restarts (the clock component advances).
"""

from hyperscale.distributed.runtime import Clock


class LogicalIdGenerator:
    """Generates ``{prefix}-{scope}-{monotonic_ns}-{sequence}`` ids.

    ``scope`` should embed the owning node's identity (host-port);
    ``clock`` is the runtime seam Clock (virtual under SIM). Both are
    REQUIRED — id generation must never fall back to module globals.
    """

    __slots__ = ("_scope", "_clock", "_sequence")

    def __init__(self, scope: str, clock: Clock) -> None:
        self._scope = scope
        self._clock = clock
        self._sequence = 0

    def generate(self, prefix: str) -> str:
        sequence = self._sequence
        self._sequence += 1
        return (
            f"{prefix}-{self._scope}-"
            f"{self._clock.monotonic_ns()}-{sequence}"
        )
