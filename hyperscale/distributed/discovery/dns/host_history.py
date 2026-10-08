"""``HostHistory`` -- pickled under the namespace
``hyperscale.distributed.discovery.dns.security`` (see that module)."""

from dataclasses import dataclass, field

from .security_shared import _DEFAULT_CLOCK


@dataclass(slots=True)
class HostHistory:
    """Tracks historical IP resolutions for a hostname."""

    last_answer: frozenset[str] = frozenset()
    """The addresses of the most recent answer for this host."""

    last_change_time: float = 0.0
    """Monotonic time of last IP change."""

    change_count: int = 0
    """Number of IP changes in the tracking window."""

    window_start_time: float = field(default_factory=lambda: _DEFAULT_CLOCK.monotonic())
    """Start of the current tracking window."""
