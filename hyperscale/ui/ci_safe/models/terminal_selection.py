from dataclasses import dataclass

from hyperscale.core.jobs.models import TerminalMode


@dataclass(slots=True, frozen=True)
class TerminalSelection:
    """The terminal mode a terminal UI -- a node's dashboard or a run's
    progress -- runs in, and, when it is not the mode configured, why
    (``None`` when it is)."""

    mode: TerminalMode
    degraded_reason: str | None
