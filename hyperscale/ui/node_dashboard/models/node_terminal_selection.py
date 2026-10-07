from dataclasses import dataclass

from hyperscale.core.jobs.models import TerminalMode


@dataclass(slots=True, frozen=True)
class NodeTerminalSelection:
    """The terminal mode a node's dashboard runs in, and -- when it is not
    the mode configured -- why (``None`` when it is)."""

    mode: TerminalMode
    degraded_reason: str | None
