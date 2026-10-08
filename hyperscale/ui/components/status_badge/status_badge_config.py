from pydantic import BaseModel

from hyperscale.ui.config.mode import TerminalDisplayMode


class StatusBadgeConfig(BaseModel):
    """A line of status badges' terminal mode."""

    terminal_mode: TerminalDisplayMode = "compatability"
