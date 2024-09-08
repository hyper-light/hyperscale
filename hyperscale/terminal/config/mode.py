from enum import Enum
from typing import Literal

TerminalDisplayMode = Literal["extended", "compatability"]


class TerminalMode(Enum):
    EXTENDED = "EXTENDED"
    COMPATIBILITY = "COMPATIBILITY"

    @classmethod
    def to_mode(self, mode: TerminalDisplayMode):
        return (
            TerminalMode.EXTENDED if mode == "extended" else TerminalMode.COMPATIBILITY
        )
