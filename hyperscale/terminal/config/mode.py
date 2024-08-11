from enum import Enum
from typing import Literal


class TerminalMode(Enum):
    EXTENDED = "EXTENDED"
    COMPATIBILITY = "COMPATIBILITY"

    @classmethod
    def to_mode(self, mode: Literal["extended", "compatability"]):
        return (
            TerminalMode.EXTENDED if mode == "extended" else TerminalMode.COMPATIBILITY
        )
