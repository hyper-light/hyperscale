from enum import Enum
from typing import Dict, Literal


class StartCharTyping(Enum):
    EMPTY = ""
    BLOCK_BRACE = "["
    PAREN = "("
    DOT_BLOCK = "⡇"


StartCharName = Literal[
    "empty",
    "block_brace",
    "paren",
    "dot_block",
]


class StartChar:
    names: Dict[StartCharName, str] = {
        char.name.lower(): char.value for char in StartCharTyping
    }

    types: Dict[StartCharTyping, str] = {char: char.value for char in StartCharTyping}

    def __iter__(self):
        for name in self.names:
            yield name

    def __contains__(self, char: str):
        return char in self.names

    @classmethod
    def by_name(
        cls,
        char: StartCharName,
        default: str | None = None,
    ):
        return cls.names.get(
            char,
            default if default else cls.names.get("empty"),
        )

    @classmethod
    def by_type(
        cls,
        char: StartCharTyping,
        default: str | None = None,
    ):
        return cls.types.get(
            char,
            default if default else cls.types.get(StartCharTyping.EMPTY),
        )
