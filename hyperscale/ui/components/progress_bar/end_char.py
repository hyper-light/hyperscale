from enum import Enum
from typing import Dict, Literal


class EndCharType(Enum):
    EMPTY = ""
    BLOCK_BRACE = "]"
    PAREN = ")"
    DOT_BLOCK = "⢸"


EndCharName = Literal[
    "empty",
    "block_brace",
    "paren",
    "dot_block",
]


class EndChar:
    names: Dict[EndCharName, str] = {
        char.name.lower(): char.value for char in EndCharType
    }

    types: Dict[EndCharType, str] = {char: char.value for char in EndCharType}

    def __iter__(self):
        for name in self.names:
            yield name

    def __contains__(self, char: str):
        return char in self.names

    @classmethod
    def by_name(
        cls,
        char: EndCharName,
        default: str | None = None,
    ):
        return cls.names.get(
            char,
            default if default else cls.names.get("empty"),
        )

    @classmethod
    def by_type(
        cls,
        char: EndCharType,
        default: str | None = None,
    ):
        return cls.types.get(
            char,
            default if default else cls.types.get(EndCharType.EMPTY),
        )
