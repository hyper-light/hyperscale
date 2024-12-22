from enum import Enum
from typing import Dict, Literal

CharsetPosition = Literal[
    "START",
    "BETWEEN",
    "END",
]


class CharsetPositionType(Enum):
    START = "START"
    BETWEEN = "BETWEEN"
    END = "END"


class CharsetPositionMap:
    def __init__(self):
        self._position_map: Dict[
            CharsetPosition,
            CharsetPositionType,
        ] = {
            "START": CharsetPositionType.START,
            "BETWEEN": CharsetPositionType.BETWEEN,
            "END": CharsetPositionType.END,
        }

    def by_name(self, position: CharsetPosition):
        return self._position_map.get(position.upper())
