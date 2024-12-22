from enum import Enum
from typing import Dict, Literal

CellAlignment = Literal[
    "LEFT",
    "CENTER",
    "RIGHT",
]


class CellAlignmentType(Enum):
    LEFT = "LEFT"
    CENTER = "CENTER"
    RIGHT = "RIGHT"


class CellAlignmentMap:
    def __init__(self):
        self._alignment_map: Dict[
            CellAlignment,
            CellAlignmentType,
        ] = {
            "LEFT": CellAlignmentType.LEFT,
            "CENTER": CellAlignmentType.CENTER,
            "RIGHT": CellAlignmentType.RIGHT,
        }

    def by_name(self, alignment: CellAlignment) -> CellAlignmentType:
        return self._alignment_map.get(
            alignment.upper(),
            CellAlignmentType.LEFT,
        )
