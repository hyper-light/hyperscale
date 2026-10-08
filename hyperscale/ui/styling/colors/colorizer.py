from typing import Callable
from .color import ColorName
from .extended_color import ExtendedColorName
from .highlight import HighlightName


Colorizer = (
    ColorName
    | ExtendedColorName
    | Callable[
        [object],
        ColorName | ExtendedColorName | None,
    ]
    | list[
        Callable[
            [object],
            ColorName | ExtendedColorName | None,
        ]
    ]
)


HighlightColorizer = (
    HighlightName
    | ExtendedColorName
    | Callable[
        [object],
        HighlightName | ExtendedColorName | None,
    ]
    | list[
        Callable[
            [object],
            HighlightName | ExtendedColorName | None,
        ]
    ]
)
