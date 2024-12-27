from typing import Callable, Any
from .color import ColorName
from .extended_color import ExtendedColorName
from .highlight import HighlightName


Colorizer = (
    ColorName
    | ExtendedColorName
    | Callable[
        [Any],
        ColorName | ExtendedColorName | None,
    ]
    | list[
        Callable[
            [Any],
            ColorName | ExtendedColorName | None,
        ]
    ]
)


HighlightColorizer = (
    HighlightName
    | ExtendedColorName
    | Callable[
        [Any],
        HighlightName | ExtendedColorName | None,
    ]
    | list[
        Callable[
            [Any],
            HighlightName | ExtendedColorName | None,
        ]
    ]
)
