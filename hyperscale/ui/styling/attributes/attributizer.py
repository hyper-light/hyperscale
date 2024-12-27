from typing import Callable, Any
from .attribute import AttributeName


Attributizer = (
    AttributeName
    | Callable[
        [Any],
        AttributeName | None,
    ]
    | list[
        Callable[
            [Any],
            AttributeName | None,
        ]
    ]
)
