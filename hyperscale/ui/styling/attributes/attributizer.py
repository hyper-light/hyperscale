from typing import Callable
from .attribute import AttributeName


Attributizer = (
    AttributeName
    | Callable[
        [object],
        AttributeName | None,
    ]
    | list[
        Callable[
            [object],
            AttributeName | None,
        ]
    ]
)
