from hyperscale.ui.styling.attributes import Attributizer
from hyperscale.ui.styling.colors import Colorizer, HighlightColorizer
from hyperscale.ui.config.mode import TerminalDisplayMode
from pydantic import BaseModel, StrictStr, StrictInt, StrictFloat
from typing import Dict, Literal, List


AnimationDirection = Literal["forward", "reverse", "bounce"]
AnimationType = Literal[
    "highlight",
    "color",
    "rotate",
    "swipe",
    "vegas",
    "stripe",
    "blink",
]

HorizontalAlignment = Literal["left", "center", "right"]

AnimationConfig = Dict[
    StrictStr,
    Dict[
        Literal[
            "animations",
            "direction",
            "primary_color",
            "primary_highlight",
            "primary_attrs",
            "secondary_color",
            "secondary_highlight",
            "secondary_attrs",
        ],
        List[AnimationType]
        | AnimationDirection
        | Colorizer
        | HighlightColorizer
        | List[Attributizer]
        | None,
    ],
]

TimeUnit = Literal["h", "m", "s"]


class AnimatedStatusBarConfig(BaseModel):
    default_status: StrictStr
    animations: AnimationConfig | None = None
    horizontal_padding: StrictInt = 0
    animation_duration: StrictInt | StrictFloat = 0.5
    animation_duration_unit: TimeUnit = "s"
    animation_direction: AnimationDirection = "forward"
    animation_type: List[AnimationType] = ["highlight"]
    horizontal_alignment: HorizontalAlignment = "center"
    terminal_mode: TerminalDisplayMode = "compatability"
