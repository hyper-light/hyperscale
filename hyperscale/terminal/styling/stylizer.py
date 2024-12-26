import inspect
from typing import Tuple, Dict, Any
from .attributes import Attributizer
from .colors import Colorizer, HighlightColorizer


def get_style(
    stylizer: Colorizer | HighlightColorizer | Attributizer,
    *args: Tuple[Any, ...],
    **kwargs: Dict[str, Any],
):
    if isinstance(stylizer, str):
        return stylizer

    elif inspect.isfunction(stylizer):
        return stylizer(*args, **kwargs)

    elif isinstance(stylizer, list):
        for style_func in stylizer:
            if style := style_func(*args, **kwargs):
                return style
