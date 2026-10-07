import inspect
from .attributes import Attributizer
from .colors import Colorizer, HighlightColorizer


def get_style(
    stylizer: Colorizer | HighlightColorizer | Attributizer,
    *args: object,
    **kwargs: object,
):
    if isinstance(stylizer, str):
        return stylizer

    elif inspect.isfunction(stylizer):
        return stylizer(*args, **kwargs)

    elif isinstance(stylizer, list):
        for style_func in stylizer:
            if style := style_func(*args, **kwargs):
                return style
