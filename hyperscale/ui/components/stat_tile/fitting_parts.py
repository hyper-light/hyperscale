from hyperscale.ui.styling.tones import PaletteColor

# A run of text and the color it is drawn in.
TextPart = tuple[str, PaletteColor]


def fitting_parts(parts: list[TextPart], width: int, separator_width: int) -> list[TextPart]:
    """The leading ``parts`` that fit in ``width`` columns, joined by a
    separator ``separator_width`` wide: the first part that does not fit
    ends them, so a less important part never shows in place of a more
    important one."""
    used_width = -separator_width
    shown: list[TextPart] = []
    for text, color in parts:
        used_width += separator_width + len(text)
        if used_width > width:
            break

        shown.append((text, color))

    return shown
