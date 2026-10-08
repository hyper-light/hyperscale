def line_is_full(used_width: int, item_width: int, line_width: int) -> bool:
    """Whether an item ``item_width`` wide (with its gap) passes the end of
    a line ``used_width`` of whose ``line_width`` is used; never for an
    empty line, which takes any item."""
    return used_width > 0 and used_width + item_width > line_width


def flow_into_lines(item_widths: list[int], line_width: int, gap_width: int) -> list[list[int]]:
    """Items laid out left to right in their order, ``gap_width`` apart,
    wrapping to a new line where the next would pass ``line_width``: each
    line's item indexes."""
    lines: list[list[int]] = [[]]
    # Every item is counted with a gap before it, and the line with a gap
    # before its start, so the first item's gap costs nothing.
    used_width = 0
    for item_index, item_width in enumerate(item_widths):
        if line_is_full(used_width, item_width + gap_width, line_width + gap_width):
            lines.append([])
            used_width = 0

        lines[-1].append(item_index)
        used_width += item_width + gap_width

    return lines
