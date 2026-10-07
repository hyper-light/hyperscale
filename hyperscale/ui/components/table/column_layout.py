"""Column widths sized to a table's content.

A table that sizes its columns to their content (``TableConfig.
size_columns_to_content``) never truncates its identifying column -- the
first, and any ``fixed`` one -- whatever the width:

- every column first gets the width of its widest value; the identifying
  columns also the width of their header (another column's header is cut
  to its values' width before any of them is dropped);
- the space left goes back to the cut headers, left to right, then is
  shared evenly, so the table spans its width;
- when even the values do not fit, columns are dropped from the right --
  the lowest-priority column is the last one -- never the first column or
  a fixed one, until the rest fit.
"""

from collections.abc import Callable, Mapping

# The border characters a cell takes besides its text: (index, count of
# visible columns) -> characters.
CellBorderLength = Callable[[int, int], int]


def values_width(texts: list[str]) -> int:
    """The width of the widest of a column's values."""
    return max(map(len, texts), default=0)


def natural_widths(visible: list[str], column_texts: Mapping[str, list[str]]) -> list[int]:
    """Each column's width with its header and every value whole."""
    return [max(len(header), values_width(column_texts[header])) for header in visible]


def minimum_widths(visible: list[str], column_texts: Mapping[str, list[str]], identifying: set[str]) -> list[int]:
    """Each column's narrowest width: its values whole, and for an
    identifying column its header too."""
    return [
        max(values_width(column_texts[header]), len(header) * (header in identifying), 1)
        for header in visible
    ]


def border_lengths(visible: list[str], cell_border_length: CellBorderLength) -> list[int]:
    """Each visible column's border characters."""
    return [cell_border_length(index, len(visible)) for index in range(len(visible))]


def grown_widths(minimum: list[int], natural: list[int], slack: int) -> list[int]:
    """``minimum`` widths given ``slack`` more columns: cut headers are
    restored left to right, then what is left is shared evenly."""
    widths = list(minimum)
    for index in range(len(widths)):
        growth = min(natural[index] - widths[index], slack)
        widths[index] += growth
        slack -= growth

    share, remainder = divmod(slack, len(widths))
    return [width + share + (index < remainder) for index, width in enumerate(widths)]


def fitted_sizes(
    visible: list[str],
    column_texts: Mapping[str, list[str]],
    identifying: set[str],
    max_width: int,
    cell_border_length: CellBorderLength,
) -> list[int] | None:
    """The visible columns' sizes (text and borders) spanning ``max_width``,
    or None when their values do not fit in it."""
    borders = border_lengths(visible, cell_border_length)
    minimum = minimum_widths(visible, column_texts, identifying)
    slack = max_width - sum(borders) - sum(minimum)
    if slack < 0:
        return None

    widths = grown_widths(minimum, natural_widths(visible, column_texts), slack)
    return [border + width for border, width in zip(borders, widths)]


def last_droppable(visible: list[str], identifying: set[str]) -> str | None:
    """The lowest-priority column that may be dropped: the last one that
    is not identifying (the first column never is)."""
    return next((header for header in reversed(visible[1:]) if header not in identifying), None)


def without(visible: list[str], dropped: str) -> list[str]:
    """``visible`` with the column ``dropped`` left out."""
    return [header for header in visible if header != dropped]


def even_sizes(count: int, max_width: int) -> list[int]:
    """``max_width`` shared evenly by ``count`` columns."""
    share, remainder = divmod(max_width, count)
    return [share + (index < remainder) for index in range(count)]


def sizes_or_even(sizes: list[int] | None, visible: list[str], max_width: int) -> list[int]:
    """``sizes``, or -- where not even the identifying columns' values fit
    the width -- the width shared evenly (the table is narrower than one
    identifier; nothing is left to drop)."""
    return sizes if sizes is not None else even_sizes(len(visible), max_width)


def content_column_layout(
    headers: list[str],
    column_texts: Mapping[str, list[str]],
    identifying: set[str],
    max_width: int,
    cell_border_length: CellBorderLength,
) -> tuple[list[str], list[int]]:
    """The columns a ``max_width`` table shows, and each one's size."""
    visible = list(headers)
    while (sizes := fitted_sizes(visible, column_texts, identifying, max_width, cell_border_length)) is None and (
        dropped := last_droppable(visible, identifying)
    ) is not None:
        visible = without(visible, dropped)

    return visible, sizes_or_even(sizes, visible, max_width)
