"""A multi-series plot's legend: which series it lists, in what order,
and each entry's text."""

from .models import PlotSeries
from .plot_config import PlotConfig
from .point_char import PointChar

# The space between a legend entry's point character, name and reading.
ENTRY_GAP = " "


def legend_order(config: PlotConfig) -> list[str]:
    """The names of the series the legend lists, in its order: the
    config's ``legend_order``, else the order the series are declared (and
    drawn) in."""
    if config.legend_order is not None:
        return config.legend_order

    return [series.name for series in config.series]


def legend_series(config: PlotConfig) -> list[PlotSeries]:
    """The series the legend lists, in its order."""
    series_by_name = {series.name: series for series in config.series}
    return [series_by_name[name] for name in legend_order(config)]


def legend_entry_text(series: PlotSeries, readings: dict[str, str]) -> str:
    """A series' legend entry: its point character, its name and -- where
    the update gives one -- its current reading."""
    return ENTRY_GAP.join(filter(None, (PointChar.by_name(series.point_char), series.name, readings.get(series.name))))
