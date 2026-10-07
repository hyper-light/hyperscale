import itertools
import math
from collections import deque

ChartPoint = tuple[float, float]


class NodeDashboardChartSeries:
    """The recent samples of a dashboard's charts, as the points each chart
    plots.

    It holds the last ``window_seconds`` of samples at the dashboard's
    sampling interval -- a bounded deque, so a node that runs for days
    holds no more than one window -- and plots each sample at its place in
    the window: the newest at ``window_seconds``, one a window old at 0.
    The points move left as samples arrive, like a strip chart.
    """

    def __init__(self, window_seconds: float, sample_interval_seconds: float) -> None:
        self.window_seconds = window_seconds
        self._samples: deque[tuple[float, list[float | None]]] = deque(
            maxlen=math.ceil(window_seconds / sample_interval_seconds)
        )
        self._newest_sampled_at = 0.0

    def record(self, sampled_at: float, chart_values: list[float | None]) -> None:
        """Add one sample: its time on the node's monotonic clock and one
        value per chart (``None`` where a chart has none)."""
        self._samples.append((sampled_at, chart_values))
        self._newest_sampled_at = sampled_at

    def points(self, chart_index: int) -> list[ChartPoint]:
        """The points of chart ``chart_index``: (place in the window in
        seconds, value) for each held sample inside the window that has a
        value for it. A sample can be held yet older than the window when
        sampling stalled or the clock leapt; it is not plotted."""
        window_start = self._newest_sampled_at - self.window_seconds
        # Samples are held oldest first: those inside the window are the
        # newest ones.
        return [
            (sampled_at - window_start, chart_values[chart_index])
            for sampled_at, chart_values in itertools.dropwhile(
                lambda sample: sample[0] <= window_start, self._samples
            )
            if chart_values[chart_index] is not None
        ]
