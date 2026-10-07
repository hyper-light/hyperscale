import itertools
import math
from collections import deque

ChartPoint = tuple[float, float]


class NodeDashboardChartSeries:
    """The recent samples of a dashboard's charts, as the points each chart
    plots.

    It holds the last ``window_seconds`` of samples at the dashboard's
    sampling interval -- a bounded deque, so a node that runs for days
    holds no more than one window -- and plots each sample at its time in
    seconds since the oldest sample in the window, as the run UI plots its
    completions against the seconds since its run began: a dashboard
    started moments ago spreads its few samples across its chart, and once
    a window of samples is held the points move left as samples arrive,
    like a strip chart.
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

    def _window_samples(self) -> list[tuple[float, list[float | None]]]:
        """The held samples inside the window, oldest first."""
        window_start = self._newest_sampled_at - self.window_seconds
        # Samples are held oldest first: those inside the window are the
        # newest ones.
        return list(itertools.dropwhile(lambda sample: sample[0] <= window_start, self._samples))

    def newest_value(self, chart_index: int) -> float | None:
        """Chart ``chart_index``'s newest value inside the window -- its
        current reading (a sample without a value, such as one taken
        before the clock moved, keeps the one before) -- or None when the
        window holds none."""
        return next(
            (
                chart_values[chart_index]
                for _, chart_values in reversed(self._window_samples())
                if chart_values[chart_index] is not None
            ),
            None,
        )

    def points(self, chart_index: int) -> list[ChartPoint]:
        """The points of chart ``chart_index``: (seconds since the oldest
        sample inside the window, value) for each held sample inside the
        window that has a value for it. A sample can be held yet older than
        the window when sampling stalled or the clock leapt; it is not
        plotted. Every chart measures from the same oldest sample, whether
        or not it has a value for that chart."""
        window_samples = self._window_samples()
        # With none in the window there is no point to place.
        oldest_sampled_at, _ = next(iter(window_samples), (0.0, []))
        return [
            (sampled_at - oldest_sampled_at, chart_values[chart_index])
            for sampled_at, chart_values in window_samples
            if chart_values[chart_index] is not None
        ]
