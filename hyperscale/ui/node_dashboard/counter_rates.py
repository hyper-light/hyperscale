class CounterRates:
    """Per-second rates of cumulative counters between successive samples.

    It holds the counts and the time of the last sample it advanced from;
    the first rates are over the interval since it was constructed with the
    node's counts then. A sample taken at the same instant as the last
    (a clock that has not moved) yields no rates and keeps the baseline, so
    the next sample's rates cover the whole interval.
    """

    def __init__(self, counts: dict[str, int], sampled_at: float) -> None:
        self._last_counts = counts
        self._last_sampled_at = sampled_at

    def advance(self, counts: dict[str, int], sampled_at: float) -> dict[str, float | None]:
        """Each counter's rate per second since the last sample, ``None``
        for every counter when no time has passed."""
        if (elapsed_seconds := sampled_at - self._last_sampled_at) <= 0:
            return dict.fromkeys(counts)

        last_counts = self._last_counts
        rates: dict[str, float | None] = {
            name: (count - last_counts.get(name, 0)) / elapsed_seconds for name, count in counts.items()
        }
        self._last_counts = counts
        self._last_sampled_at = sampled_at
        return rates
