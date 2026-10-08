"""
``RetransmissionTimeout``: the client's RFC 6298 retransmission timeout,
estimated from the round trips its job submissions measure.
"""

# RFC 6298 section 2.3: the smoothing gain of the round-trip time and of its
# variation, and the variation's multiplier in the timeout.
SMOOTHED_ROUND_TRIP_GAIN = 1 / 8
ROUND_TRIP_VARIATION_GAIN = 1 / 4
ROUND_TRIP_VARIATION_MULTIPLIER = 4


class RetransmissionTimeout:
    """
    How long a client waits before sending again a request whose exchange
    failed outright -- a timeout, a refused or reset connection -- or was
    refused without the server's retry hint (RFC 6298).

    Each measured round trip (a request's send to its answer, refusal or
    acceptance) updates the smoothed round-trip time and its variation as
    RFC 6298 section 2.3 does; the timeout is the smoothed round trip plus
    four variations, never below the minimum (section 2.4), and the minimum
    itself before any round trip has been measured (section 2.1). The
    retry ladder doubles it per failed attempt (section 5.5). Clock
    granularity, section 2.3's G, is omitted: the monotonic clock's
    resolution is far below any round trip.
    """

    __slots__ = (
        "_minimum_seconds",
        "_smoothed_round_trip_seconds",
        "_round_trip_variation_seconds",
    )

    def __init__(self, minimum_seconds: float) -> None:
        self._minimum_seconds = minimum_seconds
        self._smoothed_round_trip_seconds: float | None = None
        self._round_trip_variation_seconds = 0.0

    @property
    def seconds(self) -> float:
        """The current retransmission timeout."""
        if self._smoothed_round_trip_seconds is None:
            return self._minimum_seconds
        return max(
            self._minimum_seconds,
            self._smoothed_round_trip_seconds
            + ROUND_TRIP_VARIATION_MULTIPLIER * self._round_trip_variation_seconds,
        )

    def record_round_trip(self, round_trip_seconds: float) -> None:
        """Fold one measured round trip into the estimate (RFC 6298 sections 2.2, 2.3)."""
        if (smoothed_round_trip_seconds := self._smoothed_round_trip_seconds) is None:
            self._smoothed_round_trip_seconds = round_trip_seconds
            self._round_trip_variation_seconds = round_trip_seconds / 2
            return
        self._round_trip_variation_seconds = (
            1 - ROUND_TRIP_VARIATION_GAIN
        ) * self._round_trip_variation_seconds + ROUND_TRIP_VARIATION_GAIN * abs(
            smoothed_round_trip_seconds - round_trip_seconds
        )
        self._smoothed_round_trip_seconds = (
            1 - SMOOTHED_ROUND_TRIP_GAIN
        ) * smoothed_round_trip_seconds + SMOOTHED_ROUND_TRIP_GAIN * round_trip_seconds
