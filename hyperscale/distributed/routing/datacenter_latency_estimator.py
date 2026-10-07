"""
Per-datacenter latency estimates for routing and spillover (AD-36, AD-45).
"""

from collections.abc import Callable, Iterable

from hyperscale.distributed.models.coordinates import NetworkCoordinate
from hyperscale.distributed.swim.coordinates.coordinate_tracker import (
    CoordinateTracker,
)


class DatacenterLatencyEstimator:
    """
    Estimates the latency to each datacenter from the evidence the gate has.

    Two kinds of evidence, each with its own confidence: a datacenter's
    Vivaldi RTT upper confidence bound (AD-35), weighted by the quality of
    the coordinate it came from, and its observed time to accept a dispatch
    (AD-45), weighted by the observations' confidence. Whatever weight the
    evidence lacks goes to a conservative prior -- the farthest latency
    evidenced for any datacenter (AD-36 Part 2: a missing coordinate keeps
    a datacenter but scores it conservatively):

        predicted = quality * rtt_ucb + (1 - quality) * prior
        estimate  = confidence * observed + (1 - confidence) * predicted

    So a datacenter the gate knows nothing about is estimated as far as the
    farthest one it knows, a low-quality coordinate is pulled toward that
    bound in proportion to its doubt, and with no evidence for any
    datacenter every estimate is the same and latency drops out of the
    ranking. No estimate is below AD-35's floor for an RTT estimate
    (``VivaldiConfig.rtt_min_ms``); the prior starts there, so a score
    multiplied by the estimate never collapses to zero.

    While the gate's own coordinate has not converged (AD-36 Part 6),
    Vivaldi distances measured from it are not evidence: only observed
    latency separates datacenters.

    Every caller passes the same universe -- every datacenter the gate
    knows -- so routing and spillover estimate against one prior.
    """

    def __init__(
        self,
        coordinate_tracker: CoordinateTracker,
        get_datacenter_coordinate: Callable[[str], NetworkCoordinate | None],
        get_observed_latency: Callable[[str], tuple[float, float]],
    ) -> None:
        self._coordinate_tracker = coordinate_tracker
        self._get_datacenter_coordinate = get_datacenter_coordinate
        self._get_observed_latency = get_observed_latency
        self._latency_floor_ms = coordinate_tracker.get_config().rtt_min_ms

    def estimate(self, datacenter_ids: Iterable[str]) -> dict[str, float]:
        """Latency estimates in milliseconds, keyed by datacenter id."""
        coordinates_are_evidence = self._coordinate_tracker.is_converged()
        prior_ms = self._latency_floor_ms
        evidence: list[tuple[str, float, float, float, float]] = []
        for datacenter_id in datacenter_ids:
            datacenter_evidence, prior_ms = self._datacenter_evidence(
                datacenter_id, coordinates_are_evidence, prior_ms
            )
            evidence.append(datacenter_evidence)

        return {
            datacenter_id: max(
                self._latency_floor_ms,
                confidence * observed_ms
                + (1.0 - confidence)
                * (quality * rtt_ucb_ms + (1.0 - quality) * prior_ms),
            )
            for datacenter_id, rtt_ucb_ms, quality, observed_ms, confidence in evidence
        }

    def _datacenter_evidence(
        self,
        datacenter_id: str,
        coordinates_are_evidence: bool,
        prior_ms: float,
    ) -> tuple[tuple[str, float, float, float, float], float]:
        """One datacenter's (id, rtt_ucb_ms, quality, observed_ms,
        confidence) and the conservative prior raised by whatever of it is
        evidence (AD-36 Part 2, AD-45)."""
        coordinate = (
            self._get_datacenter_coordinate(datacenter_id) if coordinates_are_evidence else None
        )
        rtt_ucb_ms, quality, prior_ms = self._coordinate_evidence(coordinate, prior_ms)

        observed_ms, confidence = self._get_observed_latency(datacenter_id)
        if confidence > 0.0:
            prior_ms = max(prior_ms, observed_ms)

        return (datacenter_id, rtt_ucb_ms, quality, observed_ms, confidence), prior_ms

    def _coordinate_evidence(
        self,
        coordinate: NetworkCoordinate | None,
        prior_ms: float,
    ) -> tuple[float, float, float]:
        """A coordinate's (rtt_ucb_ms, quality, prior_ms): its Vivaldi RTT
        upper bound counts (AD-35) only when its quality is positive."""
        if coordinate is None:
            return 0.0, 0.0, prior_ms
        if not (quality := self._coordinate_tracker.coordinate_quality(coordinate)) > 0.0:
            return 0.0, quality, prior_ms
        rtt_ucb_ms = self._coordinate_tracker.estimate_rtt_ucb_ms(coordinate)
        return rtt_ucb_ms, quality, max(prior_ms, rtt_ucb_ms)
