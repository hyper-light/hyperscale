"""
Candidate filtering for datacenter and manager selection (AD-36 Part 2).

Applies hard excludes and soft demotions based on health, staleness,
and circuit breaker state.
"""

from dataclasses import dataclass
from enum import Enum


class ExclusionReason(str, Enum):
    """Reason a candidate was excluded."""
    UNHEALTHY_STATUS = "unhealthy_status"
    NO_REGISTERED_MANAGERS = "no_registered_managers"
    ALL_MANAGERS_CIRCUIT_OPEN = "all_managers_circuit_open"
    CIRCUIT_BREAKER_OPEN = "circuit_breaker_open"
    HEARTBEAT_STALE = "heartbeat_stale"


class DemotionReason(str, Enum):
    """Reason a candidate was demoted (not excluded)."""
    STALE_HEALTH = "stale_health"
    MISSING_COORDINATES = "missing_coordinates"


@dataclass(slots=True)
class DatacenterCandidate:
    """A datacenter candidate for job routing."""

    datacenter_id: str
    health_bucket: str  # HEALTHY, BUSY, DEGRADED, UNHEALTHY
    available_cores: int
    total_cores: int
    queue_depth: int
    lhm_multiplier: float
    circuit_breaker_pressure: float  # Fraction of managers with open circuits

    # Vivaldi coordinate data
    has_coordinate: bool = False
    rtt_ucb_ms: float = 100.0  # Default conservative RTT
    coordinate_quality: float = 0.0

    # Manager count
    total_managers: int = 0
    healthy_managers: int = 0

    # Exclusion/demotion tracking
    excluded: bool = False
    exclusion_reason: ExclusionReason | None = None
    demoted: bool = False
    demotion_reason: DemotionReason | None = None
    original_bucket: str | None = None  # If demoted, the original bucket


@dataclass(slots=True)
class ManagerCandidate:
    """A manager candidate within a datacenter."""

    manager_id: str
    datacenter_id: str
    host: str
    port: int
    available_cores: int
    total_cores: int
    queue_depth: int

    # Circuit breaker state
    circuit_state: str  # CLOSED, HALF_OPEN, OPEN

    # Health
    heartbeat_stale: bool = False
    last_heartbeat_age_seconds: float = 0.0

    # Vivaldi
    has_coordinate: bool = False
    rtt_ucb_ms: float = 100.0
    coordinate_quality: float = 0.0

    # Exclusion tracking
    excluded: bool = False
    exclusion_reason: ExclusionReason | None = None


class CandidateFilter:
    """
    Filters datacenter and manager candidates (AD-36 Part 2).

    Applies hard excludes:
    - DC: UNHEALTHY status, no managers, all circuits open
    - Manager: circuit OPEN, heartbeat stale

    Applies soft demotions:
    - DC: stale health → DEGRADED, missing coords → conservative RTT
    """

    def __init__(
        self,
        heartbeat_stale_threshold_seconds: float = 60.0,
        default_rtt_ms: float = 100.0,
    ) -> None:
        self._heartbeat_stale_threshold = heartbeat_stale_threshold_seconds
        self._default_rtt_ms = default_rtt_ms

    def filter_datacenters(
        self,
        candidates: list[DatacenterCandidate],
    ) -> tuple[list[DatacenterCandidate], list[DatacenterCandidate]]:
        """
        Filter datacenter candidates.

        Args:
            candidates: List of datacenter candidates

        Returns:
            (eligible_candidates, excluded_candidates)
        """
        eligible: list[DatacenterCandidate] = []
        excluded: list[DatacenterCandidate] = []

        for candidate in candidates:
            self._apply_dc_rules(candidate)

            if candidate.excluded:
                excluded.append(candidate)
            else:
                eligible.append(candidate)

        return eligible, excluded

    def _apply_dc_rules(self, candidate: DatacenterCandidate) -> None:
        """Apply filtering rules to a datacenter candidate."""
        # Hard exclude: UNHEALTHY status
        if candidate.health_bucket == "UNHEALTHY":
            candidate.excluded = True
            candidate.exclusion_reason = ExclusionReason.UNHEALTHY_STATUS
            return

        # Hard exclude: no registered managers
        if candidate.total_managers == 0:
            candidate.excluded = True
            candidate.exclusion_reason = ExclusionReason.NO_REGISTERED_MANAGERS
            return

        # Hard exclude: all managers circuit-open
        if candidate.healthy_managers == 0 and candidate.total_managers > 0:
            candidate.excluded = True
            candidate.exclusion_reason = ExclusionReason.ALL_MANAGERS_CIRCUIT_OPEN
            return

        # Soft demotion: missing coordinates
        if not candidate.has_coordinate:
            candidate.demoted = True
            candidate.demotion_reason = DemotionReason.MISSING_COORDINATES
            candidate.rtt_ucb_ms = self._default_rtt_ms
            candidate.coordinate_quality = 0.0

    def filter_managers(
        self,
        candidates: list[ManagerCandidate],
    ) -> tuple[list[ManagerCandidate], list[ManagerCandidate]]:
        """
        Filter manager candidates within a datacenter.

        Args:
            candidates: List of manager candidates

        Returns:
            (eligible_candidates, excluded_candidates)
        """
        eligible: list[ManagerCandidate] = []
        excluded: list[ManagerCandidate] = []

        for candidate in candidates:
            self._apply_manager_rules(candidate)

            if candidate.excluded:
                excluded.append(candidate)
            else:
                eligible.append(candidate)

        return eligible, excluded

    def _apply_manager_rules(self, candidate: ManagerCandidate) -> None:
        """Apply filtering rules to a manager candidate."""
        # Hard exclude: circuit breaker OPEN
        if candidate.circuit_state == "OPEN":
            candidate.excluded = True
            candidate.exclusion_reason = ExclusionReason.CIRCUIT_BREAKER_OPEN
            return

        # Hard exclude: heartbeat stale
        if candidate.last_heartbeat_age_seconds > self._heartbeat_stale_threshold:
            candidate.excluded = True
            candidate.exclusion_reason = ExclusionReason.HEARTBEAT_STALE
            candidate.heartbeat_stale = True
            return

        # Apply default RTT if missing coordinate
        if not candidate.has_coordinate:
            candidate.rtt_ucb_ms = self._default_rtt_ms
            candidate.coordinate_quality = 0.0
