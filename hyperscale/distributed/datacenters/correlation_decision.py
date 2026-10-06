"""``CorrelationDecision`` -- pickled under the namespace
``hyperscale.distributed.datacenters.cross_dc_correlation`` (see that module)."""

from dataclasses import dataclass, field

from .correlation_severity import CorrelationSeverity


@dataclass(slots=True)
class CorrelationDecision:
    """Result of correlation analysis."""

    severity: CorrelationSeverity
    reason: str
    affected_datacenters: list[str] = field(default_factory=list)
    recommendation: str = ""
    flapping_datacenters: list[str] = field(default_factory=list)

    # Additional correlation signals
    latency_correlated: bool = False  # True if latency elevated across DCs
    extension_correlated: bool = False  # True if extensions correlated across DCs
    lhm_correlated: bool = False  # True if LHM scores elevated across DCs

    # Detailed metrics
    avg_latency_ms: float = 0.0
    dcs_with_elevated_latency: int = 0
    dcs_with_extensions: int = 0
    dcs_with_elevated_lhm: int = 0

    @property
    def should_delay_eviction(self) -> bool:
        """Check if eviction should be delayed due to correlation."""
        # Delay on failure correlation OR if latency/extension/LHM signals suggest network issues
        if self.severity in (CorrelationSeverity.MEDIUM, CorrelationSeverity.HIGH):
            return True
        # Also delay if multiple secondary signals indicate network-wide issues
        secondary_signals = sum(
            [
                self.latency_correlated,
                self.extension_correlated,
                self.lhm_correlated,
            ]
        )
        return secondary_signals >= 2

    @property
    def likely_network_issue(self) -> bool:
        """Check if the issue is likely network-related rather than DC failure."""
        return self.latency_correlated or (
            self.extension_correlated and self.lhm_correlated
        )
