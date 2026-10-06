"""``CrossDCCorrelationConfig`` -- pickled under the namespace
``hyperscale.distributed.datacenters.cross_dc_correlation`` (see that module)."""

from dataclasses import dataclass


@dataclass(slots=True)
class CrossDCCorrelationConfig:
    """Configuration for cross-DC correlation detection."""

    # Time window for detecting simultaneous failures (seconds)
    correlation_window_seconds: float = 30.0

    # Minimum DCs failing within window to trigger LOW correlation
    low_threshold: int = 2

    # Minimum DCs failing within window to trigger MEDIUM correlation
    medium_threshold: int = 3

    # Minimum DCs failing within window to trigger HIGH correlation (count-based)
    # HIGH requires BOTH this count AND the fraction threshold
    # Default of 4 means: need at least 4 DCs failing AND >= 50% of known DCs
    # This prevents false positives when few DCs exist
    high_count_threshold: int = 4

    # Minimum fraction of known DCs failing to trigger HIGH correlation
    # HIGH requires BOTH this fraction AND the count threshold above
    high_threshold_fraction: float = 0.5

    # Backoff duration after correlation detected (seconds)
    correlation_backoff_seconds: float = 60.0

    # Maximum failures to track per DC before cleanup
    max_failures_per_dc: int = 100

    # ==========================================================================
    # Anti-flapping configuration
    # ==========================================================================

    # Minimum time a failure must persist before counting (debounce)
    # This filters out transient network blips
    failure_confirmation_seconds: float = 5.0

    # Minimum time DC must be healthy before considered recovered (hysteresis)
    # Prevents premature "all clear" signals
    recovery_confirmation_seconds: float = 30.0

    # Minimum failures in flap_detection_window to be considered flapping
    flap_threshold: int = 3

    # Time window for detecting flapping behavior
    flap_detection_window_seconds: float = 120.0

    # Cooldown after flapping detected before DC can be considered stable
    flap_cooldown_seconds: float = 300.0

    # Weight for recent failures vs older ones (exponential decay)
    # Higher = more weight on recent events
    recency_weight: float = 0.9

    # ==========================================================================
    # Latency-based correlation configuration
    # ==========================================================================

    # Enable latency-based correlation detection
    enable_latency_correlation: bool = True

    # Latency threshold for elevated state (ms)
    # If average latency exceeds this, DC is considered degraded (not failed)
    latency_elevated_threshold_ms: float = 100.0

    # Latency threshold for critical state (ms)
    # If average latency exceeds this, DC latency is considered critical
    latency_critical_threshold_ms: float = 500.0

    # Minimum latency samples required before making decisions
    min_latency_samples: int = 3

    # Latency sample window (seconds)
    latency_sample_window_seconds: float = 60.0

    # If this fraction of DCs have elevated latency, it's likely network, not DC
    latency_correlation_fraction: float = 0.5

    # ==========================================================================
    # Extension request correlation configuration
    # ==========================================================================

    # Enable extension request correlation detection
    enable_extension_correlation: bool = True

    # Minimum extension requests to consider DC under load (not failed)
    extension_count_threshold: int = 2

    # If this fraction of DCs have high extensions, treat as load spike
    extension_correlation_fraction: float = 0.5

    # Extension request tracking window (seconds)
    extension_window_seconds: float = 120.0

    # ==========================================================================
    # Local Health Multiplier (LHM) correlation configuration
    # ==========================================================================

    # Enable LHM correlation detection
    enable_lhm_correlation: bool = True

    # LHM score threshold to consider DC stressed (out of max 8)
    lhm_stressed_threshold: int = 3

    # If this fraction of DCs have high LHM, treat as systemic issue
    lhm_correlation_fraction: float = 0.5
