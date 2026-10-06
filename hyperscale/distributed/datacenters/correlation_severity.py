"""``CorrelationSeverity`` -- pickled under the namespace
``hyperscale.distributed.datacenters.cross_dc_correlation`` (see that module)."""

from enum import Enum


class CorrelationSeverity(Enum):
    """Severity level for correlated failures."""

    NONE = "none"  # No correlation detected
    LOW = "low"  # Some correlation, may be coincidence
    MEDIUM = "medium"  # Likely correlated, investigate
    HIGH = "high"  # Strong correlation, likely network issue
