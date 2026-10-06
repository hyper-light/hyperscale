"""``ExtensionTrackerConfig`` -- pickled under the namespace
``hyperscale.distributed.health.extension_tracker`` (see that module)."""

from dataclasses import dataclass

from .extension_tracker_impl import ExtensionTracker


@dataclass(slots=True)
class ExtensionTrackerConfig:
    """
    Configuration for ExtensionTracker instances.

    Attributes:
        base_deadline: Base deadline in seconds.
        min_grant: Minimum extension grant in seconds.
        max_extensions: Maximum number of extensions allowed.
        warning_threshold: Remaining extensions to trigger warning.
        grace_period: Seconds of grace after exhaustion before kill.
    """

    base_deadline: float = 30.0
    min_grant: float = 1.0
    max_extensions: int = 5
    warning_threshold: int = 1
    grace_period: float = 10.0

    def create_tracker(self, worker_id: str) -> ExtensionTracker:
        """Create an ExtensionTracker with this configuration."""
        return ExtensionTracker(
            worker_id=worker_id,
            base_deadline=self.base_deadline,
            min_grant=self.min_grant,
            max_extensions=self.max_extensions,
            warning_threshold=self.warning_threshold,
            grace_period=self.grace_period,
        )
