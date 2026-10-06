"""``ExtensionTriggerConfig`` -- pickled under the namespace
``hyperscale.distributed.nodes.worker.extension_trigger`` (see that module)."""

from __future__ import annotations

from dataclasses import dataclass
from hyperscale.distributed.taskex.util.time_parser import TimeParser


@dataclass(slots=True, frozen=True)
class ExtensionTriggerConfig:
    """Configuration for the worker autonomous extension trigger."""

    # How often the loop scans active workflows. Defaults align with
    # the worker heartbeat cadence so any extension request the
    # trigger sets piggybacks on the very next outbound heartbeat.
    poll_interval_seconds: float = 5.0
    # Fraction of the workflow's deadline at which the trigger starts
    # requesting extensions. 0.75 = "request once 75% of the budget
    # is consumed." Picked so short workflows complete normally
    # without ever requesting; only workflows running into the last
    # quarter of their budget get extensions.
    lookahead_fraction: float = 0.75
    # Hard floor on the time before the deadline at which we send
    # the first request, regardless of lookahead-fraction math.
    # Ensures very short deadlines (e.g. 4s) still leave a request
    # window long enough for the heartbeat round-trip + manager
    # processing.
    minimum_lookahead_seconds: float = 1.0

    @classmethod
    def from_env_values(
        cls,
        poll_interval_str: str,
        lookahead_fraction: float,
    ) -> "ExtensionTriggerConfig":
        """Build a config from the parsed env-var values."""
        return cls(
            poll_interval_seconds=TimeParser(poll_interval_str).time,
            lookahead_fraction=lookahead_fraction,
        )
