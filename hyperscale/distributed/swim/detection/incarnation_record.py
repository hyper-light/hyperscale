"""``IncarnationRecord`` -- pickled under the namespace
``hyperscale.distributed.swim.detection.incarnation_store`` (see that module)."""

from dataclasses import dataclass


@dataclass(slots=True)
class IncarnationRecord:
    """
    Record of a node's incarnation history.

    Stores both the last known incarnation and the timestamp when it was
    last updated. The timestamp enables time-based zombie detection.
    """

    incarnation: int
    last_updated_at: float
    node_address: str
