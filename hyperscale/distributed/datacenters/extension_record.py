"""``ExtensionRecord`` -- pickled under the namespace
``hyperscale.distributed.datacenters.cross_dc_correlation`` (see that module)."""

from dataclasses import dataclass


@dataclass(slots=True)
class ExtensionRecord:
    """Record of an extension request from a datacenter."""

    timestamp: float
    worker_id: str
    extension_count: int  # How many extensions this worker has requested
    reason: str = ""
