"""Wire model ``DatacenterListRequest`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message


@dataclass(slots=True)
class DatacenterListRequest(Message):
    """
    Request to list registered datacenters from a gate.

    Clients use this to discover available datacenters before submitting jobs.
    This is a lightweight query that returns datacenter identifiers and health status.
    """

    request_id: str = ""  # Optional request identifier for correlation
