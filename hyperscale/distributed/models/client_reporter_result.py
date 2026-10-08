"""Wire model ``ClientReporterResult`` -- pickled under the wire namespace
``hyperscale.distributed.models.client`` (see that module)."""

from dataclasses import dataclass


@dataclass(slots=True)
class ClientReporterResult:
    """Result of a reporter submission as seen by the client."""

    reporter_type: str
    success: bool
    error: str | None = None
    elapsed_seconds: float = 0.0
    source: str = ""  # "manager" or "gate"
    datacenter: str = ""  # For manager source
