"""Wire model ``RegisterCallbackResponse`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from typing import TYPE_CHECKING
from dataclasses import dataclass
from .message import Message

if TYPE_CHECKING:
    from .register_callback import RegisterCallback


@dataclass(slots=True)
class RegisterCallbackResponse(Message):
    """
    Response to RegisterCallback request.

    Indicates whether callback registration succeeded and provides
    current job status for immediate sync.
    """

    job_id: str  # Job being registered
    success: bool  # Whether registration succeeded
    status: str = ""  # Current JobStatus value
    total_completed: int = 0  # Current completion count
    total_failed: int = 0  # Current failure count
    elapsed_seconds: float = 0.0  # Time since job started
    error: str | None = None  # Error message if failed
