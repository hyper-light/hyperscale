"""
Job timeout strategies with multi-DC coordination (AD-34).

Provides adaptive timeout detection that auto-detects deployment topology:
- LocalAuthorityTimeout: Single-DC deployments (manager has full authority)
- GateCoordinatedTimeout: Multi-DC deployments (gate coordinates globally)

Integrates with AD-26 healthcheck extensions to respect legitimate long-running work.
"""

import asyncio
import time
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from hyperscale.logging.hyperscale_logger import HyperscaleLogger
from hyperscale.logging.hyperscale_logging_models import (
    ServerDebug,
    ServerError,
    ServerInfo,
    ServerWarning,
)
from hyperscale.distributed_rewrite.models.distributed import (
    JobFinalStatus,
    JobProgressReport,
    JobStatus,
    JobTimeoutReport,
)

if TYPE_CHECKING:
    from hyperscale.distributed_rewrite.nodes.manager import ManagerServer


class TimeoutStrategy(ABC):
    """
    Base timeout strategy with lifecycle management (AD-34).

    Subclasses implement either local authority (single-DC) or gate coordination
    (multi-DC) timeout detection and reporting.
    """

    @abstractmethod
    async def start_tracking(
        self,
        job_id: str,
        timeout_seconds: float,
        gate_addr: tuple[str, int] | None = None,
    ) -> None:
        """
        Start tracking timeout for a job.

        Called when job is submitted. Initializes TimeoutTrackingState in JobInfo.

        Args:
            job_id: Job to track
            timeout_seconds: Job timeout in seconds
            gate_addr: Gate address for multi-DC (None for single-DC)
        """
        pass

    @abstractmethod
    async def resume_tracking(self, job_id: str) -> None:
        """
        Resume tracking after leader transfer.

        CRITICAL: New leader calls this to continue timeout tracking.
        Reconstructs strategy state from JobInfo.timeout_tracking.

        Increments fence token to prevent stale timeout decisions.

        Args:
            job_id: Job to resume tracking
        """
        pass

    @abstractmethod
    async def report_progress(self, job_id: str, progress_type: str) -> None:
        """
        Record workflow progress event.

        Updates last_progress_at timestamp. Progress types include:
        - Workflow state transitions (e.g., "workflow_running", "workflow_completed")
        - Worker extension grants (automatically called, updates last_progress_at)

        Args:
            job_id: Job that made progress
            progress_type: Type of progress event
        """
        pass

    @abstractmethod
    async def check_timeout(self, job_id: str) -> tuple[bool, str]:
        """
        Check if job timed out.

        Returns (is_timed_out, reason).
        Idempotent - safe to call multiple times.

        Checks:
        1. Overall timeout: elapsed > effective_timeout (base + extensions)
        2. Stuck detection: no progress for stuck_threshold (120s)

        Args:
            job_id: Job to check

        Returns:
            (is_timed_out, reason) tuple
        """
        pass

    @abstractmethod
    async def handle_global_timeout(
        self, job_id: str, reason: str, fence_token: int
    ) -> bool:
        """
        Handle global timeout decision from gate.

        Validates fence token to reject stale decisions after leader transfers.

        Args:
            job_id: Job that timed out
            reason: Why gate declared timeout
            fence_token: Gate's fence token

        Returns:
            True if accepted, False if rejected (stale)
        """
        pass

    @abstractmethod
    async def record_worker_extension(
        self,
        job_id: str,
        worker_id: str,
        extension_seconds: float,
        worker_progress: float,
    ) -> None:
        """
        Record that a worker was granted an extension (AD-26 integration).

        This adjusts the job's effective timeout to account for legitimate
        long-running work. Extension also counts as progress (updates last_progress_at).

        Args:
            job_id: Job the worker is executing
            worker_id: Worker that received extension
            extension_seconds: Seconds granted
            worker_progress: Progress metric that justified extension
        """
        pass

    @abstractmethod
    async def stop_tracking(self, job_id: str, reason: str) -> None:
        """
        Stop tracking timeout for a job.

        Called when job reaches terminal state (completed, failed, cancelled, timed out).
        Must be idempotent - safe to call multiple times.

        Args:
            job_id: Job to stop tracking
            reason: Why tracking stopped (e.g., "completed", "cancelled", "timed_out")
        """
        pass

    @abstractmethod
    async def cleanup_worker_extensions(self, job_id: str, worker_id: str) -> None:
        """
        Clean up extension tracking for a failed/removed worker.

        Called when worker dies or is removed from job.
        Removes worker from active_workers_with_extensions.

        Args:
            job_id: Job ID
            worker_id: Worker to remove from extension tracking
        """
        pass
