"""
Gate TCP/UDP handler implementations.

Each handler class is responsible for processing a specific message type.
Handlers are registered with the GateServer during initialization.

Handler Categories (25 handlers total):
- Job submission: job_submission (1)
- Manager status: manager_status_update, manager_register, manager_discovery (3)
- Job progress: receive_job_status_request, receive_job_progress, workflow_result_push (3)
- Cancellation (AD-20): receive_cancel_job, receive_job_cancellation_complete, receive_cancel_single_workflow (3)
- Leadership/Lease: receive_lease_transfer, job_leadership_announcement, job_leader_manager_transfer, dc_leader_announcement (4)
- Timeout (AD-34): receive_job_progress_report, receive_job_timeout_report, receive_job_leader_transfer, receive_job_final_status (4)
- Discovery: ping, register_callback, workflow_query, datacenter_list (4)
- State sync: receive_gate_state_sync_request (1)
- Stats: windowed_stats_push, job_final_result (2)

Note: These are handler stubs with dependency protocols. Full handler
extraction will happen during composition root refactoring (15.3.7).
"""

from .tcp_job_submission import JobSubmissionDependencies
from .tcp_manager_status import ManagerStatusDependencies
from .tcp_job_progress import JobProgressDependencies
from .tcp_cancellation import CancellationDependencies
from .tcp_leadership import LeadershipDependencies
from .tcp_timeout import TimeoutDependencies
from .tcp_discovery import DiscoveryDependencies
from .tcp_sync import SyncDependencies
from .tcp_stats import StatsDependencies

__all__ = [
    "JobSubmissionDependencies",
    "ManagerStatusDependencies",
    "JobProgressDependencies",
    "CancellationDependencies",
    "LeadershipDependencies",
    "TimeoutDependencies",
    "DiscoveryDependencies",
    "SyncDependencies",
    "StatsDependencies",
]
