"""
The wire namespace of the distributed models.

Messages travel (and persist) pickled by reference: module path plus
class name. Every model below lives in a file of its own, and is
re-homed here -- its ``__module__`` set to this module -- so its pickled
form names this module, exactly as before the split: mixed-version
clusters keep talking and data written earlier keeps loading. The
package ``__init__`` imports this module, so the re-homing holds before
any model can be pickled.
"""

from .node_role import NodeRole
from .job_status import JobStatus
from .workflow_status import WorkflowStatus
from .worker_state_enum import WorkerState
from .manager_state import ManagerState
from .gate_state import GateState
from .datacenter_health import DatacenterHealth
from .datacenter_registration_status import DatacenterRegistrationStatus
from .update_tier import UpdateTier
from .node_info import NodeInfo
from .manager_info import ManagerInfo
from .manager_peer_registration import ManagerPeerRegistration
from .manager_peer_registration_response import ManagerPeerRegistrationResponse
from .registration_response import RegistrationResponse
from .manager_to_worker_registration import ManagerToWorkerRegistration
from .manager_to_worker_registration_ack import ManagerToWorkerRegistrationAck
from .workflow_progress_ack import WorkflowProgressAck
from .gate_info import GateInfo
from .gate_heartbeat import GateHeartbeat
from .manager_registration_response import ManagerRegistrationResponse
from .gate_registration_request import GateRegistrationRequest
from .gate_registration_response import GateRegistrationResponse
from .manager_discovery_broadcast import ManagerDiscoveryBroadcast
from .worker_discovery_broadcast import WorkerDiscoveryBroadcast
from .job_progress_ack import JobProgressAck
from .worker_registration import WorkerRegistration
from .worker_heartbeat import WorkerHeartbeat
from .manager_heartbeat import ManagerHeartbeat
from .job_submission import JobSubmission
from .job_ack import JobAck
from .workflow_dispatch import WorkflowDispatch
from .workflow_dispatch_ack import WorkflowDispatchAck
from .job_cancel_request import JobCancelRequest
from .job_cancel_response import JobCancelResponse
from .workflow_cancel_request import WorkflowCancelRequest
from .cancel_job_workflows_request import CancelJobWorkflowsRequest
from .cancel_job_workflows_response import CancelJobWorkflowsResponse
from .workflow_cancel_response import WorkflowCancelResponse
from .workflow_cancellation_complete import WorkflowCancellationComplete
from .job_cancellation_complete import JobCancellationComplete
from .workflow_cancellation_status import WorkflowCancellationStatus
from .single_workflow_cancel_request import SingleWorkflowCancelRequest
from .single_workflow_cancel_response import SingleWorkflowCancelResponse
from .workflow_cancellation_peer_notification import WorkflowCancellationPeerNotification
from .cancelled_workflow_info import CancelledWorkflowInfo
from .healthcheck_extension_request import HealthcheckExtensionRequest
from .healthcheck_extension_response import HealthcheckExtensionResponse
from .worker_eviction_notice import WorkerEvictionNotice
from .worker_eviction_notice_ack import WorkerEvictionNoticeAck
from .step_stats import StepStats
from .workflow_progress import WorkflowProgress
from .workflow_final_result import WorkflowFinalResult
from .workflow_final_result_ack import WorkflowFinalResultAck
from .workflow_result import WorkflowResult
from .workflow_dc_result import WorkflowDCResult
from .workflow_result_push import WorkflowResultPush
from .job_final_result import JobFinalResult
from .aggregated_job_stats import AggregatedJobStats
from .global_job_result import GlobalJobResult
from .job_progress import JobProgress
from .global_job_status import GlobalJobStatus
from .job_leadership_announcement import JobLeadershipAnnouncement
from .job_leadership_ack import JobLeadershipAck
from .job_state_sync_message import JobStateSyncMessage
from .job_state_sync_ack import JobStateSyncAck
from .job_leader_gate_transfer import JobLeaderGateTransfer
from .job_leader_gate_transfer_ack import JobLeaderGateTransferAck
from .job_leader_manager_transfer import JobLeaderManagerTransfer
from .job_leader_manager_transfer_ack import JobLeaderManagerTransferAck
from .job_leader_worker_transfer import JobLeaderWorkerTransfer
from .job_leader_worker_transfer_ack import JobLeaderWorkerTransferAck
from .pending_transfer import PendingTransfer
from .gate_leader_info import GateLeaderInfo
from .manager_leader_info import ManagerLeaderInfo
from .orphaned_job_info import OrphanedJobInfo
from .leadership_retry_policy import LeadershipRetryPolicy
from .gate_job_leader_transfer import GateJobLeaderTransfer
from .gate_job_leader_transfer_ack import GateJobLeaderTransferAck
from .manager_job_leader_transfer import ManagerJobLeaderTransfer
from .manager_job_leader_transfer_ack import ManagerJobLeaderTransferAck
from .job_status_push import JobStatusPush
from .dc_stats import DCStats
from .job_batch_push import JobBatchPush
from .register_callback import RegisterCallback
from .register_callback_response import RegisterCallbackResponse
from .job_update_record import JobUpdateRecord
from .job_update_poll_request import JobUpdatePollRequest
from .job_update_poll_response import JobUpdatePollResponse
from .reporter_result_push import ReporterResultPush
from .rate_limit_response import RateLimitResponse
from .job_progress_report import JobProgressReport
from .job_timeout_report import JobTimeoutReport
from .job_global_timeout import JobGlobalTimeout
from .job_leader_transfer import JobLeaderTransfer
from .job_final_status import JobFinalStatus
from .worker_state_snapshot import WorkerStateSnapshot
from .manager_state_snapshot import ManagerStateSnapshot
from .gate_state_snapshot import GateStateSnapshot
from .state_sync_request import StateSyncRequest
from .state_sync_response import StateSyncResponse
from .node_join_request import NodeJoinRequest
from .node_join_response import NodeJoinResponse
from .gate_state_sync_request import GateStateSyncRequest
from .gate_state_sync_response import GateStateSyncResponse
from .cancel_job import CancelJob
from .cancel_ack import CancelAck
from .workflow_cancellation_query import WorkflowCancellationQuery
from .workflow_cancellation_response import WorkflowCancellationResponse
from .datacenter_status import DatacenterStatus
from .ping_request import PingRequest
from .worker_status import WorkerStatus
from .manager_ping_response import ManagerPingResponse
from .datacenter_info import DatacenterInfo
from .gate_ping_response import GatePingResponse
from .datacenter_list_request import DatacenterListRequest
from .datacenter_list_response import DatacenterListResponse
from .workflow_query_request import WorkflowQueryRequest
from .workflow_status_info import WorkflowStatusInfo
from .workflow_query_response import WorkflowQueryResponse
from .datacenter_workflow_status import DatacenterWorkflowStatus
from .gate_workflow_query_response import GateWorkflowQueryResponse
from .eager_workflow_entry import EagerWorkflowEntry
from .manager_registration_state import ManagerRegistrationState
from .datacenter_registration_state import DatacenterRegistrationState

_WIRE_MODELS = (
    NodeRole,
    JobStatus,
    WorkflowStatus,
    WorkerState,
    ManagerState,
    GateState,
    DatacenterHealth,
    DatacenterRegistrationStatus,
    UpdateTier,
    NodeInfo,
    ManagerInfo,
    ManagerPeerRegistration,
    ManagerPeerRegistrationResponse,
    RegistrationResponse,
    ManagerToWorkerRegistration,
    ManagerToWorkerRegistrationAck,
    WorkflowProgressAck,
    GateInfo,
    GateHeartbeat,
    ManagerRegistrationResponse,
    GateRegistrationRequest,
    GateRegistrationResponse,
    ManagerDiscoveryBroadcast,
    WorkerDiscoveryBroadcast,
    JobProgressAck,
    WorkerRegistration,
    WorkerHeartbeat,
    ManagerHeartbeat,
    JobSubmission,
    JobAck,
    WorkflowDispatch,
    WorkflowDispatchAck,
    JobCancelRequest,
    JobCancelResponse,
    WorkflowCancelRequest,
    CancelJobWorkflowsRequest,
    CancelJobWorkflowsResponse,
    WorkflowCancelResponse,
    WorkflowCancellationComplete,
    JobCancellationComplete,
    WorkflowCancellationStatus,
    SingleWorkflowCancelRequest,
    SingleWorkflowCancelResponse,
    WorkflowCancellationPeerNotification,
    CancelledWorkflowInfo,
    HealthcheckExtensionRequest,
    HealthcheckExtensionResponse,
    WorkerEvictionNotice,
    WorkerEvictionNoticeAck,
    StepStats,
    WorkflowProgress,
    WorkflowFinalResult,
    WorkflowFinalResultAck,
    WorkflowResult,
    WorkflowDCResult,
    WorkflowResultPush,
    JobFinalResult,
    AggregatedJobStats,
    GlobalJobResult,
    JobProgress,
    GlobalJobStatus,
    JobLeadershipAnnouncement,
    JobLeadershipAck,
    JobStateSyncMessage,
    JobStateSyncAck,
    JobLeaderGateTransfer,
    JobLeaderGateTransferAck,
    JobLeaderManagerTransfer,
    JobLeaderManagerTransferAck,
    JobLeaderWorkerTransfer,
    JobLeaderWorkerTransferAck,
    PendingTransfer,
    GateLeaderInfo,
    ManagerLeaderInfo,
    OrphanedJobInfo,
    LeadershipRetryPolicy,
    GateJobLeaderTransfer,
    GateJobLeaderTransferAck,
    ManagerJobLeaderTransfer,
    ManagerJobLeaderTransferAck,
    JobStatusPush,
    DCStats,
    JobBatchPush,
    RegisterCallback,
    RegisterCallbackResponse,
    JobUpdateRecord,
    JobUpdatePollRequest,
    JobUpdatePollResponse,
    ReporterResultPush,
    RateLimitResponse,
    JobProgressReport,
    JobTimeoutReport,
    JobGlobalTimeout,
    JobLeaderTransfer,
    JobFinalStatus,
    WorkerStateSnapshot,
    ManagerStateSnapshot,
    GateStateSnapshot,
    StateSyncRequest,
    StateSyncResponse,
    NodeJoinRequest,
    NodeJoinResponse,
    GateStateSyncRequest,
    GateStateSyncResponse,
    CancelJob,
    CancelAck,
    WorkflowCancellationQuery,
    WorkflowCancellationResponse,
    DatacenterStatus,
    PingRequest,
    WorkerStatus,
    ManagerPingResponse,
    DatacenterInfo,
    GatePingResponse,
    DatacenterListRequest,
    DatacenterListResponse,
    WorkflowQueryRequest,
    WorkflowStatusInfo,
    WorkflowQueryResponse,
    DatacenterWorkflowStatus,
    GateWorkflowQueryResponse,
    EagerWorkflowEntry,
    ManagerRegistrationState,
    DatacenterRegistrationState,
)

for _wire_model in _WIRE_MODELS:
    _wire_model.__module__ = __name__
