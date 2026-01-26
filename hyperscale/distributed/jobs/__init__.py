"""
Jobs module - Job orchestration components.

This module contains classes for managing job execution:

Manager-side:
- JobManager: Thread-safe job/workflow state management
- WorkerPool: Worker registration and resource allocation
- WorkflowDispatcher: Workflow dispatch and dependency tracking

Worker-side:
- CoreAllocator: Thread-safe core allocation for workflow execution

Shared (Manager/Gate):
- JobLeadershipTracker: Per-job leadership tracking with fencing tokens
- WindowedStatsCollector: Time-correlated stats aggregation

Supporting types:
- TrackingToken: Globally unique workflow tracking IDs
- JobInfo, WorkflowInfo, SubWorkflowInfo: Job state containers
- WorkflowStateMachine: State machine for workflow transitions
- AllocationResult: Core allocation result container
- JobLeadership: Leadership info for a single job
- DCManagerLeadership: Per-DC manager leadership info (for gates)

Logging models:
- WorkerPoolTrace/Debug/Info/Warning/Error/Critical
- DispatcherTrace/Debug/Info/Warning/Error/Critical
- AllocatorTrace/Debug/Info/Warning/Error/Critical
"""

from hyperscale.distributed.jobs.job_manager import JobManager as JobManager
from hyperscale.distributed.models import (
    JobInfo as JobInfo,
    WorkflowInfo as WorkflowInfo,
    SubWorkflowInfo as SubWorkflowInfo,
    TrackingToken as TrackingToken,
)
from hyperscale.distributed.jobs.workflow_state_machine import (
    WorkflowStateMachine as WorkflowStateMachine,
)
from hyperscale.distributed.jobs.worker_pool import (
    WorkerPool as WorkerPool,
    WorkerInfo as WorkerInfo,
    WorkerHealth as WorkerHealth,
)
from hyperscale.distributed.jobs.workflow_dispatcher import (
    WorkflowDispatcher as WorkflowDispatcher,
)
from hyperscale.distributed.jobs.core_allocator import (
    CoreAllocator as CoreAllocator,
    AllocationResult as AllocationResult,
)
from hyperscale.distributed.jobs.windowed_stats_collector import (
    WindowedStatsCollector as WindowedStatsCollector,
    WindowedStatsPush as WindowedStatsPush,
    WorkerWindowStats as WorkerWindowStats,
    WindowBucket as WindowBucket,
)
from hyperscale.distributed.jobs.job_leadership_tracker import (
    JobLeadershipTracker as JobLeadershipTracker,
    JobLeadership as JobLeadership,
    DCManagerLeadership as DCManagerLeadership,
)
from hyperscale.distributed.jobs.logging_models import (
    WorkerPoolTrace as WorkerPoolTrace,
    WorkerPoolDebug as WorkerPoolDebug,
    WorkerPoolInfo as WorkerPoolInfo,
    WorkerPoolWarning as WorkerPoolWarning,
    WorkerPoolError as WorkerPoolError,
    WorkerPoolCritical as WorkerPoolCritical,
    DispatcherTrace as DispatcherTrace,
    DispatcherDebug as DispatcherDebug,
    DispatcherInfo as DispatcherInfo,
    DispatcherWarning as DispatcherWarning,
    DispatcherError as DispatcherError,
    DispatcherCritical as DispatcherCritical,
    AllocatorTrace as AllocatorTrace,
    AllocatorDebug as AllocatorDebug,
    AllocatorInfo as AllocatorInfo,
    AllocatorWarning as AllocatorWarning,
    AllocatorError as AllocatorError,
    AllocatorCritical as AllocatorCritical,
)
