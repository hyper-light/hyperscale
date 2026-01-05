"""
Logging models for the jobs module.

These models are used by WorkerPool, WorkflowDispatcher, and CoreAllocator
to log structured information about job orchestration operations.

Each model includes contextual fields that identify:
- The manager/datacenter context (manager_id, datacenter)
- The relevant job/workflow being operated on
- Operation-specific details (cores, workers, etc.)
"""

from hyperscale.logging.models import Entry, LogLevel


# =============================================================================
# WorkerPool Logging Models
# =============================================================================

class WorkerPoolTrace(Entry, kw_only=True):
    """Trace-level logging for WorkerPool operations."""
    manager_id: str
    datacenter: str
    worker_count: int
    healthy_worker_count: int
    total_cores: int
    available_cores: int
    level: LogLevel = LogLevel.TRACE


class WorkerPoolDebug(Entry, kw_only=True):
    """Debug-level logging for WorkerPool operations."""
    manager_id: str
    datacenter: str
    worker_count: int
    healthy_worker_count: int
    total_cores: int
    available_cores: int
    level: LogLevel = LogLevel.DEBUG


class WorkerPoolInfo(Entry, kw_only=True):
    """Info-level logging for WorkerPool operations."""
    manager_id: str
    datacenter: str
    worker_count: int
    healthy_worker_count: int
    total_cores: int
    available_cores: int
    level: LogLevel = LogLevel.INFO


class WorkerPoolWarning(Entry, kw_only=True):
    """Warning-level logging for WorkerPool operations."""
    manager_id: str
    datacenter: str
    worker_count: int
    healthy_worker_count: int
    total_cores: int
    available_cores: int
    level: LogLevel = LogLevel.WARN


class WorkerPoolError(Entry, kw_only=True):
    """Error-level logging for WorkerPool operations."""
    manager_id: str
    datacenter: str
    worker_count: int
    healthy_worker_count: int
    total_cores: int
    available_cores: int
    level: LogLevel = LogLevel.ERROR


class WorkerPoolCritical(Entry, kw_only=True):
    """Critical-level logging for WorkerPool operations."""
    manager_id: str
    datacenter: str
    worker_count: int
    healthy_worker_count: int
    total_cores: int
    available_cores: int
    level: LogLevel = LogLevel.CRITICAL


# =============================================================================
# WorkflowDispatcher Logging Models
# =============================================================================

class DispatcherTrace(Entry, kw_only=True):
    """Trace-level logging for WorkflowDispatcher operations."""
    manager_id: str
    datacenter: str
    job_id: str
    workflow_id: str
    pending_count: int
    dispatched_count: int
    level: LogLevel = LogLevel.TRACE


class DispatcherDebug(Entry, kw_only=True):
    """Debug-level logging for WorkflowDispatcher operations."""
    manager_id: str
    datacenter: str
    job_id: str
    workflow_id: str
    pending_count: int
    dispatched_count: int
    level: LogLevel = LogLevel.DEBUG


class DispatcherInfo(Entry, kw_only=True):
    """Info-level logging for WorkflowDispatcher operations."""
    manager_id: str
    datacenter: str
    job_id: str
    workflow_id: str
    pending_count: int
    dispatched_count: int
    level: LogLevel = LogLevel.INFO


class DispatcherWarning(Entry, kw_only=True):
    """Warning-level logging for WorkflowDispatcher operations."""
    manager_id: str
    datacenter: str
    job_id: str
    workflow_id: str
    pending_count: int
    dispatched_count: int
    level: LogLevel = LogLevel.WARN


class DispatcherError(Entry, kw_only=True):
    """Error-level logging for WorkflowDispatcher operations."""
    manager_id: str
    datacenter: str
    job_id: str
    workflow_id: str
    pending_count: int
    dispatched_count: int
    level: LogLevel = LogLevel.ERROR


class DispatcherCritical(Entry, kw_only=True):
    """Critical-level logging for WorkflowDispatcher operations."""
    manager_id: str
    datacenter: str
    job_id: str
    workflow_id: str
    pending_count: int
    dispatched_count: int
    level: LogLevel = LogLevel.CRITICAL


# =============================================================================
# CoreAllocator Logging Models
# =============================================================================

class AllocatorTrace(Entry, kw_only=True):
    """Trace-level logging for CoreAllocator operations."""
    worker_id: str
    workflow_id: str
    total_cores: int
    available_cores: int
    active_workflows: int
    level: LogLevel = LogLevel.TRACE


class AllocatorDebug(Entry, kw_only=True):
    """Debug-level logging for CoreAllocator operations."""
    worker_id: str
    workflow_id: str
    total_cores: int
    available_cores: int
    active_workflows: int
    level: LogLevel = LogLevel.DEBUG


class AllocatorInfo(Entry, kw_only=True):
    """Info-level logging for CoreAllocator operations."""
    worker_id: str
    workflow_id: str
    total_cores: int
    available_cores: int
    active_workflows: int
    level: LogLevel = LogLevel.INFO


class AllocatorWarning(Entry, kw_only=True):
    """Warning-level logging for CoreAllocator operations."""
    worker_id: str
    workflow_id: str
    total_cores: int
    available_cores: int
    active_workflows: int
    level: LogLevel = LogLevel.WARN


class AllocatorError(Entry, kw_only=True):
    """Error-level logging for CoreAllocator operations."""
    worker_id: str
    workflow_id: str
    total_cores: int
    available_cores: int
    active_workflows: int
    level: LogLevel = LogLevel.ERROR


class AllocatorCritical(Entry, kw_only=True):
    """Critical-level logging for CoreAllocator operations."""
    worker_id: str
    workflow_id: str
    total_cores: int
    available_cores: int
    active_workflows: int
    level: LogLevel = LogLevel.CRITICAL
