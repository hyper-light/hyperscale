"""
Logging models for the jobs module.

These models are used by WorkerPool, WorkflowDispatcher, and CoreAllocator
to log structured information about job orchestration operations.

Each model includes contextual fields that identify:
- The manager/datacenter context (manager_id, datacenter)
- The relevant job/workflow being operated on
- Operation-specific details (cores, workers, etc.)

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

from hyperscale.logging.models import Entry, LogLevel

from .allocator_critical import AllocatorCritical
from .allocator_debug import AllocatorDebug
from .allocator_error import AllocatorError
from .allocator_info import AllocatorInfo
from .allocator_trace import AllocatorTrace
from .allocator_warning import AllocatorWarning
from .dispatcher_critical import DispatcherCritical
from .dispatcher_debug import DispatcherDebug
from .dispatcher_error import DispatcherError
from .dispatcher_info import DispatcherInfo
from .dispatcher_trace import DispatcherTrace
from .dispatcher_warning import DispatcherWarning
from .job_manager_critical import JobManagerCritical
from .job_manager_debug import JobManagerDebug
from .job_manager_error import JobManagerError
from .job_manager_info import JobManagerInfo
from .job_manager_trace import JobManagerTrace
from .job_manager_warning import JobManagerWarning
from .worker_pool_critical import WorkerPoolCritical
from .worker_pool_debug import WorkerPoolDebug
from .worker_pool_error import WorkerPoolError
from .worker_pool_info import WorkerPoolInfo
from .worker_pool_trace import WorkerPoolTrace
from .worker_pool_warning import WorkerPoolWarning

_REHOMED = (
    WorkerPoolTrace,
    WorkerPoolDebug,
    WorkerPoolInfo,
    WorkerPoolWarning,
    WorkerPoolError,
    WorkerPoolCritical,
    DispatcherTrace,
    DispatcherDebug,
    DispatcherInfo,
    DispatcherWarning,
    DispatcherError,
    DispatcherCritical,
    AllocatorTrace,
    AllocatorDebug,
    AllocatorInfo,
    AllocatorWarning,
    AllocatorError,
    AllocatorCritical,
    JobManagerTrace,
    JobManagerDebug,
    JobManagerInfo,
    JobManagerWarning,
    JobManagerError,
    JobManagerCritical,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
