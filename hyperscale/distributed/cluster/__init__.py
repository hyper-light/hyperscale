"""
hyperscale.distributed.cluster — AD-52 cluster formation, membership, and joining.

This package implements the unified cluster protocol per AD-52:
deterministic bootstrap, Raft-replicated membership with joint consensus,
learner state, membership-generation fencing, seed-locator discovery, the
SWIM + phi-accrual hybrid failure detector, watch streams, disconnected
mode, and the operational primitives (drain, force-remove, freeze,
snapshot import/export).

The package extends rather than replaces:
  - hyperscale.distributed.raft     — gains joint consensus, learners, ReadIndex
  - hyperscale.distributed.swim     — gains phi-accrual integration
  - hyperscale.distributed.discovery — dispatches via the seed-locator package
  - hyperscale.distributed.server.protocol — enforces ClusterRPCFence

Module layout follows AD-52 §19. One class per file per CLAUDE.md.
"""

from .fence import ClusterRPCFence as ClusterRPCFence
from .fence import ClusterFenceError as ClusterFenceError
from .fence import FenceRejectionReason as FenceRejectionReason
from .identity import ClusterIdentity as ClusterIdentity
from .models import MemberRecord as MemberRecord
from .models import ClusterMetadata as ClusterMetadata
from .models import MemberStatus as MemberStatus
from .models import BootstrapHello as BootstrapHello
from .models import JoinHello as JoinHello
from .models import HelloResponse as HelloResponse
from .models import JoinRequest as JoinRequest
from .models import JoinAccepted as JoinAccepted
from .models import WatchOpen as WatchOpen
from .models import WatchSnapshot as WatchSnapshot
from .models import WatchDelta as WatchDelta
from .membership_log import EnterJoint as EnterJoint
from .membership_log import LeaveJoint as LeaveJoint
from .membership_log import AddLearner as AddLearner
from .membership_log import Promote as Promote
from .membership_log import Remove as Remove
from .membership_log import RemoveReason as RemoveReason
from .membership_log import UpdateMetadata as UpdateMetadata
from .membership_log import RegisterDatacenter as RegisterDatacenter
from .membership_log import ResizeCluster as ResizeCluster
from .membership_log import MembershipLogEntry as MembershipLogEntry
