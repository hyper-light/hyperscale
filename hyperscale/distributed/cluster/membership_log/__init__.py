"""
Membership log entries (AD-52 §6).

Each entry type is its own frozen dataclass — CLAUDE.md "one class per
file." All entries carry a schema_version (AD-52 §14), a committed_at_term,
and a committed_at_epoch as common metadata.

The MembershipLogEntry union covers the eight entry types. The
schema_dispatch module maps (entry_type_name, schema_version) → decoder.
"""

from typing import Union

from .add_learner import AddLearner as AddLearner
from .base import EntryMetadata as EntryMetadata
from .enter_joint import EnterJoint as EnterJoint
from .leave_joint import LeaveJoint as LeaveJoint
from .promote import Promote as Promote
from .register_datacenter import RegisterDatacenter as RegisterDatacenter
from .remove import Remove as Remove
from .remove import RemoveReason as RemoveReason
from .resize_cluster import ResizeCluster as ResizeCluster
from .update_metadata import UpdateMetadata as UpdateMetadata
from .schema_dispatch import SchemaDispatcher as SchemaDispatcher


MembershipLogEntry = Union[
    EnterJoint,
    LeaveJoint,
    AddLearner,
    Promote,
    Remove,
    UpdateMetadata,
    RegisterDatacenter,
    ResizeCluster,
]
