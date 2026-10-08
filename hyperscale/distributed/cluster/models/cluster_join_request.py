from dataclasses import dataclass

from hyperscale.distributed.models.message import Message


@dataclass(slots=True)
class ClusterJoinRequest(Message):
    """A node asking a formed cluster's membership group to take it in
    (AD-52 join): it follows the group's log as a learner and votes once
    promoted. ``founders_digest`` is the founding cohort it was configured
    with -- only a node of the cluster's cohort joins it."""

    member_id: str
    founders_digest: str
    # The newest log entry schema the joiner reads (AD-52 section 14): a
    # group writing a newer one refuses it -- it could apply nothing.
    schema_version: int = 1
