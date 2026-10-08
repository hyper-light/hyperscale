"""
The content-hash duplicate suppressor must NEVER drop a control / RPC
message — only idempotent epidemic dissemination.

This pins the category boundary that a real bug crossed: leadership
heartbeats (byte-identical within a term) were dropped as "duplicates",
so a follower's lease renewed once per term and then expired → the DC
leader oscillated continuously. The property is now structural — each
handler declares ``dedup_eligible`` (default False = process every
message; opt in only where reprocessing is provably idempotent
dissemination). These tests would have failed the day the bug shipped.
"""

from hyperscale.distributed.swim.message_handling.core.base_handler import (
    BaseHandler,
)
from hyperscale.distributed.swim.message_handling.leadership.leader_heartbeat_handler import (
    LeaderHeartbeatHandler,
)
from hyperscale.distributed.swim.message_handling.leadership.leader_heartbeat_ack_handler import (
    LeaderHeartbeatAckHandler,
)
from hyperscale.distributed.swim.message_handling.leadership.leader_claim_handler import (
    LeaderClaimHandler,
)
from hyperscale.distributed.swim.message_handling.leadership.leader_vote_handler import (
    LeaderVoteHandler,
)
from hyperscale.distributed.swim.message_handling.leadership.pre_vote_req_handler import (
    PreVoteReqHandler,
)
from hyperscale.distributed.swim.message_handling.leadership.pre_vote_resp_handler import (
    PreVoteRespHandler,
)
from hyperscale.distributed.swim.message_handling.leadership.leader_elected_handler import (
    LeaderElectedHandler,
)
from hyperscale.distributed.swim.message_handling.leadership.leader_stepdown_handler import (
    LeaderStepdownHandler,
)
from hyperscale.distributed.swim.message_handling.probing.probe_handler import (
    ProbeHandler,
)
from hyperscale.distributed.swim.message_handling.probing.ping_req_handler import (
    PingReqHandler,
)
from hyperscale.distributed.swim.message_handling.suspicion.suspect_handler import (
    SuspectHandler,
)
from hyperscale.distributed.swim.message_handling.suspicion.alive_handler import (
    AliveHandler,
)
from hyperscale.distributed.swim.message_handling.membership.join_handler import (
    JoinHandler,
)
from hyperscale.distributed.swim.message_handling.membership.leave_handler import (
    LeaveHandler,
)


# Handlers whose messages carry PER-RECEIPT semantics — a heartbeat
# renews a lease, a probe owes an ack, a vote is counted. Dropping a
# repeat is a liveness bug; they must be processed every time.
_CONTROL_HANDLERS = (
    LeaderHeartbeatHandler,
    LeaderHeartbeatAckHandler,
    LeaderClaimHandler,
    LeaderVoteHandler,
    PreVoteReqHandler,
    PreVoteRespHandler,
    LeaderElectedHandler,
    LeaderStepdownHandler,
    ProbeHandler,
    PingReqHandler,
)

# Handlers whose reprocessing is idempotent epidemic dissemination —
# content-hash dedup is safe flood control.
_DISSEMINATION_HANDLERS = (
    SuspectHandler,
    AliveHandler,
    JoinHandler,
    LeaveHandler,
)


def test_default_is_process_not_dedup():
    """A new/unclassified handler must default to PROCESS (never drop) —
    dropping a message is the dangerous operation."""
    assert BaseHandler.dedup_eligible is False


def test_control_messages_are_never_dedup_eligible():
    for handler_cls in _CONTROL_HANDLERS:
        assert handler_cls.dedup_eligible is False, (
            f"{handler_cls.__name__} handles a control/RPC message that must "
            f"be processed on every receipt, but is marked dedup_eligible — "
            f"a byte-identical repeat would be dropped (the leadership-churn bug)"
        )


def test_dissemination_messages_are_dedup_eligible():
    for handler_cls in _DISSEMINATION_HANDLERS:
        assert handler_cls.dedup_eligible is True, (
            f"{handler_cls.__name__} is idempotent gossip dissemination and "
            f"should opt into content-hash flood control"
        )
