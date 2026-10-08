"""
Gate settings derived from ``Env``: the gate reads its configuration from
``Env`` directly, and the settings here are the ones computed from several
of its fields.
"""

from hyperscale.distributed.env import Env


def derive_gate_orphan_grace_seconds(env: Env) -> float:
    """How long a gate waits on a job whose leader gate's lease lapsed before
    the gate tier's leader takes it over: the time the tier needs to reach
    its verdict on that gate -- the surviving gates agree it is dead (one
    suspicion window, the no-witness one at worst), elect a tier leader if
    the dead gate led (pre-vote, election timeout and its jitter), and
    deliver the leadership announcement of a gate that did take the job
    (one standard request). A verdict of death takes the job over at once;
    observed rescues taking longer raise the grace
    (GateRuntimeState.longest_orphan_rescue_seconds); AD-26 extensions
    lengthen it while the job's leader is still heard from."""
    return (
        max(env.SWIM_SUSPICION_MAX_TIMEOUT, env.SWIM_NO_WITNESS_SUSPICION_TIMEOUT)
        + env.LEADER_PRE_VOTE_TIMEOUT
        + env.LEADER_ELECTION_TIMEOUT_BASE
        + env.LEADER_ELECTION_TIMEOUT_JITTER
        + env.GATE_TCP_TIMEOUT_STANDARD
    )


def derive_datacenter_leader_failover_seconds(env: Env) -> float:
    """How long a datacenter can be without a leader that accepts jobs while
    it replaces one: its managers agree the leader is dead (one suspicion
    window, the no-witness one at worst -- a gate cannot see whether a
    datacenter's managers have witnesses), then elect (pre-vote, election
    timeout and its jitter). A dispatch answered "retry" for longer than
    this is not waiting on an election."""
    return (
        max(env.SWIM_SUSPICION_MAX_TIMEOUT, env.SWIM_NO_WITNESS_SUSPICION_TIMEOUT)
        + env.LEADER_PRE_VOTE_TIMEOUT
        + env.LEADER_ELECTION_TIMEOUT_BASE
        + env.LEADER_ELECTION_TIMEOUT_JITTER
    )
