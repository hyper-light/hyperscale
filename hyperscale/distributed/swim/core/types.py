"""
Type definitions for SWIM + Lifeguard protocol.
"""

from typing import Literal

Message = Literal[
    b"ack",
    b"nack",
    b"join",
    b"leave",
    b"probe",
    b"ping-req",
    b"ping-req-ack",
    b"suspect",
    b"alive",
    b"leader-claim",
    b"leader-vote",
    b"leader-elected",
    b"leader-heartbeat",
    b"leader-stepdown",
    b"pre-vote-req",
    b"pre-vote-resp",
]

Status = Literal[b"UNCONFIRMED", b"JOIN", b"OK", b"SUSPECT", b"DEAD"]

UpdateType = Literal["alive", "suspect", "dead", "join", "leave"]

LeaderRole = Literal["follower", "candidate", "leader"]

NodeAddr = tuple[str, int]

SwimContextKey = str | NodeAddr
"""Context keys: setting names from ``Env.get_swim_init_context`` plus the
per-peer addresses the join handler writes its acceptance marker under."""

SwimContextValue = int | float | bytes
"""Context values: the numeric SWIM settings plus the join marker ``b"OK"``."""

Ctx = dict[SwimContextKey, SwimContextValue]
