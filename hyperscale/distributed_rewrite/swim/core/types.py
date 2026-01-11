"""
Type definitions for SWIM + Lifeguard protocol.
"""

import asyncio
from typing import Literal

# Message types for the SWIM protocol
Message = Literal[
    b'ack', 
    b'nack', 
    b'join', 
    b'leave', 
    b'probe',
    b'ping-req',  # Indirect probe request (ask another node to probe target)
    b'ping-req-ack',  # Response from indirect probe
    b'suspect',  # Suspicion message
    b'alive',  # Refutation/alive message
    # Leadership messages
    b'leader-claim',     # Claim local leadership: leader-claim:term:lhm>addr
    b'leader-vote',      # Vote for candidate: leader-vote:term>candidate_addr
    b'leader-elected',   # Announce election win: leader-elected:term>leader_addr
    b'leader-heartbeat', # Leader heartbeat: leader-heartbeat:term>leader_addr
    b'leader-stepdown',  # Voluntary stepdown: leader-stepdown:term>addr
    # Pre-voting (split-brain prevention)
    b'pre-vote-req',     # Pre-vote request: pre-vote-req:term:lhm>candidate_addr
    b'pre-vote-resp',    # Pre-vote response: pre-vote-resp:term:granted>candidate_addr
]

# Node status in the membership list (AD-29 compliant)
# UNCONFIRMED: Peer discovered but not yet confirmed via bidirectional communication
# JOIN: Peer just joined the cluster
# OK: Peer is alive and healthy (confirmed)
# SUSPECT: Peer suspected of failure (only from OK state, never from UNCONFIRMED)
# DEAD: Peer confirmed dead
Status = Literal[b'UNCONFIRMED', b'JOIN', b'OK', b'SUSPECT', b'DEAD']

# Type of membership update for gossip
UpdateType = Literal['alive', 'suspect', 'dead', 'join', 'leave']

# Leadership role states
LeaderRole = Literal['follower', 'candidate', 'leader']

# Node address type
NodeAddr = tuple[str, int]

# Dictionary of nodes with their status queues
Nodes = dict[NodeAddr, asyncio.Queue[tuple[int, Status]]]

# Context type for the server
Ctx = dict[Literal['nodes'], Nodes]

