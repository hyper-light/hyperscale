"""
Leader state tracking for Raft-like leadership with pre-voting.
"""

import time
from dataclasses import dataclass, field
from typing import Callable

from ..core.types import LeaderRole

# Maximum term value - prevents overflow and wrap-around attacks
# Using 2^53 - 1 to stay within JavaScript safe integer range for JSON serialization
MAX_TERM = (2**53) - 1


@dataclass
class LeaderState:
    """
    Tracks the leadership state for a node.
    
    Implements a simplified Raft-like state machine with pre-voting:
    - FOLLOWER: Not a leader, following current leader
    - CANDIDATE: Running for election
    - LEADER: Currently the leader, must send heartbeats
    
    Pre-voting (split-brain prevention):
    - Before starting an election, gather pre-votes
    - Only proceed if pre-vote succeeds
    - Prevents disrupting a healthy leader
    
    Fencing tokens:
    - Each term acts as a fencing token
    - Operations can be tagged with term for safety
    
    Integrates with Lifeguard for health-aware leadership.
    """
    role: LeaderRole = 'follower'
    current_term: int = 0
    
    # Current known leader (None if no leader)
    current_leader: tuple[str, int] | None = None
    leader_term: int = 0
    
    # Lease tracking
    leader_lease_start: float = 0.0
    lease_duration: float = 5.0  # Seconds
    
    # Election state
    votes_received: set[tuple[str, int]] = field(default_factory=set)
    voted_for: tuple[str, int] | None = None
    voted_in_term: int = -1
    election_timeout: float = 10.0  # Seconds
    last_heartbeat_time: float = 0.0
    
    # Pre-voting state (split-brain prevention)
    pre_votes_received: set[tuple[str, int]] = field(default_factory=set)
    pre_vote_term: int = 0
    pre_voting_in_progress: bool = False
    
    # Fencing token (for operation safety)
    # The term serves as the fencing token - operations tagged with
    # a term lower than current are rejected
    fencing_token: int = 0
    
    # Callbacks (set by owner)
    _on_become_leader: Callable[[], None] | None = None
    _on_lose_leadership: Callable[[], None] | None = None
    _on_leader_change: Callable[[tuple[str, int] | None], None] | None = None
    
    def set_callbacks(
        self,
        on_become_leader: Callable[[], None] | None = None,
        on_lose_leadership: Callable[[], None] | None = None,
        on_leader_change: Callable[[tuple[str, int] | None], None] | None = None,
    ) -> None:
        """Set callback functions for leadership events."""
        self._on_become_leader = on_become_leader
        self._on_lose_leadership = on_lose_leadership
        self._on_leader_change = on_leader_change
    
    def is_leader(self) -> bool:
        """Check if this node is currently the leader."""
        return self.role == 'leader'
    
    def is_follower(self) -> bool:
        """Check if this node is currently a follower."""
        return self.role == 'follower'
    
    def is_candidate(self) -> bool:
        """Check if this node is currently a candidate."""
        return self.role == 'candidate'
    
    def has_leader(self) -> bool:
        """Check if there's a known leader."""
        return self.current_leader is not None and self.is_lease_valid()
    
    def is_lease_valid(self) -> bool:
        """Check if the current leader's lease is still valid."""
        if self.current_leader is None:
            return False
        return time.monotonic() < self.leader_lease_start + self.lease_duration
    
    def time_until_lease_expiry(self) -> float:
        """Get time until leader lease expires."""
        expiry = self.leader_lease_start + self.lease_duration
        return max(0, expiry - time.monotonic())
    
    def should_start_election(self) -> bool:
        """Check if we should start a new election."""
        if self.role == 'leader':
            return False
        return not self.is_lease_valid()
    
    def is_term_valid(self, term: int) -> bool:
        """Check if a term value is within valid range."""
        return 0 <= term <= MAX_TERM
    
    def next_term(self) -> int:
        """
        Get the next term value safely.
        
        Returns MAX_TERM if we've reached the limit, preventing overflow.
        In practice, this should never happen with normal operation.
        """
        if self.current_term >= MAX_TERM:
            return MAX_TERM
        return self.current_term + 1
    
    def is_term_exhausted(self) -> bool:
        """Check if term space is exhausted (indicates potential attack or bug)."""
        return self.current_term >= MAX_TERM
    
    def start_election(self, new_term: int) -> bool:
        """
        Transition to candidate state and start election.
        
        Returns False if term is invalid (e.g., overflow).
        """
        if not self.is_term_valid(new_term):
            return False
        self.role = 'candidate'
        self.current_term = new_term
        self.votes_received.clear()
        self.voted_for = None
        self.voted_in_term = -1
        return True
    
    def record_vote(self, voter: tuple[str, int]) -> int:
        """Record a vote received. Returns total vote count."""
        self.votes_received.add(voter)
        return len(self.votes_received)
    
    def become_leader(self, term: int) -> bool:
        """
        Transition to leader state.
        
        Returns False if term is invalid (e.g., overflow).
        """
        if not self.is_term_valid(term):
            return False
        
        was_leader = self.role == 'leader'
        self.role = 'leader'
        self.current_term = term
        self.leader_term = term
        self.leader_lease_start = time.monotonic()
        self.current_leader = None  # We are the leader, set by caller
        
        if not was_leader and self._on_become_leader:
            self._on_become_leader()
        
        return True
    
    def become_follower(self, term: int, leader: tuple[str, int] | None = None) -> None:
        """Transition to follower state."""
        was_leader = self.role == 'leader'
        old_leader = self.current_leader
        
        self.role = 'follower'
        self.current_term = max(self.current_term, term)
        self.votes_received.clear()
        
        if leader:
            self.current_leader = leader
            self.leader_term = term
            self.leader_lease_start = time.monotonic()
        
        if was_leader and self._on_lose_leadership:
            self._on_lose_leadership()
        
        if leader != old_leader and self._on_leader_change:
            self._on_leader_change(leader)
    
    def update_heartbeat(self, leader: tuple[str, int], term: int) -> None:
        """Update lease on receiving leader heartbeat."""
        if term >= self.leader_term:
            self.current_leader = leader
            self.leader_term = term
            self.leader_lease_start = time.monotonic()
            self.last_heartbeat_time = time.monotonic()
            
            if self.role != 'follower':
                self.become_follower(term, leader)
    
    def renew_lease(self) -> None:
        """Renew leader lease (called by leader on heartbeat send)."""
        if self.role == 'leader':
            self.leader_lease_start = time.monotonic()
    
    def can_vote_for(self, candidate: tuple[str, int], term: int) -> bool:
        """Check if we can vote for a candidate."""
        # Can't vote if already voted in this term for someone else
        if self.voted_in_term == term and self.voted_for != candidate:
            return False
        # Can only vote for higher or equal term
        return term >= self.current_term
    
    def vote_for(self, candidate: tuple[str, int], term: int) -> None:
        """Record that we voted for a candidate."""
        self.voted_for = candidate
        self.voted_in_term = term
        self.current_term = max(self.current_term, term)
    
    # Pre-voting methods (split-brain prevention)
    def start_pre_vote(self, term: int) -> None:
        """Start a pre-vote phase before real election."""
        self.pre_vote_term = term
        self.pre_votes_received.clear()
        self.pre_voting_in_progress = True
    
    def record_pre_vote(self, voter: tuple[str, int]) -> int:
        """Record a pre-vote received. Returns total pre-vote count."""
        self.pre_votes_received.add(voter)
        return len(self.pre_votes_received)
    
    def end_pre_vote(self) -> bool:
        """
        End pre-vote phase.
        Returns True if we should proceed to real election.
        """
        had_votes = len(self.pre_votes_received) > 0
        self.pre_voting_in_progress = False
        self.pre_votes_received.clear()  # Clean up for next pre-vote
        return had_votes
    
    def abort_pre_vote(self) -> None:
        """
        Abort an in-progress pre-vote.
        
        Used when:
        - A higher term leader is discovered
        - The node becomes ineligible
        - Pre-vote timeout is interrupted
        """
        self.pre_voting_in_progress = False
        self.pre_votes_received.clear()
        self.pre_vote_term = 0
    
    def can_grant_pre_vote(
        self, 
        candidate: tuple[str, int], 
        term: int,
        candidate_lhm: int,
        max_leader_lhm: int,
    ) -> bool:
        """
        Check if we should grant a pre-vote.
        
        Grant pre-vote if:
        1. Candidate's term is >= our term
        2. We don't have a valid leader lease
        3. Candidate's LHM is acceptable
        """
        # Don't grant if candidate's term is too low
        if term < self.current_term:
            return False
        
        # Don't grant if we have a healthy leader
        if self.is_lease_valid():
            return False
        
        # Don't grant if candidate is unhealthy
        if candidate_lhm > max_leader_lhm:
            return False
        
        return True
    
    # Fencing token methods
    def get_fencing_token(self) -> int:
        """
        Get current fencing token for this node.
        
        The fencing token is used to tag operations so that
        stale leaders cannot corrupt state. Operations with
        a token lower than current are rejected.
        """
        if self.role == 'leader':
            return self.current_term
        return self.leader_term
    
    def update_fencing_token(self, term: int) -> None:
        """Update fencing token when term changes."""
        self.fencing_token = max(self.fencing_token, term)
    
    def is_fencing_token_valid(self, token: int) -> bool:
        """
        Check if a fencing token is still valid.
        
        A token is valid if it's >= our current term.
        This prevents stale leaders from performing operations.
        """
        return token >= self.current_term
    
    # Term-based resolution (split-brain healing)
    def should_yield_to(
        self, 
        other_addr: tuple[str, int], 
        other_term: int,
        self_addr: tuple[str, int],
    ) -> bool:
        """
        Determine if we should yield leadership to another node.
        
        Used for split-brain resolution when two leaders discover each other.
        
        Rules:
        1. Higher term always wins
        2. Same term: lower address wins (deterministic tiebreaker)
        """
        if other_term > self.current_term:
            return True
        if other_term == self.current_term and self.role == 'leader':
            # Same term, both leaders - use address as tiebreaker
            # Lower address wins
            return other_addr < self_addr
        return False

