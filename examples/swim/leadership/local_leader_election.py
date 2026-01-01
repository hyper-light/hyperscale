"""
Local leader election with pre-voting and split-brain prevention.
"""

import asyncio
import random
from dataclasses import dataclass, field
from typing import Callable, Awaitable, Any, Protocol

from .leader_state import LeaderState
from .leader_eligibility import LeaderEligibility
from .flapping_detector import FlappingDetector
from ..core.errors import ElectionError, ElectionTimeoutError, SplitBrainError, UnexpectedError, NotEligibleError

from hyperscale.logging.hyperscale_logging_models import ServerDebug


class TaskRunnerProtocol(Protocol):
    """Protocol for TaskRunner to avoid circular imports."""
    def run(self, call: Callable, *args, **kwargs) -> Any: ...


class LoggerProtocol(Protocol):
    """Protocol for logger to avoid circular imports."""
    async def log(self, entry: Any) -> None: ...


@dataclass
class LocalLeaderElection:
    """
    Manages local (within-datacenter) leader election.
    
    Uses a lease-based approach with LHM-aware eligibility and split-brain prevention:
    
    Election Flow:
    1. Nodes monitor leader lease expiry
    2. When lease expires, eligible nodes start PRE-VOTE phase
    3. Only if pre-vote succeeds, proceed to real election
    4. Candidates request votes from peers
    5. Candidate with majority wins
    6. Leader sends periodic heartbeats to renew lease
    
    Split-Brain Prevention:
    - Pre-voting prevents disrupting healthy leaders
    - Term-based resolution when two leaders discover each other
    - Deterministic tiebreaker (lower address wins)
    - Fencing tokens for operation safety
    
    This is designed for low-latency, single-datacenter operation.
    """
    state: LeaderState = field(default_factory=LeaderState)
    eligibility: LeaderEligibility = field(default_factory=LeaderEligibility)
    flapping_detector: FlappingDetector = field(default_factory=FlappingDetector)
    
    # Configuration
    heartbeat_interval: float = 2.0  # Seconds between leader heartbeats
    election_timeout_base: float = 5.0  # Base election timeout
    election_timeout_jitter: float = 2.0  # Random jitter added to timeout
    pre_vote_timeout: float = 2.0  # Timeout for pre-vote phase
    
    # Datacenter identification
    dc_id: str = "default"
    
    # Reference to node address (set by owner)
    self_addr: tuple[str, int] | None = None
    
    # Callbacks for sending messages (set by owner)
    _broadcast_message: Callable[[bytes], None] | None = None
    _get_member_count: Callable[[], int] | None = None
    _get_lhm_score: Callable[[], int] | None = None
    _send_to_node: Callable[[tuple[str, int], bytes], None] | None = None
    _should_refuse_leadership: Callable[[], bool] | None = None  # Graceful degradation check
    
    # Error handler callback (set by owner)
    _on_error: Callable[[ElectionError], Awaitable[None]] | None = None
    
    # TaskRunner for managed async operations (optional)
    _task_runner: TaskRunnerProtocol | None = None
    
    # Background tasks
    _heartbeat_task: asyncio.Task | None = field(default=None, repr=False)
    _election_task: asyncio.Task | None = field(default=None, repr=False)
    _running: bool = False
    
    # Track fallback tasks created when TaskRunner not available
    _pending_error_tasks: set[asyncio.Task] = field(default_factory=set, repr=False)
    _unmanaged_tasks_created: int = 0
    
    # Logger for structured logging (optional)
    _logger: LoggerProtocol | None = None
    _node_host: str = ""
    _node_port: int = 0
    _node_id: int = 0
    
    def set_logger(
        self,
        logger: LoggerProtocol,
        node_host: str,
        node_port: int,
        node_id: int,
    ) -> None:
        """Set logger for structured logging."""
        self._logger = logger
        self._node_host = node_host
        self._node_port = node_port
        self._node_id = node_id
        # Also set logger on child components
        self.flapping_detector.set_logger(logger, node_host, node_port, node_id)
    
    async def _log_debug(self, message: str) -> None:
        """Log a debug message."""
        if self._logger:
            try:
                await self._logger.log(ServerDebug(
                    message=f"[LocalLeaderElection] {message}",
                    node_host=self._node_host,
                    node_port=self._node_port,
                    node_id=self._node_id,
                ))
            except Exception:
                pass  # Don't let logging errors propagate
    
    def set_callbacks(
        self,
        broadcast_message: Callable[[bytes], None],
        get_member_count: Callable[[], int],
        get_lhm_score: Callable[[], int],
        self_addr: tuple[str, int],
        send_to_node: Callable[[tuple[str, int], bytes], None] | None = None,
        on_error: Callable[[ElectionError], Awaitable[None]] | None = None,
        should_refuse_leadership: Callable[[], bool] | None = None,
        task_runner: TaskRunnerProtocol | None = None,
    ) -> None:
        """Set callback functions for election operations."""
        self._broadcast_message = broadcast_message
        self._get_member_count = get_member_count
        self._get_lhm_score = get_lhm_score
        self.self_addr = self_addr
        self._on_error = on_error
        self._send_to_node = send_to_node
        self._should_refuse_leadership = should_refuse_leadership
        self._task_runner = task_runner
    
    def get_election_timeout(self) -> float:
        """Get randomized election timeout, adjusted for flapping."""
        base = self.election_timeout_base
        
        # If flapping, use the escalated cooldown
        if self.flapping_detector.is_flapping:
            base = max(base, self.flapping_detector.current_cooldown)
        
        jitter = random.uniform(0, self.election_timeout_jitter)
        return base + jitter
    
    async def _record_leader_change(
        self,
        old_leader: tuple[str, int] | None,
        new_leader: tuple[str, int] | None,
        reason: str,
    ) -> None:
        """Record a leadership change for flapping detection."""
        await self.flapping_detector.record_change(
            old_leader=old_leader,
            new_leader=new_leader,
            term=self.state.current_term,
            reason=reason,
        )
    
    def is_self_eligible(self) -> bool:
        """Check if this node is eligible to become leader."""
        # Check graceful degradation first
        if self._should_refuse_leadership and self._should_refuse_leadership():
            return False
        
        if not self._get_lhm_score:
            return True
        lhm = self._get_lhm_score()
        return self.eligibility.is_eligible(lhm, b'OK', False)
    
    def should_step_down(self) -> bool:
        """Check if leader should step down due to high load."""
        if not self.state.is_leader():
            return False
        if not self._get_lhm_score:
            return False
        return self.eligibility.should_step_down(self._get_lhm_score())
    
    async def start(self) -> None:
        """Start the leader election process."""
        self._running = True
        self._election_task = asyncio.create_task(self._election_loop())
    
    async def stop(self) -> None:
        """Stop the leader election process."""
        self._running = False
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self._heartbeat_task = None
        
        if self._election_task:
            self._election_task.cancel()
            try:
                await self._election_task
            except asyncio.CancelledError:
                pass
            self._election_task = None
        
        # Cancel any pending error handler tasks
        for task in list(self._pending_error_tasks):
            if not task.done():
                task.cancel()
        self._pending_error_tasks.clear()
    
    async def _election_loop(self) -> None:
        """Main election monitoring loop."""
        while self._running:
            try:
                if self.state.is_leader():
                    # Leader: check if we should step down
                    if self.should_step_down():
                        await self._step_down()
                    else:
                        await asyncio.sleep(self.heartbeat_interval)
                        await self._send_heartbeat()
                
                elif self.state.should_start_election():
                    # No leader or lease expired: maybe start election
                    
                    # Check flapping - delay election if needed
                    should_delay, delay = self.flapping_detector.should_delay_election()
                    if should_delay:
                        await asyncio.sleep(delay)
                        continue
                    
                    if self.is_self_eligible():
                        await self._run_election()
                    else:
                        # Not eligible, log and wait for someone else
                        lhm = self._get_lhm_score() if self._get_lhm_score else 0
                        await self._handle_error(
                            NotEligibleError(
                                reason="LHM too high or degradation active",
                                lhm_score=lhm,
                                max_lhm=self.eligibility.max_leader_lhm,
                            )
                        )
                        await asyncio.sleep(self.get_election_timeout())
                
                else:
                    # Following a leader, wait for lease to expire
                    wait_time = self.state.time_until_lease_expiry()
                    await asyncio.sleep(min(wait_time + 0.5, self.heartbeat_interval))
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                await self._handle_error(
                    UnexpectedError(e, "election_loop")
                )
                await asyncio.sleep(1)
    
    async def _handle_error(self, error: ElectionError) -> None:
        """Handle an election error via callback or fallback to logging."""
        if self._on_error:
            try:
                await self._on_error(error)
            except Exception as e:
                # Log the callback failure
                await self._log_debug(
                    f"Error callback failed: {type(e).__name__}: {e} "
                    f"(original error: {error})"
                )
        # Error is logged by the handler, no need to log here
    
    def _handle_error_sync(self, error: ElectionError) -> None:
        """
        Handle an election error synchronously by scheduling async handler.
        
        Used by sync methods like handle_claim that need to report errors.
        """
        if self._on_error:
            # Use TaskRunner if available for proper lifecycle management
            if self._task_runner:
                self._task_runner.run(self._handle_error, error)
            else:
                try:
                    # Fall back to raw asyncio if no TaskRunner - track task for cleanup
                    loop = asyncio.get_running_loop()
                    
                    async def error_handler_wrapper():
                        try:
                            await self._handle_error(error)
                        finally:
                            # Remove self from pending tasks when done
                            self._pending_error_tasks.discard(asyncio.current_task())
                    
                    task = loop.create_task(error_handler_wrapper())
                    self._pending_error_tasks.add(task)
                    self._unmanaged_tasks_created += 1
                except RuntimeError:
                    # No running loop - error cannot be logged asynchronously
                    # This is an edge case; error is silently dropped
                    pass
    
    async def _run_pre_vote(self) -> bool:
        """
        Run pre-vote phase before election.
        
        Pre-voting prevents split-brain by checking if other nodes
        would vote for us before actually starting an election.
        This prevents disrupting a healthy leader.
        
        Returns True if pre-vote succeeded and we should proceed.
        """
        if not self.self_addr or not self._broadcast_message:
            return False
        
        # Abort if already in pre-vote (concurrent election attempt)
        if self.state.pre_voting_in_progress:
            return False
        
        new_term = self.state.current_term + 1
        
        # Validate term before starting
        if not self.state.is_term_valid(new_term):
            return False
        
        self.state.start_pre_vote(new_term)
        
        try:
            # Add self pre-vote
            self.state.record_pre_vote(self.self_addr)
            
            # Broadcast pre-vote request
            lhm = self._get_lhm_score() if self._get_lhm_score else 0
            pre_vote_msg = (
                b'pre-vote-req:' +
                str(new_term).encode() + b':' +
                str(lhm).encode() + b'>' +
                f'{self.self_addr[0]}:{self.self_addr[1]}'.encode()
            )
            self._broadcast_message(pre_vote_msg)
            
            # Wait for pre-votes with timeout protection
            await asyncio.sleep(self.pre_vote_timeout)
            
            # Check if a valid leader was discovered during pre-vote
            # This prevents continuing with election if we've already
            # received a heartbeat from a healthy leader
            if self.state.is_lease_valid() and self.state.current_leader:
                # A leader emerged during our pre-vote - abort
                return False
            
            # Check if our term became outdated (higher term seen)
            if self.state.current_term >= new_term:
                # Our pre-vote term is now stale - abort
                return False
            
            # Check if we got enough pre-votes
            n_members = self._get_member_count() if self._get_member_count else 1
            # Pre-vote needs majority: floor(n/2) + 1, but at least 1
            pre_votes_needed = max(1, (n_members // 2) + 1)
            
            success = len(self.state.pre_votes_received) >= pre_votes_needed
            return success
        except asyncio.CancelledError:
            # Pre-vote cancelled - clean up state
            return False
        finally:
            # Always clean up pre-vote state
            self.state.end_pre_vote()
    
    async def _run_election(self) -> None:
        """Run a leader election with pre-voting for split-brain prevention."""
        if not self.self_addr or not self._broadcast_message:
            return
        
        # Phase 1: Pre-vote (split-brain prevention)
        pre_vote_success = await self._run_pre_vote()
        
        if not pre_vote_success:
            # Pre-vote failed - don't start election
            # This likely means there's a healthy leader we can't reach
            # or other nodes wouldn't vote for us
            return
        
        # Phase 2: Real election
        new_term = self.state.next_term()
        
        # Check for term exhaustion (indicates attack or severe bug)
        if self.state.is_term_exhausted():
            # Log and bail - this should never happen in normal operation
            await self._log_debug(f"CRITICAL: Term exhausted at {self.state.current_term}")
            return
        
        if not self.state.start_election(new_term):
            # Term overflow - shouldn't happen with next_term()
            return
        self.state.update_fencing_token(new_term)
        
        # Vote for self
        self.state.vote_for(self.self_addr, new_term)
        self.state.record_vote(self.self_addr)
        
        # Broadcast claim
        lhm = self._get_lhm_score() if self._get_lhm_score else 0
        claim_msg = (
            b'leader-claim:' + 
            str(new_term).encode() + b':' +
            str(lhm).encode() + b'>' +
            f'{self.self_addr[0]}:{self.self_addr[1]}'.encode()
        )
        self._broadcast_message(claim_msg)
        
        # Wait for votes
        await asyncio.sleep(self.get_election_timeout())
        
        # Check if we won
        if self.state.role == 'candidate':  # Still candidate
            n_members = self._get_member_count() if self._get_member_count else 1
            # Majority = floor(n/2) + 1, equivalent to (n // 2) + 1
            votes_needed = (n_members // 2) + 1
            
            if len(self.state.votes_received) >= votes_needed:
                # We won!
                old_leader = self.state.current_leader
                if not self.state.become_leader(new_term):
                    # Term became invalid (shouldn't happen)
                    return
                self.state.current_leader = self.self_addr
                await self._record_leader_change(old_leader, self.self_addr, 'election')
                self.state.update_fencing_token(new_term)
                
                # Announce victory
                elected_msg = (
                    b'leader-elected:' +
                    str(new_term).encode() + b'>' +
                    f'{self.self_addr[0]}:{self.self_addr[1]}'.encode()
                )
                self._broadcast_message(elected_msg)
                
                # Start heartbeating
                await self._send_heartbeat()
    
    async def _send_heartbeat(self) -> None:
        """Send leader heartbeat."""
        if not self.state.is_leader() or not self.self_addr or not self._broadcast_message:
            return
        
        self.state.renew_lease()
        
        heartbeat_msg = (
            b'leader-heartbeat:' +
            str(self.state.current_term).encode() + b'>' +
            f'{self.self_addr[0]}:{self.self_addr[1]}'.encode()
        )
        self._broadcast_message(heartbeat_msg)
    
    async def _step_down(self) -> None:
        """Voluntarily step down from leadership."""
        if not self.state.is_leader() or not self.self_addr or not self._broadcast_message:
            return
        
        stepdown_msg = (
            b'leader-stepdown:' +
            str(self.state.current_term).encode() + b'>' +
            f'{self.self_addr[0]}:{self.self_addr[1]}'.encode()
        )
        self._broadcast_message(stepdown_msg)
        
        await self._record_leader_change(self.self_addr, None, 'stepdown')
        self.state.become_follower(self.state.current_term)
    
    def handle_claim(
        self,
        candidate: tuple[str, int],
        term: int,
        candidate_lhm: int,
    ) -> bytes | None:
        """
        Handle a leader-claim message.
        Returns vote message if we vote for the candidate, None otherwise.
        """
        if not self.self_addr:
            return None
        
        # Ignore claims from lower terms
        if term < self.state.current_term:
            return None
        
        # Check if candidate is eligible (based on their LHM)
        if not self.eligibility.is_eligible(candidate_lhm, b'OK', False):
            self._handle_error_sync(
                NotEligibleError(
                    reason=f"Candidate {candidate} has LHM {candidate_lhm} above threshold",
                    lhm_score=candidate_lhm,
                    max_lhm=self.eligibility.max_leader_lhm,
                )
            )
            return None
        
        # Check if we can vote for them
        if not self.state.can_vote_for(candidate, term):
            return None
        
        # Vote for the candidate
        self.state.vote_for(candidate, term)
        
        vote_msg = (
            b'leader-vote:' +
            str(term).encode() + b'>' +
            f'{candidate[0]}:{candidate[1]}'.encode()
        )
        return vote_msg
    
    def handle_vote(self, voter: tuple[str, int], term: int) -> bool:
        """
        Handle a leader-vote message.
        Returns True if this vote wins the election.
        """
        if term != self.state.current_term or not self.state.is_candidate():
            return False
        
        vote_count = self.state.record_vote(voter)
        n_members = self._get_member_count() if self._get_member_count else 1
        votes_needed = (n_members // 2) + 1
        
        return vote_count >= votes_needed
    
    async def handle_elected(self, leader: tuple[str, int], term: int) -> None:
        """Handle a leader-elected message."""
        if term >= self.state.current_term:
            old_leader = self.state.current_leader
            self.state.become_follower(term, leader)
            if old_leader != leader:
                await self._record_leader_change(old_leader, leader, 'elected')
    
    async def handle_heartbeat(self, leader: tuple[str, int], term: int) -> None:
        """Handle a leader-heartbeat message."""
        old_leader = self.state.current_leader
        self.state.update_heartbeat(leader, term)
        # Record if this is first time seeing this leader
        if old_leader != leader and leader is not None:
            await self._record_leader_change(old_leader, leader, 'heartbeat')
    
    async def handle_stepdown(self, leader: tuple[str, int], term: int) -> None:
        """Handle a leader-stepdown message."""
        if leader == self.state.current_leader:
            await self._record_leader_change(leader, None, 'remote_stepdown')
            self.state.current_leader = None
            # Will trigger election on next loop iteration
    
    def handle_pre_vote_request(
        self,
        candidate: tuple[str, int],
        term: int,
        candidate_lhm: int,
    ) -> bytes | None:
        """
        Handle a pre-vote request.
        
        Returns pre-vote response if we would vote for them, None otherwise.
        Pre-votes don't update our state - they're just a check.
        """
        if not self.self_addr:
            return None
        
        # Check if candidate's LHM makes them ineligible
        if candidate_lhm > self.eligibility.max_leader_lhm:
            self._handle_error_sync(
                NotEligibleError(
                    reason=f"Pre-vote denied: candidate {candidate} LHM {candidate_lhm} too high",
                    lhm_score=candidate_lhm,
                    max_lhm=self.eligibility.max_leader_lhm,
                )
            )
        
        # Check if we would grant this pre-vote
        can_grant = self.state.can_grant_pre_vote(
            candidate=candidate,
            term=term,
            candidate_lhm=candidate_lhm,
            max_leader_lhm=self.eligibility.max_leader_lhm,
        )
        
        # Build response: pre-vote-resp:term:granted>candidate_addr
        granted = b'1' if can_grant else b'0'
        resp_msg = (
            b'pre-vote-resp:' +
            str(term).encode() + b':' +
            granted + b'>' +
            f'{candidate[0]}:{candidate[1]}'.encode()
        )
        return resp_msg
    
    def handle_pre_vote_response(
        self,
        voter: tuple[str, int],
        term: int,
        granted: bool,
    ) -> None:
        """
        Handle a pre-vote response.
        
        Records the pre-vote if it was granted and matches our pre-vote term.
        """
        if term != self.state.pre_vote_term or not self.state.pre_voting_in_progress:
            return
        
        if granted:
            self.state.record_pre_vote(voter)
    
    def handle_discovered_leader(
        self,
        other_leader: tuple[str, int],
        other_term: int,
    ) -> bool:
        """
        Handle discovering another leader (split-brain detection).
        
        Called when we receive a heartbeat from another leader while
        we are also a leader. This should not happen in normal operation.
        
        Returns True if we should step down (yield to the other leader).
        """
        if not self.state.is_leader() or not self.self_addr:
            return False
        
        should_yield = self.state.should_yield_to(
            other_addr=other_leader,
            other_term=other_term,
            self_addr=self.self_addr,
        )
        
        return should_yield
    
    def get_fencing_token(self) -> int:
        """
        Get current fencing token for operations.
        
        Operations should include this token, and workers should
        reject operations with tokens lower than what they've seen.
        """
        return self.state.get_fencing_token()
    
    def validate_fencing_token(self, token: int) -> bool:
        """
        Validate a fencing token for an operation.
        
        Returns True if the operation should proceed,
        False if it's from a stale leader.
        """
        return self.state.is_fencing_token_valid(token)
    
    def get_current_leader(self) -> tuple[str, int] | None:
        """Get the current leader, if any."""
        if self.state.is_leader() and self.self_addr:
            return self.self_addr
        return self.state.current_leader if self.state.is_lease_valid() else None
    
    def get_status(self) -> dict:
        """Get current leadership status for debugging."""
        return {
            'role': self.state.role,
            'term': self.state.current_term,
            'leader': self.get_current_leader(),
            'lease_remaining': self.state.time_until_lease_expiry(),
            'eligible': self.is_self_eligible(),
            'votes': len(self.state.votes_received) if self.state.is_candidate() else 0,
            'fencing_token': self.get_fencing_token(),
            'pre_voting': self.state.pre_voting_in_progress,
            'pre_votes': len(self.state.pre_votes_received),
            'flapping': self.flapping_detector.is_flapping,
            'flapping_cooldown': self.flapping_detector.current_cooldown,
        }
    
    def get_flapping_stats(self) -> dict:
        """Get flapping detector statistics."""
        return self.flapping_detector.get_stats()

