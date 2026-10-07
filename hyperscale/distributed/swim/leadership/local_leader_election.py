"""
Local leader election with pre-voting and split-brain prevention.
"""

import asyncio
from dataclasses import dataclass, field
from typing import Callable, Awaitable

from hyperscale.distributed.runtime import Clock, Random, RealClock, RealRandom
from .leader_state import LeaderState
from .leader_eligibility import LeaderEligibility
from .flapping_detector import FlappingDetector
from hyperscale.distributed.swim.core.errors import ElectionError, UnexpectedError, NotEligibleError

from hyperscale.logging.hyperscale_logging_models import ServerDebug


from hyperscale.distributed.swim.core.protocols import LoggerProtocol, TaskRunnerProtocol


_DEFAULT_CLOCK: Clock = RealClock()
_DEFAULT_RANDOM: Random = RealRandom()


@dataclass(slots=True)
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
    
    # Callbacks for sending messages (set by owner). The broadcast is
    # awaitable because the only real implementation
    # (HealthAwareServer._broadcast_leadership_message) is async — it
    # reads current_timeout from the server context and dispatches sends
    # via the task runner. Earlier this field was typed as sync; calls
    # silently produced a coroutine that was dropped on the floor and
    # NO leadership messages ever left the node, so multi-manager
    # leader election never converged.
    _broadcast_message: Callable[[bytes], Awaitable[None]] | None = None
    _get_member_count: Callable[[], int] | None = None
    # Whether a vote from this address counts toward a majority: only the
    # members the majority is counted over vote (Raft section 5.2).
    _is_cohort_voter: Callable[[tuple[str, int]], bool] | None = None
    _get_lhm_score: Callable[[], int] | None = None
    _send_to_node: Callable[[tuple[str, int], bytes], None] | None = None
    _should_refuse_leadership: Callable[[], bool] | None = None  # Graceful degradation check
    # Whether this node, while LEADING, can no longer do a leader's work
    # and must hand leadership off (a role's leadership refusals: e.g. a
    # gate with no reachable datacenter while a peer gate is ready, AD-19).
    # Checked on every lead tick, so a leader elected before the refusal
    # held (no peer was ready yet) does not keep the role it now refuses.
    _should_relinquish_leadership: Callable[[], bool] | None = None
    
    # Error handler callback (set by owner)
    _on_error: Callable[[ElectionError], Awaitable[None]] | None = None
    
    # Metrics callbacks (set by owner)
    _on_election_started: Callable[[], None] | None = None
    _on_heartbeat_sent: Callable[[], None] | None = None
    
    # TaskRunner for managed async operations (optional)
    _task_runner: TaskRunnerProtocol | None = None
    
    # Background tasks
    _heartbeat_task: asyncio.Task | None = field(default=None, repr=False)
    _election_task: asyncio.Task | None = field(default=None, repr=False)
    _election_wake_event: asyncio.Event | None = field(default=None, repr=False)
    # When this node, leaderless, stands for election: drawn once per
    # leaderless spell (see ``_election_loop``), cleared on following or
    # leading.
    _stand_for_election_at: float | None = field(default=None, repr=False)
    # When the election loop's current wait ends at the latest -- the
    # candidacy wait, a pre-vote or vote round's timeout, a flapping delay,
    # a follower's lease check: the instant it next decides. Read by
    # ``seconds_until_next_decision``; 0.0 before the loop first waits.
    _next_decision_at: float = field(default=0.0, repr=False)
    _running: bool = False
    
    # Track fallback tasks created when TaskRunner not available
    _pending_error_tasks: set[asyncio.Task] = field(default_factory=set, repr=False)
    _unmanaged_tasks_created: int = 0
    # Debug log records lost because the logger's write itself failed.
    _log_write_failures: int = 0
    
    # Logger for structured logging (optional)
    _logger: LoggerProtocol | None = None
    _node_host: str = ""
    _node_port: int = 0
    _node_id: int = 0

    # Phase 5 DI seams — every wall/monotonic read and randomized
    # backoff draw routes through these so Phase 6 SIM mode can drive
    # election timing deterministically. Defaults to module-level
    # ``RealClock`` / ``RealRandom`` so existing callers see
    # byte-identical behavior.
    _clock: Clock = field(default_factory=lambda: _DEFAULT_CLOCK, repr=False)
    _random: Random = field(default_factory=lambda: _DEFAULT_RANDOM, repr=False)

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
                # The logger itself failed: nowhere left to report it but
                # this election's status.
                self._log_write_failures += 1

    def _log_debug_sync(self, message: str) -> None:
        """Schedule a debug log from a synchronous code path.

        Pre-vote / claim / vote handlers run synchronously but still need
        observability. Schedule the log via the task runner if available;
        otherwise drop on the floor (logging is best-effort).
        """
        if not self._can_schedule_debug_log():
            return
        try:
            self._task_runner.run(
                self._logger.log,
                ServerDebug(
                    message=f"[LocalLeaderElection] {message}",
                    node_host=self._node_host,
                    node_port=self._node_port,
                    node_id=self._node_id,
                ),
            )
        except Exception:
            self._log_write_failures += 1
    
    def _can_schedule_debug_log(self) -> bool:
        """Whether a logger and a task runner are both set for sync-path debug logs."""
        return bool(self._logger) and self._task_runner is not None

    def _member_count_or(self, default: int) -> int:
        """The member count, or ``default`` when no member-count callback is set."""
        return self._get_member_count() if self._get_member_count else default

    def _member_count_label(self) -> str | int:
        """The member count for log context; ``?`` when no member-count callback is set."""
        return self._get_member_count() if self._get_member_count else '?'

    def _lhm_score_or_zero(self) -> int:
        """This node's LHM score, or 0 when no LHM callback is set."""
        return self._get_lhm_score() if self._get_lhm_score else 0

    def _lacks_campaign_callbacks(self) -> bool:
        """Whether the self address or broadcast callback needed to campaign or lead is unset."""
        return not self.self_addr or not self._broadcast_message

    def _cannot_lead_broadcast(self) -> bool:
        """Whether this node is not the leader or cannot broadcast as one."""
        return not self.state.is_leader() or self._lacks_campaign_callbacks()

    def _rejects_voter(self, voter: tuple[str, int]) -> bool:
        """Whether ``voter`` falls outside the election cohort, when a cohort callback is set."""
        return self._is_cohort_voter is not None and not self._is_cohort_voter(voter)

    def _pre_vote_majority(self) -> int:
        """Pre-votes needed: a majority, floor(n/2) + 1, but at least 1."""
        return max(1, (self._member_count_or(1) // 2) + 1)

    def set_callbacks(
        self,
        broadcast_message: Callable[[bytes], Awaitable[None]],
        get_member_count: Callable[[], int],
        get_lhm_score: Callable[[], int],
        self_addr: tuple[str, int],
        send_to_node: Callable[[tuple[str, int], bytes], None] | None = None,
        on_error: Callable[[ElectionError], Awaitable[None]] | None = None,
        should_refuse_leadership: Callable[[], bool] | None = None,
        should_relinquish_leadership: Callable[[], bool] | None = None,
        task_runner: TaskRunnerProtocol | None = None,
        on_election_started: Callable[[], None] | None = None,
        on_heartbeat_sent: Callable[[], None] | None = None,
        is_cohort_voter: Callable[[tuple[str, int]], bool] | None = None,
    ) -> None:
        """Set callback functions for election operations."""
        self._broadcast_message = broadcast_message
        self._get_member_count = get_member_count
        self._is_cohort_voter = is_cohort_voter
        self._get_lhm_score = get_lhm_score
        self.self_addr = self_addr
        self._on_error = on_error
        self._send_to_node = send_to_node
        self._should_refuse_leadership = should_refuse_leadership
        self._should_relinquish_leadership = should_relinquish_leadership
        self._task_runner = task_runner
        self._on_election_started = on_election_started
        self._on_heartbeat_sent = on_heartbeat_sent
    
    def get_election_timeout(self) -> float:
        """Get randomized election timeout, adjusted for flapping."""
        base = self.election_timeout_base
        
        # If flapping, use the escalated cooldown
        if self.flapping_detector.is_flapping:
            base = max(base, self.flapping_detector.current_cooldown)
        
        jitter = self._random.uniform(0, self.election_timeout_jitter)
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

    async def _record_election_failure(self, reason: str) -> None:
        """Record a failed election attempt for flapping/backoff detection."""
        await self.flapping_detector.record_change(
            old_leader=self.state.current_leader,
            new_leader=self.state.current_leader,
            term=self.state.current_term,
            reason=reason,
        )
    
    def is_self_eligible(self) -> bool:
        """Check if this node is eligible to become leader."""
        # Check graceful degradation first
        if self._refuses_leadership():
            return False
        
        if not self._get_lhm_score:
            return True
        lhm = self._get_lhm_score()
        return self.eligibility.is_eligible(lhm, b'OK', False)
    
    def _refuses_leadership(self) -> bool:
        """Whether graceful degradation asks this node to refuse leadership."""
        return self._should_refuse_leadership and self._should_refuse_leadership()

    def should_step_down(self) -> bool:
        """Check if the leader should step down: its role refuses
        leadership now (it cannot do a leader's work while a peer can),
        or its load (LHM) warrants handing off."""
        if not self.state.is_leader():
            return False
        return self._relinquishes_leadership() or self._lhm_warrants_step_down()

    def _relinquishes_leadership(self) -> bool:
        """Whether this node's role asks it to hand leadership off."""
        return self._should_relinquish_leadership is not None and self._should_relinquish_leadership()

    def _lhm_warrants_step_down(self) -> bool:
        """Whether this leader's LHM warrants stepping down to a healthier peer."""
        # If we are the only member, there is no peer to hand off to.
        # Stepping down here would leave the DC leaderless until LHM
        # recovers, blocking the submit path on a single-node cluster
        # for no benefit. The Lifeguard step-down dance only buys
        # availability when a healthier peer exists.
        if not self._get_lhm_score or self._is_sole_member():
            return False
        return self.eligibility.should_step_down(self._get_lhm_score())

    def _is_sole_member(self) -> bool:
        """Whether this node is the cluster's only member."""
        return self._get_member_count is not None and self._get_member_count() <= 1
    
    async def start(self) -> None:
        """Start the leader election process."""
        self._running = True
        self._election_wake_event = asyncio.Event()
        # Phase 6b: explicit ``loop.create_task`` so the task binds to
        # the loop ``start`` was called from rather than implicitly going
        # through ``get_running_loop`` at task-creation time.
        self._election_task = asyncio.get_running_loop().create_task(
            self._election_loop()
        )
    
    async def stop(self) -> None:
        """Stop the leader election process."""
        self._running = False
        if self._heartbeat_task:
            await self._cancel_and_await_task(self._heartbeat_task)
            self._heartbeat_task = None
        
        if self._election_task:
            await self._cancel_and_await_task(self._election_task)
            self._election_task = None
        self._election_wake_event = None

        # Cancel any pending error handler tasks
        self._cancel_pending_error_tasks()

    @staticmethod
    async def _cancel_and_await_task(task: asyncio.Task) -> None:
        """Cancel ``task`` and wait for it to end, re-raising only a cancel aimed at the caller."""
        task.cancel()
        cancels_requested_before_wait = asyncio.current_task().cancelling()
        try:
            await task
        except asyncio.CancelledError:
            # The task we cancelled ended; a cancel aimed at this task
            # while it waited goes on.
            if asyncio.current_task().cancelling() > cancels_requested_before_wait:
                raise

    def _cancel_pending_error_tasks(self) -> None:
        """Cancel every unfinished fallback error-handler task, then forget them all."""
        for task in list(self._pending_error_tasks):
            if not task.done():
                task.cancel()
        self._pending_error_tasks.clear()

    def _wake_election_loop(self) -> None:
        """Wake the election loop after leadership state changes."""
        if self._election_wake_event is not None:
            self._election_wake_event.set()

    async def _wait_for_election_wake(self, timeout: float) -> None:
        """Wait until either the election loop is woken or ``timeout`` elapses.

        Progress floor (the a85533ee chokepoint idiom): callers pass
        COMPOSED-float remainders (flapping cooldowns, lease expiries,
        election timeouts), and a positive sub-quantum artifact would arm
        a timer the quantized clock cannot honor — the election loop
        would re-evaluate the same instant forever (SIM livelock,
        100%-CPU micro-spin on a real host). Flooring at 1ms guarantees
        the clock moves every iteration; genuine election waits (>=
        hundreds of ms) are unaffected — the floor binds only in the
        artifact regime, which previously meant livelock.
        """
        if timeout <= 0:
            return
        timeout = max(timeout, 0.001)
        self._next_decision_at = self._clock.monotonic() + timeout

        if self._election_wake_event is None:
            await self._clock.sleep(timeout)
            return

        await self._wait_for_wake_event(self._election_wake_event, timeout)

    async def _wait_for_wake_event(self, event: asyncio.Event, timeout: float) -> None:
        """Consume a pending wake, else wait up to ``timeout`` for one; the event is left clear."""
        if event.is_set():
            event.clear()
            return

        try:
            await self._clock.wait_for(event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            return
        finally:
            event.clear()
    
    async def _election_loop(self) -> None:
        """Main election monitoring loop."""
        await self._log_debug(
            f"election_loop start self_addr={self.self_addr} dc={self.dc_id}"
        )
        while self._running:
            if not await self._election_iteration():
                break

    async def _election_iteration(self) -> bool:
        """One election-loop pass; False once cancelled, an unexpected error reported and waited out."""
        try:
            await self._election_tick()
            return True
        except asyncio.CancelledError:
            return False
        except Exception as e:
            await self._handle_error(
                UnexpectedError(e, "election_loop")
            )
            await self._wait_for_election_wake(1)
            return True

    async def _election_tick(self) -> None:
        """Act on this node's role: lead, stand for election, or follow."""
        if self.state.is_leader():
            await self._lead_tick()

        elif self.state.should_start_election():
            # No leader or lease expired: maybe start election
            await self._candidate_tick()

        else:
            await self._follow_tick()

    async def _lead_tick(self) -> None:
        """Leader: step down when LHM warrants it, else wait a heartbeat interval and beat."""
        self._stand_for_election_at = None
        # Leader: check if we should step down
        if self.should_step_down():
            await self._log_debug(
                f"step_down triggered (leadership refusal or LHM) "
                f"term={self.state.current_term}"
            )
            await self._step_down()
        else:
            await self._wait_for_election_wake(
                self.heartbeat_interval
            )
            await self._send_heartbeat()

    async def _candidate_tick(self) -> None:
        """Leaderless: stand once the randomized election wait (Raft section 5.2) has passed."""
        # Raft's randomized election timeout (section 5.2): stand
        # only after a random wait, drawn once per leaderless
        # spell. Every follower of a leader that went quiet saw
        # the same last heartbeat (and every node boots
        # leaderless together): standing at one instant, each
        # voted for itself -- a split first round, and a full
        # election timeout lost, on every failover and boot.
        # The first to stand now asks peers still waiting, who
        # vote for it. Later rounds are already spread by the
        # randomized vote wait in ``_run_election``.
        now = self._clock.monotonic()
        if self._stand_for_election_at is None:
            self._stand_for_election_at = now + self._random.uniform(
                0, self.election_timeout_jitter
            )
        if now < self._stand_for_election_at:
            await self._wait_for_election_wake(
                self._stand_for_election_at - now
            )
            return

        await self._stand_unless_flapping()

    async def _stand_unless_flapping(self) -> None:
        """Stand for election unless the flapping detector delays it."""
        # Check flapping - delay election if needed
        should_delay, delay = self.flapping_detector.should_delay_election()
        if should_delay:
            await self._log_debug(
                f"election delayed by flapping detector ({delay:.2f}s)"
            )
            await self._wait_for_election_wake(delay)
            return

        await self._stand_for_election()

    async def _stand_for_election(self) -> None:
        """Run an election when eligible, else report ineligibility and wait for another node."""
        eligible = self.is_self_eligible()
        await self._log_debug(
            f"election_loop tick: starting election "
            f"term={self.state.current_term} eligible={eligible} "
            f"members={self._member_count_label()}"
        )
        if eligible:
            await self._run_election()
        else:
            await self._wait_out_ineligibility()

    async def _wait_out_ineligibility(self) -> None:
        """Report this node ineligible to lead, then wait an election timeout for someone else."""
        # Not eligible, log and wait for someone else
        lhm = self._lhm_score_or_zero()
        await self._handle_error(
            NotEligibleError(
                reason="LHM too high or degradation active",
                lhm_score=lhm,
                max_lhm=self.eligibility.max_leader_lhm,
            )
        )
        await self._wait_for_election_wake(
            self.get_election_timeout()
        )

    async def _follow_tick(self) -> None:
        """Follower: wait for the lease to lapse, at most a heartbeat interval."""
        # Following a leader, wait for lease to expire. Woken at
        # expiry, a lapsed lease starts the randomized wait
        # above -- the spread a fixed slack here never gave.
        self._stand_for_election_at = None
        wait_time = self.state.time_until_lease_expiry()
        await self._wait_for_election_wake(
            min(wait_time, self.heartbeat_interval)
        )
    
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
        if not self._on_error:
            return
        # Use TaskRunner if available for proper lifecycle management
        if self._task_runner:
            self._task_runner.run(self._handle_error, error)
            return
        self._spawn_error_handler_task(error)

    def _spawn_error_handler_task(self, error: ElectionError) -> None:
        """Run the error handler as a tracked loop task when no TaskRunner is set."""
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
            # No running loop to report through: the sync caller
            # gets the error itself rather than losing it.
            raise error
    
    async def _run_pre_vote(self) -> bool:
        """
        Run pre-vote phase before election.

        Pre-voting prevents split-brain by checking if other nodes
        would vote for us before actually starting an election.
        This prevents disrupting a healthy leader.

        Returns True if pre-vote succeeded and we should proceed.
        """
        await self._log_debug(
            f"pre_vote start self={self.self_addr} "
            f"broadcast_callable={self._broadcast_message is not None} "
            f"members={self._member_count_label()} "
            f"current_term={self.state.current_term}"
        )
        if self._lacks_campaign_callbacks():
            await self._log_debug(
                "pre_vote abort: self_addr or broadcast callback unset"
            )
            return False

        return await self._open_pre_vote()

    async def _open_pre_vote(self) -> bool:
        """Open a pre-vote round for the next term, unless one is running or the term is invalid."""
        # Abort if already in pre-vote (concurrent election attempt)
        if self.state.pre_voting_in_progress:
            return False
        
        new_term = self.state.current_term + 1
        
        # Validate term before starting
        if not self.state.is_term_valid(new_term):
            return False
        
        self.state.start_pre_vote(new_term)

        return await self._conduct_pre_vote(new_term)

    async def _conduct_pre_vote(self, new_term: int) -> bool:
        """Gather and tally the opened pre-vote, always ending it (even on cancellation)."""
        try:
            return await self._gather_pre_votes(new_term)
        except asyncio.CancelledError:
            # Pre-vote cancelled — re-raise so the outer election loop's
            # ``except asyncio.CancelledError: break`` can exit cleanly.
            # Returning False here masked the cancellation, the election
            # loop saw a benign "no quorum" and re-iterated forever, and
            # the supervisor's leak detector caught the loop task as
            # surviving cancellation. ``finally`` still runs to clean
            # up pre-vote state before the exception propagates.
            raise
        finally:
            # Always clean up pre-vote state
            self.state.end_pre_vote()

    async def _gather_pre_votes(self, new_term: int) -> bool:
        """Pre-vote for self, broadcast the request, wait for a majority, then tally."""
        # Add self pre-vote
        self.state.record_pre_vote(self.self_addr)
        
        # Broadcast pre-vote request
        lhm = self._lhm_score_or_zero()
        pre_vote_msg = (
            b'pre-vote-req:' +
            str(new_term).encode() + b':' +
            str(lhm).encode() + b'>' +
            f'{self.self_addr[0]}:{self.self_addr[1]}'.encode()
        )
        await self._broadcast_message(pre_vote_msg)

        # Wait for pre-votes with timeout protection -- unless our own
        # already makes the majority (a cohort of one): nothing more can
        # arrive that the outcome waits on.
        if len(self.state.pre_votes_received) < self._pre_vote_majority():
            await self._wait_for_election_wake(self.pre_vote_timeout)

        return await self._tally_pre_votes(new_term)

    async def _tally_pre_votes(self, new_term: int) -> bool:
        """Whether the pre-vote won a majority without a leader or higher term appearing meanwhile."""
        if self._pre_vote_superseded(new_term):
            return False
        
        # Check if we got enough pre-votes
        n_members = self._member_count_or(1)
        # Pre-vote needs majority: floor(n/2) + 1, but at least 1
        pre_votes_needed = max(1, (n_members // 2) + 1)

        success = len(self.state.pre_votes_received) >= pre_votes_needed
        await self._log_debug(
            f"pre_vote result self={self.self_addr} term={new_term} "
            f"members={n_members} needed={pre_votes_needed} "
            f"got={len(self.state.pre_votes_received)} "
            f"voters={list(self.state.pre_votes_received)} "
            f"success={success}"
        )
        return success

    def _pre_vote_superseded(self, new_term: int) -> bool:
        """Whether a leader emerged, or a higher term was seen, during the pre-vote."""
        # Check if a valid leader was discovered during pre-vote
        # This prevents continuing with election if we've already
        # received a heartbeat from a healthy leader (a leader emerged
        # during our pre-vote - abort), or if our term became outdated
        # (higher term seen: our pre-vote term is now stale - abort).
        return (
            self.state.is_lease_valid() and self.state.current_leader
        ) or self.state.current_term >= new_term
    
    async def _run_election(self) -> None:
        """Run a leader election with pre-voting for split-brain prevention."""
        if self._lacks_campaign_callbacks():
            await self._log_debug(
                "run_election abort: self_addr or broadcast callback unset"
            )
            return

        # Phase 1: Pre-vote (split-brain prevention)
        pre_vote_success = await self._run_pre_vote()

        if not pre_vote_success:
            # Pre-vote failed - don't start election
            # This likely means there's a healthy leader we can't reach
            # or other nodes wouldn't vote for us
            await self._log_debug(
                "run_election abort: pre_vote did not reach majority"
            )
            await self._record_election_failure("pre_vote_failed")
            return

        await self._campaign()

    async def _campaign(self) -> None:
        """Phase 2, the real election: take the next term, then claim leadership in it."""
        # Phase 2: Real election
        new_term = self.state.next_term()

        if await self._open_election(new_term):
            await self._claim_leadership(new_term)

    async def _open_election(self, new_term: int) -> bool:
        """Become candidate for ``new_term``; False (recorded as a failure) when the term is refused."""
        # Check for term exhaustion (indicates attack or severe bug)
        if self.state.is_term_exhausted():
            # Log and bail - this should never happen in normal operation
            await self._log_debug(f"CRITICAL: Term exhausted at {self.state.current_term}")
            await self._record_election_failure("term_exhausted")
            return False

        if not self.state.start_election(new_term):
            # Term overflow - shouldn't happen with next_term()
            await self._log_debug(
                f"run_election abort: start_election rejected term={new_term}"
            )
            await self._record_election_failure("start_election_rejected")
            return False
        self.state.update_fencing_token(new_term)
        return True

    async def _claim_leadership(self, new_term: int) -> None:
        """Vote for self, broadcast the claim, wait for votes, then tally (Raft section 5.2)."""
        # Notify that election has started (for metrics)
        if self._on_election_started:
            self._on_election_started()

        # Vote for self
        self.state.vote_for(self.self_addr, new_term)
        self.state.record_vote(self.self_addr)

        # Broadcast claim
        lhm = self._lhm_score_or_zero()
        claim_msg = (
            b'leader-claim:' +
            str(new_term).encode() + b':' +
            str(lhm).encode() + b'>' +
            f'{self.self_addr[0]}:{self.self_addr[1]}'.encode()
        )
        await self._log_debug(
            f"run_election broadcast leader-claim term={new_term} lhm={lhm}"
        )
        await self._broadcast_message(claim_msg)

        # Wait for votes -- unless our own already makes the majority (a
        # cohort of one): Raft's candidate becomes leader the moment it
        # holds a majority (section 5.2), as ``handle_vote`` wakes us for
        # one that peers' votes complete.
        if len(self.state.votes_received) < (
            self._member_count_or(1) // 2
        ) + 1:
            await self._wait_for_election_wake(self.get_election_timeout())

        await self._tally_election(new_term)

    async def _tally_election(self, new_term: int) -> None:
        """Count the votes if still a candidate."""
        # Check if we won
        if self.state.role == 'candidate':  # Still candidate
            await self._count_votes(new_term)
        else:
            await self._log_debug(
                f"run_election ended as {self.state.role}; not tallying votes"
            )

    async def _count_votes(self, new_term: int) -> None:
        """Take leadership on a majority of votes, else record the lost election."""
        n_members = self._member_count_or(1)
        # Majority = floor(n/2) + 1, equivalent to (n // 2) + 1
        votes_needed = (n_members // 2) + 1

        await self._log_debug(
            f"run_election tally term={new_term} members={n_members} "
            f"needed={votes_needed} got={len(self.state.votes_received)} "
            f"voters={list(self.state.votes_received)}"
        )

        if len(self.state.votes_received) >= votes_needed:
            await self._take_leadership(new_term)
        else:
            await self._log_debug(
                f"run_election lost: not enough votes "
                f"({len(self.state.votes_received)} < {votes_needed})"
            )
            await self._record_election_failure("election_lost_no_quorum")

    async def _take_leadership(self, new_term: int) -> None:
        """Become leader for ``new_term``, announce the victory and start heartbeating."""
        # We won!
        old_leader = self.state.current_leader
        if not self.state.become_leader(new_term):
            # Term became invalid (shouldn't happen)
            await self._log_debug(
                f"run_election abort: become_leader rejected "
                f"term={new_term}"
            )
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
        await self._log_debug(
            f"run_election won — broadcasting leader-elected term={new_term}"
        )
        await self._broadcast_message(elected_msg)

        # Start heartbeating
        await self._send_heartbeat()
    
    async def _send_heartbeat(self) -> None:
        """Send leader heartbeat."""
        if self._cannot_lead_broadcast():
            return
        
        self.state.renew_lease()

        # Monotonic per-beat sequence + authoritative lease grant (fixes
        # 2/4 and 4/4): ``leader-heartbeat:{term}:{seq}:{lease_ms}>{addr}``.
        # ``seq`` makes each beat unique on the wire and lets the follower
        # apply beats monotonically; ``lease_ms`` is the lease length this
        # leader grants, so followers honor the leader's duration rather
        # than their own local config.
        self.state.heartbeat_seq += 1
        lease_ms = int(self.state.lease_duration * 1000)
        heartbeat_msg = (
            b'leader-heartbeat:' +
            str(self.state.current_term).encode() + b':' +
            str(self.state.heartbeat_seq).encode() + b':' +
            str(lease_ms).encode() + b'>' +
            f'{self.self_addr[0]}:{self.self_addr[1]}'.encode()
        )
        await self._broadcast_message(heartbeat_msg)
        
        # Notify metrics
        if self._on_heartbeat_sent:
            self._on_heartbeat_sent()
    
    async def _step_down(self) -> None:
        """Voluntarily step down from leadership."""
        if self._cannot_lead_broadcast():
            return
        
        stepdown_msg = (
            b'leader-stepdown:' +
            str(self.state.current_term).encode() + b'>' +
            f'{self.self_addr[0]}:{self.self_addr[1]}'.encode()
        )
        await self._broadcast_message(stepdown_msg)
        
        await self._record_leader_change(self.self_addr, None, 'stepdown')
        self.state.become_follower(self.state.current_term)
        self._wake_election_loop()
    
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

        return self._vote_on_claim(candidate, term, candidate_lhm)

    def _vote_on_claim(
        self,
        candidate: tuple[str, int],
        term: int,
        candidate_lhm: int,
    ) -> bytes | None:
        """Vote for an eligible candidate this node can still vote for; the vote message, else None."""
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
        if self._rejects_vote(voter, term):
            return False

        vote_count = self.state.record_vote(voter)
        n_members = self._member_count_or(1)
        votes_needed = (n_members // 2) + 1

        won = vote_count >= votes_needed
        if won:
            self._wake_election_loop()

        return won
    
    def _rejects_vote(self, voter: tuple[str, int], term: int) -> bool:
        """Whether a vote is for another term, arrives while not a candidate, or is from outside the cohort."""
        return (
            term != self.state.current_term
            or not self.state.is_candidate()
            or self._rejects_voter(voter)
        )

    async def handle_elected(self, leader: tuple[str, int], term: int) -> None:
        """Handle a leader-elected message."""
        if term >= self.state.current_term:
            old_leader = self.state.current_leader
            self.state.become_follower(term, leader)
            if old_leader != leader:
                await self._record_leader_change(old_leader, leader, 'elected')
            self._wake_election_loop()

    async def handle_heartbeat(
        self,
        leader: tuple[str, int],
        term: int,
        heartbeat_seq: int = -1,
        lease_duration: float | None = None,
    ) -> None:
        """Handle a leader-heartbeat message.

        ``heartbeat_seq`` / ``lease_duration`` carry the monotonic beat
        sequence and the authoritative lease grant (fixes 2/4, 4/4); a
        sequence-less beat (``heartbeat_seq < 0``) still renews.
        """
        old_leader = self.state.current_leader
        self.state.update_heartbeat(leader, term, heartbeat_seq, lease_duration)
        # Record if this is first time seeing this leader
        if old_leader != leader and leader is not None:
            await self._record_leader_change(old_leader, leader, 'heartbeat')
            self._wake_election_loop()

    async def handle_stepdown(self, leader: tuple[str, int], term: int) -> None:
        """Handle a leader-stepdown message."""
        if leader == self.state.current_leader:
            await self._record_leader_change(leader, None, 'remote_stepdown')
            self.state.current_leader = None
            self._wake_election_loop()
    
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
        self._log_debug_sync(
            f"handle_pre_vote_request self={self.self_addr} "
            f"candidate={candidate} term={term} candidate_lhm={candidate_lhm} "
            f"current_term={self.state.current_term}"
        )
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
        
        return self._pre_vote_response(candidate, term, can_grant)

    @staticmethod
    def _pre_vote_response(candidate: tuple[str, int], term: int, can_grant: bool) -> bytes:
        """The pre-vote answer to ``candidate``."""
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
        self._log_debug_sync(
            f"handle_pre_vote_response voter={voter} term={term} granted={granted} "
            f"self_pre_vote_term={self.state.pre_vote_term} "
            f"pre_voting_in_progress={self.state.pre_voting_in_progress}"
        )
        if self._rejects_pre_vote(voter, term):
            return

        if granted:
            self._record_granted_pre_vote(voter)

    def _rejects_pre_vote(self, voter: tuple[str, int], term: int) -> bool:
        """Whether a pre-vote is for another round, arrives with none running, or is from outside the cohort."""
        return (
            term != self.state.pre_vote_term
            or not self.state.pre_voting_in_progress
            or self._rejects_voter(voter)
        )

    def _record_granted_pre_vote(self, voter: tuple[str, int]) -> None:
        """Record a granted pre-vote, waking the election loop once a majority is in."""
        self.state.record_pre_vote(voter)
        if len(self.state.pre_votes_received) >= self._pre_vote_majority():
            self._wake_election_loop()
    
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
    
    def seconds_until_next_decision(self) -> float:
        """
        Seconds until this node's election loop next decides, at the latest.

        The loop decides when its current wait ends: a leaderless node's
        randomized candidacy wait (Raft section 5.2), the pre-vote or vote
        round it has in flight, a flapping delay, or a follower's lease
        check. A peer's vote or heartbeat can end the wait sooner, never
        later. 0.0 while the loop is acting rather than waiting -- its
        decision is being made now -- and before it first waits.
        Control plane: read when a submission is refused for want of a
        known leader, to tell the submitter when to come back.
        """
        return max(self._next_decision_at - self._clock.monotonic(), 0.0)

    def get_current_leader(self) -> tuple[str, int] | None:
        """Get the current leader, if any."""
        if self.state.is_leader() and self.self_addr:
            return self.self_addr
        return self._leased_leader()

    def _leased_leader(self) -> tuple[str, int] | None:
        """The known leader while its lease is valid."""
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
            'log_write_failures': self._log_write_failures,
            'flapping_cooldown': self.flapping_detector.current_cooldown,
        }
    
    def get_flapping_stats(self) -> dict:
        """Get flapping detector statistics."""
        return self.flapping_detector.get_stats()
